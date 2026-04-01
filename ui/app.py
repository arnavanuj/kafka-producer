import subprocess
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils.state_store import STATE_DIR, TOPICS, ensure_state_file, load_state, reset_runtime_metrics, update_state


st.set_page_config(page_title="Kafka Partition Skew Demo", layout="wide")

PRODUCER_SCRIPTS = OrderedDict(
    {
        "infra": BASE_DIR / "producers" / "infra_producer.py",
        "db": BASE_DIR / "producers" / "db_producer.py",
        "app": BASE_DIR / "producers" / "app_producer.py",
    }
)
CONSUMER_SCRIPT = BASE_DIR / "consumer" / "consumer.py"
PYTHON_EXECUTABLE = sys.executable
LOG_DIR = STATE_DIR / "process_logs"
GOOD_COLOR = "#166534"
BAD_COLOR = "#b91c1c"
NEUTRAL_COLOR = "#1f2937"


def process_handles() -> dict[str, subprocess.Popen]:
    if "process_handles" not in st.session_state:
        st.session_state.process_handles = {}
    return st.session_state.process_handles


def log_path(name: str) -> Path:
    return LOG_DIR / f"{name}.log"


def launch_process(name: str, script: Path) -> None:
    handles = process_handles()
    handle = handles.get(name)
    if handle and handle.poll() is None:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    output_handle = open(log_path(name), "a", encoding="utf-8")
    handles[name] = subprocess.Popen(
        [PYTHON_EXECUTABLE, str(script)],
        cwd=str(BASE_DIR),
        stdout=output_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )


def stop_process(name: str) -> None:
    handle = process_handles().get(name)
    if not handle or handle.poll() is not None:
        return
    handle.terminate()
    try:
        handle.wait(timeout=5)
    except subprocess.TimeoutExpired:
        handle.kill()


def read_process_output() -> None:
    logs = st.session_state.setdefault("process_logs", {})
    for name in list(process_handles()):
        path = log_path(name)
        if not path.exists():
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        logs[name] = list(reversed(lines[-20:]))


def refresh_process_metadata() -> None:
    handles = process_handles()

    def mutator(state: dict) -> None:
        state["processes"]["consumer_running"] = bool(
            handles.get("consumer") and handles["consumer"].poll() is None
        )
        for source in PRODUCER_SCRIPTS:
            if state["controls"]["producer_active"][source]:
                state["controls"]["producer_active"][source] = bool(
                    handles.get(source) and handles[source].poll() is None
                )

    update_state(mutator)


def set_producer_state(source: str, active: bool) -> None:
    def mutator(state: dict) -> None:
        state["controls"]["producer_active"][source] = active

    update_state(mutator)


def set_key_design_mode(mode: str) -> None:
    reset_runtime_metrics()

    def mutator(state: dict) -> None:
        state["controls"]["key_design_mode"] = mode

    update_state(mutator)


def set_burst_mode(enabled: bool) -> None:
    def mutator(state: dict) -> None:
        state["controls"]["burst_mode"] = enabled
        state["controls"]["producer_interval_seconds"] = 0.25 if enabled else 1.0

    update_state(mutator)


def set_processing_rate(value: float) -> None:
    def mutator(state: dict) -> None:
        state["controls"]["consumer_processing_rate_per_sec"] = value

    update_state(mutator)


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def seconds_ago(ts: str | None) -> str:
    parsed = parse_iso(ts)
    if not parsed:
        return "idle"
    delta = datetime.now(timezone.utc) - parsed
    return f"{int(delta.total_seconds())}s ago"


def status_color(is_bad: bool) -> str:
    return BAD_COLOR if is_bad else GOOD_COLOR


def partition_card(partition_id: str, partition_state: dict) -> None:
    lag = partition_state["lag"]
    is_hot = lag >= 5 or partition_state["produced"] > max(1, partition_state["consumed"] + 3)
    color = status_color(is_hot)
    recent_keys = ", ".join(partition_state["recent_keys"][:6]) or "No traffic"
    st.markdown(
        f"""
        <div style="padding:16px;border-radius:16px;border:2px solid {color};background:{color}12;min-height:190px;">
            <div style="font-size:1rem;color:#374151;">Partition {partition_id}</div>
            <div style="font-size:2rem;font-weight:700;color:{color};margin:8px 0;">{partition_state['produced']}</div>
            <div style="color:#111827;">produced</div>
            <div style="margin-top:10px;color:#111827;">Consumed: <b>{partition_state['consumed']}</b></div>
            <div style="color:#111827;">Lag: <b>{lag}</b></div>
            <div style="color:#111827;">Recent keys: <b>{recent_keys}</b></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def consumer_card(partition_id: str, partition_state: dict) -> None:
    consumer_name = partition_state["consumer_name"] or f"consumer_{int(partition_id) + 1}"
    last_seen = parse_iso(partition_state["last_consumed_at"])
    recently_active = False
    if last_seen:
        recently_active = (datetime.now(timezone.utc) - last_seen).total_seconds() <= 2
    busy = partition_state["lag"] > 0 or recently_active
    color = BAD_COLOR if partition_state["lag"] > 3 else (GOOD_COLOR if busy else NEUTRAL_COLOR)
    label = "busy" if busy else "idle"
    st.markdown(
        f"""
        <div style="padding:14px;border-radius:14px;border:1px solid {color};background:{color}10;min-height:135px;">
            <div style="font-size:1rem;font-weight:700;color:#111827;">{consumer_name}</div>
            <div style="color:#4b5563;">assigned to partition {partition_id}</div>
            <div style="margin-top:10px;font-size:1.2rem;font-weight:700;color:{color};">{label}</div>
            <div style="color:#111827;">processed: <b>{partition_state['consumed']}</b></div>
            <div style="color:#111827;">last activity: <b>{seconds_ago(partition_state['last_consumed_at'])}</b></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def lag_table_rows(state: dict) -> list[dict]:
    rows = []
    for partition_id, partition_state in state["partitions"].items():
        rows.append(
            {
                "partition": partition_id,
                "produced": partition_state["produced"],
                "consumed": partition_state["consumed"],
                "lag": partition_state["lag"],
            }
        )
    return rows


def render_flow_events(state: dict) -> None:
    produced_events = [event for event in state.get("flow_events", []) if event["stage"] == "produced"]
    if not produced_events:
        st.write("No produced messages yet.")
        return
    for event in produced_events[:9]:
        st.code(
            f"[{event['key']}] -> partition {event['partition']} ({event['topic']})",
            language="text",
        )


ensure_state_file()
STATE_DIR.mkdir(parents=True, exist_ok=True)
read_process_output()
refresh_process_metadata()
state = load_state()

st.title("Kafka Bad Key Design Demo")
st.caption(
    "Good keys spread load across partitions. A hot key overloads one partition, leaves other consumers idle, and pushes the system into SLA breach."
)

scenario_cols = st.columns([1.2, 1, 1])
with scenario_cols[0]:
    st.subheader("Scenario Toggle")
    mode_label = "GOOD KEY DESIGN" if state["controls"]["key_design_mode"] == "good" else "BAD KEY DESIGN"
    selected = st.radio(
        "Key design",
        ["GOOD KEY DESIGN", "BAD KEY DESIGN"],
        index=0 if state["controls"]["key_design_mode"] == "good" else 1,
        horizontal=True,
    )
    next_mode = "good" if selected == "GOOD KEY DESIGN" else "bad"
    if next_mode != state["controls"]["key_design_mode"]:
        set_key_design_mode(next_mode)
        state = load_state()
    st.caption(
        "Good mode cycles 10 keys: host_1 through host_10. Bad mode sends every message with hot_key so one partition becomes the hotspot."
    )

with scenario_cols[1]:
    burst_enabled = st.toggle("Burst Mode", value=state["controls"]["burst_mode"])
    if burst_enabled != state["controls"]["burst_mode"]:
        set_burst_mode(burst_enabled)
        state = load_state()
    st.metric("Producer Cadence", "4 msg/s" if state["controls"]["burst_mode"] else "1 msg/s")

with scenario_cols[2]:
    processing_rate = st.slider(
        "Consumer rate per partition",
        min_value=0.5,
        max_value=2.0,
        step=0.5,
        value=float(state["controls"]["consumer_processing_rate_per_sec"]),
    )
    if processing_rate != float(state["controls"]["consumer_processing_rate_per_sec"]):
        set_processing_rate(processing_rate)
        state = load_state()
    st.metric("SLA", f"<= {state['sla']['threshold_seconds']} sec")

control_cols = st.columns(4)
for index, source in enumerate(PRODUCER_SCRIPTS):
    with control_cols[index]:
        running = bool(process_handles().get(source) and process_handles()[source].poll() is None)
        st.write(f"**{source} producer**")
        if st.button(f"Start {source}", key=f"start_{source}", use_container_width=True):
            set_producer_state(source, True)
            launch_process(source, PRODUCER_SCRIPTS[source])
        if st.button(f"Stop {source}", key=f"stop_{source}", use_container_width=True):
            set_producer_state(source, False)
            stop_process(source)
        st.caption("Running" if running else "Stopped")

with control_cols[3]:
    st.write("**consumer simulation**")
    if st.button("Start Consumer", use_container_width=True):
        launch_process("consumer", CONSUMER_SCRIPT)
    if st.button("Stop Consumer", use_container_width=True):
        stop_process("consumer")
    if st.button("Reset Metrics", use_container_width=True):
        reset_runtime_metrics()
        state = load_state()
    st.caption("Running" if state["processes"]["consumer_running"] else "Stopped")

sla_breach = state["sla"]["violations"] > 0 or any(
    partition["lag"] >= 5 for partition in state["partitions"].values()
)
if sla_breach:
    st.markdown(
        f"""
        <div style="padding:14px;border-radius:14px;background:{BAD_COLOR}18;border:2px solid {BAD_COLOR};color:{BAD_COLOR};font-weight:700;">
            SLA BREACH: Messages are being delayed due to partition skew.
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f"""
        <div style="padding:14px;border-radius:14px;background:{GOOD_COLOR}18;border:2px solid {GOOD_COLOR};color:{GOOD_COLOR};font-weight:700;">
            SLA healthy: Load is balanced and processing delay is within target.
        </div>
        """,
        unsafe_allow_html=True,
    )

summary_cols = st.columns(5)
total_produced = sum(topic["produced"] for topic in state["topics"].values())
total_consumed = sum(topic["consumed"] for topic in state["topics"].values())
total_lag = sum(partition["lag"] for partition in state["partitions"].values())
summary_cols[0].metric("Total Produced", total_produced)
summary_cols[1].metric("Total Consumed", total_consumed)
summary_cols[2].metric("Total Lag", total_lag)
summary_cols[3].metric("Scenario", mode_label)
summary_cols[4].metric("Run", state["run_id"][:8])

st.subheader("Partition Load View")
partition_cols = st.columns(3)
for column, partition_id in zip(partition_cols, ["0", "1", "2"]):
    with column:
        partition_card(partition_id, state["partitions"][partition_id])

st.subheader("Consumer Utilization")
consumer_cols = st.columns(3)
for column, partition_id in zip(consumer_cols, ["0", "1", "2"]):
    with column:
        consumer_card(partition_id, state["partitions"][partition_id])

st.subheader("Lag Visualization")
st.table(lag_table_rows(state))

st.subheader("SLA Monitor")
sla_cols = st.columns(3)
sla_cols[0].metric("Avg Processing Delay", f"{state['sla']['avg_delay_seconds']} sec")
sla_cols[1].metric("Max Delay", f"{state['sla']['max_delay_seconds']} sec")
sla_cols[2].metric("SLA Violations", state['sla']['violations'])

st.subheader("Flow Visualization")
render_flow_events(state)

st.subheader("Why This Happens")
explain_cols = st.columns(2)
with explain_cols[0]:
    st.markdown(
        """
        **Good key design**

        - `host_1` through `host_10` spread across partitions
        - all three consumers get work
        - lag stays low and stable
        - reset creates a fresh run id, so old Kafka backlog is ignored
        """
    )
with explain_cols[1]:
    st.markdown(
        """
        **Bad key design**

        - `hot_key` goes to one partition only
        - one consumer becomes overloaded
        - other partitions stay mostly idle
        - lag builds and SLA violations appear
        """
    )

st.subheader("Process Output")
process_output_columns = st.columns(4)
for column, name in zip(process_output_columns, ["infra", "db", "app", "consumer"]):
    with column:
        st.markdown(f"**{name}**")
        for line in st.session_state.get("process_logs", {}).get(name, [])[:8]:
            st.text(line)

st.caption("Auto-refreshes every second so skew, lag, and SLA breaches are easy to see live.")
time.sleep(1)
st.rerun()

