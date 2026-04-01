import json
from collections import deque
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from filelock import FileLock

from utils.log_generator import BAD_KEY, GOOD_KEYS, PARTITION_COUNT


BASE_DIR = Path(__file__).resolve().parents[1]
STATE_DIR = BASE_DIR / "ui" / "state"
STATE_FILE = STATE_DIR / "demo_state.json"
LOCK_FILE = STATE_DIR / "demo_state.lock"

TOPICS = {
    "infra": "infra_logs",
    "db": "db_logs",
    "app": "app_logs",
}

MAX_RECENT_LOGS = 24
MAX_EVENT_LOG = 40
MAX_PARTITION_KEYS = 12
MAX_DELAY_SAMPLES = 120
SLA_SECONDS = 5


def new_run_id() -> str:
    return uuid4().hex


def _default_partition_state() -> dict[str, Any]:
    return {
        "produced": 0,
        "consumed": 0,
        "lag": 0,
        "recent_keys": [],
        "latest_event": None,
        "last_consumed_at": None,
        "consumer_name": None,
    }


def _default_topic_state() -> dict[str, Any]:
    return {
        "produced": 0,
        "consumed": 0,
        "latest_produced": None,
        "latest_consumed": None,
        "recent_logs": [],
    }


def default_state() -> dict[str, Any]:
    return {
        "topics": {topic: _default_topic_state() for topic in TOPICS.values()},
        "partitions": {str(partition): _default_partition_state() for partition in range(PARTITION_COUNT)},
        "controls": {
            "producer_active": {source: False for source in TOPICS},
            "key_design_mode": "good",
            "producer_interval_seconds": 1.0,
            "consumer_processing_rate_per_sec": 1.0,
            "burst_mode": False,
        },
        "processes": {
            "consumer_running": False,
        },
        "consumer_group": "demo-log-dashboard",
        "partition_count": PARTITION_COUNT,
        "good_keys": GOOD_KEYS,
        "bad_key": BAD_KEY,
        "run_id": new_run_id(),
        "flow_events": [],
        "sla": {
            "threshold_seconds": SLA_SECONDS,
            "avg_delay_seconds": 0.0,
            "max_delay_seconds": 0.0,
            "violations": 0,
            "processed": 0,
            "delay_samples": [],
        },
        "last_updated": None,
    }


def _merge_dicts(defaults: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(defaults)
    for key, value in current.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _trim_prepended(values: list[Any], limit: int) -> list[Any]:
    return values[:limit]


def _trim_appended(values: list[Any], limit: int) -> list[Any]:
    return list(deque(values, maxlen=limit))


def normalize_state(state: dict[str, Any]) -> dict[str, Any]:
    state["good_keys"] = GOOD_KEYS
    state["bad_key"] = BAD_KEY

    for topic_state in state.get("topics", {}).values():
        topic_state["produced"] = max(0, int(topic_state.get("produced", 0)))
        topic_state["consumed"] = max(0, min(int(topic_state.get("consumed", 0)), topic_state["produced"]))

    for partition_state in state.get("partitions", {}).values():
        partition_state["produced"] = max(0, int(partition_state.get("produced", 0)))
        partition_state["consumed"] = max(
            0,
            min(int(partition_state.get("consumed", 0)), partition_state["produced"]),
        )
        partition_state["lag"] = max(0, partition_state["produced"] - partition_state["consumed"])
        partition_state["recent_keys"] = _trim_prepended(partition_state.get("recent_keys", []), MAX_PARTITION_KEYS)

    state["flow_events"] = _trim_prepended(state.get("flow_events", []), MAX_EVENT_LOG)

    sla = state.get("sla", {})
    samples = [float(sample) for sample in sla.get("delay_samples", [])][-MAX_DELAY_SAMPLES:]
    processed = sum(topic["consumed"] for topic in state.get("topics", {}).values())
    violations = sum(1 for sample in samples if sample > sla.get("threshold_seconds", SLA_SECONDS))
    sla["delay_samples"] = samples
    sla["processed"] = processed
    sla["violations"] = violations
    sla["avg_delay_seconds"] = round(sum(samples) / len(samples), 2) if samples else 0.0
    sla["max_delay_seconds"] = round(max(samples), 2) if samples else 0.0
    state["sla"] = sla
    return state


def ensure_state_file() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    defaults = default_state()
    with FileLock(str(LOCK_FILE)):
        if not STATE_FILE.exists():
            STATE_FILE.write_text(json.dumps(defaults, indent=2), encoding="utf-8")
            return

        current = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if "run_id" not in current:
            current["run_id"] = defaults["run_id"]
        merged = _merge_dicts(defaults, current)
        merged["run_id"] = current.get("run_id", defaults["run_id"])
        normalized = normalize_state(merged)
        if normalized != current:
            STATE_FILE.write_text(json.dumps(normalized, indent=2), encoding="utf-8")


def load_state() -> dict[str, Any]:
    ensure_state_file()
    with FileLock(str(LOCK_FILE)):
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        normalized = normalize_state(state)
        if normalized != state:
            STATE_FILE.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
        return normalized


def update_state(mutator: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    ensure_state_file()
    with FileLock(str(LOCK_FILE)):
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        mutator(state)
        state = normalize_state(state)
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
        return deepcopy(state)


def _record_flow_event(state: dict[str, Any], topic: str, payload: dict[str, Any], stage: str, event_ts: str) -> None:
    state["flow_events"] = _trim_prepended(
        [
            {
                "topic": topic,
                "stage": stage,
                "timestamp": event_ts,
                "key": payload["key"],
                "partition": payload["partition"],
                "message": payload["message"],
                "source": payload["source"],
            }
        ]
        + state["flow_events"],
        MAX_EVENT_LOG,
    )
    state["last_updated"] = event_ts


def record_produced(topic: str, payload: dict[str, Any], event_ts: str) -> None:
    def mutator(state: dict[str, Any]) -> None:
        topic_state = state["topics"][topic]
        topic_state["produced"] += 1
        topic_state["latest_produced"] = payload
        topic_state["recent_logs"] = _trim_prepended([payload] + topic_state["recent_logs"], MAX_RECENT_LOGS)

        partition_state = state["partitions"][str(payload["partition"])]
        partition_state["produced"] += 1
        partition_state["lag"] = max(0, partition_state["produced"] - partition_state["consumed"])
        partition_state["recent_keys"] = _trim_prepended(
            [payload["key"]] + partition_state["recent_keys"],
            MAX_PARTITION_KEYS,
        )
        partition_state["latest_event"] = payload

        _record_flow_event(state, topic, payload, "produced", event_ts)

    update_state(mutator)


def record_consumed(
    topic: str,
    payload: dict[str, Any],
    event_ts: str,
    consumer_name: str,
    processing_delay_seconds: float,
) -> None:
    def mutator(state: dict[str, Any]) -> None:
        if payload.get("run_id") != state.get("run_id"):
            return

        topic_state = state["topics"][topic]
        if topic_state["consumed"] >= topic_state["produced"]:
            return

        topic_state["consumed"] += 1
        topic_state["latest_consumed"] = payload
        topic_state["recent_logs"] = _trim_prepended([payload] + topic_state["recent_logs"], MAX_RECENT_LOGS)

        partition_state = state["partitions"][str(payload["partition"])]
        if partition_state["consumed"] < partition_state["produced"]:
            partition_state["consumed"] += 1
        partition_state["lag"] = max(0, partition_state["produced"] - partition_state["consumed"])
        partition_state["last_consumed_at"] = event_ts
        partition_state["consumer_name"] = consumer_name
        partition_state["latest_event"] = payload

        sla = state["sla"]
        samples = _trim_appended(sla["delay_samples"] + [processing_delay_seconds], MAX_DELAY_SAMPLES)
        sla["delay_samples"] = samples
        sla["processed"] += 1
        sla["avg_delay_seconds"] = round(sum(samples) / len(samples), 2)
        sla["max_delay_seconds"] = round(max(samples), 2)
        if processing_delay_seconds > sla["threshold_seconds"]:
            sla["violations"] += 1

        _record_flow_event(state, topic, payload, "consumed", event_ts)

    update_state(mutator)


def reset_runtime_metrics() -> None:
    def mutator(state: dict[str, Any]) -> None:
        defaults = default_state()
        state["topics"] = defaults["topics"]
        state["partitions"] = defaults["partitions"]
        state["sla"] = defaults["sla"]
        state["flow_events"] = []
        state["last_updated"] = None
        state["run_id"] = new_run_id()
        state["good_keys"] = GOOD_KEYS
        state["bad_key"] = BAD_KEY

    update_state(mutator)
