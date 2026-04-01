import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils.log_generator import BAD_KEY, GOOD_KEYS, build_log, partition_for_key
from utils.state_store import TOPICS, ensure_state_file, load_state, record_produced


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def create_producer() -> KafkaProducer:
    retries = 15
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}). Retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka on localhost:9092")


def wait_for_next_tick(source: str, delay_seconds: float) -> None:
    waited = 0.0
    while waited < delay_seconds:
        state = load_state()
        if not state["controls"]["producer_active"][source]:
            return
        time.sleep(0.1)
        waited += 0.1


def choose_key(mode: str, key_index: int) -> str:
    if mode == "bad":
        return BAD_KEY
    return GOOD_KEYS[key_index % len(GOOD_KEYS)]


def run_producer(source: str) -> None:
    ensure_state_file()
    topic = TOPICS[source]
    producer = create_producer()
    key_index = 0
    sequence = 0
    active_run_id = None

    print(f"{source} producer connected. Sending logs to topic '{topic}'.", flush=True)

    while True:
        state = load_state()
        controls = state["controls"]
        current_run_id = state["run_id"]
        if current_run_id != active_run_id:
            sequence = 0
            key_index = 0
            active_run_id = current_run_id

        if not controls["producer_active"][source]:
            time.sleep(0.2)
            continue

        key = choose_key(controls["key_design_mode"], key_index)
        partition = partition_for_key(key, state["partition_count"])
        payload = build_log(source, key, sequence, partition, current_run_id)

        future = producer.send(topic, key=key, value=payload, partition=partition)
        metadata = future.get(timeout=10)
        payload["partition"] = metadata.partition

        record_produced(topic, payload, payload["timestamp"])
        print(
            f"PRODUCED -> topic={topic} key={key} partition={metadata.partition} run={current_run_id[:8]}",
            flush=True,
        )

        key_index += 1
        sequence += 1
        wait_for_next_tick(source, controls["producer_interval_seconds"])
