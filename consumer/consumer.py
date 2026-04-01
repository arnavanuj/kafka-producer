import json
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from utils.state_store import ensure_state_file, load_state, record_consumed


TOPICS = ["infra_logs", "db_logs", "app_logs"]


def create_consumer() -> KafkaConsumer:
    retries = 15
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers="localhost:9092",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            assignments = []
            for topic in TOPICS:
                for partition in range(3):
                    assignments.append(TopicPartition(topic, partition))
            consumer.assign(assignments)
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}). Retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka on localhost:9092")


def main() -> None:
    ensure_state_file()
    consumer = create_consumer()
    queues = defaultdict(deque)
    last_processed_at = {0: 0.0, 1: 0.0, 2: 0.0}
    active_run_id = None
    print("Consumer simulation connected. consumer_1->p0 consumer_2->p1 consumer_3->p2", flush=True)

    while True:
        state = load_state()
        current_run_id = state["run_id"]
        if current_run_id != active_run_id:
            queues = defaultdict(deque)
            active_run_id = current_run_id

        messages = consumer.poll(timeout_ms=250)
        for _, batch in messages.items():
            for message in batch:
                payload = message.value
                if payload.get("run_id") != active_run_id:
                    continue
                payload["partition"] = message.partition
                queues[message.partition].append((message.topic, payload))

        processing_interval = 1 / max(state["controls"]["consumer_processing_rate_per_sec"], 0.1)
        now_monotonic = time.monotonic()

        for partition in range(3):
            if not queues[partition]:
                continue
            if now_monotonic - last_processed_at[partition] < processing_interval:
                continue

            topic, payload = queues[partition].popleft()
            consumed_at = datetime.now(timezone.utc).isoformat()
            produced_at = datetime.fromisoformat(payload["timestamp"])
            delay_seconds = (datetime.fromisoformat(consumed_at) - produced_at).total_seconds()
            consumer_name = f"consumer_{partition + 1}"
            record_consumed(topic, payload, consumed_at, consumer_name, round(delay_seconds, 2))
            print(
                f"CONSUMED -> consumer={consumer_name} key={payload['key']} partition={partition} run={active_run_id[:8]}",
                flush=True,
            )
            last_processed_at[partition] = now_monotonic

        time.sleep(0.05)


if __name__ == "__main__":
    main()
