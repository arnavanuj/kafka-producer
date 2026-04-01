from datetime import datetime, timezone
import zlib


PARTITION_COUNT = 3
GOOD_KEYS = [f"host_{index}" for index in range(1, 11)]
BAD_KEY = "hot_key"

MESSAGES_BY_SOURCE = {
    "infra": [
        "Node heartbeat received",
        "CPU check completed",
        "Disk health check completed",
        "Ingress health probe passed",
    ],
    "db": [
        "Primary write transaction committed",
        "Replica status check completed",
        "Connection pool check completed",
        "Slow query audit finished",
    ],
    "app": [
        "Checkout request processed",
        "Catalog cache refreshed",
        "Session validation completed",
        "Notification job dispatched",
    ],
}


def stable_hash(key: str) -> int:
    if key.startswith("host_"):
        suffix = key.split("_")[-1]
        if suffix.isdigit():
            return int(suffix) - 1
    if key == BAD_KEY:
        return 1
    return zlib.crc32(key.encode("utf-8"))


def partition_for_key(key: str, partition_count: int = PARTITION_COUNT) -> int:
    return stable_hash(key) % partition_count


def build_log(source: str, key: str, sequence: int, partition: int, run_id: str) -> dict:
    messages = MESSAGES_BY_SOURCE[source]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "key": key,
        "partition": partition,
        "hash_value": stable_hash(key),
        "message": messages[sequence % len(messages)],
        "sequence": sequence,
        "run_id": run_id,
    }
