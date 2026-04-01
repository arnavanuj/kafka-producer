# Kafka Partition Skew and SLA Demo

This project demonstrates how **key choice** affects Kafka partition balance, consumer lag, throughput, and SLA.

## Scenarios

### Good Key Design

Keys rotate across **10 keys**:

- `host_1`
- `host_2`
- `host_3`
- `host_4`
- `host_5`
- `host_6`
- `host_7`
- `host_8`
- `host_9`
- `host_10`

The demo maps those keys across three partitions so load stays balanced.

Expected outcome:

- partition load stays much more even
- all three consumers stay active
- lag remains low
- SLA is met

### Bad Key Design

Every message uses:

- `hot_key`

Expected outcome:

- one partition becomes overloaded
- the other partitions stay mostly idle
- only one consumer stays busy
- lag builds up
- processing delay crosses the SLA threshold

## Consumer Simulation

The consumer process simulates three logical consumers:

- `consumer_1` -> partition 0
- `consumer_2` -> partition 1
- `consumer_3` -> partition 2

Each partition consumer processes at a fixed rate of **1 message per second** by default.

## SLA

The dashboard tracks:

- average processing delay
- max processing delay
- SLA violations

Defined SLA:

- **Message must be processed within 5 seconds**

## UI Highlights

- **Partition Load View**: shows skew clearly by partition
- **Consumer Utilization**: shows which simulated consumers are busy or idle
- **Lag Visualization**: produced vs consumed vs lag per partition
- **SLA Monitor**: highlights when the 5-second delay target is breached
- **Flow Visualization**: makes repeated `hot_key -> one partition` behavior obvious
- **Scenario Toggle**: switch between good and bad key design live
- **Burst Mode**: optional higher producer rate to worsen skew faster
- **Run Isolation**: each reset creates a fresh run id so stale Kafka backlog does not corrupt live lag numbers

## Run Instructions

If you used earlier versions, recreate Kafka so topics definitely have three partitions:

```bash
docker-compose down -v
docker-compose up -d
```

Create the topics:

```bash
docker exec demo-kafka kafka-topics --create --if-not-exists --topic infra_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec demo-kafka kafka-topics --create --if-not-exists --topic db_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec demo-kafka kafka-topics --create --if-not-exists --topic app_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Start the dashboard:

```bash
streamlit run ui/app.py
```

Then from the UI:

1. Start the consumer simulation
2. Start the three producers
3. Watch **GOOD KEY DESIGN** first
4. Switch to **BAD KEY DESIGN**
5. Optionally enable **Burst Mode** to worsen skew faster

## What To Observe

### In Good Key Design

- traffic spreads across partitions 0, 1, and 2
- `consumer_1`, `consumer_2`, and `consumer_3` all stay active
- lag stays low
- SLA stays green

### In Bad Key Design

- `hot_key` keeps landing on one partition
- one consumer becomes overloaded
- the other consumers go idle
- lag climbs quickly
- average and max delay rise
- SLA violations appear

## CI

GitHub Actions runs on every push and pull request and verifies that the project dependencies install and the Python files compile successfully.

## Notes

- This is a teaching demo, not a production Kafka stack.
- The demo uses a simple deterministic teaching hash so the good-key example is visibly balanced.
- The dashboard launches local Python processes, so keep the page open while using the controls.
