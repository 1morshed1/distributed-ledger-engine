# Spark Fraud Detector (Phase 3)

Local PySpark Structured Streaming job. Consumes `transaction-events` (Kafka),
decodes the protobuf, applies three fraud rules, and writes alerts to the
`fraud_alerts` Postgres table and the `fraud-alerts` Kafka topic.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./fetch_jars.sh        # Kafka connector jars into ./jars (one-time)
```

## Run

```bash
# stack must be up (make up in repo root) and the Go API running to produce events
./run.sh               # spark-submit local[*]
./loadgen.sh 8 100     # fire 8 rapid transfers -> trips the velocity rule
```

## Rules (`fraud_detector.py`)

| Rule            | Method                          | Trigger (default)                    |
|-----------------|---------------------------------|--------------------------------------|
| velocity        | sliding-window count            | > 5 transfers / 60s (10s slide)      |
| amount_anomaly  | stateful Welford mean/std       | amount > mean + 3·std (after 5 tx)   |
| rapid_drain     | windowed outflow sum            | outflow > 500000 cents / 5 min       |

Thresholds are env-overridable — see `config.py`.

## Files

- `fraud_detector.py` — Spark session, Kafka source, protobuf decode, the 3 rules
- `sinks.py` — write alerts to Postgres + Kafka
- `config.py` — brokers, topics, thresholds (env-overridable)
- `events_pb2.py` — generated from `../proto/events.proto`
- `run.sh` / `fetch_jars.sh` / `loadgen.sh` — helpers

See `../docs/phase-3/01-spark-fraud-detection.md` for the full write-up.
