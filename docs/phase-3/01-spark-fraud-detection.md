# Phase 3 — Real-Time Fraud Detection (PySpark)

Code: `spark-fraud-detector/` (`fraud_detector.py`, `sinks.py`, `config.py`).
Runs as a **local PySpark job** (`run.sh`), not in a container.

---

## 1. The shape of the pipeline

```
transaction-events (Kafka)
        │  Spark Structured Streaming reads it
        ▼
   decode protobuf (UDF) ──► parsed rows (from_account_id, amount, event_time, …)
        │
        ├─► rule 1: velocity        (sliding-window count)
        ├─► rule 2: amount_anomaly  (stateful running mean/std)
        └─► rule 3: rapid_drain     (windowed outflow sum)
        │
        ▼  each rule's foreachBatch
   fraud_alerts (Postgres)   +   fraud-alerts (Kafka topic)
```

Spark reads the same stream the ledger produces, so detection is **decoupled**
from transactions: the ledger doesn't know Spark exists, and Spark can be
restarted / replayed without touching the ledger.

---

## 2. Decoding protobuf in Spark

The events are protobuf bytes (Phase 2). We generate Python classes from the same
`proto/events.proto` (`events_pb2.py`) and decode the Kafka `value` in a UDF that
returns a typed struct. A decode failure returns `None` (filtered out) so one
poison message can't kill the stream.

> Chose a Python UDF over Spark's native `from_protobuf` to avoid wiring a
> compiled descriptor set, and because the decoded struct flows naturally into
> the stateful Python rule.

---

## 3. The three rules

### Rule 1 — Velocity (sliding window)
More than `N` transfers from one account inside a 60-second window that slides
every 10s. A watermark (30s) bounds how long Spark waits for late events before
finalizing a window.

```python
parsed.withWatermark("event_time", "30 seconds")
  .groupBy(window("event_time","60 seconds","10 seconds"), "from_account_id")
  .agg(count("*").alias("tx_count"))
  .filter(col("tx_count") > 5)
```

**Verified**: 8 rapid transfers → alerts with `tx_count=8 > 5`. The sliding window
emits several overlapping windows, so one burst produces a few alert rows
(dedup would be a production refinement).

### Rule 2 — Amount anomaly (stateful, Welford)
A transfer whose amount exceeds `mean + 3·std` for that account. This needs
**per-account running statistics across micro-batches**, which is
`applyInPandasWithState`. We keep `(count, mean, M2)` and update them with
**Welford's online algorithm** — numerically stable, one pass, no need to store
history.

```
for each incoming transfer for an account:
    if count >= MIN and amount > mean + k*std:  ->  flag it
    # then fold it into the running stats
    count += 1; delta = amount-mean; mean += delta/count; m2 += delta*(amount-mean)
```

**Verified**: 5 varied transfers (mean=114, std=20.59) then one of 50000 →
exactly one `amount_anomaly` alert on the outlier, none on the history. The state
persisted across batches.

### Rule 3 — Rapid drain (windowed sum)
Total outflow from an account exceeding a threshold within a 5-minute window —
signals an account being emptied.

```python
parsed.withWatermark(...).groupBy(window("event_time","5 minutes"),"from_account_id")
  .agg(sum("amount").alias("outflow")).filter(col("outflow") > THRESHOLD)
```

The spec's stricter form is "> 80% of the account balance". That needs the live
balance joined in — do it in `foreachBatch` with a JDBC/gRPC balance lookup
(`GetAccountBalance`). We ship the absolute-threshold version; the balance-relative
upgrade is a noted TODO. **Verified**: no false positive when outflow stayed under
threshold.

---

## 4. Sinks — two of them, per rule

Each rule's `foreachBatch` calls `emit_alerts`, which writes to **both**:

- **Postgres `fraud_alerts`** — so the REST API can serve them
  (`GET /api/v1/fraud-alerts`). Written with `psycopg2` (batches are small: only
  flagged rows).
- **Kafka `fraud-alerts` topic** — for a downstream alerting/notification service
  (key = account_id, value = JSON).

**Verified end to end**: alert appeared in the table, on the Kafka topic, and in
the API response with the JSONB `detail` rendered as real JSON.

---

## 5. Running it

Dev choice: **local PySpark** (fast iteration, no cluster containers).

```bash
cd spark-fraud-detector
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./fetch_jars.sh          # one-time: Kafka connector jars into ./jars
./run.sh                 # spark-submit local[*], reads localhost:9092
# in another shell, trip the rules:
./loadgen.sh 8 100
```

### Dependency notes (Python 3.12)
- `distutils` was removed in 3.12 → `setuptools` restores it (pyspark needs it).
- `applyInPandasWithState` needs `pandas` + `pyarrow`.
- We pass the connector via `--jars` (not `--packages`) because ivy otherwise
  resolves a ~50MB hadoop tree that pyspark **already bundles**. `fetch_jars.sh`
  grabs just the 4 small jars needed.

---

## Next

Phase 4 — Airflow nightly reconciliation (recompute every balance from `entries`,
flag drift) + monthly rollups, writing reports to S3.
