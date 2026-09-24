"""Real-time fraud detection over the transaction-events Kafka stream.

Run (from this directory, venv active, stack up):

  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    --py-files events_pb2.py,config.py,sinks.py \
    fraud_detector.py

Three rules, each its own streaming query writing to fraud_alerts (Postgres) and
the fraud-alerts Kafka topic:
  1. velocity      - > N transfers from one account in a sliding window
  2. amount_anomaly- a transfer > mean + k*std for that account (stateful)
  3. rapid_drain   - total outflow from one account > threshold in a window
"""
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

import config
import events_pb2
from sinks import emit_alerts

# ---- protobuf decode -------------------------------------------------------

DECODED_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("transfer_id", LongType()),
    StructField("from_account_id", LongType()),
    StructField("to_account_id", LongType()),
    StructField("amount", LongType()),
    StructField("currency", StringType()),
    StructField("event_time", TimestampType()),
])


def _decode(raw):
    """bytes -> tuple matching DECODED_SCHEMA. Returns None on undecodable input
    so a poison message can't crash the stream."""
    try:
        ev = events_pb2.TransactionEvent()
        ev.ParseFromString(bytes(raw))
        p = ev.payload
        ts = ev.timestamp.ToDatetime() if ev.HasField("timestamp") else dt.datetime.utcnow()
        return (ev.event_id, ev.event_type, p.transfer_id, p.from_account_id,
                p.to_account_id, p.amount, p.currency, ts)
    except Exception:
        return None


def parsed_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    decode_udf = F.udf(_decode, DECODED_SCHEMA)
    return (
        raw.select(decode_udf(F.col("value")).alias("e"))
        .filter(F.col("e").isNotNull())
        .select("e.*")
    )


# ---- rule 1: velocity (sliding window count) -------------------------------

def run_velocity(spark, parsed):
    windowed = (
        parsed.withWatermark("event_time", config.WATERMARK)
        .groupBy(
            F.window("event_time", config.VELOCITY_WINDOW, config.VELOCITY_SLIDE),
            F.col("from_account_id"),
        )
        .agg(F.count("*").alias("tx_count"))
        .filter(F.col("tx_count") > config.VELOCITY_MAX_TX)
    )

    def sink(batch_df, _epoch):
        rows = []
        for r in batch_df.collect():
            rows.append({
                "account_id": r["from_account_id"],
                "rule": "velocity",
                "severity": "high",
                "detail": {"tx_count": r["tx_count"], "threshold": config.VELOCITY_MAX_TX},
                "window_start": r["window"]["start"],
                "window_end": r["window"]["end"],
            })
        emit_alerts(spark, rows)

    return (
        windowed.writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", config.CHECKPOINT_DIR + "/velocity")
        .start()
    )


# ---- rule 3: rapid drain (windowed outflow sum) ----------------------------

def run_rapid_drain(spark, parsed):
    windowed = (
        parsed.withWatermark("event_time", config.WATERMARK)
        .groupBy(
            F.window("event_time", config.DRAIN_WINDOW),
            F.col("from_account_id"),
        )
        .agg(F.sum("amount").alias("outflow"))
        .filter(F.col("outflow") > config.DRAIN_THRESHOLD)
    )

    def sink(batch_df, _epoch):
        rows = []
        for r in batch_df.collect():
            rows.append({
                "account_id": r["from_account_id"],
                "rule": "rapid_drain",
                "severity": "critical",
                "detail": {"outflow": r["outflow"], "threshold": config.DRAIN_THRESHOLD},
                "window_start": r["window"]["start"],
                "window_end": r["window"]["end"],
            })
        emit_alerts(spark, rows)

    return (
        windowed.writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", config.CHECKPOINT_DIR + "/rapid_drain")
        .start()
    )


# ---- rule 2: amount anomaly (stateful running mean/std, Welford) -----------

ANOMALY_OUT = StructType([
    StructField("from_account_id", LongType()),
    StructField("transfer_id", LongType()),
    StructField("amount", LongType()),
    StructField("mean", StringType()),
    StructField("std", StringType()),
])
ANOMALY_STATE = StructType([
    StructField("count", LongType()),
    StructField("mean", StringType()),
    StructField("m2", StringType()),  # sum of squared deviations (Welford)
])


def _anomaly_fn(key, pdfs, state):
    """Per-account stateful anomaly check. Maintains count/mean/M2 (Welford's
    online variance) across micro-batches; emits any transfer whose amount exceeds
    mean + k*std once enough history exists."""
    import math
    import pandas as pd

    if state.exists:
        count, mean, m2 = state.get
        count = int(count); mean = float(mean); m2 = float(m2)
    else:
        count, mean, m2 = 0, 0.0, 0.0

    out_rows = []
    for pdf in pdfs:
        for amount, tid in zip(pdf["amount"], pdf["transfer_id"]):
            amount = float(amount)
            if count >= config.ANOMALY_MIN_COUNT:
                std = math.sqrt(m2 / count) if count > 0 else 0.0
                if std > 0 and amount > mean + config.ANOMALY_STD_MULT * std:
                    out_rows.append((int(key[0]), int(tid), int(amount),
                                     f"{mean:.2f}", f"{std:.2f}"))
            # Welford update (include this observation for future checks).
            count += 1
            delta = amount - mean
            mean += delta / count
            m2 += delta * (amount - mean)

    state.update((count, f"{mean:.6f}", f"{m2:.6f}"))
    if out_rows:
        return iter([pd.DataFrame(out_rows, columns=[
            "from_account_id", "transfer_id", "amount", "mean", "std"])])
    import pandas as pd
    return iter([pd.DataFrame(columns=[
        "from_account_id", "transfer_id", "amount", "mean", "std"])])


def run_amount_anomaly(spark, parsed):
    from pyspark.sql.streaming.state import GroupStateTimeout

    flagged = (
        parsed.groupBy("from_account_id")
        .applyInPandasWithState(
            _anomaly_fn, ANOMALY_OUT, ANOMALY_STATE,
            "update", GroupStateTimeout.NoTimeout,
        )
    )

    def sink(batch_df, _epoch):
        rows = []
        for r in batch_df.collect():
            rows.append({
                "account_id": r["from_account_id"],
                "rule": "amount_anomaly",
                "severity": "high",
                "detail": {"transfer_id": r["transfer_id"], "amount": r["amount"],
                           "mean": r["mean"], "std": r["std"],
                           "k": config.ANOMALY_STD_MULT},
            })
        emit_alerts(spark, rows)

    return (
        flagged.writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", config.CHECKPOINT_DIR + "/amount_anomaly")
        .start()
    )


def main():
    spark = (
        SparkSession.builder.appName("ledger-fraud-detector")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    parsed = parsed_stream(spark)

    queries = [run_velocity(spark, parsed), run_rapid_drain(spark, parsed)]
    # Amount anomaly uses applyInPandasWithState; isolate so a failure to start
    # doesn't take down the other rules.
    try:
        queries.append(run_amount_anomaly(spark, parsed))
    except Exception as exc:  # pragma: no cover
        print(f"[WARN] amount_anomaly rule did not start: {exc}")

    print(f"fraud detector running: {len(queries)} rule(s) active")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
