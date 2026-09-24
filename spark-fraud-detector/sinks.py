"""Alert sinks: Postgres (fraud_alerts table) and the fraud-alerts Kafka topic.

Both are called from a streaming query's foreachBatch. The batch DataFrame is
small (only flagged rows), so collecting to the driver for the psycopg2 insert is
fine at dev volume. A high-volume deployment would use the JDBC sink instead.
"""
import json

import psycopg2

import config


def write_alerts_to_postgres(rows):
    """rows: list of dicts with keys account_id, rule, severity, detail,
    window_start, window_end."""
    if not rows:
        return
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO fraud_alerts
                    (account_id, rule, severity, detail, window_start, window_end)
                VALUES (%(account_id)s, %(rule)s, %(severity)s, %(detail)s,
                        %(window_start)s, %(window_end)s)
                """,
                [
                    {
                        "account_id": r["account_id"],
                        "rule": r["rule"],
                        "severity": r.get("severity", "high"),
                        "detail": json.dumps(r.get("detail", {})),
                        "window_start": r.get("window_start"),
                        "window_end": r.get("window_end"),
                    }
                    for r in rows
                ],
            )
    finally:
        conn.close()


def write_alerts_to_kafka(spark, rows):
    """Publish alerts to the fraud-alerts topic. key = account_id, value = JSON."""
    if not rows:
        return
    payload = [
        (str(r["account_id"]), json.dumps({
            "account_id": r["account_id"],
            "rule": r["rule"],
            "severity": r.get("severity", "high"),
            "detail": r.get("detail", {}),
        }))
        for r in rows
    ]
    df = spark.createDataFrame(payload, ["key", "value"])
    (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("topic", config.ALERT_TOPIC)
        .save()
    )


def emit_alerts(spark, rows):
    """Fan an alert batch out to both sinks. Postgres for API queries, Kafka for
    downstream alerting services."""
    if not rows:
        return
    write_alerts_to_postgres(rows)
    write_alerts_to_kafka(spark, rows)
    for r in rows:
        print(f"[ALERT] rule={r['rule']} account={r['account_id']} detail={r.get('detail')}")
