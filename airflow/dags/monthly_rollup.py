"""monthly_rollup DAG.

On the 1st of each month, aggregate the previous month's transfers per account
(count, total outgoing volume, average) and upsert into monthly_summaries for
fast dashboard queries. Idempotent via the (account_id, month) unique key.
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task

import ledger_common as lc


@dag(
    dag_id="monthly_rollup",
    schedule="0 4 1 * *",                 # 04:00 on the 1st
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    tags=["ledger", "rollup"],
)
def monthly_rollup():

    @task
    def rollup(data_interval_start=None) -> dict:
        # For a monthly schedule, the data interval is the previous month.
        month_start = data_interval_start.replace(day=1).date()

        select_sql = """
            SELECT from_account_id AS account_id,
                   COUNT(*)          AS tx_count,
                   COALESCE(SUM(amount), 0) AS total_volume,
                   COALESCE(AVG(amount), 0) AS avg_amount
            FROM transfers
            WHERE status = 'completed'
              AND created_at >= %(start)s
              AND created_at <  (%(start)s::date + INTERVAL '1 month')
            GROUP BY from_account_id
        """
        upsert_sql = """
            INSERT INTO monthly_summaries
                (account_id, month, tx_count, total_volume, avg_amount)
            VALUES (%(account_id)s, %(month)s, %(tx_count)s, %(total_volume)s, %(avg_amount)s)
            ON CONFLICT (account_id, month) DO UPDATE SET
                tx_count     = EXCLUDED.tx_count,
                total_volume = EXCLUDED.total_volume,
                avg_amount   = EXCLUDED.avg_amount
        """
        with lc.ledger_conn() as conn, conn.cursor() as cur:
            cur.execute(select_sql, {"start": month_start})
            rows = cur.fetchall()
            for r in rows:
                cur.execute(upsert_sql, {
                    "account_id": r["account_id"],
                    "month": month_start,
                    "tx_count": r["tx_count"],
                    "total_volume": r["total_volume"],
                    "avg_amount": round(float(r["avg_amount"]), 2),
                })
        print(f"rolled up {len(rows)} accounts for {month_start}")
        return {"month": str(month_start), "accounts": len(rows)}

    rollup()


monthly_rollup()
