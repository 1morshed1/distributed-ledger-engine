"""nightly_reconciliation DAG.

Every night it proves the ledger is internally consistent:
  1. balance_check  - recompute each account balance from the immutable `entries`
                      and compare it to the stored accounts.balance. Any mismatch
                      is a bug (money appeared or vanished).
  2. entry_parity   - total entries should equal 2 x completed transfers (each
                      transfer writes one debit + one credit).
  3. report         - write a JSON + CSV report to MinIO under the run date.
  4. alert          - fail loudly if any discrepancy was found.

This catches classes of bugs that real-time processing can miss.
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task

import ledger_common as lc


@dag(
    dag_id="nightly_reconciliation",
    schedule="0 2 * * *",                 # 02:00 daily
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    tags=["ledger", "reconciliation"],
)
def nightly_reconciliation():

    @task
    def balance_check() -> list[dict]:
        """Return accounts whose stored balance != SUM(entries)."""
        sql = """
            SELECT a.id AS account_id,
                   a.balance AS stored_balance,
                   COALESCE(SUM(e.amount), 0) AS computed_balance
            FROM accounts a
            LEFT JOIN entries e ON e.account_id = a.id
            GROUP BY a.id, a.balance
            HAVING a.balance <> COALESCE(SUM(e.amount), 0)
        """
        with lc.ledger_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    @task
    def entry_parity() -> dict:
        """Total entries vs 2 x completed transfers."""
        with lc.ledger_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM entries")
            entries = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) AS n FROM transfers WHERE status = 'completed'")
            completed = cur.fetchone()["n"]
        return {
            "entries": entries,
            "completed_transfers": completed,
            "expected_entries": completed * 2,
            "balanced": entries == completed * 2,
        }

    @task
    def report(discrepancies: list[dict], parity: dict, ds=None) -> dict:
        """Write JSON + CSV report to MinIO. `ds` is Airflow's run date (YYYY-MM-DD)."""
        summary = {
            "date": ds,
            "discrepancy_count": len(discrepancies),
            "discrepancies": discrepancies,
            "entry_parity": parity,
            "status": "PASS" if not discrepancies and parity["balanced"] else "FAIL",
        }
        base = f"reconciliation/{ds}"
        json_uri = lc.upload_json(f"{base}/report.json", summary)
        csv_uri = lc.upload_csv(
            f"{base}/discrepancies.csv",
            ["account_id", "stored_balance", "computed_balance"],
            [[d["account_id"], d["stored_balance"], d["computed_balance"]] for d in discrepancies],
        )
        print(f"report -> {json_uri}, {csv_uri}, status={summary['status']}")
        return summary

    @task
    def alert(summary: dict):
        """Fail the DAG (which would page/notify in prod) if inconsistent."""
        if summary["status"] == "FAIL":
            raise ValueError(
                f"RECONCILIATION FAILED: {summary['discrepancy_count']} balance "
                f"discrepancies; entry_parity={summary['entry_parity']}"
            )
        print("reconciliation OK: ledger is internally consistent")

    d = balance_check()
    p = entry_parity()
    s = report(d, p)
    alert(s)


nightly_reconciliation()
