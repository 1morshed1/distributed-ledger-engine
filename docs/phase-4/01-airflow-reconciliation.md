# Phase 4 — Batch Reconciliation & Rollups (Airflow)

Code: `airflow/dags/` (`nightly_reconciliation.py`, `monthly_rollup.py`,
`ledger_common.py`). Infra: `docker-compose.yml` (airflow + s3mock).

---

## 1. Why batch jobs on top of real-time

Real-time processing (Spark) is great for *now*, but some checks are inherently
periodic and holistic:

- **Reconciliation** — is the whole ledger internally consistent? This is a
  full-table integrity proof, run when the system is quiet.
- **Rollups** — precompute monthly aggregates so dashboards don't rescan millions
  of rows.

Airflow orchestrates these as **DAGs** (directed acyclic graphs of tasks) on a
cron schedule, with retries, backfills, and a UI.

---

## 2. The reconciliation invariant

The ledger has two representations of every balance:

- `accounts.balance` — the fast, cached number.
- `SUM(entries.amount)` — the truth, recomputable from the immutable ledger.

They must be equal. `nightly_reconciliation` proves it every night:

1. **balance_check** — `SELECT ... HAVING a.balance <> COALESCE(SUM(e.amount),0)`
   returns every account where the two disagree.
2. **entry_parity** — total `entries` must equal `2 × completed transfers` (each
   transfer writes one debit + one credit).
3. **report** — writes a JSON + CSV report to S3 under
   `reconciliation/YYYY-MM-DD/`.
4. **alert** — raises (which would page/notify in prod) if anything is off.

### It actually caught drift

On the first run it found **25 discrepancies** — because test data seeded
`accounts.balance` directly (and a manual `UPDATE` during development) *without*
matching entries. That's precisely the bug class reconciliation exists to catch.
After repairing balances to `SUM(entries)`, the next run reported `status=PASS`
and "ledger is internally consistent". Both branches verified, report objects
confirmed in S3 (`report.json` + `discrepancies.csv`).

> Lesson baked in: **never mutate `accounts.balance` outside a transfer.** Every
> change must go through `TransferTx`, which writes the paired entries. Seed money
> the same way (a funding transfer), or reconciliation will — correctly — flag it.

---

## 3. monthly_rollup

On the 1st of each month, aggregate the previous month's completed transfers per
account (count, total outgoing volume, average) and **upsert** into
`monthly_summaries`. The `(account_id, month)` unique key makes re-runs
idempotent (`ON CONFLICT ... DO UPDATE`).

Verified: rolled up 13 accounts for 2026-09; e.g. account 7 = 20 transfers,
volume 59674, avg 2983.70 (its 50000 anomaly included).

> Airflow's monthly timetable means the run *dated* month N processes the data
> interval ending at N — i.e. the previous month. The DAG reads
> `data_interval_start`, not "today", so it always rolls up the correct period.

---

## 4. Infra choices & the S3 saga

- **Airflow**: official image, **LocalExecutor**, its own Postgres metadata DB
  (kept separate from the ledger DB on purpose). Webserver on **:8088** (8080 is
  our Go API). `_PIP_ADDITIONAL_REQUIREMENTS` adds `boto3`/`psycopg2` at boot.
- **Object store**: the spec says S3; locally we wanted **MinIO**. But MinIO
  deleted its Docker Hub images and this host's Docker can't auth to quay.io, and
  LocalStack's image now needs a Pro license. So we use **`adobe/s3mock`** — a
  license-free S3 mock on Docker Hub. Because the DAG speaks S3 through **boto3
  with a custom endpoint**, switching to real AWS S3 later is a one-line
  endpoint/credential change. Two dev-server quirks handled in `ledger_common.py`:
  path-style addressing, and stripping the `Expect: 100-continue` header that
  s3mock mishandles.

The DAGs reach services by compose DNS name: ledger DB at `postgres:5432`, S3 at
`s3mock:9090`.

---

## 5. Run it

```bash
make up                       # brings up airflow (init runs once), s3mock, etc.
# Airflow UI: http://localhost:8088  (admin / admin)

# run a DAG synchronously without the scheduler:
docker exec ledger-airflow-scheduler airflow dags test nightly_reconciliation 2026-09-24
docker exec ledger-airflow-scheduler airflow dags test monthly_rollup       2026-11-01
```

---

## Next

Phase 5 — Infrastructure & deployment: Terraform + Kubernetes (kind/minikube
local), CI/CD. Plus the cross-cutting observability stack (Prometheus/Grafana,
Jaeger, Loki).
