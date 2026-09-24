# Architecture Overview

One map of the whole system. Read this first; the per-phase docs
(`docs/phase-1..5`) go deep on each piece.

---

## What it is

A production-realistic financial ledger. It accepts money transfers with ACID
guarantees, streams every transfer into a real-time analytics/fraud pipeline, and
runs nightly batch jobs that prove the books are consistent. Correctness first
(you cannot lose money), then throughput, then auditability.

---

## The pieces and why

| Layer | Tech | Why |
|---|---|---|
| REST API | Go + Gin | external clients; fast, middleware-rich |
| Internal API | gRPC + protobuf | service-to-service; typed, compact |
| Ledger DB | PostgreSQL | ACID, row locks for concurrent balances |
| Cache | Redis | sub-ms balance reads; Asynq backend |
| Async tasks | Asynq | retries/DLQ off the request path |
| Event stream | Kafka | durable, replayable transfer log |
| Stream processing | Spark (PySpark) | windowed + stateful fraud rules |
| Batch orchestration | Airflow | scheduled reconciliation + rollups |
| Object store | S3 (s3mock local) | reconciliation reports |
| Observability | Prometheus, Jaeger, Loki | metrics, traces, logs |
| Packaging/infra | Docker, k8s, Terraform | build → deploy path |

---

## Data flow — the journey of one transfer

```
          ┌──────────┐   POST /transfers (Idempotency-Key)
 client ──▶  Gin API  │   or gRPC Transfer
          └────┬─────┘
               │  ┌──────────────────────── TransferTx (ONE db transaction) ──────────────────────┐
               │  │ 1 insert transfer (idempotency_key UNIQUE)                                     │
               ▼  │ 2 insert debit entry (-amt)   3 insert credit entry (+amt)                      │
        ┌───────────┐ 4 update both balances, LOWER account id locked first (no deadlock)          │
        │ PostgreSQL│ 5 reject if balance < 0 (rollback)   6 mark completed                          │
        └─────┬─────┘ └───────────────────────────────────────────────────────────────────────────┘
              │  write-through
              ▼
        ┌──────────┐        enqueue publish task (Asynq, retried)
        │  Redis    │◀── balance cache        │
        └──────────┘                          ▼
                                        ┌───────────┐  acks=all, key=from_account_id
                                        │   Kafka    │  topic: transaction-events (protobuf)
                                        └─────┬─────┘
                                              ▼
                                 ┌───────────────────────┐  decode protobuf (UDF)
                                 │  Spark Structured      │  rules: velocity / amount-anomaly / rapid-drain
                                 │  Streaming (PySpark)   │
                                 └───────┬───────────────┘
                          ┌──────────────┴───────────────┐
                          ▼                               ▼
                  fraud_alerts (Postgres)         fraud-alerts (Kafka topic)
                          │                               │
                          ▼                               ▼
                  GET /api/v1/fraud-alerts        (alerting service)

  ── nightly (Airflow) ────────────────────────────────────────────────────────
   nightly_reconciliation: for every account, assert accounts.balance == SUM(entries);
   entry parity == 2 x completed transfers; write JSON+CSV report to S3; alert on drift.
   monthly_rollup: per-account volume/count/avg -> monthly_summaries.
```

A single request carries a **trace id** (OpenTelemetry) that is written into the
Kafka event's `metadata.trace_id`, so one transfer is followable from the API span
through Kafka into the Spark logs — visible in Jaeger.

---

## The core invariants (what makes it a *ledger*)

1. **Money is integer cents** (`BIGINT`), never a float.
2. **Double-entry**: every balance change writes an immutable `entries` row; the
   entries of a transfer sum to zero. Money moves, never appears/vanishes.
3. **`accounts.balance == SUM(entries.amount)`** for every account, always. The
   balance is a cache of the entry ledger; reconciliation proves they agree.
4. **Idempotency**: a client-supplied UUID makes a retried transfer a no-op
   (UNIQUE constraint → 409 with the original).
5. **Deadlock-free concurrency**: transfers always lock the lower account id
   first, so concurrent transfers can't form a lock cycle.

Rule of thumb baked into every layer: **never mutate `accounts.balance` outside
`TransferTx`.** Even seeding money must go through a transfer, or reconciliation
will (correctly) flag it.

---

## Repository map

```
cmd/api/              server entrypoint (HTTP :8080 + gRPC :9090 + worker)
internal/
  api/                Gin handlers, auth middleware, validation, error mapping
  gapi/               gRPC service (same db.Store)
  db/                 sqlc queries + Store (TransferTx), integration tests
  token/              PASETO maker
  cache/              Redis write-through balance cache
  worker/             Asynq distributor + processor (notify, publish-to-Kafka)
  events/             Kafka producer (protobuf)
  observability/      Prometheus metrics, OTel tracing, slog logging
  util/               config, password hashing, currency validation
  pb/                 generated protobuf/gRPC
db/migration/         golang-migrate SQL (up/down)
db/query/             sqlc source queries
proto/                ledger.proto (service) + events.proto (stream)
spark-fraud-detector/ PySpark job + rules + sinks
airflow/dags/         nightly_reconciliation, monthly_rollup
observability/        prometheus/loki/promtail/grafana configs
k8s/                  manifests (namespace, deploy, svc, hpa, cfg, secret)
terraform/            AWS infra (VPC/EKS/RDS/Redis/MSK/S3) — write-only
docs/                 these write-ups
```

See `RUNBOOK.md` for ports and how to run each part, `docs/api-reference.md` for
the endpoint contracts.
