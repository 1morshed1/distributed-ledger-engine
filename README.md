# Distributed Ledger Engine

A production-realistic financial ledger: double-entry accounting with ACID
transfers, real-time analytics, fraud detection, and batch reconciliation.

Built in phases (see [`plan.md`](plan.md)). **Phase 1 (core ledger) is done.**

---

## Status

| Phase | Scope                                             | State |
|-------|---------------------------------------------------|-------|
| 1     | Go ledger: Postgres, transfers, auth, REST API    | ✅ done |
| 1b    | gRPC service, Redis cache, Asynq async tasks       | ✅ done |
| 2     | Kafka event stream                                 | ✅ done |
| 3     | Spark Structured Streaming fraud detection (PySpark)| ✅ done |
| 4     | Airflow nightly reconciliation + monthly rollup    | ✅ done |
| 5     | Terraform + Kubernetes + CI/CD (local-first)       | ⬜ next |

### Phase 1 core — what works today

Double-entry ledger • deadlock-safe concurrent transfers • idempotent transfers •
overdraft protection • PASETO auth (access + revocable refresh tokens) •
bcrypt passwords • ownership authorization • cursor pagination • correct HTTP
error mapping. All covered by passing tests + an end-to-end smoke test.

---

## Architecture (Phase 1)

```
client ──HTTP──> Gin API (internal/api)
                    │
                    ▼
             Store (internal/db)  ── TransferTx: atomic, row-locked, idempotent
                    │
                    ▼
              PostgreSQL   accounts · entries · transfers · users · sessions
```

Money is stored as `BIGINT` cents — never floats. Every balance change writes an
immutable `entries` row, so any balance is recomputable from history.

---

## Prerequisites

- Docker + Docker Compose
- Go 1.24+  (this machine: `/usr/local/go/bin` — add to `PATH`)
- `sqlc`, `migrate` CLIs (`~/go/bin`)

```bash
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
```

> Local ports are remapped to avoid clashes: **Postgres 5433**, **Redis 6380**.

---

## Quick start

```bash
make up          # start postgres + redis (docker compose)
make migrateup   # apply db migrations
make sqlc        # (re)generate type-safe db code  [only after editing SQL]
make test        # run unit + integration tests
make server      # run the REST API on :8080
```

### Try it

```bash
BASE=http://localhost:8080/api/v1

# register + login
curl -X POST $BASE/users -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"secret123","full_name":"Alice","email":"a@x.com"}'
TOKEN=$(curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"secret123"}' | jq -r .access_token)

# create an account
curl -X POST $BASE/accounts -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"currency":"USD"}'

# transfer (idempotency key required)
curl -X POST $BASE/transfers -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' \
  -d '{"from_account_id":1,"to_account_id":2,"amount":5000,"currency":"USD"}'
```

---

## Project layout

```
cmd/api/              # server entry point
internal/
  api/                # Gin handlers, middleware, validation, error mapping
  db/                 # sqlc-generated queries + Store (TransferTx), tests
  token/              # PASETO maker (Maker interface)
  util/               # config, password hashing, currency validation
db/
  migration/          # golang-migrate SQL (up/down)
  query/              # sqlc source queries
docs/phase-1/         # teaching write-ups (read these!)
```

## Docs (read in order)

1. [`docs/phase-1/01-ledger-design.md`](docs/phase-1/01-ledger-design.md) — money as integers, double-entry, idempotency, sqlc, keyset pagination
2. [`docs/phase-1/02-transaction-and-deadlocks.md`](docs/phase-1/02-transaction-and-deadlocks.md) — the transfer transaction, lock ordering, race conditions
3. [`docs/phase-1/03-api-and-auth.md`](docs/phase-1/03-api-and-auth.md) — REST API, PASETO auth, error → status mapping
4. [`docs/phase-1/04-grpc.md`](docs/phase-1/04-grpc.md) — gRPC LedgerService, protobuf codegen, status codes
5. [`docs/phase-1/05-async-tasks.md`](docs/phase-1/05-async-tasks.md) — Asynq queue, retries, priorities
6. [`docs/phase-2/01-kafka-events.md`](docs/phase-2/01-kafka-events.md) — Kafka event stream, protobuf schema, producer guarantees
7. [`docs/phase-3/01-spark-fraud-detection.md`](docs/phase-3/01-spark-fraud-detection.md) — PySpark streaming, 3 fraud rules, stateful Welford, sinks
8. [`docs/phase-4/01-airflow-reconciliation.md`](docs/phase-4/01-airflow-reconciliation.md) — Airflow DAGs, reconciliation invariant, monthly rollup, S3 reports

## Testing

```bash
make test    # needs the docker stack up (integration tests hit real Postgres)
```
