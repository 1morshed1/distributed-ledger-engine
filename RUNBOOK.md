# Runbook — Local Operations

How to run every part, the full port map, and the gotchas that already bit us
(so they don't bite again).

---

## Toolchain (this machine)

Go and the codegen tools aren't on `PATH` by default:

```bash
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin:$HOME/.local/bin
```

| Tool | Location | Notes |
|---|---|---|
| go 1.26 | `/usr/local/go/bin` | go.mod requires 1.26 (validator/v10) |
| sqlc, migrate | `$HOME/go/bin` | `go install`ed |
| protoc | `$HOME/.local/bin` | well-known types in `$HOME/.local/include` |
| grpcurl | `$HOME/go/bin` | gRPC testing |
| Java 17 | system | PySpark runtime |

---

## Port map

Host ports are remapped to avoid clashes with other projects and between our own
services.

| Service | Host port | Container | URL / use |
|---|---|---|---|
| Postgres (ledger) | **5433** | 5432 | `postgresql://ledger:secret@localhost:5433/ledger` |
| Redis | **6380** | 6379 | cache + Asynq |
| Kafka | **9092** | 9092 | host apps; containers use `kafka:9094` |
| API — HTTP | **8080** | — | REST |
| API — gRPC | **9090** | — | gRPC |
| s3mock | **9095** | 9090 | S3 API (containers: `s3mock:9090`) |
| Airflow UI | **8088** | 8080 | admin/admin |
| Prometheus | **9091** | 9090 | 9090 taken by gRPC/s3mock |
| Grafana | **3000** | 3000 | admin/admin (anon view on) |
| Jaeger UI | **16686** | — | traces; OTLP on 4318/4317 |
| Loki | **3100** | 3100 | via Grafana |

> Why the remaps: 5432/6379 were held by other projects; gRPC (9090) collides
> with Prometheus and s3mock defaults; Airflow's 8080 collides with the API.

---

## Start / stop

```bash
# 1. infra (all containers)
make up                      # docker compose up -d
docker compose ps            # wait for healthy

# 2. schema
make migrateup

# 3. the app (HTTP + gRPC + Asynq worker + Kafka producer + tracing)
make server                  # foreground; Ctrl-C to stop

# stop everything
docker compose down          # add -v to also drop volumes (wipes data)
```

Airflow first-run only: `docker compose up airflow-init` creates the metadata DB
+ admin user (idempotent).

---

## Run each phase

**Phase 1–2 (ledger + Kafka)** — just `make server`. Transfers publish to Kafka.
Watch events:
```bash
docker exec ledger-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transaction-events --from-beginning
```

**Phase 3 (Spark fraud)**:
```bash
cd spark-fraud-detector
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # first time
./fetch_jars.sh              # first time: Kafka connector jars
./run.sh                     # spark-submit local[*]
./loadgen.sh 8 100           # trip the velocity rule
docker exec ledger-postgres psql -U ledger -d ledger -c \
  "SELECT rule,count(*) FROM fraud_alerts GROUP BY rule;"
```

**Phase 4 (Airflow)** — UI at http://localhost:8088, or CLI:
```bash
docker exec ledger-airflow-scheduler airflow dags test nightly_reconciliation 2026-09-24
docker exec ledger-airflow-scheduler airflow dags test monthly_rollup       2026-11-01
```

**Phase 5 (observability / build)**:
```bash
# after make server + some traffic:
open http://localhost:16686   # Jaeger traces
open http://localhost:9091    # Prometheus (query: ledger_http_requests_total)
open http://localhost:3000    # Grafana

docker build -t ledger-api:ci .   # image
# k8s (needs kind+kubectl): kind create cluster; kind load docker-image ledger-api:ci; kubectl apply -k k8s/
```

---

## Regenerating generated code

```bash
make sqlc     # after editing db/query/*.sql or db/migration/*.sql
make proto    # after editing proto/*.proto (Go). Python: see below
protoc --proto_path=proto --proto_path=$HOME/.local/include \
  --python_out=spark-fraud-detector proto/events.proto   # events_pb2.py
```

---

## Troubleshooting

**`bind: address already in use`** — another process holds the port. Check with
`ss -tlnp | grep :<port>`. gRPC (9090) vs s3mock/Prometheus is the classic; those
are already remapped in compose.

**`go.mod requires go >= 1.26`** — use Go 1.26. The `validator/v10` dep forces it;
the Dockerfile/CI use `golang:1.26`.

**Migrations: `Dirty database version`** — a migration half-applied. Clear it:
`migrate -path db/migration -database "$DB_URL" force <last_good_version>` then
`... up`.

**FK violation adding `fk_accounts_owner`** — old `accounts` rows have owners not
in `users`. Truncate dev data (`TRUNCATE transfers, entries, accounts CASCADE;`)
and re-migrate.

**Reconciliation reports discrepancies** — expected if any balance was set
outside a transfer (test seed / manual `UPDATE`). That's the check working. Repair
with `UPDATE accounts a SET balance = COALESCE((SELECT SUM(amount) FROM entries
WHERE account_id=a.id),0);` or, better, only move money via transfers.

**Spark: slow/stuck jar download** — we pass connector jars via `--jars`
(`fetch_jars.sh`) instead of `--packages` precisely to avoid ivy resolving a
~50MB hadoop tree that pyspark already bundles.

**Spark: `No module named 'distutils'` / `Pandas not found`** — Python 3.12
removed distutils; `requirements.txt` pins `setuptools`, `pandas`, `pyarrow`.
`applyInPandasWithState` (amount-anomaly rule) needs pandas+pyarrow.

**Airflow env change not taking effect** — env is baked at container creation:
`docker compose up -d --force-recreate airflow-webserver airflow-scheduler`.

**Object store** — MinIO's Docker Hub images were removed and quay/LocalStack are
unusable here, so we run `adobe/s3mock`. The DAG uses boto3 with a custom
endpoint; switching to real AWS S3 is an endpoint/cred change. s3mock quirks
(path-style, `Expect: 100-continue`) are handled in `airflow/dags/ledger_common.py`.
