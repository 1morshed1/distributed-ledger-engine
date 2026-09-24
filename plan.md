## Project 1: High-Throughput Financial Transaction & Analytics Engine

### Concept & Why It Matters

You are building a simplified but production-realistic financial ledger system. It accepts transactions (transfers between accounts), processes them with ACID guarantees, streams every event into a real-time analytics pipeline, detects anomalies, and produces nightly reconciliation reports.

This matters because financial systems are the gold standard of backend engineering. They demand correctness (you cannot lose money), performance (thousands of transactions per second), and auditability (every event must be traceable). If you can build this convincingly, you can build anything.

### Architecture Diagram

```
                              ┌──────────────────────────────────────────────────────┐
                              │                   KUBERNETES CLUSTER                 │
                              │                                                      │
  Clients                     │  ┌─────────────┐    ┌─────────────┐                  │
  (REST/gRPC)                 │  │  Go API Pod  │    │  Go API Pod │  (HPA-scaled)    │
       │                      │  │  (Gin/gRPC)  │    │  (Gin/gRPC) │                  │
       ▼                      │  └──────┬───────┘    └──────┬──────┘                  │
  ┌─────────┐                 │         │                   │                         │
  │  Nginx  │─────────────────┼─────────┴───────────────────┘                         │
  │ Ingress │                 │         │                                             │
  └─────────┘                 │         ▼                                             │
                              │  ┌─────────────┐      ┌──────────┐                   │
                              │  │  PostgreSQL  │      │  Redis    │                   │
                              │  │  (Ledger DB) │      │ (Cache +  │                   │
                              │  │              │      │  Asynq)   │                   │
                              │  └─────────────┘      └──────────┘                   │
                              │         │                                             │
                              │         │  CDC / Application-level emit               │
                              │         ▼                                             │
                              │  ┌──────────────────────────────────┐                 │
                              │  │         Apache Kafka             │                 │
                              │  │  topic: transaction-events       │                 │
                              │  │  topic: fraud-alerts             │                 │
                              │  │  topic: balance-snapshots        │                 │
                              │  └──────────────┬───────────────────┘                 │
                              │                 │                                     │
                              │       ┌─────────┴──────────┐                          │
                              │       ▼                    ▼                          │
                              │ ┌───────────┐     ┌──────────────┐                    │
                              │ │   Spark    │     │  Spark Batch │                    │
                              │ │ Streaming  │     │  (nightly)   │                    │
                              │ │ (fraud     │     │              │                    │
                              │ │  detect)   │     └──────┬───────┘                    │
                              │ └─────┬─────┘            │                            │
                              │       │                  │                            │
                              │       ▼                  ▼                            │
                              │ ┌───────────┐   ┌──────────────┐                      │
                              │ │ fraud-     │   │  AWS S3      │                      │
                              │ │ alerts     │   │ (historical  │                      │
                              │ │ topic      │   │  reports)    │                      │
                              │ └───────────┘   └──────────────┘                      │
                              │                                                      │
                              │  ┌──────────────────────────────┐                     │
                              │  │  Apache Airflow               │                    │
                              │  │  DAG: nightly_reconciliation  │                    │
                              │  │  DAG: monthly_rollup          │                    │
                              │  └──────────────────────────────┘                     │
                              │                                                      │
                              │  ┌──────────────────────────────┐                     │
                              │  │  Observability Stack          │                    │
                              │  │  Prometheus + Grafana          │                   │
                              │  │  Jaeger (distributed tracing) │                    │
                              │  │  Loki (centralized logging)   │                    │
                              │  └──────────────────────────────┘                     │
                              └──────────────────────────────────────────────────────┘
```

### Tech Stack Breakdown

| Layer | Technology | Why This Choice |
|---|---|---|
| REST API | Go + Gin | Gin is the most battle-tested Go HTTP framework. Fast routing, middleware support, structured logging. |
| Internal Services | gRPC + Protobuf | Type-safe, code-generated clients. 10x faster serialization than JSON. Used at Google, Uber, Netflix for inter-service communication. |
| Primary Database | PostgreSQL | ACID-compliant. Supports row-level locking for concurrent balance updates. Rich ecosystem (pgx driver in Go is excellent). |
| Caching | Redis | Sub-millisecond reads for balance lookups. Also serves as the backend for Asynq (async task queue). |
| Async Tasks | Asynq (Go library) | Redis-backed task queue. Handles retries, dead-letter queues, scheduled tasks. Cleaner than building your own worker system. |
| Event Streaming | Apache Kafka | Industry standard for event-driven architectures. Handles millions of events/sec. Provides durability guarantees that RabbitMQ does not. |
| Stream Processing | Apache Spark Structured Streaming | Processes Kafka events in micro-batches or continuous mode. Windowed aggregations (tumbling, sliding, session windows) are built in. |
| Batch Orchestration | Apache Airflow | DAG-based workflow orchestration. Schedules nightly reconciliation jobs. Has native connectors for Postgres, S3, Spark. |
| Infrastructure | Terraform + AWS EKS | Terraform provisions the entire cloud stack declaratively. EKS is managed Kubernetes on AWS. |
| CI/CD | GitHub Actions | Free for public repos. Workflows for lint, test, build Docker images, deploy to EKS. |
| Auth | PASETO tokens | Safer alternative to JWT. No algorithm confusion attacks. The `v4.public` variant is recommended. |

### Phase-by-Phase Build Plan

#### Phase 1: The Core Ledger Service (Go + PostgreSQL)

This is the heart of the project. Everything else consumes data from this service.

**Database Schema (PostgreSQL):**

```sql
CREATE TABLE accounts (
    id              BIGSERIAL PRIMARY KEY,
    owner           VARCHAR(255) NOT NULL,
    balance         BIGINT NOT NULL DEFAULT 0,   -- Store in cents/smallest unit. NEVER use floats for money.
    currency        VARCHAR(3) NOT NULL DEFAULT 'USD',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE entries (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES accounts(id),
    amount          BIGINT NOT NULL,              -- positive = credit, negative = debit
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transfers (
    id              BIGSERIAL PRIMARY KEY,
    from_account_id BIGINT NOT NULL REFERENCES accounts(id),
    to_account_id   BIGINT NOT NULL REFERENCES accounts(id),
    amount          BIGINT NOT NULL,              -- must be positive
    idempotency_key UUID UNIQUE NOT NULL,         -- prevents duplicate transfers
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, completed, failed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entries_account_id ON entries(account_id);
CREATE INDEX idx_transfers_from_account ON transfers(from_account_id);
CREATE INDEX idx_transfers_to_account ON transfers(to_account_id);
CREATE INDEX idx_transfers_idempotency ON transfers(idempotency_key);
```

**Critical Implementation Detail — Deadlock Prevention:**

When transferring money from Account A to Account B, you must always lock accounts in a consistent order (e.g., always lock the lower ID first). If one goroutine locks A then B, and another locks B then A, you get a deadlock. This is the single most common mistake in financial backend code:

```go
func (store *SQLStore) TransferTx(ctx context.Context, arg TransferParams) (TransferResult, error) {
    var result TransferResult

    err := store.execTx(ctx, func(q *Queries) error {
        // Always lock the lower ID first to prevent deadlocks
        if arg.FromAccountID < arg.ToAccountID {
            // lock from_account first, then to_account
            result.FromAccount, result.ToAccount, _ = addMoney(ctx, q, arg.FromAccountID, -arg.Amount, arg.ToAccountID, arg.Amount)
        } else {
            // lock to_account first, then from_account
            result.ToAccount, result.FromAccount, _ = addMoney(ctx, q, arg.ToAccountID, arg.Amount, arg.FromAccountID, -arg.Amount)
        }
        return nil
    })
    return result, err
}
```

**API Endpoints (Gin REST + gRPC):**

The REST API is for external clients (frontend, mobile apps). The gRPC service is for internal microservice communication (the Spark job querying account data, the Airflow DAG triggering reconciliation).

REST endpoints to implement:
- `POST /api/v1/accounts` — Create a new account. Validate currency codes against ISO 4217.
- `GET /api/v1/accounts/:id` — Get account details + current balance.
- `POST /api/v1/transfers` — Initiate a transfer. Requires `idempotency_key` header. Returns 409 Conflict if the key was already used.
- `GET /api/v1/transfers` — List transfers with pagination (cursor-based, not offset-based — offset pagination degrades at scale).
- `POST /api/v1/auth/login` — Returns a PASETO token.
- `POST /api/v1/auth/refresh` — Refreshes an expired token using a refresh token stored in the DB.

gRPC service definition:

```protobuf
service LedgerService {
    rpc CreateAccount(CreateAccountRequest) returns (Account);
    rpc GetAccount(GetAccountRequest) returns (Account);
    rpc Transfer(TransferRequest) returns (TransferResult);
    rpc ListTransfers(ListTransfersRequest) returns (ListTransfersResponse);
    rpc GetAccountBalance(GetBalanceRequest) returns (BalanceResponse);  // Used by Spark
}
```

**Redis Caching Strategy:**

Cache account balances in Redis with a write-through pattern. Every time a transfer succeeds and the Postgres balance is updated, also update Redis. Set a TTL of 5 minutes as a safety net. On read, check Redis first; on miss, read from Postgres and populate cache.

Use Asynq for tasks that do not need to be synchronous: sending notification emails, generating PDF statements, publishing Kafka events (if you want guaranteed delivery with retries rather than fire-and-forget).

#### Phase 2: The Event Stream (Kafka)

After every successful transfer, your Go backend publishes an event to Kafka. This decouples your analytics pipeline from your transactional system — the ledger service does not care whether Spark is running or not.

**Kafka Topic Design:**

You need at least three topics:
- `transaction-events` — Every completed transfer. This is the primary data stream.
- `fraud-alerts` — Written to by Spark when it detects anomalies. Consumed by an alerting service.
- `balance-snapshots` — Periodic snapshots of all account balances, used for reconciliation.

**Event Schema (use Avro or Protobuf, not raw JSON):**

```json
{
  "event_id": "uuid-v4",
  "event_type": "TRANSFER_COMPLETED",
  "timestamp": "2026-05-24T14:30:00Z",
  "payload": {
    "transfer_id": 12345,
    "from_account_id": 1,
    "to_account_id": 2,
    "amount": 50000,
    "currency": "USD"
  },
  "metadata": {
    "source_service": "ledger-api",
    "trace_id": "abc123",
    "schema_version": 1
  }
}
```

**Go Kafka Producer (using `confluent-kafka-go` or `segmentio/kafka-go`):**

Key decisions you must make:
- **Partitioning:** Partition by `from_account_id`. This guarantees that all events for the same account arrive in order within a partition. Order matters for fraud detection (you need to see the sequence of transactions).
- **Acknowledgment:** Use `acks=all` for financial data. You cannot afford to lose events. This is slower but guarantees the event is replicated to all in-sync replicas before the producer gets a success response.
- **Retries:** Set `retries=3` with exponential backoff. If Kafka is temporarily unavailable, the event should not be silently dropped.
- **Idempotent Producer:** Enable `enable.idempotence=true` to prevent duplicate events on retry.

#### Phase 3: Real-Time Fraud Detection (Spark Structured Streaming)

This is where your Big Data training pays off. You will write a Spark application (in Scala or PySpark) that consumes from the `transaction-events` Kafka topic in real-time.

**Fraud Detection Rules (go beyond the trivial):**

Instead of just "3 transactions in 10 seconds" (which is a toy example), implement these realistic detection patterns:

1. **Velocity Check (Sliding Window):** More than 5 transactions from the same `from_account_id` within a 60-second sliding window. Use Spark's `window()` function with a 60-second window and a 10-second slide interval.

2. **Amount Anomaly (Statistical):** A single transaction amount that exceeds 3 standard deviations above the historical mean for that account. This requires maintaining a running mean and standard deviation per account, which you can do with Spark's stateful streaming (`mapGroupsWithState` or `flatMapGroupsWithState`).

3. **Rapid Drain Detection:** Total outflow from a single account exceeding 80% of its balance within a 5-minute window. This signals a compromised account being emptied.

4. **Geographic Impossibility (if you add IP/location data):** Transactions from two locations more than 500km apart within 30 minutes. This is a stretch goal but very impressive.

**Spark Application Structure:**

```
spark-fraud-detector/
├── src/main/scala/
│   ├── FraudDetectorApp.scala        # Entry point, Spark session setup
│   ├── kafka/
│   │   └── TransactionConsumer.scala  # Kafka source configuration
│   ├── rules/
│   │   ├── VelocityRule.scala         # Sliding window velocity check
│   │   ├── AmountAnomalyRule.scala    # Statistical anomaly detection
│   │   └── RapidDrainRule.scala       # Balance drain detection
│   ├── models/
│   │   └── TransactionEvent.scala     # Case classes matching Kafka schema
│   └── sinks/
│       ├── KafkaAlertSink.scala       # Writes alerts to fraud-alerts topic
│       └── PostgresSink.scala         # Writes flagged transactions to DB
├── build.sbt
└── Dockerfile
```

**Windowing Example (PySpark):**

```python
from pyspark.sql.functions import window, count, col

windowed_counts = (
    transactions_df
    .withWatermark("timestamp", "30 seconds")
    .groupBy(
        window(col("timestamp"), "60 seconds", "10 seconds"),
        col("from_account_id")
    )
    .agg(count("*").alias("tx_count"))
    .filter(col("tx_count") > 5)
)
```

When a fraud rule triggers, write the alert to the `fraud-alerts` Kafka topic and also directly to a `fraud_alerts` table in Postgres for the API to query.

#### Phase 4: Batch Reconciliation (Airflow)

The nightly reconciliation DAG ensures that your system's data is internally consistent. This catches bugs that real-time processing might miss.

**DAG: `nightly_reconciliation`**

Schedule: `0 2 * * *` (2:00 AM UTC daily)

Steps:
1. **Extract:** Query all transfers from the last 24 hours from Postgres.
2. **Validate:** For each transfer, verify that the corresponding debit entry + credit entry exist and that the amounts match. Flag any orphaned entries.
3. **Balance Check:** Recalculate every account balance from scratch by summing all entries. Compare with the stored `accounts.balance`. Flag discrepancies.
4. **Report:** Generate a reconciliation report (JSON + CSV) and upload to S3 at path `s3://your-bucket/reconciliation/YYYY-MM-DD/report.json`.
5. **Alert:** If any discrepancies are found, send a Slack notification (or email) with the details.

**DAG: `monthly_rollup`**

Schedule: `0 4 1 * *` (4:00 AM on the 1st of each month)

Steps:
1. Aggregate total transaction volume, count, and average per account for the previous month.
2. Store the rollup in a `monthly_summaries` table for fast dashboard queries.
3. Archive raw transaction data older than 90 days to S3 (data lifecycle management).

#### Phase 5: Infrastructure & Deployment (Terraform + Kubernetes + CI/CD)

**Terraform Modules:**

```
terraform/
├── modules/
│   ├── vpc/           # VPC, subnets, NAT gateway
│   ├── eks/           # EKS cluster, node groups (use spot instances for workers)
│   ├── rds/           # Postgres RDS instance (use db.t3.medium for dev)
│   ├── elasticache/   # Redis cluster
│   ├── msk/           # Managed Kafka (AWS MSK) or self-hosted
│   ├── s3/            # Buckets for reports and archives
│   └── iam/           # Service accounts, IRSA for pod-level AWS access
├── environments/
│   ├── dev/
│   │   └── main.tf
│   └── prod/
│       └── main.tf
└── backend.tf         # S3 backend for Terraform state
```

**Kubernetes Manifests:**

- Deployment for the Go API (2 replicas minimum, HPA scaling based on CPU at 70%).
- StatefulSet for Kafka (if self-hosted) or use MSK.
- CronJob for the Spark batch reconciliation job.
- ConfigMaps for application configuration (database URLs, Kafka brokers).
- Secrets (managed via AWS Secrets Manager + External Secrets Operator, never hardcoded).

**CI/CD Pipeline (GitHub Actions):**

```yaml
# Simplified workflow
on:
  push:
    branches: [main]

jobs:
  test:
    # Run Go unit tests, integration tests with testcontainers
  lint:
    # golangci-lint, hadolint for Dockerfiles
  build:
    # Build Docker image, push to ECR
  deploy-dev:
    # Apply Terraform for dev, kubectl apply
  integration-test:
    # Run smoke tests against dev cluster
  deploy-prod:
    # Manual approval gate, then deploy to prod
```

### Observability Strategy

This is where most portfolio projects fall short. Implement all three pillars:

**Metrics (Prometheus + Grafana):**
- Go API: request latency (p50, p95, p99), request rate, error rate, active goroutines, DB connection pool utilization.
- Kafka: consumer lag per partition, producer throughput, broker disk usage.
- Spark: processing time per micro-batch, records processed per second.

**Tracing (Jaeger + OpenTelemetry):**
- Instrument the Go API with OpenTelemetry SDK. Every incoming request gets a trace ID.
- Propagate the trace ID into the Kafka event (in the `metadata.trace_id` field).
- The Spark consumer logs the trace ID alongside its processing. This lets you follow a single transaction from the REST API all the way through Kafka, Spark, and back to the fraud-alerts topic.

**Logging (Loki or ELK):**
- Structured JSON logging in the Go API (use `slog` or `zerolog`).
- Every log line includes the trace ID, request ID, and account ID for correlation.
- Ship logs from all pods via Promtail to Loki, or Fluentd to Elasticsearch.

### Load Testing & Benchmarking

Use `ghz` for gRPC load testing and `k6` for HTTP:

```bash
# gRPC load test: 10,000 requests, 50 concurrent workers
ghz --insecure \
    --proto ledger.proto \
    --call LedgerService.Transfer \
    --total 10000 \
    --concurrency 50 \
    --data '{"from_account_id":1,"to_account_id":2,"amount":100}' \
    localhost:9090
```

Document these metrics in your README:
- Requests per second (target: 5,000+ for the Go API).
- p99 latency (target: under 50ms for balance reads, under 200ms for transfers).
- Kafka throughput (target: 10,000+ events/sec sustained).
- How many pods the HPA scaled to under peak load.

### Cost Management

Running this on AWS can get expensive. Here is how to keep it under $50/month for development:

- Use **Minikube** or **kind** (Kubernetes in Docker) locally for 90% of your development. Only deploy to EKS for final integration tests.
- Use **Docker Compose** for local Kafka, Postgres, and Redis. There is no reason to pay for MSK or ElastiCache during development.
- On AWS, use **spot instances** for EKS worker nodes (60-70% cheaper than on-demand).
- Use **RDS db.t3.micro** (free tier eligible) for Postgres.
- Set up **billing alerts** at $25 and $50 thresholds.
- **Tear down the cluster** when not actively testing. Write a `make destroy` target that runs `terraform destroy`.

---
---