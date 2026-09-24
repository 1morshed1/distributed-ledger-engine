# Phase 2 — Part 1: The Event Stream (Kafka)

Code: `proto/events.proto`, `internal/events/*`, `internal/worker/task_publish_event.go`.
Infra: `docker-compose.yml` (kafka service).

---

## 1. Why Kafka at all — decoupling

Phase 1 committed a transfer and returned. Now, after each transfer, we publish an
event to Kafka. Why bother?

**Decoupling.** The ledger doesn't know or care who consumes transfers. Fraud
detection (Spark), analytics, notifications — each reads the stream independently.
We can add a new consumer without touching the ledger. If Spark is down, the
ledger keeps working; events wait durably in Kafka until Spark catches up.

Kafka over RabbitMQ here because Kafka is a **durable log**: events are retained
and replayable. A new consumer can re-read history from the beginning. A classic
queue deletes on consume — no replay.

---

## 2. Topics (plan)

| Topic                | Written by | Read by            |
|----------------------|-----------|--------------------|
| `transaction-events` | ledger    | Spark, analytics   |
| `fraud-alerts`       | Spark     | alerting service   |
| `balance-snapshots`  | ledger    | reconciliation     |

Phase 2 implements `transaction-events` (the primary stream). The others come with
Spark (Phase 3) and reconciliation (Phase 4).

---

## 3. Event schema — protobuf, versioned

`proto/events.proto` → `TransactionEvent` with `payload` + `metadata`. Protobuf,
not raw JSON, because:

- **Compact + fast** on a high-volume stream.
- **Schema-checked** — consumers decode with the same generated types; a typo
  can't silently ship a malformed event.
- **Versioned** — `metadata.schema_version` lets consumers evolve. Adding a field
  is backward-compatible in protobuf.

`metadata.trace_id` carries the originating request's trace through Kafka into
Spark — one transaction is followable end to end (wired fully in the
observability phase).

---

## 4. Producer decisions (the ones that matter for money)

`internal/events/kafka.go`, using `segmentio/kafka-go`:

| Setting | Value | Why |
|---|---|---|
| `RequiredAcks` | `RequireAll` | Write succeeds only after all in-sync replicas have it. Never trade durability for speed on financial data. |
| `Balancer` | `Hash` on Key | Key = `from_account_id`, so all of an account's events go to one partition and stay **ordered**. Fraud detection needs the true sequence. |
| `MaxAttempts` | 3 | A brief broker hiccup retries instead of dropping the event. |

> **Ordering only holds per partition.** By keying on the account, each account's
> events are ordered — which is exactly the ordering fraud detection needs. Global
> ordering across all accounts is neither guaranteed nor required.

Production would also enable the idempotent producer (`enable.idempotence`) to
dedupe events on retry; kafka-go achieves the durability half via `RequireAll` +
bounded retries. We also carry our own `event_id` (uuid) so a downstream consumer
can dedupe defensively.

---

## 5. Publishing via Asynq — guaranteed delivery, off the hot path

We do **not** publish inline in the transfer handler. Instead the handler enqueues
a `task:publish_transfer_event` Asynq job (Phase 1's queue). The worker publishes
to Kafka.

Why:

- **Off the hot path** — the transfer response returns immediately; Kafka latency
  never slows a transfer.
- **Retries** — if Kafka is briefly down, the publish handler returns an error and
  Asynq retries with backoff (`MaxRetry(5)`). The event isn't lost.

```
transfer commits ──► enqueue publish task ──► worker ──► Kafka (acks=all)
                                                 │ error?
                                                 └─ Asynq retries w/ backoff
```

The honest caveat: enqueue is best-effort *after* commit, so a crash in the
microsecond between commit and enqueue could miss an event. The bullet-proof fix
is the **transactional outbox** pattern — write the event to an `outbox` table in
the *same* DB transaction as the transfer, and a separate relay publishes it. A
good Phase 2.5 upgrade; noted, not yet built.

---

## 6. Run it

```bash
make up   # now also starts Kafka (KRaft, port 9092)

# after a transfer, watch events land:
docker exec ledger-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transaction-events --from-beginning
```

(The values are protobuf bytes, so the console shows binary. Phase 3's Spark job
decodes them properly.)

---

## Next

Phase 3 — Spark Structured Streaming consumes `transaction-events` for real-time
fraud detection (velocity, amount anomaly, rapid drain). PySpark.
