# Phase 1 — Part 5: Async Tasks with Asynq

Code: `internal/worker/*`. Wired in `cmd/api/main.go`, enqueued from
`internal/api/transfer.go` and `internal/gapi/rpc_ledger.go`.

---

## 1. Why an async queue

Some work shouldn't block the transfer response:

- sending a notification / email
- generating a PDF statement
- publishing the transfer to Kafka (Phase 2) with delivery guarantees

If we did these inline, a slow email server would slow every transfer, and a
failure could roll back a *committed* transfer. Instead we **commit the transfer,
then enqueue a job**. The response returns immediately; the job runs
independently, with retries.

**Asynq** is a Redis-backed task queue for Go. We already run Redis (for the
balance cache), so there's no new infrastructure.

---

## 2. Two halves: distributor and processor

```
REST / gRPC handler
      │  DistributeTaskTransferNotification(payload)   (producer)
      ▼
   Redis  ── asynq queue: "critical" ──►  Processor  (consumer)
                                             │  ProcessTaskTransferNotification
                                             ▼
                                        send notification
```

- **`TaskDistributor`** (`distributor.go`) — the producer. Handlers depend on this
  interface, so business code never imports Asynq directly (and tests can fake it).
- **`TaskProcessor`** (`processor.go`) — the consumer. Runs in its own goroutine
  from `main`, pulls tasks, dispatches to handlers via `asynq.ServeMux`.

The task type string (`"task:transfer_notification"`) is the routing key that
links the two.

---

## 3. Queues and priority

```go
Queues: map[string]int{
    QueueCritical: 10,  // polled ~10x more often
    QueueDefault:  1,
}
```

Asynq weights queues. Critical jobs (a fraud-related notification) shouldn't be
starved behind a backlog of bulk jobs (monthly statements). We enqueue the
transfer notification on `critical`.

---

## 4. Retries and failure semantics

Enqueue options:

```go
asynq.MaxRetry(3),               // retry up to 3 times with backoff
asynq.Queue(worker.QueueCritical)
```

- If `ProcessTask...` returns an error, Asynq **retries** with exponential
  backoff. Transient failures (email server briefly down) self-heal.
- After `MaxRetry` it goes to the **dead-letter** ("archived") set for inspection
  — no silent loss.
- A **permanent** failure (malformed payload that will never succeed) is wrapped
  with `asynq.SkipRetry` so we don't waste retries:

  ```go
  return fmt.Errorf("bad payload: %w: %w", err, asynq.SkipRetry)
  ```

Enqueue itself is **best-effort** at the call site (`_ = Distribute...`): the
transfer already committed and is the source of truth. We never fail a committed
transfer because a follow-up couldn't be queued. (A stricter design would enqueue
inside the same DB transaction via an outbox table — a good Phase 2 upgrade when
we need guaranteed Kafka delivery.)

---

## 5. Verified

Smoke test: a REST transfer returned `201`, then the logs showed:

```
enqueued task=task:transfer_notification queue=critical id=...
processed task=task:transfer_notification transfer_id=77 amount=250 (notification sent)
```

Producer and consumer both working over Redis.

---

## Phase 1 complete

Core ledger • deadlock-safe transfers • idempotency • PASETO auth • REST + gRPC •
Redis write-through cache • Asynq async tasks. All tested.

**Next: Phase 2 — Kafka.** The `transfer-events` stream. The Asynq notification
task is the natural place to also publish the Kafka event with guaranteed
delivery (or we introduce a transactional outbox). See `plan.md` Phase 2.
