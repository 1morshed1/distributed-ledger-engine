# Phase 1 — Part 4: gRPC Internal Service

Code: `proto/ledger.proto`, `internal/pb/*` (generated), `internal/gapi/*`.

---

## 1. Why gRPC *and* REST

They serve different callers:

- **REST (Gin)** — external clients: browsers, mobile apps, third parties. Human-
  readable JSON, works everywhere, easy to curl.
- **gRPC** — internal service-to-service: the Spark fraud job asking for a
  balance, Airflow triggering reconciliation. Benefits that matter *between*
  services:
  - **Code-generated clients** — the `.proto` is the contract; both sides
    generate matching types. No hand-written client, no drift.
  - **Binary (protobuf)** — ~10x smaller and faster to (de)serialize than JSON.
  - **Strong typing + streaming** — the schema is enforced at compile time.

Crucially, both front-ends call the **same `db.Store`**. No business logic is
duplicated — `gapi` is a thin protobuf adapter over the exact `TransferTx` the
REST layer uses.

---

## 2. The codegen pipeline

```
proto/ledger.proto
        │  protoc  (+ protoc-gen-go, protoc-gen-go-grpc)
        ▼
internal/pb/ledger.pb.go        <- message structs (Account, TransferRequest, …)
internal/pb/ledger_grpc.pb.go   <- LedgerServiceServer interface + client
```

Regenerate with `make proto`. Never edit the generated files by hand.

`option go_package = ".../internal/pb;pb"` in the proto tells protoc the import
path and package name of the output. The Makefile passes
`--go_opt=module=<module>` so files land at the right path inside the module.

`protoc` needs its bundled "well-known types" (e.g.
`google/protobuf/timestamp.proto`); those live in `~/.local/include` and are on
the `--proto_path`.

---

## 3. Server wiring

`gapi.Server` embeds `pb.UnimplementedLedgerServiceServer`:

```go
type Server struct {
    pb.UnimplementedLedgerServiceServer // forward-compat default methods
    config util.Config
    store  db.Store
    cache  cache.BalanceCache
}
```

Embedding the `Unimplemented...` base means adding a new RPC to the proto later
won't break the build — the base provides a default "unimplemented" method until
we write the real one.

`main.go` runs gRPC and HTTP side by side:

```go
go runGRPCServer(config, store, balanceCache) // :9090, own goroutine
runHTTPServer(config, store, balanceCache)     // :8080, blocks main
```

`reflection.Register(grpcServer)` enables server reflection, so tools like
`grpcurl` and the `ghz` load tester can introspect the service **without** the
`.proto` file on hand.

---

## 4. Error mapping — gRPC status codes

Same domain errors as REST, mapped to gRPC's `codes` instead of HTTP status:

| Situation                 | HTTP | gRPC code                    |
|---------------------------|------|------------------------------|
| Bad input                 | 400  | `InvalidArgument`            |
| Not found                 | 404  | `NotFound`                   |
| Duplicate idempotency key | 409  | `AlreadyExists`              |
| Insufficient funds        | 422  | `FailedPrecondition`         |
| Unexpected                | 500  | `Internal`                   |

Verified with grpcurl: `Transfer` returned `completed`; a repeated
`idempotency_key` returned `AlreadyExists`; an oversized amount returned
`FailedPrecondition`.

---

## 5. Testing it yourself

```bash
make server   # starts HTTP :8080 and gRPC :9090

grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"account_id":1}' \
  localhost:9090 ledger.v1.LedgerService/GetAccountBalance
grpcurl -plaintext -d '{"from_account_id":1,"to_account_id":2,"amount":500,
  "currency":"USD","idempotency_key":"'$(uuidgen)'"}' \
  localhost:9090 ledger.v1.LedgerService/Transfer
```

Load-test target (plan): `ghz` against `LedgerService.Transfer`.

---

## Next

`05-async-tasks.md` — Asynq (Redis-backed) for work that shouldn't block a
transfer response: notifications, statement generation, guaranteed event publish.
