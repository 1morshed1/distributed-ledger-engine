# API Reference

Two surfaces over the same `db.Store`:
- **REST** (Gin) on `:8080` — external clients. JSON.
- **gRPC** (`LedgerService`) on `:9090` — internal service-to-service. protobuf.

All money amounts are **integer cents** (`5000` = $50.00). Auth is a PASETO
bearer token from `/auth/login`.

---

## REST

Base URL: `http://localhost:8080`. Auth header on protected routes:
`Authorization: Bearer <access_token>`.

### Conventions
- Errors: `{"error": "<message>"}`.
- Auth failures → `401`; wrong owner → `403`; not found → `404`.
- Amounts and balances are cents (int64).

### Operational (no auth)

| Method | Path       | Purpose                         |
|--------|------------|---------------------------------|
| GET    | `/healthz` | liveness → `{"status":"ok"}`    |
| GET    | `/metrics` | Prometheus exposition           |

---

### `POST /api/v1/users` — register
Body:
```json
{"username":"alice","password":"secret123","full_name":"Alice A","email":"a@x.com"}
```
`201` → user (no password hash). `409` if username/email exists. `400` on bad input.

### `POST /api/v1/auth/login`
Body: `{"username":"alice","password":"secret123"}`
`200` →
```json
{
  "session_id":"...","access_token":"v4.local...","access_token_expires_at":"...",
  "refresh_token":"v4.local...","refresh_token_expires_at":"...",
  "user":{"username":"alice","full_name":"Alice A","email":"a@x.com","created_at":"..."}
}
```
`401` on bad credentials (same message for unknown user vs wrong password).

### `POST /api/v1/auth/refresh`
Body: `{"refresh_token":"v4.local..."}`
`200` → `{"access_token":"...","access_token_expires_at":"..."}`.
`401` if the session is missing, blocked, mismatched, or expired.

---

### `POST /api/v1/accounts` — create account *(auth)*
Body: `{"currency":"USD"}` (ISO 4217 subset: USD/EUR/GBP/CAD/JPY).
Owner is taken from the token, not the body.
`201` → account. `409` if you already have an account in that currency.

### `GET /api/v1/accounts/:id` *(auth)*
`200` → account (only if you own it). `403` otherwise. `404` if missing.

### `GET /api/v1/accounts/:id/balance` *(auth)*
Cache-first read.
`200` → `{"account_id":7,"balance":94000,"source":"cache|db"}`.

### `GET /api/v1/accounts?page_id=1&page_size=20` *(auth)*
Your accounts, offset-paginated. `200` → `[account, ...]`.

---

### `POST /api/v1/transfers` *(auth)*
Headers: `Idempotency-Key: <uuid>` (required).
Body:
```json
{"from_account_id":7,"to_account_id":8,"amount":5000,"currency":"USD"}
```
- `from` account must be yours and match the currency; `to` must exist + match.
- `201` → `{transfer, from_account, to_account, from_entry, to_entry}`.
- `409` (same idempotency key) → `{"message":"...already processed","transfer":<original>}`.
- `422` insufficient funds. `400` bad input / currency mismatch. `403` not your account.

Example:
```bash
curl -X POST http://localhost:8080/api/v1/transfers \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d '{"from_account_id":7,"to_account_id":8,"amount":5000,"currency":"USD"}'
```

### `GET /api/v1/transfers?account_id=7&after_id=0&page_size=20` *(auth)*
Cursor (keyset) pagination. `account_id` must be yours.
`200` → `{"transfers":[...],"next_after_id":<last id>}`. Pass `next_after_id` as
`after_id` for the next page.

### `GET /api/v1/fraud-alerts?after_id=0&page_size=20` *(auth)*
Alerts written by the Spark job, newest first.
`200` →
```json
{"alerts":[{"id":6,"account_id":7,"rule":"velocity","severity":"high",
  "detail":{"tx_count":8,"threshold":5},"window_start":"...","window_end":"...",
  "created_at":"..."}],"next_after_id":5}
```

---

## gRPC — `ledger.v1.LedgerService`

Address: `localhost:9090` (plaintext, dev). Server reflection is on, so `grpcurl`
needs no `.proto`. See `proto/ledger.proto`.

| RPC | Request → Response | Notes |
|---|---|---|
| `CreateAccount` | `CreateAccountRequest{owner,currency}` → `Account` | `AlreadyExists` on dup currency |
| `GetAccount` | `GetAccountRequest{id}` → `Account` | `NotFound` |
| `Transfer` | `TransferRequest{from,to,amount,currency,idempotency_key}` → `TransferResult` | `FailedPrecondition` = insufficient funds; `AlreadyExists` = dup key |
| `ListTransfers` | `ListTransfersRequest{account_id,after_id,page_size}` → `ListTransfersResponse{transfers,next_after_id}` | keyset paging |
| `GetAccountBalance` | `GetBalanceRequest{account_id}` → `BalanceResponse{account_id,balance,currency}` | cache-first; used by Spark |

### Status code mapping (mirror of REST)
| Situation | gRPC code |
|---|---|
| bad input | `InvalidArgument` |
| not found | `NotFound` |
| duplicate idempotency key | `AlreadyExists` |
| insufficient funds | `FailedPrecondition` |
| unexpected | `Internal` |

### Examples
```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext -d '{"account_id":7}' \
  localhost:9090 ledger.v1.LedgerService/GetAccountBalance
grpcurl -plaintext -d '{"from_account_id":7,"to_account_id":8,"amount":500,
  "currency":"USD","idempotency_key":"'$(uuidgen)'"}' \
  localhost:9090 ledger.v1.LedgerService/Transfer
```

### Load testing (plan targets)
```bash
ghz --insecure --proto proto/ledger.proto --call ledger.v1.LedgerService.Transfer \
    --total 10000 --concurrency 50 \
    --data '{"from_account_id":7,"to_account_id":8,"amount":100,"currency":"USD","idempotency_key":"{{.RequestNumber}}"}' \
    localhost:9090
# HTTP: k6 against :8080
```
