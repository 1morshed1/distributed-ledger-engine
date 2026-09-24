# Phase 1 — Part 3: HTTP API, Auth & Error Mapping

Code: `internal/api/*`, `internal/token/*`, `internal/util/{password,currency}.go`.

---

## 1. Layers, and why they're separated

```
HTTP request
   │
   ▼
internal/api        <- Gin handlers: parse/validate input, map errors to status codes
   │  (calls)
   ▼
internal/db (Store) <- business transactions (TransferTx) + generated queries
   │
   ▼
PostgreSQL
```

Handlers never write SQL. The store never knows about HTTP. This is what lets us
reuse the exact same `Store` from the gRPC server (Phase 1b) and the Airflow jobs
(Phase 4) without rewriting logic.

---

## 2. Authentication vs Authorization

Two different questions, both enforced:

- **Authentication** ("who are you?") — `authMiddleware` verifies the PASETO token
  on the `Authorization: Bearer <token>` header and puts the payload in the
  request context. No valid token → `401 Unauthorized`.
- **Authorization** ("are you allowed to touch *this* resource?") — each handler
  compares `account.Owner` to the token's `Owner`. Wrong owner → `403 Forbidden`.

The owner of a new account is taken from the **token**, never the request body:

```go
authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
server.store.CreateAccount(ctx, db.CreateAccountParams{Owner: authPayload.Owner, ...})
```

If we trusted a body field, a user could create accounts as anyone.

Verified in the smoke test: Bob got `403` trying to transfer *from* Alice's
account; a request with no token got `401`.

---

## 3. Access token + refresh token

- **Access token** — short life (15m). Sent on every request. If stolen, it
  expires fast.
- **Refresh token** — long life (24h). Used only at `/auth/refresh` to mint new
  access tokens. Stored in the `sessions` table so it can be **revoked**
  (`is_blocked`) and checked for reuse.

`renewAccessToken` re-reads the session from the DB every time. So even a
cryptographically valid refresh token is rejected if its session is blocked,
belongs to a different user, or doesn't match the stored token string (theft
signal). Statelessness is convenient but a pure-stateless design can't revoke —
the session table buys us revocation.

---

## 4. Passwords: bcrypt, never plaintext, never reversible

`util.HashPassword` uses **bcrypt** (`internal/util/password.go`):

- **Salted automatically** — two users with the same password get different
  hashes, so a stolen DB can't be attacked with precomputed rainbow tables.
- **Deliberately slow** — bcrypt's cost factor makes brute force expensive. Fast
  hashes (SHA-256) are the *wrong* choice for passwords precisely because they're
  fast.

Login returns the same `401 invalid credentials` whether the username is unknown
or the password is wrong — never leak which, or you hand attackers a user
enumeration oracle.

`userResponse` deliberately omits `hashed_password`. The hash never leaves the DB.

---

## 5. Input validation — declarative, at the edge

Gin uses `go-playground/validator` via struct tags. Bad input is rejected with
`400` before any business logic runs:

```go
type createTransferRequest struct {
    FromAccountID int64  `json:"from_account_id" binding:"required,min=1"`
    Amount        int64  `json:"amount"          binding:"required,gt=0"`
    Currency      string `json:"currency"        binding:"required,currency"`
}
```

`currency` is a **custom** validator (`internal/api/validator.go`) registered in
`NewServer`, backed by `util.IsSupportedCurrency` (ISO 4217 subset). Amounts must
be positive integers (cents) — floats and negatives can't even enter the system.

Currencies must also match across both accounts in a transfer — you can't move USD
into a EUR account (`validAccount` checks this → `400`).

---

## 6. Mapping domain errors → HTTP status codes

This is where a "toy" API and a real one differ. Each failure has a *correct*
code:

| Situation                          | Detected via                         | Status |
|------------------------------------|--------------------------------------|--------|
| Bad/missing input                  | binding tags                         | 400    |
| No / bad / expired token           | `authMiddleware`                     | 401    |
| Account isn't yours                | owner check                          | 403    |
| Account doesn't exist              | `db.ErrRecordNotFound`               | 404    |
| Duplicate idempotency key          | `db.ErrorCode == UniqueViolation`    | 409    |
| Insufficient funds                 | `db.ErrInsufficientFunds`            | 422    |
| Unexpected DB error                | fallthrough                          | 500    |

`internal/db/errors.go` turns a raw Postgres `PgError` into a SQLSTATE string
(`23505` = unique, `23503` = FK) so handlers can branch on it cleanly instead of
string-matching error text.

### The idempotency 409, concretely

`POST /transfers` requires an `Idempotency-Key: <uuid>` header. On a duplicate,
`TransferTx` hits the `UNIQUE` constraint; the handler catches it, looks up the
**original** transfer, and returns it with `409` — so a retrying client learns
"already done" *and* gets the original result. Smoke test confirmed: same key
twice → one `201`, one `409`, money moved once.

---

## 7. Endpoint summary (Phase 1)

| Method | Path                     | Auth | Purpose                          |
|--------|--------------------------|------|----------------------------------|
| POST   | /api/v1/users            | no   | register (bcrypt-hashed)         |
| POST   | /api/v1/auth/login       | no   | issue access + refresh tokens    |
| POST   | /api/v1/auth/refresh     | no   | renew access token from refresh  |
| POST   | /api/v1/accounts         | yes  | create account (owner = you)     |
| GET    | /api/v1/accounts/:id     | yes  | get your account                 |
| GET    | /api/v1/accounts         | yes  | list your accounts (paginated)   |
| POST   | /api/v1/transfers        | yes  | transfer (idempotency-key hdr)   |
| GET    | /api/v1/transfers        | yes  | list transfers (cursor paginated)|

---

## Next

Phase 1 core is complete and tested. Remaining Phase 1 items before moving to
Kafka (Phase 2): **gRPC service** (`LedgerService`), **Redis write-through cache**
for balances, and **Asynq** for async tasks. See `README.md` → Roadmap.
