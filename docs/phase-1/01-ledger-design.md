# Phase 1 — Part 1: Ledger Design & The Money Rules

This doc explains **why the schema and queries look the way they do**. Read it
before the code so the code reads as obvious.

---

## 1. Money is an integer, never a float

`balance BIGINT` — stored in the **smallest currency unit** (cents for USD).

`$50.00` is stored as `5000`.

Why not `NUMERIC` or `float`?

- **Floats lie.** `0.1 + 0.2 != 0.3` in IEEE-754. In a ledger that runs
  millions of additions, rounding error accumulates into real, unaccounted money.
- **Integers are exact and fast.** `BIGINT` (int64) holds up to
  ~9.2 quintillion cents ≈ 92 quadrillion dollars. Enough.
- `NUMERIC` is exact too, but slower and heavier for every arithmetic op. Integer
  cents is the standard in real payment systems (Stripe does this).

**Rule:** the API accepts/returns amounts in the smallest unit. Formatting to
`$50.00` is a *presentation* concern, done at the very edge (UI), never in the DB
or business logic.

---

## 2. Double-entry accounting: the `entries` table

We don't just mutate `accounts.balance` and hope. Every balance change writes an
immutable **entry**:

| account_id | amount | meaning        |
|-----------|--------|----------------|
| 1         | -5000  | debit (out)    |
| 2         | +5000  | credit (in)    |

Invariant: **for any transfer, the entries sum to zero.** Money is conserved —
it moves, never appears or vanishes.

Two sources of truth on purpose:

- `accounts.balance` — the *fast* answer ("what's my balance now?"), a cache.
- `SUM(entries.amount)` — the *true* answer, recomputable from history.

Reconciliation (Phase 4) recomputes `SUM(entries)` per account and compares it to
`accounts.balance`. Any drift = a bug caught. `SumEntriesForAccount` in
`entry.sql` is that query.

`entries` is append-only. Never `UPDATE` or `DELETE` a row — that would destroy
the audit trail. To "reverse" a transaction you write a *new* opposing entry.

---

## 3. The `transfers` table and idempotency

A `transfer` is the *request* ("move 5000 from A to B"). Processing it produces
2 entries + 2 balance updates.

`idempotency_key UUID UNIQUE` is the safety net for the real world:

> A client sends a transfer. The network drops the response. The client retries.
> Without dedup, you move the money **twice**.

The client generates one UUID per *intended* transfer and sends it with every
retry. The `UNIQUE` constraint means the 2nd insert fails at the DB level — we
catch that, look up the original transfer, and return it (HTTP 409). The money
moves exactly once no matter how many times the client retries.

`amount BIGINT CHECK (amount > 0)` — a transfer is always positive. Direction is
expressed by which account is `from` vs `to`, not by sign. (Sign lives on
`entries`.)

---

## 4. Why `sqlc` instead of an ORM

`sqlc` reads our raw SQL (`db/query/*.sql`) + the schema and **generates**
type-safe Go functions. We write SQL; it writes the boilerplate.

- No runtime reflection / query-builder magic (unlike GORM). What you see is the
  SQL that runs.
- Compile-time safety: rename a column, the generated Go stops compiling.
- Zero performance overhead — it's just `pgx` calls underneath.

`--emit-interface` also generates a `Querier` interface, which lets us mock the DB
in unit tests without a real Postgres.

The magic comments matter:

```sql
-- name: GetAccount :one     -> func GetAccount(...) (Account, error)
-- name: ListEntries :many   -> func ListEntries(...) ([]Entry, error)
-- name: DeleteAccount :exec -> func DeleteAccount(...) error
```

---

## 5. Cursor (keyset) pagination — see `ListTransfers`

`OFFSET 1000000` makes Postgres scan and throw away a million rows every page.
It degrades linearly.

Keyset pagination remembers the **last id seen** and asks for `id > @after_id`.
That's an index seek — same speed on page 1 and page 100,000. That's why
`ListTransfers` takes `after_id` instead of `offset`.

---

## Next

`02-transaction-tx.md` — the transfer transaction, row locking, and the deadlock
trap the spec calls "the single most common mistake in financial backend code."
