# Phase 1 — Part 2: The Transfer Transaction & Deadlock Prevention

Code: `internal/db/store.go`, `internal/db/store_transfer.go`.
Proof: `internal/db/store_transfer_test.go` (3 passing tests).

---

## 1. Why a database transaction at all

A transfer is not one write. It's **five**:

1. insert a `transfer` row
2. insert the debit `entry` (−amount)
3. insert the credit `entry` (+amount)
4. subtract from sender balance
5. add to receiver balance

If the process crashes after step 4 but before step 5, money **vanishes**. A DB
transaction makes all five **atomic**: they all commit together or none do
(the "A" and "C" in ACID).

`execTx` in `store.go` is the only place that calls `Begin` / `Commit` /
`Rollback`:

```go
func (store *SQLStore) execTx(ctx, fn func(*Queries) error) error {
    tx, _ := store.connPool.Begin(ctx)
    q := New(tx)                 // queries now run INSIDE this tx
    if err := fn(q); err != nil {
        tx.Rollback(ctx)         // any error -> undo everything
        return err
    }
    return tx.Commit(ctx)
}
```

`TransferTx` just passes a closure describing the 5 steps. Clean separation:
transaction *mechanics* in one spot, transaction *logic* in another.

---

## 2. The deadlock — "the single most common mistake in financial code"

A deadlock needs two transactions grabbing the same two locks in **opposite
order**:

```
Goroutine 1 (transfer A -> B)      Goroutine 2 (transfer B -> A)
  lock account A          ✓          lock account B          ✓
  lock account B   ...waits on G2    lock account A   ...waits on G1
        \__________ both wait forever __________/
```

Postgres will actually detect this and kill one transaction with a
`deadlock detected` error — but that's a failed transfer and an angry user. We
prevent it instead.

### The fix: a global lock order

**Always acquire account locks lowest-id-first, regardless of transfer
direction.** Then two concurrent transfers between the same accounts always ask
for the locks in the same sequence — no cycle, no deadlock.

```go
if arg.FromAccountID < arg.ToAccountID {
    // update the lower id (from) first, then the higher (to)
    result.FromAccount, result.ToAccount, err = addMoney(ctx, q,
        arg.FromAccountID, -arg.Amount, arg.ToAccountID, arg.Amount)
} else {
    // to has the lower id -> update it first
    result.ToAccount, result.FromAccount, err = addMoney(ctx, q,
        arg.ToAccountID, arg.Amount, arg.FromAccountID, -arg.Amount)
}
```

`addMoney` applies the two `AddAccountBalance` updates in the exact order given.
The `UPDATE ... SET balance = balance + $1` statement takes the row lock as it
runs. Because we control the call order, we control the lock order.

`TestTransferTxConcurrent` fires 20 simultaneous transfers between the same two
accounts. Without the ordering it deadlocks; with it, all 20 succeed and the
balances are exact.

---

## 3. Why `balance = balance + $1` and not read-then-write

Tempting but **wrong**:

```go
acc := GetAccount(id)          // read 100
acc.Balance -= amount          // compute in Go
UpdateAccount(id, acc.Balance) // write 90
```

Two concurrent transfers both read 100, both write 90 — one update is lost. This
is a classic **race condition / lost update**.

`AddAccountBalance` does the arithmetic **in the database**, in one statement:

```sql
UPDATE accounts SET balance = balance + $1 WHERE id = $2 RETURNING *;
```

The row lock is held for the whole statement, so concurrent updates serialize and
none is lost. Never compute a new balance in application code and write it back.

---

## 4. Overdraft handling — apply then check, inside the tx

```go
if result.FromAccount.Balance < 0 {
    return ErrInsufficientFunds   // returning an error rolls back the tx
}
```

We let the balance go negative *inside the uncommitted transaction*, then reject
it. Because nothing has committed, returning the error triggers `Rollback` and the
negative balance never becomes real. `TestTransferTxInsufficientFunds` proves the
balances are untouched afterward.

(A production system might also add a `CHECK (balance >= 0)` constraint as a
belt-and-suspenders DB-level guard.)

---

## 5. Idempotency at the transaction layer

Step 1 inserts the `transfer` with its `idempotency_key`. A duplicate submit hits
the `UNIQUE` constraint and `CreateTransfer` returns an error → the whole tx rolls
back → money moves **zero** extra times. `TestTransferTxIdempotency` shows the
balance drops by 1000 once, not twice, even though the same key is used twice.

The API layer (next doc) turns that duplicate-key error into a friendly
`409 Conflict` that returns the *original* transfer.

---

## Next

`03-api-and-auth.md` — Gin REST handlers, request validation, PASETO auth, and
mapping domain errors (insufficient funds, duplicate key) to HTTP status codes.
