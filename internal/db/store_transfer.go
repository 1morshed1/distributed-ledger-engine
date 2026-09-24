package db

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrInsufficientFunds is returned when the source account cannot cover a transfer.
var ErrInsufficientFunds = errors.New("insufficient funds")

type TransferTxParams struct {
	FromAccountID  int64
	ToAccountID    int64
	Amount         int64 // positive, smallest currency unit
	IdempotencyKey uuid.UUID
}

// TransferTxResult is the full picture of one transfer: the transfer row, both
// updated accounts, and both ledger entries. Handy for the API response and tests.
type TransferTxResult struct {
	Transfer    Transfer `json:"transfer"`
	FromAccount Account  `json:"from_account"`
	ToAccount   Account  `json:"to_account"`
	FromEntry   Entry    `json:"from_entry"`
	ToEntry     Entry    `json:"to_entry"`
}

// TransferTx moves money from one account to another inside a single DB
// transaction. It creates the transfer record, two ledger entries, and updates
// both balances — all atomically. Either everything commits or nothing does.
func (store *SQLStore) TransferTx(ctx context.Context, arg TransferTxParams) (TransferTxResult, error) {
	var result TransferTxResult

	err := store.execTx(ctx, func(q *Queries) error {
		var err error

		// 1) Create the transfer record with its idempotency key.
		result.Transfer, err = q.CreateTransfer(ctx, CreateTransferParams{
			FromAccountID:  arg.FromAccountID,
			ToAccountID:    arg.ToAccountID,
			Amount:         arg.Amount,
			IdempotencyKey: arg.IdempotencyKey,
			Status:         "pending",
		})
		if err != nil {
			return err // UNIQUE violation on idempotency_key bubbles up here
		}

		// 2) Append the two immutable ledger entries (sum to zero).
		result.FromEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.FromAccountID,
			Amount:    -arg.Amount, // debit
		})
		if err != nil {
			return err
		}
		result.ToEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.ToAccountID,
			Amount:    arg.Amount, // credit
		})
		if err != nil {
			return err
		}

		// 3) Update both balances.
		//
		// DEADLOCK PREVENTION — the single most common bug in ledger code:
		// if goroutine 1 locks A then B while goroutine 2 locks B then A, they
		// wait on each other forever. Fix: ALWAYS acquire locks in a consistent
		// global order. We lock the lower account id first, regardless of
		// transfer direction. AddAccountBalance's UPDATE takes the row lock.
		if arg.FromAccountID < arg.ToAccountID {
			result.FromAccount, result.ToAccount, err = addMoney(
				ctx, q, arg.FromAccountID, -arg.Amount, arg.ToAccountID, arg.Amount)
		} else {
			result.ToAccount, result.FromAccount, err = addMoney(
				ctx, q, arg.ToAccountID, arg.Amount, arg.FromAccountID, -arg.Amount)
		}
		if err != nil {
			return err
		}

		// 4) Reject overdraft AFTER applying — the negative balance only exists
		// inside this uncommitted tx; returning an error rolls it all back.
		if result.FromAccount.Balance < 0 {
			return ErrInsufficientFunds
		}

		// 5) Mark completed.
		result.Transfer, err = q.UpdateTransferStatus(ctx, UpdateTransferStatusParams{
			ID:     result.Transfer.ID,
			Status: "completed",
		})
		return err
	})

	return result, err
}

// addMoney applies two balance deltas in a fixed argument order so the caller
// controls lock ordering. Returns the two updated accounts in the SAME order the
// ids were passed.
func addMoney(
	ctx context.Context,
	q *Queries,
	id1, amount1 int64,
	id2, amount2 int64,
) (account1 Account, account2 Account, err error) {
	account1, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{ID: id1, Amount: amount1})
	if err != nil {
		return
	}
	account2, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{ID: id2, Amount: amount2})
	return
}
