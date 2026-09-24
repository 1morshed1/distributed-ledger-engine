package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func createTestUser(t *testing.T) string {
	t.Helper()
	username := uuid.NewString()
	_, err := testStore.CreateUser(context.Background(), CreateUserParams{
		Username:       username,
		HashedPassword: "x", // password hashing tested separately
		FullName:       "Test User",
		Email:          username + "@example.com",
	})
	require.NoError(t, err)
	return username
}

func createTestAccount(t *testing.T, balance int64) Account {
	t.Helper()
	acc, err := testStore.CreateAccount(context.Background(), CreateAccountParams{
		Owner:    createTestUser(t), // owner must reference a real user (FK)
		Balance:  balance,
		Currency: "USD",
	})
	require.NoError(t, err)
	return acc
}

// TestTransferTxConcurrent fires N transfers between the same two accounts
// concurrently. This is the real test: if lock ordering is wrong it deadlocks;
// if the transaction isn't atomic, money is created or destroyed.
func TestTransferTxConcurrent(t *testing.T) {
	account1 := createTestAccount(t, 100000)
	account2 := createTestAccount(t, 100000)

	const n = 20
	const amount = int64(1000)

	errs := make(chan error, n)
	results := make(chan TransferTxResult, n)

	for i := 0; i < n; i++ {
		go func() {
			res, err := testStore.TransferTx(context.Background(), TransferTxParams{
				FromAccountID:  account1.ID,
				ToAccountID:    account2.ID,
				Amount:         amount,
				IdempotencyKey: uuid.New(),
			})
			errs <- err
			results <- res
		}()
	}

	for i := 0; i < n; i++ {
		require.NoError(t, <-errs)
		res := <-results
		require.NotEmpty(t, res)
		require.Equal(t, "completed", res.Transfer.Status)
		// entries sum to zero: money is conserved per transfer
		require.Equal(t, res.FromEntry.Amount, -res.ToEntry.Amount)
	}

	// Final balances: account1 sent n*amount, account2 received it.
	updated1, err := testStore.GetAccount(context.Background(), account1.ID)
	require.NoError(t, err)
	updated2, err := testStore.GetAccount(context.Background(), account2.ID)
	require.NoError(t, err)

	require.Equal(t, int64(100000)-n*amount, updated1.Balance)
	require.Equal(t, int64(100000)+n*amount, updated2.Balance)

	// Global invariant: total money unchanged.
	require.Equal(t, int64(200000), updated1.Balance+updated2.Balance)
}

// TestTransferTxInsufficientFunds ensures an overdraft rolls back entirely.
func TestTransferTxInsufficientFunds(t *testing.T) {
	from := createTestAccount(t, 500)
	to := createTestAccount(t, 0)

	_, err := testStore.TransferTx(context.Background(), TransferTxParams{
		FromAccountID:  from.ID,
		ToAccountID:    to.ID,
		Amount:         1000, // more than balance
		IdempotencyKey: uuid.New(),
	})
	require.ErrorIs(t, err, ErrInsufficientFunds)

	// Balances must be untouched (full rollback).
	f, _ := testStore.GetAccount(context.Background(), from.ID)
	tacc, _ := testStore.GetAccount(context.Background(), to.ID)
	require.Equal(t, int64(500), f.Balance)
	require.Equal(t, int64(0), tacc.Balance)
}

// TestTransferTxIdempotency ensures the same key can't move money twice.
func TestTransferTxIdempotency(t *testing.T) {
	from := createTestAccount(t, 10000)
	to := createTestAccount(t, 0)
	key := uuid.New()

	_, err := testStore.TransferTx(context.Background(), TransferTxParams{
		FromAccountID: from.ID, ToAccountID: to.ID, Amount: 1000, IdempotencyKey: key,
	})
	require.NoError(t, err)

	// Second call with the SAME key must fail (UNIQUE violation), not move money.
	_, err = testStore.TransferTx(context.Background(), TransferTxParams{
		FromAccountID: from.ID, ToAccountID: to.ID, Amount: 1000, IdempotencyKey: key,
	})
	require.Error(t, err)

	f, _ := testStore.GetAccount(context.Background(), from.ID)
	require.Equal(t, int64(9000), f.Balance) // moved once, not twice
}
