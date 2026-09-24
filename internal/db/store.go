package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store bundles all DB operations: the generated single-statement queries
// (via the embedded Querier) plus multi-statement transactions like TransferTx.
type Store interface {
	Querier
	TransferTx(ctx context.Context, arg TransferTxParams) (TransferTxResult, error)
}

// SQLStore is the concrete Store backed by a pgx connection pool.
type SQLStore struct {
	*Queries // promotes all generated query methods onto SQLStore
	connPool *pgxpool.Pool
}

func NewStore(connPool *pgxpool.Pool) Store {
	return &SQLStore{
		connPool: connPool,
		Queries:  New(connPool),
	}
}

// execTx runs fn inside a single DB transaction. If fn returns an error the tx
// is rolled back; otherwise it commits. Callers never touch Begin/Commit/Rollback
// directly — this is the one place that logic lives.
func (store *SQLStore) execTx(ctx context.Context, fn func(*Queries) error) error {
	tx, err := store.connPool.Begin(ctx)
	if err != nil {
		return err
	}

	q := New(tx) // queries bound to THIS transaction
	if err := fn(q); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("tx err: %v, rollback err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit(ctx)
}
