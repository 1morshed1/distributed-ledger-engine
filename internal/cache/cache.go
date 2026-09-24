package cache

import (
	"context"
	"time"
)

// BalanceCache is the hot-path balance store. Balance reads are the most frequent
// operation in a ledger; serving them from Redis keeps them sub-millisecond and
// off the primary DB. The interface lets tests use an in-memory fake.
type BalanceCache interface {
	// GetBalance returns (balance, true, nil) on hit, (0, false, nil) on miss.
	GetBalance(ctx context.Context, accountID int64) (int64, bool, error)
	// SetBalance writes a balance with the default TTL (write-through).
	SetBalance(ctx context.Context, accountID int64, balance int64) error
	// Invalidate removes a cached balance (use when unsure it's fresh).
	Invalidate(ctx context.Context, accountID int64) error
}

// defaultTTL is a safety net: even if a write-through update is ever missed, a
// stale entry self-heals within this window by falling back to Postgres.
const defaultTTL = 5 * time.Minute
