package db

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testStore talks to the local dockerized Postgres (host 5433). Integration
// tests need a real DB because we're testing real locking/transaction behavior
// that a mock cannot exercise.
var testStore Store

const testDBSource = "postgresql://ledger:secret@localhost:5433/ledger?sslmode=disable"

func TestMain(m *testing.M) {
	// Allow override so CI / other machines can point elsewhere.
	source := os.Getenv("DB_SOURCE")
	if source == "" {
		source = testDBSource
	}

	pool, err := pgxpool.New(context.Background(), source)
	if err != nil {
		log.Fatalf("cannot connect to db: %v", err)
	}
	defer pool.Close()

	testStore = NewStore(pool)
	os.Exit(m.Run())
}
