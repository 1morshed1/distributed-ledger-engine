package db

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Postgres SQLSTATE codes we care about.
const (
	UniqueViolation     = "23505"
	ForeignKeyViolation = "23503"
)

// ErrRecordNotFound is the sentinel for "no rows" (pgx returns pgx.ErrNoRows).
var ErrRecordNotFound = pgx.ErrNoRows

// ErrorCode extracts the Postgres SQLSTATE from an error, or "" if it isn't a
// PgError. Handlers use this to turn a duplicate key into 409, etc.
func ErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
