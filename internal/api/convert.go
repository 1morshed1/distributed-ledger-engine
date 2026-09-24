package api

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// toTimestamptz converts a Go time.Time into the pgtype value sqlc expects.
func toTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}
