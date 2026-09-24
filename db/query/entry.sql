-- name: CreateEntry :one
INSERT INTO entries (account_id, amount)
VALUES ($1, $2)
RETURNING *;

-- name: GetEntry :one
SELECT * FROM entries
WHERE id = $1 LIMIT 1;

-- name: ListEntries :many
SELECT * FROM entries
WHERE account_id = $1
ORDER BY id
LIMIT $2 OFFSET $3;

-- name: SumEntriesForAccount :one
-- Recompute a balance from the immutable ledger. Used by reconciliation.
SELECT COALESCE(SUM(amount), 0)::bigint AS balance
FROM entries
WHERE account_id = $1;
