-- name: CreateTransfer :one
INSERT INTO transfers (from_account_id, to_account_id, amount, idempotency_key, status)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetTransfer :one
SELECT * FROM transfers
WHERE id = $1 LIMIT 1;

-- name: GetTransferByIdempotencyKey :one
-- Used to return the original transfer on a duplicate submit (409 path).
SELECT * FROM transfers
WHERE idempotency_key = $1 LIMIT 1;

-- name: UpdateTransferStatus :one
UPDATE transfers
SET status = $2
WHERE id = $1
RETURNING *;

-- name: ListTransfers :many
-- Cursor-based pagination: pass the last-seen id as @after_id (0 for first page).
-- Offset pagination degrades at scale; keyset stays O(log n) on the index.
SELECT * FROM transfers
WHERE (from_account_id = @account_id OR to_account_id = @account_id)
  AND id > @after_id
ORDER BY id
LIMIT @page_size;
