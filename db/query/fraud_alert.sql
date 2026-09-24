-- name: ListFraudAlerts :many
-- Cursor-based: pass last-seen id as @after_id (0 for first page). Newest first
-- by id is achieved by scanning descending; we page with id < after_id when a
-- cursor is given, else from the top.
SELECT * FROM fraud_alerts
WHERE (@after_id::bigint = 0 OR id < @after_id)
ORDER BY id DESC
LIMIT @page_size;

-- name: ListFraudAlertsByAccount :many
SELECT * FROM fraud_alerts
WHERE account_id = @account_id
ORDER BY id DESC
LIMIT @page_size;
