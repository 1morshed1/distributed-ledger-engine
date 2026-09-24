-- 000004_monthly_summaries.up.sql
-- Precomputed per-account monthly aggregates for fast dashboard queries.

CREATE TABLE monthly_summaries (
    id           BIGSERIAL     PRIMARY KEY,
    account_id   BIGINT        NOT NULL,
    month        DATE          NOT NULL,          -- first day of the month
    tx_count     BIGINT        NOT NULL,
    total_volume BIGINT        NOT NULL,          -- sum of outgoing transfer amounts (cents)
    avg_amount   NUMERIC(20,2) NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, month)                    -- idempotent monthly upsert
);

CREATE INDEX idx_monthly_summaries_month ON monthly_summaries(month);
