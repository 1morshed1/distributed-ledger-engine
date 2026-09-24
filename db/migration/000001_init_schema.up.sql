-- 000001_init_schema.up.sql
-- Core double-entry ledger schema.
-- Money is stored as BIGINT in the smallest currency unit (cents). NEVER floats.

CREATE TABLE accounts (
    id              BIGSERIAL PRIMARY KEY,
    owner           VARCHAR(255) NOT NULL,
    balance         BIGINT       NOT NULL DEFAULT 0,   -- smallest unit (cents)
    currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- One immutable row per balance change. The audit trail.
-- amount: positive = credit (money in), negative = debit (money out).
CREATE TABLE entries (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT      NOT NULL REFERENCES accounts(id),
    amount          BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per transfer request. amount must be positive.
CREATE TABLE transfers (
    id              BIGSERIAL   PRIMARY KEY,
    from_account_id BIGINT      NOT NULL REFERENCES accounts(id),
    to_account_id   BIGINT      NOT NULL REFERENCES accounts(id),
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    idempotency_key UUID        UNIQUE NOT NULL,       -- dedupes duplicate submits
    status          VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending|completed|failed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entries_account_id     ON entries(account_id);
CREATE INDEX idx_transfers_from_account ON transfers(from_account_id);
CREATE INDEX idx_transfers_to_account   ON transfers(to_account_id);
-- idempotency_key already has a unique index from the UNIQUE constraint.
