-- 000002_auth.up.sql
-- Users (login identities) and sessions (refresh-token records).

CREATE TABLE users (
    username        VARCHAR(255) PRIMARY KEY,
    hashed_password VARCHAR(255) NOT NULL,          -- bcrypt hash, never plaintext
    full_name       VARCHAR(255) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- One row per issued refresh token. Lets us revoke a session and detect reuse.
CREATE TABLE sessions (
    id            UUID         PRIMARY KEY,          -- == refresh token payload id
    username      VARCHAR(255) NOT NULL REFERENCES users(username),
    refresh_token TEXT         NOT NULL,
    user_agent    VARCHAR(255) NOT NULL DEFAULT '',
    client_ip     VARCHAR(255) NOT NULL DEFAULT '',
    is_blocked    BOOLEAN      NOT NULL DEFAULT FALSE,
    expires_at    TIMESTAMPTZ  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Tie accounts to a real user. owner now references users.username.
-- (Existing rows: none in dev; safe. In prod this would need a backfill.)
ALTER TABLE accounts
    ADD CONSTRAINT fk_accounts_owner
    FOREIGN KEY (owner) REFERENCES users(username);

-- A user shouldn't hold two accounts in the same currency.
CREATE UNIQUE INDEX idx_accounts_owner_currency ON accounts(owner, currency);
