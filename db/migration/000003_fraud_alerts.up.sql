-- 000003_fraud_alerts.up.sql
-- Flagged transactions written by the Spark fraud detector. No FK to accounts:
-- Spark writes independently and must not be coupled to the ledger's row lifecycle.

CREATE TABLE fraud_alerts (
    id           BIGSERIAL   PRIMARY KEY,
    account_id   BIGINT      NOT NULL,
    rule         VARCHAR(50) NOT NULL,          -- velocity | amount_anomaly | rapid_drain
    severity     VARCHAR(20) NOT NULL DEFAULT 'high',
    detail       JSONB       NOT NULL,          -- rule-specific evidence
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fraud_alerts_account ON fraud_alerts(account_id);
CREATE INDEX idx_fraud_alerts_rule    ON fraud_alerts(rule);
