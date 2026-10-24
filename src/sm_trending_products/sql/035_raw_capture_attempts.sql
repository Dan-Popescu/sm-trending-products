ALTER TABLE collection_tasks ADD COLUMN IF NOT EXISTS capture_attempt bigint NOT NULL DEFAULT 0
    CHECK(capture_attempt>=0);

-- Explicit source removal authority for inputs not yet registered as datasets.
-- Credential expiry/denial alone is not authorization to destroy a pending body.
CREATE TABLE IF NOT EXISTS capture_spool_revocations (
    source text NOT NULL,
    account_id text NOT NULL,
    synthetic boolean NOT NULL,
    captured_before timestamptz NOT NULL,
    reason text NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(source,account_id,synthetic,captured_before)
);

INSERT INTO schema_migrations(version) VALUES(35) ON CONFLICT DO NOTHING;
