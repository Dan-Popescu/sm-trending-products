CREATE TABLE IF NOT EXISTS operational_usage (
    usage_id text PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), counters jsonb NOT NULL
);
INSERT INTO schema_migrations(version) VALUES(7) ON CONFLICT DO NOTHING;
