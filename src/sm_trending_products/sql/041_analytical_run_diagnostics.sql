CREATE TABLE analytical_run_diagnostics (
    attempt_id text PRIMARY KEY CHECK(attempt_id ~ '^[0-9a-f]{32}$'),
    operation text NOT NULL CHECK(operation IN ('assessment','freeze')),
    synthetic boolean NOT NULL,
    cutoff timestamptz,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    state text NOT NULL CHECK(state IN ('running','succeeded','failed')),
    result_id varchar(128),
    error_type varchar(128),
    summary jsonb NOT NULL CHECK(octet_length(summary::text)<=32768),
    CHECK((state='running')=(finished_at IS NULL)),
    CHECK(state<>'succeeded' OR (cutoff IS NOT NULL AND result_id IS NOT NULL)),
    CHECK((state='failed')=(error_type IS NOT NULL))
);
CREATE INDEX analytical_run_diagnostics_recent ON analytical_run_diagnostics(synthetic,started_at DESC,attempt_id DESC);
INSERT INTO schema_migrations(version) VALUES(41);
