CREATE TABLE IF NOT EXISTS forward_frontier (
    evaluation_id text PRIMARY KEY REFERENCES evaluation_runs(evaluation_id),
    synthetic boolean NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','running','complete','invalidated')),
    next_due_at timestamptz NOT NULL,
    lease_token text,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
    last_report_id text REFERENCES evaluation_runs(evaluation_id),
    last_checked_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK((state='running' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
       OR (state<>'running' AND lease_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS forward_due_frontier
    ON forward_frontier(synthetic,next_due_at,created_at,evaluation_id) WHERE state='pending';
CREATE INDEX IF NOT EXISTS forward_expired_leases
    ON forward_frontier(lease_until) WHERE state='running';
INSERT INTO forward_frontier(evaluation_id,synthetic,next_due_at)
    SELECT evaluation_id,synthetic,COALESCE(decision_time,created_at)
    FROM evaluation_runs WHERE kind='forward' AND NOT invalidated
    ON CONFLICT DO NOTHING;
INSERT INTO schema_migrations(version) VALUES(9) ON CONFLICT DO NOTHING;
