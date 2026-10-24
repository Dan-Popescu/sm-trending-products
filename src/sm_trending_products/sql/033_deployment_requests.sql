CREATE TABLE deployment_requests (
    target text NOT NULL REFERENCES deployment_targets(target),
    sequence bigint NOT NULL CHECK(sequence>0),
    request jsonb NOT NULL,
    release_id text NOT NULL REFERENCES deployment_runs(release_id),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(target,sequence)
);
INSERT INTO schema_migrations(version) VALUES(33) ON CONFLICT DO NOTHING;
