-- One knowledge timestamp per canonical source replay, not a second publication state.
CREATE TABLE IF NOT EXISTS replay_availability (
    replay_dataset_id text PRIMARY KEY,
    source_dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    available_at timestamptz NOT NULL
);
INSERT INTO schema_migrations(version) VALUES(17) ON CONFLICT DO NOTHING;
