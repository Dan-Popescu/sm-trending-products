CREATE TABLE IF NOT EXISTS derived_publications (
    object_key text PRIMARY KEY, evidence_datasets jsonb NOT NULL,
    state text NOT NULL CHECK(state IN ('staged','committed','invalidated','deleted')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS derived_evidence_lookup ON derived_publications USING gin(evidence_datasets);
INSERT INTO schema_migrations(version) VALUES(6) ON CONFLICT DO NOTHING;
