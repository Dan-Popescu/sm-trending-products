-- Keep collection due time intact while advancing past inspected blocked rows.
-- A finite sentinel survives SELECT e.* through standard psycopg datetime loaders.
ALTER TABLE entities ADD COLUMN IF NOT EXISTS last_scheduler_inspected_at timestamptz
    NOT NULL DEFAULT '1970-01-01 00:00:00+00';

CREATE INDEX IF NOT EXISTS entities_scheduler_scan_frontier ON entities(
    synthetic, GREATEST(next_due_at,last_scheduler_inspected_at), entity_key
) WHERE entity_type IN ('video','ad','commercial_content');

INSERT INTO schema_migrations(version) VALUES(10) ON CONFLICT DO NOTHING;
