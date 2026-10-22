ALTER TABLE collection_tasks ADD COLUMN IF NOT EXISTS schedule_reason jsonb NOT NULL DEFAULT '{}';
CREATE INDEX IF NOT EXISTS discovery_query_history ON collection_tasks((schedule_reason->>'query_id'),created_at DESC);
INSERT INTO schema_migrations(version) VALUES(2) ON CONFLICT DO NOTHING;
