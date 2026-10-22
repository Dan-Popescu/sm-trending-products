ALTER TABLE collection_tasks ADD COLUMN IF NOT EXISTS page_index integer NOT NULL DEFAULT 0;
INSERT INTO schema_migrations(version) VALUES(5) ON CONFLICT DO NOTHING;
