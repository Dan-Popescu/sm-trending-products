CREATE INDEX IF NOT EXISTS dataset_entity_manifest_lookup ON datasets USING gin(manifest jsonb_path_ops);
INSERT INTO schema_migrations(version) VALUES(8) ON CONFLICT DO NOTHING;
