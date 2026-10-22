-- Keep identifier keysets, equality and indexes consistent with Python's UTF-8 order,
-- independently of the database or observation_id column's locale.
DROP INDEX IF EXISTS observation_page_lookup;
CREATE INDEX observation_page_lookup ON observation_deliveries
    (entity_key,observed_at,observation_id COLLATE "C",available_at);
DROP INDEX IF EXISTS observation_conflict_lookup;
CREATE INDEX observation_conflict_lookup ON observation_deliveries
    (observation_id COLLATE "C",entity_key,available_at);
INSERT INTO schema_migrations(version) VALUES(15) ON CONFLICT DO NOTHING;
