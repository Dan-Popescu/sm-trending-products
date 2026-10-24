-- Progress belongs to the existing immutable publication tombstone, not a second queue.
-- Building the partial index scans an existing catalog once during this atomic upgrade.
SET LOCAL statement_timeout='10min';
ALTER TABLE derived_publications ADD COLUMN relations_purged boolean NOT NULL DEFAULT false;
CREATE INDEX retired_artifact_relation_work ON derived_publications(object_key)
    WHERE state='deleted' AND NOT relations_purged;
INSERT INTO schema_migrations(version) VALUES(25) ON CONFLICT DO NOTHING;
SET LOCAL statement_timeout='30s';
