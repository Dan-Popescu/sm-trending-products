-- Keep each bounded maintenance pass on active candidates, not retired history.
CREATE INDEX IF NOT EXISTS compaction_group_candidates ON datasets(
    synthetic,source,transform_version,((manifest->>'partition_buckets')),
    row_count,created_at,dataset_id
) WHERE state='committed' AND compacted_into IS NULL AND row_count>0 AND quarantine_count=0;

CREATE INDEX IF NOT EXISTS retention_expired_candidates ON datasets(source,created_at,dataset_id)
WHERE state IN ('committed','staged');
CREATE INDEX IF NOT EXISTS retention_finalization_candidates ON datasets(dataset_id)
WHERE state='invalidated' AND invalid_reason='retention_expired';
CREATE INDEX IF NOT EXISTS retention_pending_keyset ON retention_objects(created_at,object_key)
WHERE state='pending';

INSERT INTO schema_migrations(version) VALUES(24) ON CONFLICT DO NOTHING;
