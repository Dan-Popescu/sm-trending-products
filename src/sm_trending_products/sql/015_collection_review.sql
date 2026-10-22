-- Keep operator plan pages independent of the total task-history sort size.
CREATE INDEX IF NOT EXISTS collection_review_page
    ON collection_tasks(synthetic,created_at DESC,task_id)
    WHERE schedule_reason<>'{}'::jsonb;
INSERT INTO schema_migrations(version) VALUES(15) ON CONFLICT DO NOTHING;
