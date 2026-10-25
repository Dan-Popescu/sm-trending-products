-- Recovery visits a bounded progressing page of potentially unsettled inputs.
-- Due/lease expiry is evaluated after admission to the indexed page, so future
-- work cannot force an unbounded filtered scan. No observation state is copied.
CREATE INDEX IF NOT EXISTS collection_capture_recovery_frontier ON collection_tasks
    (synthetic,task_id)
    WHERE state IN ('pending','leased')
       OR (state='quarantined' AND last_error='attempt_limit');

INSERT INTO schema_migrations(version) VALUES(36) ON CONFLICT DO NOTHING;
