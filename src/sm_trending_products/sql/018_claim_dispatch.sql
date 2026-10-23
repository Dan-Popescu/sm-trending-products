-- Match the priority-ordered ready frontier without sorting/evaluating every
-- pending task before one claim. Expired leases retain the same fenced protocol.
CREATE INDEX IF NOT EXISTS collection_active_dispatch ON collection_tasks
    (synthetic,priority DESC,due_at,task_id)
    WHERE state IN ('pending','leased');

-- Claim-time exhaustion cleanup must not rescan the entire active queue.
CREATE INDEX IF NOT EXISTS collection_attempt_frontier ON collection_tasks
    (synthetic,state,attempts,lease_until)
    WHERE state IN ('pending','leased');

INSERT INTO schema_migrations(version) VALUES(18) ON CONFLICT DO NOTHING;
