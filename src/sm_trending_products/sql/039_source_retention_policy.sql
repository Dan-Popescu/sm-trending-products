-- Cross this boundary only after every retained reader supports verified NULL
-- source caps. Existing finite policies and immutable retirements are unchanged.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=38) THEN
        RAISE EXCEPTION 'Source retention policy requires SQL schema 038';
    END IF;
END $$;
ALTER TABLE source_policies ALTER COLUMN retention_days DROP NOT NULL;
ALTER TABLE source_policies ADD CONSTRAINT source_policy_unlimited_verified
    CHECK (retention_days IS NOT NULL OR verified);
COMMENT ON COLUMN source_policies.retention_days IS
    'Verified source-imposed expiry cap in days; NULL explicitly verifies no source expiry cap.';

CREATE OR REPLACE FUNCTION validate_capture_spool_retirement() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE task collection_tasks%ROWTYPE; policy source_policies%ROWTYPE;
        dataset datasets%ROWTYPE; identity_text text; deadline timestamptz; grace integer;
BEGIN
    -- Serialize qualification with legal removal/publication, including direct
    -- operational SQL callers rather than trusting Python's earlier preview.
    PERFORM pg_advisory_xact_lock(492371205);
    NEW.retired_at:=clock_timestamp();
    SELECT * INTO task FROM collection_tasks WHERE task_id=NEW.task_id FOR KEY SHARE;
    IF NOT FOUND OR task.source IS DISTINCT FROM NEW.source
       OR task.account_id IS DISTINCT FROM NEW.account_id
       OR task.synthetic IS DISTINCT FROM NEW.synthetic OR NEW.page_index<0 OR NEW.capture_attempt<0
       OR NEW.task_id !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Retired capture scope does not match its task';
    END IF;
    identity_text:=CASE WHEN NEW.capture_attempt=0 THEN
        '{"capture_of":"'||NEW.task_id||'","page_index":'||NEW.page_index||',"protocol":"1"}'
    ELSE '{"capture_attempt":'||NEW.capture_attempt||',"capture_of":"'||NEW.task_id||
         '","page_index":'||NEW.page_index||',"protocol":"1"}' END;
    IF NEW.capture_id<>encode(sha256(convert_to(identity_text,'UTF8')),'hex')
       OR NEW.started_at>NEW.retired_at
       OR (NEW.complete_receipt AND (NEW.captured_at IS NULL OR NEW.captured_at<NEW.started_at
                                   OR NEW.captured_at>NEW.retired_at))
       OR (NOT NEW.complete_receipt AND NEW.captured_at IS NOT NULL) THEN
        RAISE EXCEPTION 'Invalid retired capture identity or physical clock';
    END IF;
    IF NEW.authority_kind='source_revocation' THEN
        IF NEW.reason<>'capture_source_removed' OR NOT EXISTS (
            SELECT 1 FROM capture_spool_revocations r WHERE r.source=NEW.source
            AND r.account_id=NEW.account_id AND r.synthetic=NEW.synthetic
            AND r.captured_before=(NEW.authority_ref->>'captured_before')::timestamptz
            AND r.requested_at=(NEW.authority_ref->>'requested_at')::timestamptz
            AND r.reason=NEW.authority_ref->>'reason'
            AND r.requested_at<=NEW.retired_at
            AND r.captured_before>=LEAST(NEW.started_at,COALESCE(NEW.captured_at,NEW.started_at))
        ) THEN RAISE EXCEPTION 'Retired capture lacks exact scoped revocation authority'; END IF;
    ELSIF NEW.authority_kind='retention_expiry' THEN
        SELECT * INTO policy FROM source_policies WHERE source=NEW.source AND verified FOR SHARE;
        grace:=(NEW.authority_ref->>'reservation_grace_seconds')::integer;
        IF NOT FOUND OR policy.retention_days IS NULL
           OR NEW.reason<>'capture_retention_expired' OR grace IS NULL
           OR (NEW.complete_receipt AND grace<>0)
           OR (NOT NEW.complete_receipt AND grace NOT BETWEEN 5 AND 600)
           OR policy.retention_days IS DISTINCT FROM
                (NEW.authority_ref->>'retention_days')::integer
           OR policy.updated_at IS DISTINCT FROM
                (NEW.authority_ref->>'policy_updated_at')::timestamptz THEN
            RAISE EXCEPTION 'Retired capture lacks exact verified retention authority';
        END IF;
        deadline:=COALESCE(NEW.captured_at,NEW.started_at)
            +policy.retention_days*interval '1 day'+grace*interval '1 second';
        IF deadline>NEW.retired_at OR deadline IS DISTINCT FROM
                (NEW.authority_ref->>'deadline')::timestamptz THEN
            RAISE EXCEPTION 'Retired capture retention deadline has not elapsed';
        END IF;
    ELSIF NEW.authority_kind='dataset_invalidation' THEN
        SELECT * INTO dataset FROM datasets WHERE dataset_id=NEW.capture_id FOR KEY SHARE;
        IF NOT FOUND OR NEW.reason<>'capture_source_removed'
           OR dataset.state NOT IN ('invalidated','deleted')
           OR dataset.source IS DISTINCT FROM NEW.source OR dataset.synthetic IS DISTINCT FROM NEW.synthetic
           OR dataset.task_id IS DISTINCT FROM NEW.task_id
           OR dataset.manifest->'capture'->>'page_index' IS DISTINCT FROM NEW.page_index::text
           OR COALESCE(dataset.manifest->'capture'->>'capture_attempt','0')<>NEW.capture_attempt::text
           OR NEW.authority_ref->>'dataset_id' IS DISTINCT FROM NEW.capture_id
           OR NEW.authority_ref->>'state' IS DISTINCT FROM dataset.state
           OR NEW.authority_ref->>'invalid_reason' IS DISTINCT FROM dataset.invalid_reason THEN
            RAISE EXCEPTION 'Retired capture lacks exact dataset invalidation authority';
        END IF;
    ELSE RAISE EXCEPTION 'Unsupported capture retirement authority';
    END IF;
    RETURN NEW;
END $$;

INSERT INTO schema_migrations(version) VALUES(39) ON CONFLICT DO NOTHING;
