-- Disposable eligibility only: a committed v1 child remains the completion fact.
-- Drain old/new row-locking claimers and writers before lease-trigger DDL/FK
-- backfill, avoiding a parent-row -> lease-table -> parent-FK lock cycle.
-- Ordinary reads and the independent source HTTP/spool path remain available.
LOCK TABLE datasets IN EXCLUSIVE MODE;

CREATE TABLE capture_publication_pending (
    dataset_id text PRIMARY KEY REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    synthetic boolean NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE INDEX capture_publication_pending_scan
    ON capture_publication_pending(synthetic,created_at,dataset_id);
CREATE INDEX capture_committed_normalization_parent ON datasets(supersedes)
    WHERE supersedes IS NOT NULL AND transform_version='1' AND state='committed';

-- A claim can hold an older statement snapshot even after another claimant
-- committed its parent lock and lease. Protect that ownership at mutation time,
-- including compatible old images whose UPSERT has no conflict WHERE clause.
CREATE FUNCTION guard_capture_publication_takeover() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.worker_id,NEW.fence) IS DISTINCT FROM (OLD.worker_id,OLD.fence)
       AND (OLD.lease_until>=clock_timestamp() OR OLD.retry_at>clock_timestamp()) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER capture_publication_takeover_guard BEFORE UPDATE ON capture_publication_leases
    FOR EACH ROW EXECUTE FUNCTION guard_capture_publication_takeover();

CREATE FUNCTION refresh_capture_publication_pending(parent_id text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE parent datasets%ROWTYPE;
BEGIN
    -- Serialize competing child transitions with their parent. NO KEY UPDATE
    -- remains compatible with the publisher's preceding parent KEY SHARE lock;
    -- no lease is acquired here, so parent-before-lease ordering is preserved.
    SELECT * INTO parent FROM datasets WHERE dataset_id=parent_id FOR NO KEY UPDATE;
    IF FOUND AND parent.state='committed'
       AND parent.manifest->>'kind'='raw_capture'
       AND parent.manifest->>'acquisition_protocol'='1'
       AND NOT EXISTS (
           SELECT 1 FROM datasets child WHERE child.supersedes=parent.dataset_id
           AND child.transform_version='1' AND child.state='committed'
       ) THEN
        INSERT INTO capture_publication_pending(dataset_id,synthetic,created_at)
            VALUES(parent.dataset_id,parent.synthetic,parent.created_at)
            ON CONFLICT(dataset_id) DO UPDATE
            SET synthetic=EXCLUDED.synthetic,created_at=EXCLUDED.created_at;
    ELSE
        DELETE FROM capture_publication_pending WHERE dataset_id=parent_id;
    END IF;
END $$;

CREATE FUNCTION maintain_capture_publication_pending() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE affected text[] := ARRAY[]::text[];
DECLARE parent_id text;
BEGIN
    IF TG_OP<>'DELETE' THEN
        IF NEW.manifest->>'kind'='raw_capture' THEN
            affected := array_append(affected,NEW.dataset_id);
        END IF;
        IF NEW.supersedes IS NOT NULL AND NEW.transform_version='1' AND NEW.state='committed' THEN
            affected := array_append(affected,NEW.supersedes);
        END IF;
    END IF;
    IF TG_OP<>'INSERT' THEN
        IF OLD.manifest->>'kind'='raw_capture' THEN
            affected := array_append(affected,OLD.dataset_id);
        END IF;
        IF OLD.supersedes IS NOT NULL AND OLD.transform_version='1' AND OLD.state='committed' THEN
            affected := array_append(affected,OLD.supersedes);
        END IF;
    END IF;
    FOR parent_id IN SELECT DISTINCT value FROM unnest(affected) AS ids(value) ORDER BY value LOOP
        PERFORM refresh_capture_publication_pending(parent_id);
    END LOOP;
    RETURN NULL;
END $$;
CREATE TRIGGER capture_publication_pending_changes
    AFTER INSERT OR DELETE OR UPDATE OF state,manifest,synthetic,created_at,supersedes,transform_version
    ON datasets FOR EACH ROW EXECUTE FUNCTION maintain_capture_publication_pending();

-- This full-history reconciliation is performed once at schema expansion, not
-- at process startup or on each claim. Compatible old writers use the same hooks.
INSERT INTO capture_publication_pending(dataset_id,synthetic,created_at)
    SELECT parent.dataset_id,parent.synthetic,parent.created_at FROM datasets parent
    WHERE parent.state='committed' AND parent.manifest->>'kind'='raw_capture'
    AND parent.manifest->>'acquisition_protocol'='1' AND NOT EXISTS (
        SELECT 1 FROM datasets child WHERE child.supersedes=parent.dataset_id
        AND child.transform_version='1' AND child.state='committed'
    );

INSERT INTO schema_migrations(version) VALUES(37) ON CONFLICT DO NOTHING;
