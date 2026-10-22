ALTER TABLE datasets ADD COLUMN IF NOT EXISTS compacted_into text REFERENCES datasets(dataset_id);
CREATE INDEX IF NOT EXISTS active_dataset_scan ON datasets(synthetic,source,created_at)
WHERE state='committed' AND compacted_into IS NULL;

CREATE TABLE IF NOT EXISTS dataset_object_refs (
    dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    object_key text NOT NULL, PRIMARY KEY(dataset_id,object_key)
);
CREATE INDEX IF NOT EXISTS object_dataset_lookup ON dataset_object_refs(object_key);
CREATE TABLE IF NOT EXISTS dataset_lineage (
    parent_dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    child_dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    PRIMARY KEY(parent_dataset_id,child_dataset_id),
    CHECK(parent_dataset_id<>child_dataset_id)
);
CREATE INDEX IF NOT EXISTS lineage_child_lookup ON dataset_lineage(child_dataset_id);
CREATE TABLE IF NOT EXISTS retention_objects (
    object_key text PRIMARY KEY,
    state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','deleted')),
    reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz, attempts integer NOT NULL DEFAULT 0, last_error text
);
CREATE INDEX IF NOT EXISTS pending_retention_objects ON retention_objects(created_at)
WHERE state='pending';

CREATE OR REPLACE FUNCTION manifest_object_keys(manifest jsonb)
RETURNS TABLE(object_key text) LANGUAGE sql IMMUTABLE AS $$
    SELECT manifest->'raw'->>'key' WHERE manifest->'raw'->>'key' IS NOT NULL
    UNION SELECT manifest->>'manifest_key' WHERE manifest->>'manifest_key' IS NOT NULL
    UNION SELECT value->>'key' FROM jsonb_array_elements(COALESCE(manifest->'normalized','[]'))
        WHERE value->>'key' IS NOT NULL
$$;

CREATE OR REPLACE FUNCTION guard_dataset_publication()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('staged','committed') THEN
        PERFORM pg_advisory_xact_lock_shared(492371205);
        IF EXISTS (
            SELECT 1 FROM manifest_object_keys(NEW.manifest) keys
            JOIN retention_objects retired ON retired.object_key=keys.object_key
        ) THEN
            RAISE EXCEPTION 'Dataset references retired evidence';
        END IF;
        IF NEW.supersedes IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM datasets WHERE dataset_id=NEW.supersedes AND state='committed'
        ) THEN RAISE EXCEPTION 'Replay parent is unavailable'; END IF;
        IF EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(
                COALESCE(NEW.manifest->'parent_dataset_ids','[]')) parent
            LEFT JOIN datasets d ON d.dataset_id=parent.value
            WHERE d.dataset_id IS NULL OR d.state<>'committed'
        ) THEN RAISE EXCEPTION 'Compaction parent is unavailable'; END IF;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS dataset_publication_guard ON datasets;
CREATE TRIGGER dataset_publication_guard BEFORE INSERT OR UPDATE ON datasets
FOR EACH ROW EXECUTE FUNCTION guard_dataset_publication();

CREATE OR REPLACE FUNCTION index_dataset_manifest()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM dataset_object_refs WHERE dataset_id=NEW.dataset_id;
    INSERT INTO dataset_object_refs(dataset_id,object_key)
        SELECT NEW.dataset_id,object_key FROM manifest_object_keys(NEW.manifest)
        ON CONFLICT DO NOTHING;
    DELETE FROM dataset_lineage WHERE child_dataset_id=NEW.dataset_id;
    INSERT INTO dataset_lineage(parent_dataset_id,child_dataset_id)
        SELECT value,NEW.dataset_id FROM jsonb_array_elements_text(
            COALESCE(NEW.manifest->'parent_dataset_ids','[]'))
        UNION SELECT NEW.supersedes,NEW.dataset_id WHERE NEW.supersedes IS NOT NULL
        ON CONFLICT DO NOTHING;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS dataset_manifest_index ON datasets;
CREATE TRIGGER dataset_manifest_index AFTER INSERT OR UPDATE OF manifest,supersedes ON datasets
FOR EACH ROW EXECUTE FUNCTION index_dataset_manifest();

INSERT INTO dataset_object_refs(dataset_id,object_key)
    SELECT d.dataset_id,keys.object_key FROM datasets d
    CROSS JOIN LATERAL manifest_object_keys(d.manifest) keys ON CONFLICT DO NOTHING;
INSERT INTO dataset_lineage(parent_dataset_id,child_dataset_id)
    SELECT value,d.dataset_id FROM datasets d
    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(d.manifest->'parent_dataset_ids','[]'))
    UNION SELECT supersedes,dataset_id FROM datasets WHERE supersedes IS NOT NULL
    ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS assessment_evidence_lookup ON assessment_runs USING gin(evidence_datasets);

CREATE OR REPLACE FUNCTION guard_assessment_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT NEW.invalidated THEN
        PERFORM pg_advisory_xact_lock_shared(492371205);
        IF EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(NEW.evidence_datasets) evidence
            LEFT JOIN datasets d ON d.dataset_id=evidence.value
            WHERE d.dataset_id IS NULL OR d.state<>'committed'
        ) THEN RAISE EXCEPTION 'Assessment references unavailable evidence'; END IF;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS assessment_evidence_guard ON assessment_runs;
CREATE TRIGGER assessment_evidence_guard BEFORE INSERT OR UPDATE ON assessment_runs
FOR EACH ROW EXECUTE FUNCTION guard_assessment_evidence();

CREATE OR REPLACE FUNCTION guard_evaluation_dependency()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    IF NOT EXISTS (SELECT 1 FROM datasets WHERE dataset_id=NEW.dataset_id AND state='committed') THEN
        RAISE EXCEPTION 'Evaluation references unavailable evidence';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS evaluation_evidence_guard ON evaluation_dependencies;
CREATE TRIGGER evaluation_evidence_guard BEFORE INSERT ON evaluation_dependencies
FOR EACH ROW EXECUTE FUNCTION guard_evaluation_dependency();

INSERT INTO schema_migrations(version) VALUES(3) ON CONFLICT DO NOTHING;
