-- Immutable analytical artifacts reuse the existing derived publication lifecycle.
-- Leaves own bounded source dependencies; parents reference committed children.
ALTER TABLE derived_publications ADD COLUMN IF NOT EXISTS object_sha256 text;
ALTER TABLE derived_publications ADD COLUMN IF NOT EXISTS object_bytes bigint
    CHECK(object_bytes IS NULL OR object_bytes>=0);
CREATE TABLE IF NOT EXISTS derived_artifact_inputs (
    object_key text NOT NULL REFERENCES derived_publications(object_key),
    dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    PRIMARY KEY(object_key,dataset_id)
);
CREATE INDEX IF NOT EXISTS derived_artifact_dataset_lookup
    ON derived_artifact_inputs(dataset_id,object_key);
CREATE TABLE IF NOT EXISTS derived_artifact_edges (
    parent_key text NOT NULL REFERENCES derived_publications(object_key),
    child_key text NOT NULL REFERENCES derived_publications(object_key),
    PRIMARY KEY(parent_key,child_key), CHECK(parent_key<>child_key)
);
CREATE INDEX IF NOT EXISTS derived_artifact_parent_lookup
    ON derived_artifact_edges(child_key,parent_key);
CREATE TABLE IF NOT EXISTS evaluation_artifacts (
    evaluation_id text NOT NULL REFERENCES evaluation_runs(evaluation_id),
    object_key text NOT NULL REFERENCES derived_publications(object_key),
    PRIMARY KEY(evaluation_id,object_key)
);
CREATE INDEX IF NOT EXISTS evaluation_artifact_lookup
    ON evaluation_artifacts(object_key,evaluation_id);

CREATE OR REPLACE FUNCTION dataset_publication_closure(root_ids text[])
RETURNS TABLE(dataset_id text) LANGUAGE sql STABLE AS $$
    WITH RECURSIVE affected(dataset_id) AS (
        SELECT unnest(root_ids)
        UNION SELECT edge.dataset_id FROM affected current
        CROSS JOIN LATERAL (
            SELECT child_dataset_id AS dataset_id FROM dataset_lineage
                WHERE parent_dataset_id=current.dataset_id
            UNION SELECT shared.dataset_id FROM dataset_object_refs origin
                JOIN dataset_object_refs shared ON shared.object_key=origin.object_key
                WHERE origin.dataset_id=current.dataset_id
        ) edge
    ) SELECT dataset_id FROM affected
$$;

CREATE OR REPLACE FUNCTION analytical_artifact_ancestors(dataset_ids text[])
RETURNS TABLE(object_key text) LANGUAGE sql STABLE AS $$
    WITH RECURSIVE affected(object_key) AS (
        SELECT p.object_key FROM derived_publications p WHERE p.state<>'deleted'
            AND p.evidence_datasets ?| dataset_ids
        UNION SELECT i.object_key FROM derived_artifact_inputs i
            JOIN derived_publications p ON p.object_key=i.object_key
            WHERE i.dataset_id=ANY(dataset_ids) AND p.state<>'deleted'
        UNION SELECT edge.parent_key FROM derived_artifact_edges edge
            JOIN affected child ON child.object_key=edge.child_key
            JOIN derived_publications parent ON parent.object_key=edge.parent_key
            WHERE parent.state<>'deleted'
    ) SELECT object_key FROM affected
$$;

CREATE OR REPLACE FUNCTION guard_derived_artifact_input()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_key=NEW.object_key AND state='staged') THEN
        RAISE EXCEPTION 'Only staged artifacts can acquire input dependencies';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM datasets
        WHERE dataset_id=NEW.dataset_id AND state='committed') THEN
        RAISE EXCEPTION 'Artifact input is unavailable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER derived_artifact_input_guard BEFORE INSERT ON derived_artifact_inputs
    FOR EACH ROW EXECUTE FUNCTION guard_derived_artifact_input();

CREATE OR REPLACE FUNCTION guard_derived_artifact_edge()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_key=NEW.parent_key AND state='staged') THEN
        RAISE EXCEPTION 'Committed artifact edges are immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_key=NEW.child_key AND state='committed') THEN
        RAISE EXCEPTION 'Artifact child is unavailable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER derived_artifact_edge_guard BEFORE INSERT ON derived_artifact_edges
    FOR EACH ROW EXECUTE FUNCTION guard_derived_artifact_edge();

CREATE OR REPLACE FUNCTION guard_evaluation_artifact()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_key=NEW.object_key AND state='committed') THEN
        RAISE EXCEPTION 'Evaluation artifact is unavailable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER evaluation_artifact_guard BEFORE INSERT ON evaluation_artifacts
    FOR EACH ROW EXECUTE FUNCTION guard_evaluation_artifact();
INSERT INTO schema_migrations(version) VALUES(19) ON CONFLICT DO NOTHING;
