-- Keep public immutable object keys on their authoritative publication row only.
-- This SQL-only atomic rebuild needs a maintenance window and temporary disk
-- headroom for the old and compact association layouts, like migration 020.
SET LOCAL statement_timeout='10min';

ALTER TABLE derived_publications ADD COLUMN object_id bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE derived_publications ADD CONSTRAINT derived_publication_id UNIQUE(object_id);

CREATE TABLE derived_artifact_inputs_compact (
    object_id bigint NOT NULL REFERENCES derived_publications(object_id),
    dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    PRIMARY KEY(object_id,dataset_id)
);
INSERT INTO derived_artifact_inputs_compact
    SELECT p.object_id,i.dataset_id FROM derived_artifact_inputs i
    JOIN derived_publications p USING(object_key);
DROP TABLE derived_artifact_inputs;
ALTER TABLE derived_artifact_inputs_compact RENAME TO derived_artifact_inputs;
CREATE INDEX derived_artifact_dataset_lookup ON derived_artifact_inputs(dataset_id,object_id);

CREATE TABLE derived_artifact_edges_compact (
    parent_id bigint NOT NULL REFERENCES derived_publications(object_id),
    child_id bigint NOT NULL REFERENCES derived_publications(object_id),
    PRIMARY KEY(parent_id,child_id), CHECK(parent_id<>child_id)
);
INSERT INTO derived_artifact_edges_compact
    SELECT parent.object_id,child.object_id FROM derived_artifact_edges e
    JOIN derived_publications parent ON parent.object_key=e.parent_key
    JOIN derived_publications child ON child.object_key=e.child_key;
DROP TABLE derived_artifact_edges;
ALTER TABLE derived_artifact_edges_compact RENAME TO derived_artifact_edges;
CREATE INDEX derived_artifact_parent_lookup ON derived_artifact_edges(child_id,parent_id);

CREATE TABLE evaluation_artifacts_compact (
    evaluation_id text NOT NULL REFERENCES evaluation_runs(evaluation_id),
    object_id bigint NOT NULL REFERENCES derived_publications(object_id),
    PRIMARY KEY(evaluation_id,object_id)
);
INSERT INTO evaluation_artifacts_compact
    SELECT e.evaluation_id,p.object_id FROM evaluation_artifacts e
    JOIN derived_publications p USING(object_key);
DROP TABLE evaluation_artifacts;
ALTER TABLE evaluation_artifacts_compact RENAME TO evaluation_artifacts;
CREATE INDEX evaluation_artifact_lookup ON evaluation_artifacts(object_id,evaluation_id);

CREATE FUNCTION analytical_artifact_ancestor_ids(dataset_ids text[])
RETURNS TABLE(object_id bigint) LANGUAGE sql STABLE AS $$
    WITH RECURSIVE affected(object_id) AS (
        SELECT p.object_id FROM derived_publications p WHERE p.state<>'deleted'
            AND p.evidence_datasets ?| dataset_ids
        UNION SELECT i.object_id FROM derived_artifact_inputs i
            JOIN derived_publications p USING(object_id)
            WHERE i.dataset_id=ANY(dataset_ids) AND p.state<>'deleted'
        UNION SELECT edge.parent_id FROM derived_artifact_edges edge
            JOIN affected child ON child.object_id=edge.child_id
            JOIN derived_publications parent ON parent.object_id=edge.parent_id
            WHERE parent.state<>'deleted'
    ) SELECT object_id FROM affected
$$;

CREATE OR REPLACE FUNCTION analytical_artifact_ancestors(dataset_ids text[])
RETURNS TABLE(object_key text) LANGUAGE sql STABLE AS $$
    SELECT p.object_key FROM analytical_artifact_ancestor_ids(dataset_ids) affected
        JOIN derived_publications p USING(object_id)
$$;

CREATE OR REPLACE FUNCTION guard_derived_artifact_input()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_id=NEW.object_id AND state='staged') THEN
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
        WHERE object_id=NEW.parent_id AND state='staged') THEN
        RAISE EXCEPTION 'Committed artifact edges are immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM derived_publications
        WHERE object_id=NEW.child_id AND state='committed') THEN
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
        WHERE object_id=NEW.object_id AND state='committed') THEN
        RAISE EXCEPTION 'Evaluation artifact is unavailable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER evaluation_artifact_guard BEFORE INSERT ON evaluation_artifacts
    FOR EACH ROW EXECUTE FUNCTION guard_evaluation_artifact();

INSERT INTO schema_migrations(version) VALUES(26) ON CONFLICT DO NOTHING;
SET LOCAL statement_timeout='30s';
