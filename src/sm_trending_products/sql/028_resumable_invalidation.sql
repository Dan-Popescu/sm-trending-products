-- Durable progress for removals too large for one bounded atomic transaction.
-- The namespace fence is deliberately fail-closed until every dependent is retired.
CREATE TABLE source_invalidations (
    invalidation_id text PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    synthetic boolean NOT NULL,
    reason text NOT NULL,
    phase text NOT NULL DEFAULT 'discover' CHECK(phase IN ('discover','apply','complete')),
    discovered_count bigint NOT NULL DEFAULT 0,
    applied_count bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);
CREATE UNIQUE INDEX active_invalidation_namespace ON source_invalidations(synthetic)
    WHERE phase<>'complete';
CREATE TABLE source_invalidation_work (
    invalidation_id text NOT NULL REFERENCES source_invalidations(invalidation_id),
    kind text NOT NULL CHECK(kind IN ('dataset','artifact','assessment','evaluation','alert','current','entity','object','restore','shared_object')),
    target_key text COLLATE "C" NOT NULL,
    discovered boolean NOT NULL DEFAULT false,
    applied boolean NOT NULL DEFAULT false,
    after_kind text COLLATE "C" NOT NULL DEFAULT '',
    after_key text COLLATE "C" NOT NULL DEFAULT '',
    PRIMARY KEY(invalidation_id,kind,target_key)
);
CREATE INDEX invalidation_discovery_frontier ON source_invalidation_work(invalidation_id,kind,target_key)
    WHERE NOT discovered;
CREATE INDEX invalidation_apply_frontier ON source_invalidation_work(invalidation_id,kind,target_key)
    WHERE NOT applied;
CREATE INDEX shared_object_invalidation_cursor ON dataset_object_refs(object_key,dataset_id);
CREATE INDEX invalidation_alert_cursor ON alerts(run_id,alert_id);
CREATE INDEX compacted_parent_invalidation_cursor ON datasets(compacted_into,dataset_id)
    WHERE state='committed';
CREATE INDEX current_assessment_invalidation_cursor ON current_assessments(run_id,product_id);
CREATE INDEX assessment_object_lookup ON assessment_runs(object_key);
CREATE INDEX evaluation_parent_lookup ON evaluation_runs(parent_id,evaluation_id);
CREATE INDEX evaluation_dataset_cursor ON evaluation_dependencies(dataset_id,evaluation_id);
CREATE INDEX entity_origin_dataset_lookup ON entities((origin->>'dataset_id'),synthetic,entity_key);

-- Count bounded immediate work, not a complete shared-object Cartesian closure.
CREATE FUNCTION source_invalidation_wide(root_ids text[]) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS(
        SELECT 1 FROM unnest(root_ids) root_id WHERE (
            SELECT count(*) FROM (
                SELECT child_dataset_id AS target FROM dataset_lineage WHERE parent_dataset_id=root_id
                UNION ALL SELECT shared.dataset_id FROM dataset_object_refs origin
                    JOIN dataset_object_refs shared USING(object_key)
                    WHERE origin.dataset_id=root_id AND shared.dataset_id<>root_id
                UNION ALL SELECT object_id::text FROM derived_artifact_inputs WHERE dataset_id=root_id
                LIMIT 1001
            ) immediate_work
        )>1000
    )
$$;

CREATE FUNCTION evidence_namespace(object_key text) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN split_part(object_key,'/',2)='synthetic' THEN true
                WHEN split_part(object_key,'/',2)='live' THEN false ELSE NULL END
$$;

CREATE FUNCTION assert_evidence_namespace_available(namespace boolean)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE owner text;
BEGIN
    PERFORM pg_advisory_xact_lock_shared(492371205);
    owner := current_setting('pipeline.invalidation_owner',true);
    IF owner IS NOT NULL AND owner<>'' AND EXISTS (
        SELECT 1 FROM source_invalidations WHERE invalidation_id=owner
        AND phase<>'complete' AND (namespace IS NULL OR synthetic=namespace)
    ) THEN RETURN; END IF;
    IF EXISTS(SELECT 1 FROM source_invalidations
        WHERE phase<>'complete' AND (namespace IS NULL OR synthetic=namespace)) THEN
        RAISE EXCEPTION 'Evidence invalidation pending; retry after bounded maintenance progresses'
            USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION assert_same_evidence_namespace(owner boolean, dependency boolean)
RETURNS void LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF owner IS NOT NULL AND dependency IS NOT NULL AND owner<>dependency THEN
        RAISE EXCEPTION 'Evidence dependencies cannot cross live and synthetic namespaces'
            USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION guard_invalidation_namespace_write() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE value jsonb; namespace boolean;
BEGIN
    value:=to_jsonb(NEW);
    IF TG_TABLE_NAME='feature_product_windows' THEN
        namespace:=evidence_namespace(NEW.audit_root);
    ELSIF TG_TABLE_NAME='derived_publications' THEN
        IF NEW.state NOT IN ('staged','committed') THEN RETURN NEW; END IF;
        namespace:=evidence_namespace(NEW.object_key);
    ELSE
        IF TG_TABLE_NAME='datasets' AND value->>'state' NOT IN ('staged','committed') THEN
            RETURN NEW;
        END IF;
        IF value->>'invalidated'='true' THEN RETURN NEW; END IF;
        namespace:=(value->>'synthetic')::boolean;
    END IF;
    PERFORM assert_evidence_namespace_available(namespace);
    IF TG_TABLE_NAME='datasets' THEN
        PERFORM assert_same_evidence_namespace(NEW.synthetic,d.synthetic) FROM datasets d
        WHERE d.dataset_id=NEW.supersedes OR d.dataset_id IN (
            SELECT jsonb_array_elements_text(COALESCE(NEW.manifest->'parent_dataset_ids','[]'))
        );
        PERFORM assert_same_evidence_namespace(NEW.synthetic,evidence_namespace(k.object_key))
            FROM manifest_object_keys(NEW.manifest) k;
    END IF;
    IF EXISTS(SELECT 1 FROM source_invalidations WHERE phase<>'complete') THEN
        IF TG_TABLE_NAME='datasets' THEN
            PERFORM assert_evidence_namespace_available(d.synthetic) FROM datasets d
            WHERE d.dataset_id=NEW.supersedes OR d.dataset_id IN (
                SELECT jsonb_array_elements_text(COALESCE(NEW.manifest->'parent_dataset_ids','[]'))
            );
            -- Unknown legacy paths cannot establish an independent workspace.
            PERFORM assert_evidence_namespace_available(evidence_namespace(k.object_key))
                FROM manifest_object_keys(NEW.manifest) k;
        ELSIF TG_TABLE_NAME IN ('derived_publications','assessment_runs') THEN
            PERFORM assert_evidence_namespace_available(d.synthetic) FROM datasets d
            WHERE value->'evidence_datasets' ? d.dataset_id;
        ELSIF TG_TABLE_NAME='evaluation_runs' THEN
            PERFORM assert_evidence_namespace_available(e.synthetic) FROM evaluation_runs e
            WHERE e.evaluation_id=NEW.parent_id;
        ELSIF TG_TABLE_NAME IN ('current_assessments','alerts') THEN
            PERFORM assert_evidence_namespace_available(a.synthetic) FROM assessment_runs a
            WHERE a.run_id=NEW.run_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER feature_projection_invalidation_gate BEFORE INSERT OR UPDATE ON feature_product_projections
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER feature_window_invalidation_gate BEFORE INSERT OR UPDATE ON feature_product_windows
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER current_assessment_invalidation_gate BEFORE INSERT OR UPDATE ON current_assessments
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER alert_invalidation_gate BEFORE INSERT OR UPDATE ON alerts
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER dataset_invalidation_gate BEFORE INSERT OR UPDATE ON datasets
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER artifact_invalidation_gate BEFORE INSERT OR UPDATE ON derived_publications
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER assessment_invalidation_gate BEFORE INSERT OR UPDATE ON assessment_runs
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();
CREATE TRIGGER evaluation_invalidation_gate BEFORE INSERT OR UPDATE ON evaluation_runs
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_namespace_write();

CREATE FUNCTION guard_invalidation_association() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE dependency boolean; owner boolean;
BEGIN
    IF TG_TABLE_NAME='derived_artifact_inputs' THEN
        SELECT synthetic INTO dependency FROM datasets WHERE dataset_id=NEW.dataset_id;
        SELECT evidence_namespace(object_key) INTO owner FROM derived_publications
            WHERE object_id=NEW.object_id;
    ELSIF TG_TABLE_NAME='evaluation_dependencies' THEN
        SELECT synthetic INTO dependency FROM datasets WHERE dataset_id=NEW.dataset_id;
        SELECT synthetic INTO owner FROM evaluation_runs WHERE evaluation_id=NEW.evaluation_id;
    ELSIF TG_TABLE_NAME='derived_artifact_edges' THEN
        SELECT evidence_namespace(object_key) INTO dependency FROM derived_publications
            WHERE object_id=NEW.child_id;
        SELECT evidence_namespace(object_key) INTO owner FROM derived_publications
            WHERE object_id=NEW.parent_id;
    ELSE
        SELECT evidence_namespace(object_key) INTO dependency FROM derived_publications
            WHERE object_id=NEW.object_id;
        SELECT synthetic INTO owner FROM evaluation_runs WHERE evaluation_id=NEW.evaluation_id;
    END IF;
    PERFORM assert_same_evidence_namespace(owner,dependency);
    PERFORM assert_evidence_namespace_available(owner);
    PERFORM assert_evidence_namespace_available(dependency);
    RETURN NEW;
END $$;
CREATE TRIGGER artifact_input_invalidation_gate BEFORE INSERT OR UPDATE ON derived_artifact_inputs
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_association();
CREATE TRIGGER artifact_edge_invalidation_gate BEFORE INSERT OR UPDATE ON derived_artifact_edges
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_association();
CREATE TRIGGER evaluation_dependency_invalidation_gate BEFORE INSERT OR UPDATE ON evaluation_dependencies
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_association();
CREATE TRIGGER evaluation_artifact_invalidation_gate BEFORE INSERT OR UPDATE ON evaluation_artifacts
FOR EACH ROW EXECUTE FUNCTION guard_invalidation_association();

-- Execute the private keyset frontier loop next to its durable state. Adjacency
-- statements are supplied by the fixed application module, with bound values;
-- this avoids a network round trip for every empty branch of a long lineage.
CREATE FUNCTION advance_source_invalidation_frontier(
    job_id text, adjacency jsonb, work_budget integer, seconds double precision
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    job source_invalidations%ROWTYPE;
    target record;
    edge record;
    rules jsonb;
    rule jsonb;
    branch integer;
    target_kind text;
    edge_sql text;
    last_key text;
    remaining integer;
    admitted integer;
    used integer:=0;
    finished integer:=0;
    more boolean;
    complete boolean;
    deadline timestamptz;
BEGIN
    IF work_budget<1 OR work_budget>1000 OR seconds<=0 OR seconds>120 THEN
        RAISE EXCEPTION 'Invalid source continuation budget';
    END IF;
    IF current_setting('pipeline.invalidation_owner',true) IS DISTINCT FROM job_id THEN
        RAISE EXCEPTION 'Source continuation requires its scoped processor transaction';
    END IF;
    SELECT * INTO job FROM source_invalidations WHERE invalidation_id=job_id FOR UPDATE;
    IF NOT FOUND OR job.phase<>'discover' THEN RETURN 0; END IF;
    deadline:=clock_timestamp()+make_interval(secs=>seconds);
    WHILE used<work_budget AND clock_timestamp()<deadline LOOP
        SELECT * INTO target FROM source_invalidation_work
        WHERE invalidation_id=job_id AND NOT discovered
        ORDER BY kind,target_key LIMIT 1 FOR UPDATE;
        IF NOT FOUND THEN
            UPDATE source_invalidations SET phase='apply',updated_at=clock_timestamp()
            WHERE invalidation_id=job_id;
            EXIT;
        END IF;
        rules:=COALESCE(adjacency->target.kind,'[]');
        branch:=COALESCE(NULLIF(target.after_kind,''),'0')::integer;
        remaining:=work_budget-used;
        last_key:=target.after_key;
        admitted:=0;
        more:=false;
        IF branch<jsonb_array_length(rules) THEN
            rule:=rules->branch;
            target_kind:=rule->>0;
            edge_sql:=rule->>1;
            FOR edge IN EXECUTE 'SELECT target_key::text AS target_key FROM ('||
                edge_sql||' LIMIT '||(remaining+1)::text||') bounded'
                USING target.target_key,last_key,job.synthetic,job.reason='retention_expired'
            LOOP
                IF admitted>=remaining THEN more:=true; EXIT; END IF;
                INSERT INTO source_invalidation_work(invalidation_id,kind,target_key)
                VALUES(job_id,target_kind,edge.target_key::text) ON CONFLICT DO NOTHING;
                last_key:=edge.target_key::text;
                admitted:=admitted+1;
            END LOOP;
            IF NOT more THEN branch:=branch+1; last_key:=''; END IF;
        END IF;
        complete:=branch>=jsonb_array_length(rules);
        UPDATE source_invalidation_work
        SET discovered=complete,after_kind=branch::text,after_key=last_key
        WHERE invalidation_id=job_id AND kind=target.kind AND target_key=target.target_key;
        used:=used+GREATEST(1,admitted);
        IF complete THEN finished:=finished+1; END IF;
    END LOOP;
    UPDATE source_invalidations
    SET discovered_count=discovered_count+finished,updated_at=clock_timestamp()
    WHERE invalidation_id=job_id;
    RETURN used;
END $$;

INSERT INTO schema_migrations(version) VALUES(28) ON CONFLICT DO NOTHING;
