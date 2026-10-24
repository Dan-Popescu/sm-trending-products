-- Disposable exact feature state. Source facts remain in dataset manifests/history.
CREATE SEQUENCE feature_source_generation;
CREATE TABLE feature_source_frontiers (
    entity_key text NOT NULL,
    synthetic boolean NOT NULL,
    generation bigint NOT NULL,
    max_available_at timestamptz,
    max_observed_at timestamptz,
    unknown_bounds boolean NOT NULL DEFAULT false,
    PRIMARY KEY(entity_key,synthetic)
);
CREATE TABLE feature_product_projections (
    product_id text PRIMARY KEY REFERENCES products(product_id) ON DELETE CASCADE,
    synthetic boolean NOT NULL,
    projection_version text NOT NULL,
    context_sha256 text NOT NULL,
    source_sha256 text NOT NULL,
    computed_at timestamptz NOT NULL,
    valid_through timestamptz,
    age_anchor timestamptz,
    payload jsonb NOT NULL,
    audit_root text REFERENCES derived_publications(object_key),
    input_audit_root text REFERENCES derived_publications(object_key),
    input_provenance jsonb NOT NULL DEFAULT '[]',
    observation_counts jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX feature_projection_audit_root ON feature_product_projections(audit_root)
    WHERE audit_root IS NOT NULL;
CREATE INDEX feature_projection_input_root ON feature_product_projections(input_audit_root)
    WHERE input_audit_root IS NOT NULL;

-- Updating only checkpoints or entity last-observed hints must not invalidate a
-- product. Publication, replay, compaction selection and retirement do. Include
-- OLD keys so replacing a manifest cannot leave a removed entity's state valid.
CREATE FUNCTION advance_feature_source_frontiers() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_query text;
BEGIN
    IF TG_OP='INSERT' THEN
        source_query := 'SELECT synthetic,manifest FROM new_datasets';
    ELSIF TG_OP='DELETE' THEN
        source_query := 'SELECT synthetic,manifest FROM old_datasets';
    ELSE
        source_query := 'SELECT n.synthetic,n.manifest FROM new_datasets n '
            || 'JOIN old_datasets o USING(dataset_id) WHERE '
            || '(n.state,n.manifest,n.compacted_into) IS DISTINCT FROM '
            || '(o.state,o.manifest,o.compacted_into) UNION ALL '
            || 'SELECT o.synthetic,o.manifest FROM new_datasets n '
            || 'JOIN old_datasets o USING(dataset_id) WHERE '
            || '(n.state,n.manifest,n.compacted_into) IS DISTINCT FROM '
            || '(o.state,o.manifest,o.compacted_into)';
    END IF;
    EXECUTE 'WITH changed AS (' || source_query || '), affected AS ('
        || 'SELECT c.synthetic,e.entity_key,obj FROM changed c '
        || 'CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.manifest->''normalized'',''[]'')) obj '
        || 'CROSS JOIN LATERAL jsonb_array_elements_text(obj->''entity_keys'') e(entity_key)), '
        || 'scoped AS (SELECT entity_key,synthetic, '
        || 'max((obj->>''max_available_at'')::timestamptz) AS available_at, '
        || 'max((obj->>''max_observed_at'')::timestamptz) AS observed_at, '
        || 'bool_or(obj->>''max_available_at'' IS NULL OR obj->>''max_observed_at'' IS NULL) AS unknown '
        || 'FROM affected GROUP BY entity_key,synthetic) '
        || 'INSERT INTO feature_source_frontiers(entity_key,synthetic,generation,max_available_at,'
        || 'max_observed_at,unknown_bounds) SELECT entity_key,synthetic, '
        || 'nextval(''feature_source_generation''),available_at,observed_at,unknown FROM scoped '
        || 'ORDER BY synthetic,entity_key ON CONFLICT(entity_key,synthetic) DO UPDATE SET '
        || 'generation=EXCLUDED.generation, '
        || 'max_available_at=GREATEST(feature_source_frontiers.max_available_at,EXCLUDED.max_available_at), '
        || 'max_observed_at=GREATEST(feature_source_frontiers.max_observed_at,EXCLUDED.max_observed_at), '
        || 'unknown_bounds=feature_source_frontiers.unknown_bounds OR EXCLUDED.unknown_bounds';
    RETURN NULL;
END $$;
CREATE TRIGGER feature_sources_insert AFTER INSERT ON datasets
    REFERENCING NEW TABLE AS new_datasets FOR EACH STATEMENT
    EXECUTE FUNCTION advance_feature_source_frontiers();
CREATE TRIGGER feature_sources_update AFTER UPDATE ON datasets
    REFERENCING NEW TABLE AS new_datasets OLD TABLE AS old_datasets FOR EACH STATEMENT
    EXECUTE FUNCTION advance_feature_source_frontiers();
CREATE TRIGGER feature_sources_delete AFTER DELETE ON datasets
    REFERENCING OLD TABLE AS old_datasets FOR EACH STATEMENT
    EXECUTE FUNCTION advance_feature_source_frontiers();

-- One metadata-only backfill; missing old time bounds conservatively disable
-- projection reuse rather than claiming later evidence was already visible.
INSERT INTO feature_source_frontiers(entity_key,synthetic,generation,max_available_at,
    max_observed_at,unknown_bounds)
SELECT e.entity_key,d.synthetic,nextval('feature_source_generation'),
    max(o.max_available_at),max(o.max_observed_at),
    bool_or(o.max_available_at IS NULL OR o.max_observed_at IS NULL)
FROM normalized_objects o JOIN datasets d USING(dataset_id)
CROSS JOIN LATERAL jsonb_array_elements_text(o.descriptor->'entity_keys') e(entity_key)
GROUP BY e.entity_key,d.synthetic ORDER BY d.synthetic,e.entity_key;

-- Local rolling evidence is registered before cache admission. A crash can leave
-- a missing local file, never an unowned raw-state file unknown to retirement.
ALTER TABLE retention_objects ADD COLUMN local_only boolean NOT NULL DEFAULT false;
CREATE TABLE feature_product_windows (
    product_id text PRIMARY KEY REFERENCES products(product_id) ON DELETE CASCADE,
    context_sha256 text NOT NULL,
    computed_at timestamptz NOT NULL,
    covered_object_ids bigint[] NOT NULL,
    covered_sha256 text NOT NULL,
    descriptor jsonb NOT NULL,
    audit_root text NOT NULL REFERENCES derived_publications(object_key),
    state text NOT NULL CHECK(state IN ('staged','ready'))
);
CREATE INDEX feature_window_audit_root ON feature_product_windows(audit_root);

CREATE FUNCTION retire_feature_window() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR OLD.descriptor IS DISTINCT FROM NEW.descriptor THEN
        INSERT INTO retention_objects(object_key,reason,local_only)
        VALUES(OLD.descriptor->>'key','feature_projection_superseded',true)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER feature_window_replaced AFTER DELETE OR UPDATE ON feature_product_windows
FOR EACH ROW EXECUTE FUNCTION retire_feature_window();

CREATE FUNCTION retire_feature_audit_windows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('invalidated','deleted') AND NEW.state IS DISTINCT FROM OLD.state THEN
        INSERT INTO retention_objects(object_key,reason,local_only)
        SELECT descriptor->>'key','feature_projection_source_retired',true
        FROM feature_product_windows WHERE audit_root=NEW.object_key
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER feature_audit_retired AFTER UPDATE OF state ON derived_publications
FOR EACH ROW EXECUTE FUNCTION retire_feature_audit_windows();

INSERT INTO schema_migrations(version) VALUES(21);
