-- Index descriptors rather than reloading every JSON manifest for each product batch.
CREATE TABLE IF NOT EXISTS normalized_objects (
    dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    object_key text NOT NULL,
    descriptor jsonb NOT NULL,
    observations_indexed boolean NOT NULL DEFAULT false,
    min_observed_at timestamptz,
    max_observed_at timestamptz,
    min_available_at timestamptz,
    max_available_at timestamptz,
    PRIMARY KEY(dataset_id,object_key)
);
CREATE TABLE IF NOT EXISTS normalized_object_entities (
    entity_key text NOT NULL,
    dataset_id text NOT NULL,
    object_key text NOT NULL,
    PRIMARY KEY(entity_key,dataset_id,object_key),
    FOREIGN KEY(dataset_id,object_key) REFERENCES normalized_objects ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS normalized_entity_dataset ON normalized_object_entities(dataset_id,object_key);
CREATE INDEX IF NOT EXISTS normalized_object_times ON normalized_objects(min_observed_at,max_observed_at);

CREATE TABLE IF NOT EXISTS observation_deliveries (
    dataset_id text NOT NULL,
    object_key text NOT NULL,
    observation_id text NOT NULL,
    entity_key text NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    payload_sha256 text NOT NULL,
    PRIMARY KEY(dataset_id,object_key,observation_id,payload_sha256),
    FOREIGN KEY(dataset_id,object_key) REFERENCES normalized_objects ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS observation_page_lookup ON observation_deliveries
    (entity_key,observed_at,observation_id,available_at);
CREATE INDEX IF NOT EXISTS observation_conflict_lookup ON observation_deliveries
    (observation_id,entity_key,available_at);

CREATE OR REPLACE FUNCTION index_normalized_objects()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM normalized_objects WHERE dataset_id=NEW.dataset_id;
    INSERT INTO normalized_objects(dataset_id,object_key,descriptor,
        min_observed_at,max_observed_at,min_available_at,max_available_at)
        SELECT NEW.dataset_id,obj->>'key',obj,
            (obj->>'min_observed_at')::timestamptz,(obj->>'max_observed_at')::timestamptz,
            (obj->>'min_available_at')::timestamptz,(obj->>'max_available_at')::timestamptz
        FROM jsonb_array_elements(COALESCE(NEW.manifest->'normalized','[]')) obj;
    INSERT INTO normalized_object_entities(entity_key,dataset_id,object_key)
        SELECT DISTINCT entity.value,o.dataset_id,o.object_key FROM normalized_objects o
        CROSS JOIN LATERAL jsonb_array_elements_text(o.descriptor->'entity_keys') entity
        WHERE o.dataset_id=NEW.dataset_id;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS normalized_manifest_index ON datasets;
CREATE TRIGGER normalized_manifest_index AFTER INSERT OR UPDATE OF manifest ON datasets
FOR EACH ROW EXECUTE FUNCTION index_normalized_objects();

INSERT INTO normalized_objects(dataset_id,object_key,descriptor,
    min_observed_at,max_observed_at,min_available_at,max_available_at)
    SELECT d.dataset_id,obj->>'key',obj,
        (obj->>'min_observed_at')::timestamptz,(obj->>'max_observed_at')::timestamptz,
        (obj->>'min_available_at')::timestamptz,(obj->>'max_available_at')::timestamptz
    FROM datasets d CROSS JOIN LATERAL
        jsonb_array_elements(COALESCE(d.manifest->'normalized','[]')) obj;
INSERT INTO normalized_object_entities(entity_key,dataset_id,object_key)
    SELECT DISTINCT entity.value,o.dataset_id,o.object_key FROM normalized_objects o
    CROSS JOIN LATERAL jsonb_array_elements_text(o.descriptor->'entity_keys') entity;

CREATE OR REPLACE FUNCTION retire_normalized_index()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('invalidated','deleted') THEN
        DELETE FROM normalized_objects WHERE dataset_id=NEW.dataset_id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS normalized_index_retirement ON datasets;
CREATE TRIGGER normalized_index_retirement AFTER UPDATE OF state ON datasets
FOR EACH ROW EXECUTE FUNCTION retire_normalized_index();

CREATE INDEX IF NOT EXISTS small_dataset_compaction ON datasets
    (synthetic,source,transform_version,row_count,created_at,dataset_id)
    WHERE state='committed' AND compacted_into IS NULL AND quarantine_count=0 AND row_count>0;
INSERT INTO schema_migrations(version) VALUES(14) ON CONFLICT DO NOTHING;
