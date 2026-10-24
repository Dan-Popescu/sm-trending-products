-- Preserve public identifiers while removing repeated object paths from hot indexes.
-- This atomic SQL-only rebuild needs temporary disk headroom and a maintenance window
-- on an already large catalog. No remote object or observation payload is rewritten.
SET LOCAL statement_timeout='10min';

ALTER TABLE normalized_objects ADD COLUMN object_id bigserial;
ALTER TABLE normalized_objects ADD CONSTRAINT normalized_object_id UNIQUE(object_id);
ALTER TABLE normalized_objects ADD COLUMN periods_indexed boolean NOT NULL DEFAULT false;
ALTER TABLE normalized_objects ADD COLUMN max_period_start timestamptz;
ALTER TABLE normalized_objects ADD COLUMN min_period_end timestamptz;

CREATE TABLE normalized_object_entities_compact (
    entity_key text NOT NULL,
    object_id bigint NOT NULL REFERENCES normalized_objects(object_id) ON DELETE CASCADE,
    PRIMARY KEY(entity_key,object_id)
);
INSERT INTO normalized_object_entities_compact(entity_key,object_id)
    SELECT e.entity_key,o.object_id FROM normalized_object_entities e
    JOIN normalized_objects o USING(dataset_id,object_key);
DROP TABLE normalized_object_entities;
ALTER TABLE normalized_object_entities_compact RENAME TO normalized_object_entities;
CREATE INDEX normalized_entity_object ON normalized_object_entities(object_id);

CREATE TABLE observation_deliveries_compact (
    object_id bigint NOT NULL REFERENCES normalized_objects(object_id) ON DELETE CASCADE,
    observation_id text NOT NULL,
    entity_key text NOT NULL,
    observed_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    payload_sha256 bytea NOT NULL CHECK(octet_length(payload_sha256)=32),
    PRIMARY KEY(object_id,observation_id,payload_sha256)
);
INSERT INTO observation_deliveries_compact
    SELECT n.object_id,o.observation_id,o.entity_key,o.observed_at,o.available_at,
        decode(o.payload_sha256,'hex') FROM observation_deliveries o
    JOIN normalized_objects n USING(dataset_id,object_key);
DROP TABLE observation_deliveries;
ALTER TABLE observation_deliveries_compact RENAME TO observation_deliveries;
CREATE INDEX observation_page_lookup ON observation_deliveries
    (entity_key,observed_at,observation_id COLLATE "C",available_at);
CREATE INDEX observation_conflict_lookup ON observation_deliveries
    (observation_id COLLATE "C",entity_key,available_at);

CREATE OR REPLACE FUNCTION index_normalized_objects()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND NEW.manifest IS NOT DISTINCT FROM OLD.manifest THEN
        RETURN NEW;
    END IF;
    DELETE FROM normalized_objects WHERE dataset_id=NEW.dataset_id;
    IF NEW.state NOT IN ('staged','committed') THEN
        RETURN NEW;
    END IF;
    INSERT INTO normalized_objects(dataset_id,object_key,descriptor,
        min_observed_at,max_observed_at,min_available_at,max_available_at,
        periods_indexed,max_period_start,min_period_end)
        SELECT NEW.dataset_id,obj->>'key',obj,
            (obj->>'min_observed_at')::timestamptz,(obj->>'max_observed_at')::timestamptz,
            (obj->>'min_available_at')::timestamptz,(obj->>'max_available_at')::timestamptz,
            COALESCE((obj->>'periods_indexed')::boolean,false),
            (obj->>'max_period_start')::timestamptz,(obj->>'min_period_end')::timestamptz
        FROM jsonb_array_elements(COALESCE(NEW.manifest->'normalized','[]')) obj;
    INSERT INTO normalized_object_entities(entity_key,object_id)
        SELECT DISTINCT entity.value,o.object_id FROM normalized_objects o
        CROSS JOIN LATERAL jsonb_array_elements_text(o.descriptor->'entity_keys') entity
        WHERE o.dataset_id=NEW.dataset_id;
    RETURN NEW;
END $$;

INSERT INTO schema_migrations(version) VALUES(20) ON CONFLICT DO NOTHING;
SET LOCAL statement_timeout='30s';
