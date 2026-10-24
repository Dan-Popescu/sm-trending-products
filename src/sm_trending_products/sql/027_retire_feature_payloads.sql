-- Feature payloads contain source-derived prices and histories, not just pointers.
-- Remove disposable SQL copies in the same transaction that retires their roots.
CREATE OR REPLACE FUNCTION retire_feature_audit_windows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('invalidated','deleted') AND NEW.state IS DISTINCT FROM OLD.state THEN
        INSERT INTO retention_objects(object_key,reason,local_only)
        SELECT descriptor->>'key','feature_projection_source_retired',true
        FROM feature_product_windows WHERE audit_root=NEW.object_key
        ON CONFLICT DO NOTHING;
        DELETE FROM feature_product_projections
        WHERE audit_root=NEW.object_key OR input_audit_root=NEW.object_key;
    END IF;
    RETURN NULL;
END $$;

-- Existing installations can already contain unavailable but unreadable payloads.
DELETE FROM feature_product_projections p
WHERE EXISTS(SELECT 1 FROM derived_publications d
    WHERE d.object_key IN (p.audit_root,p.input_audit_root)
    AND d.state IN ('invalidated','deleted'));
INSERT INTO schema_migrations(version) VALUES(27) ON CONFLICT DO NOTHING;
