-- Deploy compatible packed/shared archive readers to every role before this
-- boundary. Older schemas keep ordinary shared archive writes unchanged.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=39) THEN
        RAISE EXCEPTION 'Adaptive assessment archive requires SQL schema 039';
    END IF;
END $$;
INSERT INTO schema_migrations(version) VALUES(40) ON CONFLICT DO NOTHING;
