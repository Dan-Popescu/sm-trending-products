-- Leases are operational work metadata. Dataset manifests own publication state.
CREATE TABLE IF NOT EXISTS capture_publication_leases (
    dataset_id text PRIMARY KEY REFERENCES datasets(dataset_id),
    worker_id text NOT NULL,
    fence bigint NOT NULL DEFAULT 1,
    lease_until timestamptz NOT NULL,
    retry_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 1,
    last_error text
);
CREATE INDEX IF NOT EXISTS raw_capture_publication_frontier
    ON datasets(synthetic,created_at,dataset_id)
    WHERE state='committed' AND manifest->>'kind'='raw_capture';

INSERT INTO schema_migrations(version) VALUES(31) ON CONFLICT DO NOTHING;
