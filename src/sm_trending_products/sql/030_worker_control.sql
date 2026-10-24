CREATE TABLE IF NOT EXISTS collection_workers (
    worker_id text PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation > 0),
    synthetic boolean NOT NULL,
    state text NOT NULL CHECK (state IN ('running','draining','drained','stopped')),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    drain_requested_at timestamptz,
    drain_acknowledged_at timestamptz,
    stopped_at timestamptz
);
CREATE INDEX IF NOT EXISTS collection_worker_leases
    ON collection_tasks(lease_owner,lease_until) WHERE state='leased';
