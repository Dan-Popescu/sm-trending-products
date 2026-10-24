CREATE TABLE IF NOT EXISTS deployment_targets (
    target text PRIMARY KEY,
    active_manifest jsonb NOT NULL,
    active_proof jsonb NOT NULL,
    active_slot text NOT NULL CHECK(active_slot IN ('blue','green')),
    active_worker_id text NOT NULL,
    synthetic boolean NOT NULL,
    fence bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS deployment_runs (
    release_id text PRIMARY KEY,
    target text NOT NULL REFERENCES deployment_targets(target),
    candidate_manifest jsonb NOT NULL,
    candidate_proof jsonb NOT NULL,
    prior_manifest jsonb NOT NULL,
    prior_proof jsonb NOT NULL,
    prior_slot text NOT NULL CHECK(prior_slot IN ('blue','green')),
    prior_worker_id text NOT NULL,
    prior_generation bigint NOT NULL CHECK(prior_generation>0),
    candidate_worker_id text NOT NULL,
    candidate_generation bigint CHECK(candidate_generation>0),
    schema_version integer NOT NULL,
    synthetic boolean NOT NULL,
    rollback_of text REFERENCES deployment_runs(release_id),
    abort_requested boolean NOT NULL DEFAULT false,
    phase text NOT NULL DEFAULT 'planned' CHECK(phase IN
      ('planned','prepared','started','ready','draining','verified','committed','retired')),
    status text NOT NULL DEFAULT 'pending' CHECK(status IN
      ('pending','running','blocked','completed','aborted')),
    reason text,
    fence bigint NOT NULL DEFAULT 0,
    evidence jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS deployment_runs_target_time
ON deployment_runs(target,created_at DESC,release_id);
INSERT INTO schema_migrations(version) VALUES(32) ON CONFLICT DO NOTHING;
