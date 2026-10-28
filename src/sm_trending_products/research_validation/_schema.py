"""Independent application migration; capture schema and hashes stay unchanged."""

MIGRATIONS = {
    1: """
    CREATE TABLE research_protocols (
        protocol_id text PRIMARY KEY, name text NOT NULL, revision integer NOT NULL,
        synthetic boolean NOT NULL, request_id text NOT NULL, request_hash text NOT NULL,
        payload jsonb NOT NULL, registered_at timestamptz NOT NULL,
        family_registry jsonb NOT NULL,
        UNIQUE(name,revision,synthetic), UNIQUE(synthetic,request_id)
    );
    CREATE TABLE research_policy_locks (
        protocol_id text PRIMARY KEY REFERENCES research_protocols(protocol_id),
        policy_id text NOT NULL, request_id text NOT NULL, request_hash text NOT NULL,
        locked_at timestamptz NOT NULL
    );
    CREATE TABLE research_attempts (
        attempt_id text PRIMARY KEY, protocol_id text NOT NULL
            REFERENCES research_protocols(protocol_id),
        request_id text NOT NULL, request_hash text NOT NULL,
        partition_name text NOT NULL, policy_id text NOT NULL,
        as_of timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        state text NOT NULL CHECK(state IN ('running','completed','failed')),
        evaluation_id text REFERENCES evaluation_runs(evaluation_id),
        error_code text, completed_at timestamptz,
        UNIQUE(protocol_id,request_id)
    );
    CREATE INDEX research_attempts_protocol ON research_attempts(protocol_id,created_at,attempt_id);
    """,
    2: """
    CREATE TABLE research_lineage_freezes (
        name text NOT NULL, synthetic boolean NOT NULL,
        protocol_id text NOT NULL REFERENCES research_protocols(protocol_id),
        policy_id text NOT NULL, opened_at timestamptz NOT NULL,
        PRIMARY KEY(name,synthetic)
    );
    INSERT INTO research_lineage_freezes(name,synthetic,protocol_id,policy_id,opened_at)
        SELECT DISTINCT ON(p.name,p.synthetic)
            p.name,p.synthetic,p.protocol_id,a.policy_id,a.created_at
        FROM research_protocols p JOIN research_attempts a USING(protocol_id)
        JOIN research_policy_locks l ON l.protocol_id=p.protocol_id
        WHERE a.partition_name IN ('test','holdout') AND a.policy_id=l.policy_id
            AND a.as_of>=l.locked_at
        ORDER BY p.name,p.synthetic,a.created_at,a.attempt_id;
    """,
}
