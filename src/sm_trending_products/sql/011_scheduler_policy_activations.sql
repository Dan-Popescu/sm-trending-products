-- Policy definitions retain first availability; activation is a separate history.
CREATE TABLE scheduler_policy_activations (
    activation_id bigserial PRIMARY KEY,
    policy_id text NOT NULL REFERENCES scheduler_policies(policy_id),
    synthetic boolean NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX scheduler_current_policy
    ON scheduler_policy_activations(synthetic,available_at DESC,activation_id DESC);

-- Preserve the selection used before this migration for each existing workspace.
INSERT INTO scheduler_policy_activations(policy_id,synthetic,available_at)
SELECT policy_id,synthetic,available_at FROM (
    SELECT DISTINCT ON ((payload->>'synthetic')::boolean)
        policy_id,(payload->>'synthetic')::boolean AS synthetic,available_at
    FROM scheduler_policies
    ORDER BY (payload->>'synthetic')::boolean,available_at DESC,policy_id DESC
) previous;
INSERT INTO schema_migrations(version) VALUES(11);
