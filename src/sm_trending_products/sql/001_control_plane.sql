CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS source_capabilities (
    id bigserial PRIMARY KEY, source text NOT NULL, account_id text NOT NULL,
    capability text NOT NULL, state text NOT NULL CHECK(state IN ('unverified','available','denied','degraded')),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(), details jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS capabilities_lookup ON source_capabilities(source,account_id,capability,available_at DESC,id DESC);

CREATE TABLE IF NOT EXISTS source_policies (
    source text PRIMARY KEY, retention_days integer NOT NULL CHECK(retention_days>0),
    verified boolean NOT NULL DEFAULT false, details jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS request_budgets (
    source text NOT NULL, account_id text NOT NULL, capacity double precision NOT NULL CHECK(capacity>=1),
    refill_per_second double precision NOT NULL CHECK(refill_per_second>0),
    tokens double precision NOT NULL CHECK(tokens>=0), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    blocked_until timestamptz, request_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(source,account_id)
);

CREATE OR REPLACE FUNCTION reserve_request(p_source text,p_account text)
RETURNS TABLE(allowed boolean,retry_after_seconds double precision) LANGUAGE plpgsql AS $$
DECLARE b request_budgets%ROWTYPE; t timestamptz; available double precision;
BEGIN
    SELECT * INTO b FROM request_budgets WHERE source=p_source AND account_id=p_account FOR UPDATE;
    IF NOT FOUND THEN RETURN QUERY SELECT false,60.0::double precision; RETURN; END IF;
    t := clock_timestamp();
    IF b.blocked_until IS NOT NULL AND b.blocked_until>t THEN
        RETURN QUERY SELECT false,EXTRACT(EPOCH FROM b.blocked_until-t)::double precision; RETURN;
    END IF;
    available := LEAST(b.capacity,b.tokens+GREATEST(0,EXTRACT(EPOCH FROM t-b.updated_at))*b.refill_per_second);
    IF available<1 THEN
        UPDATE request_budgets SET tokens=available,updated_at=t WHERE source=p_source AND account_id=p_account;
        RETURN QUERY SELECT false,((1-available)/b.refill_per_second)::double precision;
    ELSE
        UPDATE request_budgets SET tokens=available-1,updated_at=t,request_count=request_count+1
        WHERE source=p_source AND account_id=p_account;
        RETURN QUERY SELECT true,0.0::double precision;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS collection_tasks (
    task_id text PRIMARY KEY, source text NOT NULL, account_id text NOT NULL,
    operation text NOT NULL, payload jsonb NOT NULL, synthetic boolean NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','done','quarantined','cancelled')),
    due_at timestamptz NOT NULL DEFAULT clock_timestamp(), priority double precision NOT NULL DEFAULT 0,
    lease_owner text, lease_version bigint NOT NULL DEFAULT 0, lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0, last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS task_frontier ON collection_tasks(synthetic,state,due_at,priority DESC);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id text PRIMARY KEY, source text NOT NULL, synthetic boolean NOT NULL,
    state text NOT NULL CHECK(state IN ('staged','committed','invalidated','deleted')),
    transform_version text NOT NULL DEFAULT '1', manifest jsonb NOT NULL,
    task_id text REFERENCES collection_tasks(task_id), lease_version bigint,
    min_available_at timestamptz, max_available_at timestamptz,
    row_count bigint NOT NULL DEFAULT 0, quarantine_count bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), committed_at timestamptz,
    supersedes text REFERENCES datasets(dataset_id), invalid_reason text
);
CREATE INDEX IF NOT EXISTS dataset_scan ON datasets(synthetic,state,min_available_at,max_available_at);

CREATE TABLE IF NOT EXISTS entities (
    entity_key text NOT NULL, synthetic boolean NOT NULL, source text NOT NULL,
    account_id text NOT NULL DEFAULT '', entity_type text NOT NULL,
    title text NOT NULL DEFAULT '', attributes jsonb NOT NULL DEFAULT '{}',
    origin jsonb NOT NULL DEFAULT '{}', discovered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_observed_at timestamptz, next_due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    priority double precision NOT NULL DEFAULT 0, tier text NOT NULL DEFAULT 'exploration',
    schedule_reason jsonb NOT NULL DEFAULT '{}', PRIMARY KEY(entity_key,synthetic)
);
CREATE INDEX IF NOT EXISTS entity_frontier ON entities(synthetic,next_due_at,priority DESC);

CREATE TABLE IF NOT EXISTS products (
    product_id text PRIMARY KEY, name text NOT NULL, synthetic boolean NOT NULL,
    market text, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS product_links (
    id bigserial PRIMARY KEY, decision_id text NOT NULL UNIQUE,
    product_id text NOT NULL REFERENCES products(product_id), entity_key text NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    status text NOT NULL CHECK(status IN ('confirmed','proposed','rejected')),
    confidence double precision NOT NULL CHECK(confidence BETWEEN 0 AND 1),
    method text NOT NULL, evidence_refs jsonb NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS identity_history ON product_links(entity_key,available_at DESC,id DESC);

CREATE TABLE IF NOT EXISTS family_memberships (
    id bigserial PRIMARY KEY, product_id text NOT NULL REFERENCES products(product_id),
    family_id text NOT NULL, available_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS assessment_runs (
    run_id text PRIMARY KEY, as_of timestamptz NOT NULL, synthetic boolean NOT NULL,
    policy jsonb NOT NULL, object_key text NOT NULL, evidence_datasets jsonb NOT NULL,
    invalidated boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS current_assessments (
    product_id text PRIMARY KEY REFERENCES products(product_id), run_id text NOT NULL REFERENCES assessment_runs(run_id),
    synthetic boolean NOT NULL, score double precision, payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
    alert_id text PRIMARY KEY, product_id text NOT NULL REFERENCES products(product_id),
    run_id text NOT NULL REFERENCES assessment_runs(run_id), synthetic boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), payload jsonb NOT NULL,
    invalidated boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS evaluation_runs (
    evaluation_id text PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('backtest','forward','outcome')),
    synthetic boolean NOT NULL, decision_time timestamptz, payload jsonb NOT NULL,
    parent_id text REFERENCES evaluation_runs(evaluation_id), invalidated boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS evaluation_dependencies (
    evaluation_id text NOT NULL REFERENCES evaluation_runs(evaluation_id),
    dataset_id text NOT NULL REFERENCES datasets(dataset_id), PRIMARY KEY(evaluation_id,dataset_id)
);
CREATE TABLE IF NOT EXISTS scheduler_policies (
    policy_id text PRIMARY KEY, available_at timestamptz NOT NULL DEFAULT clock_timestamp(), payload jsonb NOT NULL
);
INSERT INTO schema_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
