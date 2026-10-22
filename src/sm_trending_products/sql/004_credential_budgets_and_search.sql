CREATE INDEX IF NOT EXISTS entity_title_search ON entities USING gin(to_tsvector('simple',title));

CREATE TABLE IF NOT EXISTS credential_budgets (
    credential_key text PRIMARY KEY, capacity double precision NOT NULL,
    refill_per_second double precision NOT NULL, tokens double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), blocked_until timestamptz,
    request_count bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS credential_bindings (
    source text NOT NULL,account_id text NOT NULL,credential_key text NOT NULL REFERENCES credential_budgets,
    PRIMARY KEY(source,account_id,credential_key)
);

CREATE OR REPLACE FUNCTION reserve_request(p_source text,p_account text,p_credential text)
RETURNS TABLE(allowed boolean,retry_after_seconds double precision) LANGUAGE plpgsql AS $$
DECLARE b request_budgets%ROWTYPE; g credential_budgets%ROWTYPE;
    t timestamptz; local_tokens double precision; global_tokens double precision; wait_seconds double precision;
BEGIN
    IF p_credential IS NULL THEN
        RETURN QUERY SELECT * FROM reserve_request(p_source,p_account); RETURN;
    END IF;
    SELECT * INTO b FROM request_budgets WHERE source=p_source AND account_id=p_account FOR UPDATE;
    IF NOT FOUND THEN RETURN QUERY SELECT false,60.0::double precision; RETURN; END IF;
    INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens)
        VALUES(p_credential,b.capacity,b.refill_per_second,b.capacity) ON CONFLICT DO NOTHING;
    INSERT INTO credential_bindings VALUES(p_source,p_account,p_credential) ON CONFLICT DO NOTHING;
    SELECT * INTO g FROM credential_budgets WHERE credential_key=p_credential FOR UPDATE;
    t := clock_timestamp();
    -- A shared token uses the strictest configured budget encountered across its aliases.
    g.capacity := LEAST(g.capacity,b.capacity);
    g.refill_per_second := LEAST(g.refill_per_second,b.refill_per_second);
    local_tokens := LEAST(b.capacity,b.tokens+GREATEST(0,EXTRACT(EPOCH FROM t-b.updated_at))*b.refill_per_second);
    global_tokens := LEAST(g.capacity,g.tokens+GREATEST(0,EXTRACT(EPOCH FROM t-g.updated_at))*g.refill_per_second);
    wait_seconds := GREATEST(0,(1-local_tokens)/b.refill_per_second,(1-global_tokens)/g.refill_per_second,
        COALESCE(EXTRACT(EPOCH FROM b.blocked_until-t),0),COALESCE(EXTRACT(EPOCH FROM g.blocked_until-t),0));
    IF wait_seconds>0 THEN
        UPDATE request_budgets SET tokens=local_tokens,updated_at=t WHERE source=p_source AND account_id=p_account;
        UPDATE credential_budgets SET capacity=g.capacity,refill_per_second=g.refill_per_second,
            tokens=global_tokens,updated_at=t WHERE credential_key=p_credential;
        RETURN QUERY SELECT false,wait_seconds;
    ELSE
        UPDATE request_budgets SET tokens=local_tokens-1,updated_at=t,request_count=request_count+1
            WHERE source=p_source AND account_id=p_account;
        UPDATE credential_budgets SET capacity=g.capacity,refill_per_second=g.refill_per_second,
            tokens=global_tokens-1,updated_at=t,request_count=request_count+1 WHERE credential_key=p_credential;
        RETURN QUERY SELECT true,0.0::double precision;
    END IF;
END $$;
INSERT INTO schema_migrations(version) VALUES(4) ON CONFLICT DO NOTHING;
