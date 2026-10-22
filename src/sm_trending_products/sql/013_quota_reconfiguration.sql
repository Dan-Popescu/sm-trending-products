-- Shared limits follow the current settings of bound accounts, not historical
-- minima. Settle tokens at the old refill rate before applying a new rate.
-- Callers lock their account first; credential identity stays immutable.
DROP FUNCTION IF EXISTS request_budget_ready(text,text);
CREATE OR REPLACE FUNCTION request_budget_ready(
    p_source text,p_account text,p_credential text DEFAULT NULL)
RETURNS boolean LANGUAGE sql AS $$
    SELECT NOT EXISTS (
        SELECT 1 FROM request_budgets b WHERE b.source=p_source AND b.account_id=p_account
        AND (b.blocked_until>clock_timestamp() OR LEAST(b.capacity,b.tokens+
            GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-b.updated_at))*b.refill_per_second)<1)
    ) AND NOT EXISTS (
        -- Only a current worker-supplied fingerprint identifies its credential.
        -- An absent/unknown hint must reach the final atomic reservation.
        SELECT 1 FROM credential_budgets g WHERE g.credential_key=p_credential
        AND (g.blocked_until>clock_timestamp() OR LEAST(g.capacity,g.tokens+
            GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-g.updated_at))*g.refill_per_second)<1)
    );
$$;

DROP FUNCTION IF EXISTS refresh_credential_budget(text);
CREATE OR REPLACE FUNCTION refresh_credential_budget(
    p_credential text,p_reconfigure boolean DEFAULT true)
RETURNS credential_budgets LANGUAGE plpgsql AS $$
DECLARE g credential_budgets%ROWTYPE; t timestamptz;
    current_capacity double precision; current_refill double precision;
BEGIN
    SELECT * INTO STRICT g FROM credential_budgets
        WHERE credential_key=p_credential FOR NO KEY UPDATE;
    IF p_reconfigure THEN
        SELECT min(b.capacity),min(b.refill_per_second)
            INTO current_capacity,current_refill
            FROM credential_bindings cb JOIN request_budgets b
            ON b.source=cb.source AND b.account_id=cb.account_id
            WHERE cb.credential_key=p_credential;
    END IF;
    t := clock_timestamp();
    g.tokens := LEAST(COALESCE(current_capacity,g.capacity),g.capacity,g.tokens+
        GREATEST(0,EXTRACT(EPOCH FROM t-g.updated_at))*g.refill_per_second);
    g.capacity := COALESCE(current_capacity,g.capacity);
    g.refill_per_second := COALESCE(current_refill,g.refill_per_second);
    g.updated_at := t;
    UPDATE credential_budgets SET capacity=g.capacity,refill_per_second=g.refill_per_second,
        tokens=g.tokens,updated_at=g.updated_at WHERE credential_key=p_credential;
    RETURN g;
END $$;

CREATE OR REPLACE FUNCTION configure_request_budget(
    p_source text,p_account text,p_capacity double precision,p_refill double precision)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE b request_budgets%ROWTYPE; binding record; t timestamptz;
BEGIN
    IF p_capacity IS NULL OR p_refill IS NULL OR NOT
        (p_capacity>=1 AND p_capacity<'Infinity'::double precision
         AND p_refill>0 AND p_refill<'Infinity'::double precision) THEN
        RAISE EXCEPTION 'Request budget must have finite positive capacity and refill';
    END IF;
    INSERT INTO request_budgets(source,account_id,capacity,refill_per_second,tokens)
        VALUES(p_source,p_account,p_capacity,p_refill,p_capacity) ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT b FROM request_budgets
        WHERE source=p_source AND account_id=p_account FOR UPDATE;
    t := clock_timestamp();
    UPDATE request_budgets SET capacity=p_capacity,refill_per_second=p_refill,
        tokens=LEAST(p_capacity,b.capacity,b.tokens+
            GREATEST(0,EXTRACT(EPOCH FROM t-b.updated_at))*b.refill_per_second),
        updated_at=t WHERE source=p_source AND account_id=p_account;
    FOR binding IN SELECT credential_key FROM credential_bindings
        WHERE source=p_source AND account_id=p_account ORDER BY credential_key LOOP
        PERFORM refresh_credential_budget(binding.credential_key);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION block_request_budget(
    p_source text,p_account text,p_seconds double precision)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE binding record; blocked timestamptz;
BEGIN
    blocked := clock_timestamp()+GREATEST(0,p_seconds)*interval '1 second';
    UPDATE request_budgets SET blocked_until=GREATEST(blocked_until,blocked)
        WHERE source=p_source AND account_id=p_account;
    -- Match configuration's account -> ordered credentials lock order.
    FOR binding IN SELECT credential_key FROM credential_bindings
        WHERE source=p_source AND account_id=p_account ORDER BY credential_key LOOP
        UPDATE credential_budgets SET blocked_until=GREATEST(blocked_until,blocked)
            WHERE credential_key=binding.credential_key;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION reserve_request(p_source text,p_account text,p_credential text)
RETURNS TABLE(allowed boolean,retry_after_seconds double precision) LANGUAGE plpgsql AS $$
DECLARE b request_budgets%ROWTYPE; g credential_budgets%ROWTYPE;
    t timestamptz; local_tokens double precision; wait_seconds double precision;
    new_binding integer;
BEGIN
    IF p_credential IS NULL THEN
        RETURN QUERY SELECT * FROM reserve_request(p_source,p_account); RETURN;
    END IF;
    SELECT * INTO b FROM request_budgets WHERE source=p_source AND account_id=p_account FOR UPDATE;
    IF NOT FOUND THEN RETURN QUERY SELECT false,60.0::double precision; RETURN; END IF;
    INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens)
        VALUES(p_credential,b.capacity,b.refill_per_second,b.capacity) ON CONFLICT DO NOTHING;
    INSERT INTO credential_bindings VALUES(p_source,p_account,p_credential) ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS new_binding = ROW_COUNT;
    -- Ordinary reservations stay O(1); only a new binding/configuration changes limits.
    g := refresh_credential_budget(p_credential,new_binding>0);
    t := clock_timestamp();
    local_tokens := LEAST(b.capacity,b.tokens+
        GREATEST(0,EXTRACT(EPOCH FROM t-b.updated_at))*b.refill_per_second);
    wait_seconds := GREATEST(0,(1-local_tokens)/b.refill_per_second,
        (1-g.tokens)/g.refill_per_second,COALESCE(EXTRACT(EPOCH FROM b.blocked_until-t),0),
        COALESCE(EXTRACT(EPOCH FROM g.blocked_until-t),0));
    IF wait_seconds>0 THEN
        UPDATE request_budgets SET tokens=local_tokens,updated_at=t
            WHERE source=p_source AND account_id=p_account;
        RETURN QUERY SELECT false,wait_seconds;
    ELSE
        UPDATE request_budgets SET tokens=local_tokens-1,updated_at=t,request_count=request_count+1
            WHERE source=p_source AND account_id=p_account;
        UPDATE credential_budgets SET tokens=g.tokens-1,request_count=request_count+1
            WHERE credential_key=p_credential;
        RETURN QUERY SELECT true,0.0::double precision;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS credential_bindings_by_credential ON credential_bindings(credential_key);
INSERT INTO schema_migrations(version) VALUES(13);
