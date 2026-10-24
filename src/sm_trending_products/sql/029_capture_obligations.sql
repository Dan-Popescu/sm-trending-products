-- Capture obligations extend task identity; they are not a second dispatch queue.
CREATE TABLE capture_policies (
    contract_id text PRIMARY KEY,
    source text NOT NULL,
    account_id text NOT NULL,
    operation text NOT NULL,
    scope_id text NOT NULL,
    synthetic boolean NOT NULL,
    version text NOT NULL,
    payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(source,account_id,operation,scope_id,synthetic,version)
);
CREATE INDEX capture_policy_scope ON capture_policies
    (synthetic,source,account_id,operation,scope_id,created_at DESC,contract_id);

CREATE TABLE capture_obligations (
    obligation_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES collection_tasks(task_id) ON DELETE CASCADE,
    page_index integer NOT NULL CHECK(page_index>=0),
    source text NOT NULL,
    account_id text NOT NULL,
    operation text NOT NULL,
    scope_id text NOT NULL,
    request_payload jsonb NOT NULL CHECK(jsonb_typeof(request_payload)='object'),
    synthetic boolean NOT NULL,
    contract_id text REFERENCES capture_policies(contract_id),
    original_due_at timestamptz NOT NULL,
    deadline_at timestamptz,
    due_origin text NOT NULL CHECK(due_origin IN ('task_admission','legacy_created_bound')),
    capture_dataset_id text REFERENCES datasets(dataset_id),
    captured_at timestamptz,
    durable_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(task_id,page_index)
);
CREATE INDEX capture_obligation_frontier ON capture_obligations
    (synthetic,original_due_at,obligation_id) WHERE capture_dataset_id IS NULL;
CREATE INDEX capture_obligation_scope ON capture_obligations
    (synthetic,source,account_id,operation,scope_id,contract_id);

CREATE FUNCTION capture_policy_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Capture policy versions are immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER immutable_capture_policy BEFORE UPDATE OR DELETE ON capture_policies
    FOR EACH ROW EXECUTE FUNCTION capture_policy_immutable();

CREATE FUNCTION freeze_capture_obligation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE p capture_policies%ROWTYPE; previous capture_obligations%ROWTYPE;
    scope text; due timestamptz; origin text := 'task_admission';
BEGIN
    IF TG_OP='UPDATE' AND NEW.page_index=OLD.page_index THEN RETURN NEW; END IF;
    SELECT * INTO previous FROM capture_obligations
        WHERE task_id=NEW.task_id ORDER BY page_index LIMIT 1;
    IF FOUND THEN
        scope := previous.scope_id;
        due := previous.original_due_at;
        origin := previous.due_origin;
        SELECT * INTO p FROM capture_policies WHERE contract_id=previous.contract_id;
    ELSE
        scope := COALESCE(NULLIF(NEW.schedule_reason->>'capture_scope',''),
            NULLIF(NEW.schedule_reason->>'request_signature',''),'*');
        due := COALESCE((NEW.schedule_reason->>'original_due_at')::timestamptz,NEW.due_at);
        SELECT * INTO p FROM capture_policies
            WHERE source=NEW.source AND account_id=NEW.account_id AND operation=NEW.operation
            AND synthetic=NEW.synthetic AND scope_id IN (scope,'*')
            AND created_at<=due
            ORDER BY (scope_id=scope) DESC,created_at DESC,contract_id DESC LIMIT 1;
    END IF;
    INSERT INTO capture_obligations(obligation_id,task_id,page_index,source,account_id,
        operation,scope_id,request_payload,synthetic,contract_id,original_due_at,deadline_at,due_origin)
        VALUES(md5(NEW.task_id||':'||NEW.page_index::text),NEW.task_id,NEW.page_index,
            NEW.source,NEW.account_id,NEW.operation,scope,NEW.payload,NEW.synthetic,p.contract_id,due,
            CASE WHEN p.contract_id IS NOT NULL THEN
                due+(p.payload->>'lateness_seconds')::double precision*interval '1 second' END,
            origin) ON CONFLICT(task_id,page_index) DO NOTHING;
    RETURN NEW;
END $$;

-- Old retry due_at is not the original obligation time. Preserve an explicitly
-- unknown lower bound rather than inventing a historical sampling contract.
INSERT INTO capture_obligations(obligation_id,task_id,page_index,source,account_id,
    operation,scope_id,request_payload,synthetic,original_due_at,deadline_at,due_origin)
    SELECT md5(task_id||':'||page_index::text),task_id,page_index,source,account_id,operation,
        COALESCE(NULLIF(schedule_reason->>'capture_scope',''),
            NULLIF(schedule_reason->>'request_signature',''),'*'),payload,synthetic,
        created_at,NULL,'legacy_created_bound' FROM collection_tasks;
CREATE TRIGGER collection_capture_obligation AFTER INSERT OR UPDATE OF page_index
    ON collection_tasks FOR EACH ROW EXECUTE FUNCTION freeze_capture_obligation();

CREATE FUNCTION acknowledge_raw_capture(
    p_task text,p_page integer,p_dataset text,p_verify_only boolean DEFAULT false)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE o capture_obligations%ROWTYPE; d datasets%ROWTYPE; captured timestamptz;
BEGIN
    SELECT * INTO STRICT o FROM capture_obligations
        WHERE task_id=p_task AND page_index=p_page FOR UPDATE;
    SELECT * INTO d FROM datasets WHERE dataset_id=p_dataset;
    IF NOT FOUND OR d.state<>'committed' OR d.task_id IS DISTINCT FROM p_task
        OR d.source<>o.source OR d.synthetic<>o.synthetic
        OR d.manifest->>'kind' IS DISTINCT FROM 'raw_capture'
        OR d.manifest->>'acquisition_protocol' IS DISTINCT FROM '1'
        OR d.manifest#>>'{capture,task_id}' IS DISTINCT FROM p_task
        OR (d.manifest#>>'{capture,page_index}')::integer IS DISTINCT FROM p_page
        OR d.manifest#>>'{task_request,source}' IS DISTINCT FROM o.source
        OR d.manifest#>>'{task_request,account_id}' IS DISTINCT FROM o.account_id
        OR d.manifest#>>'{task_request,operation}' IS DISTINCT FROM o.operation
        OR (d.manifest#>'{task_request,synthetic}') IS DISTINCT FROM to_jsonb(o.synthetic)
        OR (d.manifest#>'{task_request,payload}') IS DISTINCT FROM
            (CASE WHEN o.synthetic THEN o.request_payload-'endpoint_override'-'fixture_observed_at'
                ELSE o.request_payload END)
        OR COALESCE(d.manifest#>>'{raw,key}','')=''
        OR COALESCE(d.manifest#>>'{raw,sha256}','') !~ '^[0-9a-f]{64}$'
        OR COALESCE((d.manifest#>>'{raw,bytes}')::bigint,0)<1
        OR d.committed_at IS NULL THEN
        RAISE EXCEPTION 'A committed matching raw capture is required' USING ERRCODE='23514';
    END IF;
    captured := (d.manifest#>>'{capture,captured_at}')::timestamptz;
    IF captured IS NULL OR captured<o.original_due_at OR captured>d.committed_at THEN
        RAISE EXCEPTION 'Raw capture time is outside its observable window' USING ERRCODE='23514';
    END IF;
    IF o.capture_dataset_id IS NOT NULL AND o.capture_dataset_id<>p_dataset THEN
        RAISE EXCEPTION 'Capture obligation already has a different receipt' USING ERRCODE='23514';
    END IF;
    IF p_verify_only THEN RETURN; END IF;
    UPDATE capture_obligations SET capture_dataset_id=p_dataset,captured_at=captured,
        durable_at=d.committed_at WHERE obligation_id=o.obligation_id;
END $$;
CREATE FUNCTION immutable_capture_obligation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE receipt datasets%ROWTYPE;
BEGIN
    IF ROW(NEW.obligation_id,NEW.task_id,NEW.page_index,NEW.source,NEW.account_id,
        NEW.operation,NEW.scope_id,NEW.request_payload,NEW.synthetic,NEW.contract_id,NEW.original_due_at,
        NEW.deadline_at,NEW.due_origin,NEW.created_at) IS DISTINCT FROM ROW(OLD.obligation_id,
        OLD.task_id,OLD.page_index,OLD.source,OLD.account_id,OLD.operation,OLD.scope_id,
        OLD.request_payload,OLD.synthetic,OLD.contract_id,OLD.original_due_at,OLD.deadline_at,OLD.due_origin,
        OLD.created_at) THEN
        RAISE EXCEPTION 'Original capture obligations are immutable' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.capture_dataset_id,NEW.captured_at,NEW.durable_at) IS DISTINCT FROM
        ROW(OLD.capture_dataset_id,OLD.captured_at,OLD.durable_at) THEN
        PERFORM acknowledge_raw_capture(NEW.task_id,NEW.page_index,NEW.capture_dataset_id,true);
        SELECT * INTO receipt FROM datasets WHERE dataset_id=NEW.capture_dataset_id;
        IF NEW.captured_at IS DISTINCT FROM
            (receipt.manifest#>>'{capture,captured_at}')::timestamptz
            OR NEW.durable_at IS DISTINCT FROM receipt.committed_at THEN
            RAISE EXCEPTION 'Capture receipt times must match committed evidence'
                USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER immutable_capture_window BEFORE UPDATE ON capture_obligations
    FOR EACH ROW EXECUTE FUNCTION immutable_capture_obligation();
CREATE FUNCTION capture_dataset_committed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state='committed' AND NEW.task_id IS NOT NULL
        AND NEW.manifest->>'kind'='raw_capture' THEN
        PERFORM acknowledge_raw_capture(NEW.task_id,
            (NEW.manifest#>>'{capture,page_index}')::integer,NEW.dataset_id);
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER acknowledge_capture_dataset AFTER INSERT OR UPDATE OF state ON datasets
    FOR EACH ROW EXECUTE FUNCTION capture_dataset_committed();

INSERT INTO schema_migrations(version) VALUES(29) ON CONFLICT DO NOTHING;
