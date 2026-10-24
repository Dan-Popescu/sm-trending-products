"""Original capture windows and conservative pause admission through the catalog."""

import gzip
import hashlib
from datetime import timedelta

import psycopg
import pytest

from sm_trending_products.capture_contracts import CaptureContracts, CapturePolicy
from sm_trending_products.contracts import canonical, now

pytestmark = pytest.mark.integration


def enqueue(pipeline, key, due):
    return pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {"filters": {"video_ids": [key]}},
        True,
        due_at=due,
        idempotency_key=key,
    )


def historical(**changes):
    return CapturePolicy(
        source="tiktok_display",
        account_id="fixture-account",
        operation="query",
        version="history-v1",
        cadence_seconds=60,
        lateness_seconds=10,
        recoverability="historical",
        replay_horizon_seconds=3600,
        verification_ref="controlled-history-contract",
        verified_at=now(),
        **changes,
    )


def test_unknown_source_blocks_pause_and_retry_keeps_its_original_window(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    due = now() - timedelta(minutes=2)
    task = enqueue(pipeline, "unknown", due)
    original = contracts.declare(task)
    assert original["original_due_at"] == due
    assert original["contract_id"] is None
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp()+interval '1 day',"
        "last_error='quota_exhausted' WHERE task_id=%s",
        (task,),
    )
    repeated = contracts.declare(task)
    assert repeated["obligation_id"] == original["obligation_id"]
    assert repeated["original_due_at"] == due
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=60,
        rollback_seconds=30,
        safety_seconds=20,
        catchup_requests_per_second=1,
        request_budget=100,
    )
    assert result["allowed"] is False
    assert "unknown_recoverability" in result["reasons"]
    status = contracts.status(as_of=now(), synthetic=True)["obligations"][0]
    assert status["status"] == "unqualified" and status["late_seconds"] is None
    assert contracts.status(as_of=now(), synthetic=False)["obligations"] == []


def test_scheduler_obligation_uses_the_oldest_grouped_sampling_intention(pipeline):
    from sm_trending_products.scheduler import Scheduler

    clock = now()
    due_times = [clock - timedelta(hours=index + 1) for index in range(3)]
    for index, due in enumerate(due_times):
        pipeline.catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,next_due_at) "
            "VALUES(%s,true,'tiktok_display','fixture-account','video',%s)",
            (f"tiktok_display:video:due-{index}", due),
        )
    result = Scheduler(pipeline.catalog).schedule(1, synthetic=True)
    assert result["scheduled"] == 1 and result["refresh_entities_scheduled"] == 3
    obligation = CaptureContracts(pipeline.catalog).declare(result["task_ids"][0])
    assert obligation["original_due_at"] == min(due_times)


def test_verified_history_admits_only_a_drain_that_fits_horizon_and_quota(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    identity = contracts.register(historical(), synthetic=True)
    task = enqueue(pipeline, "history", now())
    assert contracts.declare(task)["contract_id"] == identity
    good = contracts.assess_pause(
        as_of=now() + timedelta(seconds=100),
        synthetic=True,
        pause_seconds=60,
        rollback_seconds=30,
        safety_seconds=20,
        catchup_requests_per_second=1,
        request_budget=100,
    )
    assert good["allowed"] is True
    assert good["required_requests"] > 1  # Admission includes work arriving during the pause.
    assert good["catchup_seconds"] > 0
    insufficient = contracts.assess_pause(
        as_of=now() + timedelta(seconds=100),
        synthetic=True,
        pause_seconds=60,
        rollback_seconds=30,
        safety_seconds=20,
        catchup_requests_per_second=1,
        request_budget=1,
    )
    assert insufficient["allowed"] is False
    assert "insufficient_request_budget" in insufficient["reasons"]
    expired = contracts.assess_pause(
        as_of=now() + timedelta(hours=1),
        synthetic=True,
        pause_seconds=60,
        rollback_seconds=30,
        safety_seconds=20,
        catchup_requests_per_second=1,
        request_budget=100,
    )
    assert expired["allowed"] is False
    assert "replay_window_exhausted" in expired["reasons"]


def test_policy_versions_are_immutable_and_cannot_relabel_existing_obligations(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    old_task = enqueue(pipeline, "before-policy", now())
    policy = historical()
    contract_id = contracts.register(policy, synthetic=True)
    assert contracts.register(policy, synthetic=True) == contract_id
    assert contracts.declare(old_task)["contract_id"] is None
    with pytest.raises(ValueError, match="immutable"):
        contracts.register(policy.model_copy(update={"cadence_seconds": 120}), synthetic=True)
    newer = contracts.register(policy.model_copy(update={"version": "history-v2"}), synthetic=True)
    task = enqueue(pipeline, "after-policy", now())
    before = contracts.declare(task)
    assert before["contract_id"] == newer
    pipeline.catalog.execute("UPDATE collection_tasks SET page_index=1 WHERE task_id=%s", (task,))
    continuation = contracts.declare(task)
    assert continuation["page_index"] == 1
    assert continuation["original_due_at"] == before["original_due_at"]
    assert continuation["contract_id"] == before["contract_id"]


def publish_raw(pipeline, task, identifier, *, kind="raw_capture", captured_at=None, request=None):
    """Publish a real raw/manifest pair through the existing catalog seam."""
    stamp = captured_at or now()
    body = gzip.compress(canonical({"response": {"data": {"id": "once-only"}}}).encode())
    raw_key, manifest_key = f"fixture/{identifier}.gz", f"fixture/{identifier}.json"
    manifest = {
        "schema_version": "1",
        "dataset_id": identifier,
        "source": task["source"],
        "synthetic": task["synthetic"],
        "kind": kind,
        "acquisition_protocol": "1",
        "transform_version": "1",
        "row_count": 0,
        "normalized": [],
        "manifest_key": manifest_key,
        "raw": {"key": raw_key, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)},
        "capture": {
            "task_id": task["task_id"],
            "page_index": task["page_index"],
            "captured_at": stamp.isoformat(),
        },
        "task_request": request
        or {
            "source": task["source"],
            "account_id": task["account_id"],
            "operation": task["operation"],
            "synthetic": task["synthetic"],
            "payload": task["payload"],
        },
    }
    pipeline.catalog.stage_dataset(
        identifier, task["source"], task["synthetic"], manifest, task=task
    )
    pipeline.history.put(raw_key, body)
    pipeline.history.put(manifest_key, canonical(manifest).encode())
    pipeline.catalog.commit_dataset(identifier, manifest, (), task=task)
    return identifier


def test_only_committed_matching_raw_evidence_settles_an_obligation(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical().model_copy(update={"lateness_seconds": 0}), synthetic=True)
    task_id = enqueue(pipeline, "raw-proof", now())
    task = pipeline.catalog.claim("capture-fixture", 180, True, 8)
    assert task["task_id"] == task_id
    before = now()
    dataset = publish_raw(pipeline, task, "durable-raw")
    settled = contracts.declare(task_id)
    assert settled["capture_dataset_id"] == dataset
    assert settled["captured_at"] >= before and settled["durable_at"] >= settled["captured_at"]
    assert contracts.acknowledge_capture(task_id, 0, dataset) == settled
    row = contracts.status(as_of=now(), synthetic=True)["obligations"][0]
    assert row["status"] == "captured_late" and row["late_seconds"] > 0
    past = contracts.status(as_of=before, synthetic=True)["obligations"][0]
    assert past["capture_dataset_id"] is None and past["captured_at"] is None
    with pytest.raises(ValueError, match="committed matching"):
        contracts.acknowledge_capture(task_id, 1, dataset)
    pipeline.catalog.execute(
        "UPDATE datasets SET state='invalidated' WHERE dataset_id=%s", (dataset,)
    )
    assert contracts.status(as_of=now(), synthetic=True)["obligations"][0]["status"] == "retired"
    retired = contracts.assess_pause(
        as_of=now() + timedelta(hours=2),
        synthetic=True,
        pause_seconds=1,
        rollback_seconds=1,
        safety_seconds=1,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert retired["allowed"] is False and "replay_window_exhausted" in retired["reasons"]
    with pytest.raises(ValueError, match="committed matching"):
        contracts.acknowledge_capture(task_id, 0, dataset)


def test_done_task_and_ordinary_dataset_are_not_a_capture_receipt(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    task_id = enqueue(pipeline, "ordinary-proof", now() - timedelta(seconds=1))
    task = pipeline.catalog.claim("capture-fixture", 180, True, 8)
    dataset = publish_raw(pipeline, task, "ordinary-dataset", kind="ordinary")
    assert (
        pipeline.catalog.one("SELECT state FROM collection_tasks WHERE task_id=%s", (task_id,))[
            "state"
        ]
        == "done"
    )
    with pytest.raises(ValueError, match="committed matching"):
        contracts.acknowledge_capture(task_id, 0, dataset)
    assert contracts.declare(task_id)["capture_dataset_id"] is None
    with pytest.raises(psycopg.errors.CheckViolation):
        pipeline.catalog.execute(
            "UPDATE capture_obligations SET capture_dataset_id=%s,captured_at=clock_timestamp(),"
            "durable_at=clock_timestamp() WHERE task_id=%s",
            (dataset, task_id),
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        pipeline.catalog.execute(
            "UPDATE capture_obligations SET original_due_at=clock_timestamp() WHERE task_id=%s",
            (task_id,),
        )


def test_pause_accounts_for_arrivals_cursor_expiry_and_authoritative_quota(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical(cursor_lifetime_seconds=60), synthetic=True)
    task_id = enqueue(pipeline, "cursor", now())
    result = contracts.assess_pause(
        as_of=now() + timedelta(seconds=30),
        synthetic=True,
        pause_seconds=30,
        rollback_seconds=10,
        safety_seconds=10,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert result["allowed"] is False and "replay_window_exhausted" in result["reasons"]
    pipeline.catalog.configure_budget("tiktok_display", "fixture-account", 1, 0.01)
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=1,
        rollback_seconds=1,
        safety_seconds=0,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert result["allowed"] is False and "catchup_capacity_insufficient" in result["reasons"]
    assert result["effective_requests_per_second"] == 0.01
    assert contracts.declare(task_id)["capture_dataset_id"] is None


def test_zero_blackout_requires_a_live_replacement_even_for_unknown_sources(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    task = enqueue(pipeline, "rolling", now())
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=0,
        rollback_seconds=0,
        safety_seconds=30,
        catchup_requests_per_second=0,
        request_budget=0,
    )
    assert result["allowed"] is True and result["requires_live_replacement"] is True
    assert result["mode"] == "continuous"
    assert contracts.declare(task)["capture_dataset_id"] is None


def test_exact_scopes_and_workspaces_do_not_inherit_other_recovery_claims(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    scoped = contracts.register(historical(scope_id="one-scope"), synthetic=True)
    from psycopg.types.json import Jsonb

    pipeline.catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
        "schedule_reason) VALUES('scoped','tiktok_display','fixture-account','query','{}',true,%s)",
        (Jsonb({"capture_scope": "one-scope"}),),
    )
    assert contracts.declare("scoped")["contract_id"] == scoped
    unscoped = enqueue(pipeline, "unscoped", now())
    assert contracts.declare(unscoped)["contract_id"] is None
    live = pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {},
        False,
        due_at=now(),
        idempotency_key="live",
    )
    assert contracts.declare(live)["contract_id"] is None


def test_older_pending_contract_still_constrains_a_newer_horizon(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    short = historical().model_copy(update={"replay_horizon_seconds": 5})
    contracts.register(short, synthetic=True)
    enqueue(pipeline, "short-window", now())
    contracts.register(
        short.model_copy(update={"version": "long-v2", "replay_horizon_seconds": 3600}),
        synthetic=True,
    )
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=10,
        rollback_seconds=1,
        safety_seconds=1,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert result["allowed"] is False and "replay_window_exhausted" in result["reasons"]


def test_policy_cannot_qualify_a_window_before_its_first_availability(pipeline):
    due = now() - timedelta(minutes=1)
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical(), synthetic=True)
    task = enqueue(pipeline, "already-due", due)
    obligation = contracts.declare(task)
    assert obligation["original_due_at"] == due
    assert obligation["contract_id"] is None and obligation["deadline_at"] is None


def test_raw_receipt_scope_is_frozen_per_page(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    task_id = enqueue(pipeline, "frozen-request", now())
    task = pipeline.catalog.claim("capture-fixture", 180, True, 8)
    wrong = {
        "source": task["source"],
        "account_id": task["account_id"],
        "operation": task["operation"],
        "synthetic": task["synthetic"],
        "payload": {"filters": {"video_ids": ["another-scope"]}},
    }
    with pytest.raises(psycopg.errors.CheckViolation):
        publish_raw(pipeline, task, "wrong-scope", request=wrong)
    assert contracts.declare(task_id)["capture_dataset_id"] is None
    dataset = publish_raw(pipeline, task, "right-scope")
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET page_index=1,payload='{"
        '"cursor":"next-page"}\' WHERE task_id=%s',
        (task_id,),
    )
    assert contracts.declare(task_id)["request_payload"] == {"cursor": "next-page"}
    old = contracts.acknowledge_capture(task_id, 0, dataset)
    assert old["request_payload"] == task["payload"]
    assert old["capture_dataset_id"] == dataset


def test_source_blocked_until_is_part_of_the_pause_horizon(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(
        historical().model_copy(update={"replay_horizon_seconds": 60}), synthetic=True
    )
    enqueue(pipeline, "blocked-budget", now())
    pipeline.catalog.block_budget("tiktok_display", "fixture-account", 120)
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=True,
        pause_seconds=1,
        rollback_seconds=1,
        safety_seconds=1,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert result["allowed"] is False and "replay_window_exhausted" in result["reasons"]
    assert result["quota_wait_seconds"] > 100


def test_a_recovery_contract_does_not_grant_live_access_or_verify_credentials(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical(), synthetic=False)
    result = contracts.assess_pause(
        as_of=now(),
        synthetic=False,
        pause_seconds=1,
        rollback_seconds=1,
        safety_seconds=1,
        catchup_requests_per_second=100,
        request_budget=1000,
    )
    assert result["allowed"] is False
    assert {"source_access_unverified", "credential_budget_unverified"} <= set(result["reasons"])
    assert pipeline.capabilities() == []


def test_upgrade_preserves_existing_tasks_without_inventing_original_deadlines(pipeline):
    # Simulate an existing populated schema immediately before this additive migration.
    pipeline.catalog.execute("DROP TRIGGER collection_capture_obligation ON collection_tasks")
    pipeline.catalog.execute("DROP TRIGGER acknowledge_capture_dataset ON datasets")
    pipeline.catalog.execute("DROP TABLE capture_obligations CASCADE")
    pipeline.catalog.execute("DROP TABLE capture_policies CASCADE")
    for function in (
        "freeze_capture_obligation()",
        "capture_policy_immutable()",
        "acknowledge_raw_capture(text,integer,text,boolean)",
        "immutable_capture_obligation()",
        "capture_dataset_committed()",
    ):
        pipeline.catalog.execute(f"DROP FUNCTION {function} CASCADE")
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=29")
    task = enqueue(pipeline, "legacy-retry", now() - timedelta(hours=1))
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp()+interval '1 day' WHERE task_id=%s",
        (task,),
    )
    original = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task,))
    pipeline.catalog.initialize()
    contracts = CaptureContracts(pipeline.catalog)
    obligation = contracts.declare(task)
    assert obligation["due_origin"] == "legacy_created_bound"
    assert obligation["original_due_at"] == original["created_at"]
    assert obligation["deadline_at"] is None and obligation["contract_id"] is None
    pipeline.catalog.initialize()
    assert contracts.declare(task) == obligation
    assert (
        pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task,)) == original
    )
