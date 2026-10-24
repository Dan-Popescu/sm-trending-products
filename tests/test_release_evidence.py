"""Qualify releases from real committed obligation receipts, never readiness flags."""

import gzip
import hashlib
from datetime import timedelta

import pytest
from psycopg.types.json import Jsonb
from test_capture_contracts import enqueue, historical

from sm_trending_products.capture_contracts import CaptureContracts, CapturePolicy
from sm_trending_products.contracts import canonical, digest, now
from sm_trending_products.release_evidence import CatalogCaptureVerifier

pytestmark = pytest.mark.integration


def publish(pipeline, task, name, *, worker="candidate", generation=1, quarantine_count=0):
    stamp = now()
    body = gzip.compress(b"real controlled response")
    raw, manifest_key = f"release/{name}.gz", f"release/{name}.json"
    manifest = {
        "schema_version": "1",
        "dataset_id": name,
        "source": task["source"],
        "synthetic": True,
        "kind": "raw_capture",
        "acquisition_protocol": "1",
        "transform_version": "1",
        "row_count": 0,
        "quarantine_count": quarantine_count,
        "normalized": [],
        "manifest_key": manifest_key,
        "raw": {"key": raw, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)},
        "capture": {
            "task_id": task["task_id"],
            "page_index": task["page_index"],
            "captured_at": stamp.isoformat(),
            "worker_id": worker,
            "worker_generation": generation,
        },
        "task_request": {
            "source": task["source"],
            "account_id": task["account_id"],
            "operation": task["operation"],
            "synthetic": True,
            "payload": task["payload"],
        },
    }
    pipeline.catalog.stage_dataset(name, task["source"], True, manifest, task=task)
    pipeline.history.put(raw, body)
    pipeline.history.put(manifest_key, canonical(manifest).encode())
    pipeline.catalog.commit_dataset(name, manifest, (), task=task)
    return name


def test_real_receipt_qualifies_only_its_acquiring_generation(pipeline):
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    start = now()
    enqueue(pipeline, "first", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "first")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    evidence = verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
    assert evidence and evidence["capture_dataset_ids"] == ["first"]
    assert verifier.verify(worker_id="candidate", generation=2, since=start, synthetic=True) is None
    assert (
        verifier.verify(worker_id="adopting-publisher", generation=1, since=start, synthetic=True)
        is None
    )


def test_older_unknown_pending_scope_cannot_disappear_at_release_start(pipeline):
    enqueue(pipeline, "legacy-unknown", now() - timedelta(minutes=1))
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    start = now()
    enqueue(pipeline, "fresh-qualified", now())
    # Claim the older unknown task first but do not manufacture its missing proof.
    assert pipeline.catalog.claim("predecessor", 180, True, 8)
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "qualified")
    assert (
        CatalogCaptureVerifier(pipeline.catalog).verify(
            worker_id="candidate", generation=1, since=start, synthetic=True
        )
        is None
    )


@pytest.mark.parametrize("state", ["invalidated", "deleted"])
@pytest.mark.parametrize("after_deadline", [False, True])
def test_invalidated_window_receipt_is_a_breach_despite_fresh_same_scope_capture(
    pipeline, state, after_deadline
):
    from unittest.mock import patch

    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical().model_copy(update={"lateness_seconds": 120}), synthetic=True)
    start = now()
    original_task = enqueue(pipeline, "retired-window", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "window-receipt")
    pipeline.catalog.execute(
        "UPDATE datasets SET state=%s WHERE dataset_id=%s", (state, "window-receipt")
    )
    enqueue(pipeline, "fresh-same-scope", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "fresh-receipt")
    deadline = contracts.declare(original_task)["deadline_at"]
    assert now() < deadline
    cutoff = deadline + timedelta(seconds=1) if after_deadline else now()
    with patch("sm_trending_products.release_evidence.now", return_value=cutoff):
        assert (
            CatalogCaptureVerifier(pipeline.catalog).verify(
                worker_id="candidate", generation=1, since=start, synthetic=True
            )
            is None
        )


@pytest.mark.parametrize("future_due", [False, True])
def test_unrecorded_window_with_future_deadline_does_not_block_same_scope_witness(
    pipeline, future_due
):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical().model_copy(update={"lateness_seconds": 120}), synthetic=True)
    start = now()
    due = now() + timedelta(hours=1) if future_due else now()
    pending_id = enqueue(pipeline, "uncaptured-within-tolerance", due)
    if not future_due:
        pending = pipeline.catalog.claim("predecessor", 180, True, 8)
        assert pending["task_id"] == pending_id
    enqueue(pipeline, "fresh-valid-window", now())
    fresh = pipeline.catalog.claim("candidate", 180, True, 8)
    assert fresh
    publish(pipeline, fresh, "valid-receipt")
    pending = contracts.declare(pending_id)
    assert pending["capture_dataset_id"] is None and pending["deadline_at"] > now()
    assert (pending["original_due_at"] > now()) is future_due
    assert CatalogCaptureVerifier(pipeline.catalog).verify(
        worker_id="candidate", generation=1, since=start, synthetic=True
    )


def test_durable_unusable_raw_envelope_is_not_replacement_readiness(pipeline):
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    start = now()
    enqueue(pipeline, "unknown-envelope", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "quarantined-raw-parent", quarantine_count=1)
    assert (
        CatalogCaptureVerifier(pipeline.catalog).verify(
            worker_id="candidate", generation=1, since=start, synthetic=True
        )
        is None
    )


def revoke_pending(pipeline, task_id, *, attempt=0):
    task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    cutoff = now()
    pipeline.catalog.execute(
        "INSERT INTO capture_spool_revocations(source,account_id,synthetic,captured_before,reason) "
        "VALUES(%s,%s,%s,%s,'source_opt_out')",
        (task["source"], task["account_id"], task["synthetic"], cutoff),
    )
    authority = pipeline.catalog.one(
        "SELECT * FROM capture_spool_revocations WHERE source=%s", (task["source"],)
    )
    identity = {"capture_of": task_id, "page_index": 0, "protocol": "1"}
    if attempt:
        identity["capture_attempt"] = attempt
    pipeline.catalog.execute(
        "INSERT INTO capture_spool_retirements(capture_id,task_id,page_index,capture_attempt,"
        "source,account_id,synthetic,captured_at,started_at,complete_receipt,reason,"
        "authority_kind,authority_ref) VALUES(%s,%s,0,%s,%s,%s,%s,NULL,%s,false,"
        "'capture_source_removed','source_revocation',%s)",
        (
            digest(identity),
            task_id,
            attempt,
            task["source"],
            task["account_id"],
            task["synthetic"],
            task["created_at"],
            Jsonb(
                {
                    key: (authority[key].isoformat() if key != "reason" else authority[key])
                    for key in ("captured_before", "requested_at", "reason")
                }
            ),
        ),
    )


@pytest.mark.parametrize("current_attempt", [0, 1])
def test_only_exact_lawful_retirement_removes_old_pending_window(pipeline, current_attempt):
    old = enqueue(pipeline, "withdrawn-unknown", now() - timedelta(minutes=1))
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET state='cancelled',capture_attempt=%s WHERE task_id=%s",
        (current_attempt, old),
    )
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    start = now()
    enqueue(pipeline, "valid-replacement", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "qualified")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True) is None
    revoke_pending(pipeline, old, attempt=0)
    result = verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
    assert bool(result) is (current_attempt == 0)


def test_registered_daily_path_waits_for_its_own_replacement_capture(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical(), synthetic=True)
    contracts.register(
        CapturePolicy(
            source="tiktok_ads",
            account_id="fixture-account",
            operation="query",
            version="daily-v1",
            cadence_seconds=86400,
            lateness_seconds=600,
            recoverability="unknown",
        ),
        synthetic=True,
    )
    pipeline.catalog.enqueue(
        "tiktok_ads",
        "fixture-account",
        "query",
        {},
        True,
        due_at=now(),
        idempotency_key="prior-daily",
    )
    prior = pipeline.catalog.claim("old", 180, True, 8)
    assert prior
    publish(pipeline, prior, "old-daily", worker="old")
    start = now()
    enqueue(pipeline, "fresh-display", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "fresh-display")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True) is None
    pipeline.catalog.enqueue(
        "tiktok_ads",
        "fixture-account",
        "query",
        {},
        True,
        due_at=now(),
        idempotency_key="next-daily",
    )
    daily = pipeline.catalog.claim("candidate", 180, True, 8)
    assert daily
    publish(pipeline, daily, "fresh-daily")
    result = verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
    assert result and result["registered_policy_count"] == 2


def test_policy_version_and_wildcard_match_actual_request_scope(pipeline):
    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical(), synthetic=True)
    start = now()

    def scoped_task(key):
        identifier = digest({"scope-fixture": key})
        pipeline.catalog.execute(
            "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
            "due_at,schedule_reason) "
            "VALUES(%s,'tiktok_display','fixture-account','query','{}',true,%s,%s)",
            (identifier, now(), Jsonb({"capture_scope": "real-request-signature"})),
        )
        task = pipeline.catalog.claim("candidate", 180, True, 8)
        assert task
        publish(pipeline, task, key)

    scoped_task("first-version")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
    contracts.register(historical().model_copy(update={"version": "history-v2"}), synthetic=True)
    assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True) is None
    scoped_task("second-version")
    assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)


def test_long_cadence_witness_age_follows_declared_sampling_window(pipeline):
    from unittest.mock import patch

    CaptureContracts(pipeline.catalog).register(
        CapturePolicy(
            source="tiktok_display",
            account_id="fixture-account",
            operation="query",
            version="daily-v1",
            cadence_seconds=86400,
            lateness_seconds=600,
        ),
        synthetic=True,
    )
    start = now()
    enqueue(pipeline, "daily-candidate", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "daily-candidate")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    with patch(
        "sm_trending_products.release_evidence.now", return_value=now() + timedelta(hours=1)
    ):
        assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
    with patch(
        "sm_trending_products.release_evidence.now", return_value=now() + timedelta(seconds=87001)
    ):
        assert (
            verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
            is None
        )


@pytest.mark.parametrize("after_deadline", [False, True])
def test_exact_captured_receipt_withdrawal_does_not_hide_unexplained_invalidations(
    pipeline, after_deadline
):
    from unittest.mock import patch

    contracts = CaptureContracts(pipeline.catalog)
    contracts.register(historical().model_copy(update={"lateness_seconds": 120}), synthetic=True)
    start = now()
    enqueue(pipeline, "removed-window", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    capture_id = digest({"capture_of": task["task_id"], "page_index": 0, "protocol": "1"})
    publish(pipeline, task, capture_id)
    dataset = pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (capture_id,))
    captured_at = dataset["manifest"]["capture"]["captured_at"]
    pipeline.catalog.execute(
        "UPDATE datasets SET state='invalidated',invalid_reason='source_removed' "
        "WHERE dataset_id=%s",
        (capture_id,),
    )
    enqueue(pipeline, "healthy-window", now())
    fresh = pipeline.catalog.claim("candidate", 180, True, 8)
    assert fresh
    publish(pipeline, fresh, "healthy-window")
    verifier = CatalogCaptureVerifier(pipeline.catalog)
    deadline = contracts.declare(task["task_id"])["deadline_at"]
    assert now() < deadline
    cutoff = deadline + timedelta(seconds=1) if after_deadline else now()
    with patch("sm_trending_products.release_evidence.now", return_value=cutoff):
        assert (
            verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
            is None
        )
    pipeline.catalog.execute(
        "INSERT INTO capture_spool_retirements(capture_id,task_id,page_index,capture_attempt,"
        "source,account_id,synthetic,captured_at,started_at,complete_receipt,reason,"
        "authority_kind,authority_ref) "
        "VALUES(%s,%s,0,0,%s,%s,true,%s,%s,true,'capture_source_removed','dataset_invalidation',%s)",
        (
            capture_id,
            task["task_id"],
            task["source"],
            task["account_id"],
            captured_at,
            captured_at,
            Jsonb(
                {
                    "dataset_id": capture_id,
                    "state": "invalidated",
                    "invalid_reason": "source_removed",
                }
            ),
        ),
    )
    if not after_deadline:
        # A later withdrawal certificate cannot change the earlier decision.
        with patch("sm_trending_products.release_evidence.now", return_value=cutoff):
            assert (
                verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)
                is None
            )
        cutoff = now()
        assert cutoff < deadline
    with patch("sm_trending_products.release_evidence.now", return_value=cutoff):
        assert verifier.verify(worker_id="candidate", generation=1, since=start, synthetic=True)


def test_new_acquisition_of_older_due_task_can_qualify_within_its_original_deadline(pipeline):
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    enqueue(pipeline, "existing-due-task", now())
    start = now()
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "new-physical-acquisition")
    assert CatalogCaptureVerifier(pipeline.catalog).verify(
        worker_id="candidate", generation=1, since=start, synthetic=True
    )
