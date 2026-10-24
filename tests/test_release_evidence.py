"""Qualify releases from real committed obligation receipts, never readiness flags."""

import gzip
import hashlib
from datetime import timedelta

import pytest
from test_capture_contracts import enqueue, historical

from sm_trending_products.capture_contracts import CaptureContracts
from sm_trending_products.contracts import canonical, now
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


def test_invalidated_window_receipt_is_a_breach_despite_fresh_same_scope_capture(pipeline):
    CaptureContracts(pipeline.catalog).register(historical(), synthetic=True)
    start = now()
    enqueue(pipeline, "retired-window", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "window-receipt")
    pipeline.catalog.execute(
        "UPDATE datasets SET state='invalidated' WHERE dataset_id=%s", ("window-receipt",)
    )
    enqueue(pipeline, "fresh-same-scope", now())
    task = pipeline.catalog.claim("candidate", 180, True, 8)
    assert task
    publish(pipeline, task, "fresh-receipt")
    # Move only the verifier clock beyond the frozen ten-second deadline.
    from unittest.mock import patch

    with patch(
        "sm_trending_products.release_evidence.now", return_value=now() + timedelta(seconds=11)
    ):
        assert (
            CatalogCaptureVerifier(pipeline.catalog).verify(
                worker_id="candidate", generation=1, since=start, synthetic=True
            )
            is None
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
