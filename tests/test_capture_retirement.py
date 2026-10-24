"""Lawful withdrawal is immutable evidence, separate from physical capture."""

from __future__ import annotations

from datetime import timedelta

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_capture_spool import local_capture
from test_raw_capture import ACCOUNT, enqueue, one_shot_pages, page, service
from test_raw_capture_recovery import retry_now

from sm_trending_products.contracts import now, utc

pytestmark = pytest.mark.integration


def insert_marker(pipeline, task, receipt, kind, authority, reason="capture_source_removed"):
    capture = receipt["capture"]
    pipeline.catalog.execute(
        "INSERT INTO capture_spool_retirements(capture_id,task_id,page_index,capture_attempt,"
        "source,account_id,synthetic,captured_at,started_at,complete_receipt,reason,"
        "authority_kind,authority_ref) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,%s)",
        (
            capture["capture_id"],
            task["task_id"],
            task["page_index"],
            task["capture_attempt"],
            task["source"],
            task["account_id"],
            task["synthetic"],
            utc(capture["captured_at"]),
            utc(capture["started_at"]),
            reason,
            kind,
            Jsonb(authority),
        ),
    )


def test_retirement_cannot_borrow_another_account_authority_and_audit_is_immutable(pipeline):
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task, receipt, path = local_capture(pipeline)
    wrong = acquisition.request_spool_removal(
        "tiktok_display", "different-account", True, "source_opt_out"
    )
    journal = pipeline.catalog.one(
        "SELECT * FROM capture_spool_revocations WHERE account_id=%s", (wrong["account_id"],)
    )
    authority = {
        "captured_before": journal["captured_before"].isoformat(),
        "requested_at": journal["requested_at"].isoformat(),
        "reason": journal["reason"],
    }
    with pytest.raises(psycopg.errors.RaiseException, match="exact scoped revocation"):
        insert_marker(pipeline, task, receipt, "source_revocation", authority)
    assert path.exists() and pipeline.catalog.all("SELECT * FROM capture_spool_retirements") == []
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    marker = pipeline.catalog.one("SELECT * FROM capture_spool_retirements")
    assert marker["complete_receipt"] and marker["captured_at"] == utc(
        receipt["capture"]["captured_at"]
    )
    assert marker["authority_kind"] == "source_revocation"
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        pipeline.catalog.execute("UPDATE capture_spool_retirements SET reason='ordinary_failure'")
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        pipeline.catalog.execute("DELETE FROM capture_spool_revocations")


def test_retirement_rejects_unverified_or_not_yet_elapsed_retention_policy(pipeline):
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task, receipt, path = local_capture(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,false)",
        (task["source"],),
    )
    policy = pipeline.catalog.one(
        "SELECT * FROM source_policies WHERE source=%s", (task["source"],)
    )
    authority = {
        "retention_days": 1,
        "policy_updated_at": policy["updated_at"].isoformat(),
        "reservation_grace_seconds": 0,
        "deadline": (utc(receipt["capture"]["captured_at"]) + timedelta(days=1)).isoformat(),
    }
    with pytest.raises(psycopg.errors.RaiseException, match="verified retention authority"):
        insert_marker(
            pipeline, task, receipt, "retention_expiry", authority, "capture_retention_expired"
        )
    pipeline.catalog.execute("UPDATE source_policies SET verified=true")
    with pytest.raises(psycopg.errors.RaiseException, match="deadline has not elapsed"):
        insert_marker(
            pipeline, task, receipt, "retention_expiry", authority, "capture_retention_expired"
        )
    assert path.exists() and pipeline.catalog.all("SELECT * FROM capture_spool_retirements") == []


def test_durable_retirement_survives_unlink_failure_and_later_policy_change(pipeline, monkeypatch):
    acquisition = service(pipeline)
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        task, receipt, path = local_capture(pipeline)
    old = (now() - timedelta(days=2)).isoformat()
    path.write_text(
        path.read_text()
        .replace(receipt["capture"]["captured_at"], old)
        .replace(receipt["capture"]["started_at"], old)
    )
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        (task["source"],),
    )
    original = acquisition.spool._remove_files

    def interrupted(*args, **kwargs):
        raise OSError("controlled_after_authority_commit_before_unlink")

    monkeypatch.setattr(acquisition.spool, "_remove_files", interrupted)
    assert acquisition.maintain_spool(1000, apply=True)["invalid"] == 1
    marker = pipeline.catalog.one("SELECT * FROM capture_spool_retirements")
    assert marker["authority_kind"] == "retention_expiry" and path.exists()
    assert (
        pipeline.catalog.one(
            "SELECT state FROM collection_tasks WHERE task_id=%s", (task["task_id"],)
        )["state"]
        == "cancelled"
    )
    pipeline.catalog.execute(
        "UPDATE source_policies SET retention_days=30,verified=false,updated_at=clock_timestamp()"
    )
    monkeypatch.setattr(acquisition.spool, "_remove_files", original)
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1 and not path.exists()
    assert pipeline.catalog.one("SELECT * FROM capture_spool_retirements") == marker


def test_retiring_old_error_attempt_does_not_cancel_newer_active_attempt(pipeline, monkeypatch):
    acquisition = service(pipeline)

    def retain(*args, **kwargs):
        raise OSError("controlled_keep_diagnostic_copy")

    monkeypatch.setattr(acquisition, "_remove_committed_spool", retain)
    with one_shot_pages([{"error": {"code": "internal_error"}}, page()]) as (endpoint, requests):
        task_id = enqueue(pipeline, endpoint)
        assert acquisition.capture_batch(1, True)["processed"] == 0
        acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
        assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert task["state"] == "pending" and task["capture_attempt"] == 1
        marker = pipeline.catalog.one("SELECT * FROM capture_spool_retirements")
        assert marker["capture_attempt"] == 0
        retry_now(pipeline, task_id)
        assert acquisition.capture_batch(1, True)["processed"] == 1 and len(requests) == 2
    assert acquisition.publish_pending(1, True)["processed"] == 1
    # The subsequent positive-attempt capture has its own independently
    # qualified identity; retiring it cannot rewrite the old diagnostic audit.
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    retired = sum(acquisition.maintain_spool(1000, apply=True)["retired"] for _ in range(3))
    assert retired == 1
    assert {
        row["capture_attempt"]
        for row in pipeline.catalog.all("SELECT capture_attempt FROM capture_spool_retirements")
    } == {0, 1}
