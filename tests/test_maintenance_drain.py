"""Resource-budgeted maintenance drains against durable SQL and HTTP storage."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from botocore.exceptions import ClientError
from psycopg.types.json import Jsonb
from test_maintenance import record

import sm_trending_products.maintenance as maintenance_module
from sm_trending_products.contracts import now
from sm_trending_products.maintenance import Maintenance

pytestmark = pytest.mark.integration


def seed_expired_roots(pipeline, count, *, shared_raw=False, expired_count=None):
    """Seed large empty-page catalogs; remote deletion of absent keys is idempotent."""
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES('drain_fixture',1,true)"
    )
    timestamp = now()
    rows = []
    for index in range(count):
        dataset_id = f"drain-{index:05d}"
        manifest = {
            "raw": {"key": "shared-raw" if shared_raw else f"raw/{dataset_id}"},
            "normalized": [],
            "manifest_key": f"manifest/{dataset_id}",
        }
        expired = expired_count is None or index < expired_count
        created = timestamp - timedelta(days=2) if expired else timestamp
        rows.append((dataset_id, Jsonb(manifest), created))
    with pipeline.catalog.transaction() as connection, connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest,created_at) "
            "VALUES(%s,'drain_fixture',true,'committed',%s,%s)",
            rows,
        )
    return [row[0] for row in rows]


def seed_pending_keys(pipeline, count):
    keys = [f"pending/{index:05d}" for index in range(count)]
    with pipeline.catalog.transaction() as connection, connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO retention_objects(object_key,reason) VALUES(%s,'retention_expired')",
            [(key,) for key in keys],
        )
    return keys


def test_retention_drains_250_roots_and_reports_global_expiration_before_mutation(pipeline):
    roots = seed_expired_roots(pipeline, 250)
    pipeline.history.client.put_object(
        Bucket=pipeline.history.bucket, Key=f"raw/{roots[0]}", Body=b"restricted"
    )
    preview = pipeline.retention()
    assert preview["dataset_count"] == 100
    assert preview["expired_root_count"] == 250
    assert preview["oldest_expired_root_age_seconds"] >= 86400
    assert preview["preview_truncated"] is False
    result = Maintenance(pipeline).retention_drain(max_seconds=60)
    assert result["status"] == "complete" and result["stop_reason"] == "no_work"
    assert result["batches"] == 3
    assert result["dataset_count"] == 250
    assert set(result["dataset_ids"]) == set(roots)
    assert result["deleted_objects"] == 500
    assert result["pending_objects"] == result["expired_root_count"] == 0
    assert result["pending_finalization_count"] == 0
    assert result["oldest_expired_root_age_seconds"] is None
    assert (
        pipeline.catalog.one("SELECT count(*) AS n FROM datasets WHERE state='deleted'")["n"] == 250
    )
    with pytest.raises(ClientError):
        pipeline.history.client.head_object(Bucket=pipeline.history.bucket, Key=f"raw/{roots[0]}")


def test_retention_keyset_passes_a_failed_key_and_drains_more_than_1000_objects(
    pipeline, monkeypatch
):
    keys = seed_pending_keys(pipeline, 1105)
    actual_delete = pipeline.history.client.delete_object
    failed_calls = []

    def delete(**kwargs):
        if kwargs["Key"] == keys[0]:
            failed_calls.append(kwargs["Key"])
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject")
        return actual_delete(**kwargs)

    monkeypatch.setattr(pipeline.history.client, "delete_object", delete)
    result = Maintenance(pipeline).retention_drain(max_seconds=60)
    assert result["status"] == "partial" and result["stop_reason"] == "remote_errors"
    assert result["deleted_objects"] == 1104 and result["pending_objects"] == 1
    assert result["attempted_objects"] == 1105 and result["failed_objects"] == 1
    assert failed_calls == [keys[0]]
    durable = pipeline.catalog.one(
        "SELECT * FROM retention_objects WHERE object_key=%s", (keys[0],)
    )
    assert durable["state"] == "pending" and durable["attempts"] == 1
    assert durable["last_error"] == "object_delete_failed"
    monkeypatch.setattr(pipeline.history.client, "delete_object", actual_delete)
    resumed = Maintenance(pipeline).retention_drain()
    assert resumed["status"] == "complete" and resumed["deleted_objects"] == 1


def test_retention_batch_limit_and_deadline_resume_without_repeating_completed_roots(
    pipeline, monkeypatch
):
    seed_expired_roots(pipeline, 250)
    maintenance = Maintenance(pipeline)
    first = maintenance.retention_drain(max_batches=1, max_seconds=60)
    assert first["dataset_count"] == 100 and first["expired_root_count"] == 150
    assert first["status"] == "partial" and first["stop_reason"] == "batch_limit"
    assert first["deleted_objects"] == 200
    actual_retire = maintenance._retire_expired
    clock = [0.0]

    def retire(**kwargs):
        result = actual_retire(**kwargs)
        clock[0] = 2.0
        return result

    with monkeypatch.context() as patch:
        patch.setattr(maintenance_module, "monotonic", lambda: clock[0])
        patch.setattr(maintenance, "_retire_expired", retire)
        second = maintenance.retention_drain(max_seconds=1)
    assert second["stop_reason"] == "deadline" and second["operations"] == 0
    assert second["dataset_count"] == 100 and second["expired_root_count"] == 50
    assert second["pending_objects"] == 200
    resumed = maintenance.retention_drain(max_seconds=60)
    assert resumed["status"] == "complete" and resumed["dataset_count"] == 50
    assert resumed["deleted_objects"] == 300


def test_operation_budget_preserves_an_unfinished_versioned_key_for_restart(pipeline):
    key = seed_pending_keys(pipeline, 1)[0]
    client, bucket = pipeline.history.client, pipeline.history.bucket
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    for body in (b"first", b"second"):
        client.put_object(Bucket=bucket, Key=key, Body=body)
    result = Maintenance(pipeline).retention_drain(max_operations=2)
    assert result["operations"] == 2 and result["stop_reason"] == "operation_limit"
    assert result["pending_objects"] == 1 and result["failed_objects"] == 0
    assert len(client.list_object_versions(Bucket=bucket, Prefix=key)["Versions"]) == 2
    resumed = Maintenance(pipeline).retention_drain(max_operations=4)
    assert resumed["status"] == "complete" and resumed["operations"] == 4
    assert not client.list_object_versions(Bucket=bucket, Prefix=key).get("Versions")


def test_closure_preview_truncation_reflects_affected_datasets_not_just_roots(pipeline):
    seed_expired_roots(pipeline, 1105, shared_raw=True, expired_count=1)
    preview = pipeline.retention()
    assert preview["expired_root_count"] == 1
    assert preview["dataset_count"] == 1105 and len(preview["dataset_ids"]) == 1000
    assert preview["preview_truncated"] is True
    result = Maintenance(pipeline).retention_drain(max_objects=1, max_seconds=60)
    assert result["dataset_count"] == 1105 and len(result["dataset_ids"]) == 1000
    assert result["preview_truncated"] is True
    assert result["pending_objects"] == 1105
    assert result["stop_reason"] == "object_limit"
    resumed = Maintenance(pipeline).retention_drain(max_seconds=60)
    assert resumed["status"] == "complete"


def test_publication_and_retention_keep_fences_while_remote_deletion_runs(pipeline, monkeypatch):
    expired = pipeline.import_records([record("expired", now(), 1)], synthetic=True)[0]
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('maintenance_fixture',1,true)"
    )
    row = pipeline.catalog.one("SELECT manifest FROM datasets WHERE dataset_id=%s", (expired,))
    raw_key = row["manifest"]["raw"]["key"]
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (expired,),
    )
    started, release = Event(), Event()
    original_delete = pipeline.history.client.delete_object

    def delete(**kwargs):
        if not started.is_set():
            started.set()
            assert release.wait(10)
        return original_delete(**kwargs)

    monkeypatch.setattr(pipeline.history.client, "delete_object", delete)
    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(Maintenance(pipeline).retention_drain)
        try:
            assert started.wait(10)
            assert (
                pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (expired,))[
                    "state"
                ]
                == "invalidated"
            )
            with pytest.raises(ValueError, match="retired"):
                pipeline.history.put(raw_key, b"resurrection")
            with pytest.raises(ValueError, match="invalidated"):
                pipeline.catalog.stage_derivation("derived/resurrection", [expired])
            fresh = pipeline.import_records([record("fresh", now(), 2)], synthetic=True)[0]
        finally:
            release.set()
        assert running.result(timeout=30)["status"] == "complete"
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (fresh,))["state"]
        == "committed"
    )


def test_compaction_drain_processes_multiple_groups_without_rereading_new_outputs(
    pipeline, monkeypatch
):
    timestamp = now()
    roots = [
        pipeline.import_records([record(f"video-{index}", timestamp, index)], synthetic=True)[0]
        for index in range(12)
    ]
    source_read = pipeline.history.read
    reads = []

    def read(manifests, **kwargs):
        manifests = list(manifests)
        reads.extend(item["dataset_id"] for item in manifests)
        return source_read(manifests, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", read)
    maintenance = Maintenance(pipeline)
    first = maintenance.compact_drain(synthetic=True, max_datasets=4, max_batches=2)
    assert first["status"] == "partial" and first["stop_reason"] == "batch_limit"
    assert first["batches"] == 2 and first["parent_dataset_count"] == 8
    assert len(reads) == len(set(reads)) == 8
    assert set(reads) <= set(roots)
    assert not set(first["dataset_ids"]).intersection(reads)
    reads.clear()
    resumed = maintenance.compact_drain(synthetic=True, max_datasets=4)
    assert resumed["status"] == "complete"
    assert len(reads) == len(set(reads))
    assert not set(resumed["dataset_ids"]).intersection(reads)
    assert maintenance.compaction_backlog(True)["candidate_dataset_count"] >= 1
    assert pipeline.verify()["ok"] is True


def test_oversized_atomic_closure_is_reported_without_partial_retirement(pipeline, monkeypatch):
    seed_expired_roots(pipeline, 3, shared_raw=True, expired_count=1)
    monkeypatch.setattr(maintenance_module, "RETENTION_TARGET_LIMIT", 2)
    result = Maintenance(pipeline).retention_drain()
    assert result["status"] == "blocked" and result["stop_reason"] == "retention_closure_limit"
    assert result["dataset_count"] == result["operations"] == result["pending_objects"] == 0
    assert result["expired_root_count"] == 1
    assert (
        pipeline.catalog.one("SELECT count(*) AS n FROM datasets WHERE state='committed'")["n"] == 3
    )


def test_compaction_deadline_resumes_after_a_completed_atomic_group(pipeline, monkeypatch):
    timestamp = now()
    for index in range(8):
        pipeline.import_records([record(f"video-{index}", timestamp, index)], synthetic=True)
    maintenance = Maintenance(pipeline)
    original_compact = maintenance.compact
    clock = [0.0]

    def compact(*args, **kwargs):
        result = original_compact(*args, **kwargs)
        clock[0] = 2.0
        return result

    with monkeypatch.context() as patch:
        patch.setattr(maintenance_module, "monotonic", lambda: clock[0])
        patch.setattr(maintenance, "compact", compact)
        result = maintenance.compact_drain(synthetic=True, max_datasets=2, max_seconds=1)
    assert result["batches"] == 1 and result["rows"] == 2
    assert result["status"] == "partial" and result["stop_reason"] == "deadline"
    resumed = maintenance.compact_drain(synthetic=True, max_datasets=2)
    assert resumed["status"] == "complete"
    assert pipeline.verify()["ok"] is True


def test_shared_descendant_is_counted_once_across_distinct_root_batches(pipeline):
    roots = seed_expired_roots(pipeline, 250)
    child = {
        "raw": {"key": "raw/shared-descendant"},
        "normalized": [],
        "manifest_key": "manifest/shared-descendant",
        "parent_dataset_ids": [roots[0], roots[-1]],
    }
    pipeline.catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
        "VALUES('shared-descendant','drain_fixture',true,'committed',%s)",
        (Jsonb(child),),
    )
    result = Maintenance(pipeline).retention_drain(max_objects=1, max_seconds=60)
    assert result["batches"] == 3
    assert result["dataset_count"] == len(result["dataset_ids"]) == 251
    assert len(set(result["dataset_ids"])) == 251
    assert result["pending_objects"] == 501


def test_atomic_root_batches_shrink_when_their_object_fanout_exceeds_the_cap(pipeline, monkeypatch):
    seed_expired_roots(pipeline, 7)
    # Each root has two objects: a three-row atomic cap fits one root at a time.
    monkeypatch.setattr(maintenance_module, "RETENTION_TARGET_LIMIT", 3)
    result = Maintenance(pipeline).retention_drain(max_seconds=60)
    assert result["status"] == "complete" and result["batches"] == 7
    assert result["dataset_count"] == 7 and result["deleted_objects"] == 14


def test_recovered_staged_compaction_output_is_not_rewritten_in_the_same_pass(
    pipeline, monkeypatch
):
    timestamp = now()
    roots = [
        pipeline.import_records([record(f"video-{index}", timestamp, index)], synthetic=True)[0]
        for index in range(3)
    ]
    maintenance = Maintenance(pipeline)

    def failed_put(*_args, **_kwargs):
        raise RuntimeError("controlled upload interruption after staging")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.history, "put", failed_put)
        with pytest.raises(RuntimeError, match="controlled upload"):
            maintenance.compact(synthetic=True, max_datasets=2)
    staged = pipeline.catalog.one("SELECT dataset_id FROM datasets WHERE state='staged'")[
        "dataset_id"
    ]
    actual_read = pipeline.history.read
    read_ids = []

    def read(manifests, **kwargs):
        manifests = list(manifests)
        read_ids.extend(row["dataset_id"] for row in manifests)
        return actual_read(manifests, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", read)
    result = maintenance.compact_drain(synthetic=True, max_datasets=2)
    assert result["status"] == "complete" and result["batches"] == 1
    assert result["dataset_ids"] == [staged]
    assert set(read_ids) <= set(roots) and len(read_ids) == 2
    assert result["pass_backlog"]["mergeable_group_count"] == 0
    assert result["active_backlog"]["mergeable_group_count"] == 1
    assert pipeline.verify()["ok"] is True
