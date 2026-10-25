"""Full and incremental recovery sets against real HTTP object storage and SQL."""

import hashlib
import json
import runpy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from botocore.exceptions import ClientError

from sm_trending_products.contracts import canonical, now

pytestmark = pytest.mark.integration
snapshot = runpy.run_path(str(Path(__file__).parents[1] / "scripts/object_snapshot.py"))


def configure(pipeline, monkeypatch):
    s3 = pipeline.history.client
    calls = []
    real_get = s3.get_object

    def counted(**kwargs):
        calls.append(kwargs)
        return real_get(**kwargs)

    monkeypatch.setattr(s3, "get_object", counted)
    snapshot["capture"].__globals__["client"] = lambda: s3
    snapshot["restore"].__globals__["client"] = lambda: s3
    monkeypatch.setenv("S3_BUCKET", pipeline.history.bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("PIPELINE_NAMESPACE", pipeline.settings.pipeline_namespace)
    return s3, calls


def put(s3, bucket, key, body):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/plain",
        Metadata={"sha256": hashlib.sha256(body).hexdigest(), "purpose": "backup-test"},
    )


def test_base_two_deltas_and_unchanged_backup_restore_latest_inventory(
    pipeline, tmp_path, monkeypatch
):
    s3, calls = configure(pipeline, monkeypatch)
    bucket = pipeline.history.bucket
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    put(s3, bucket, "unchanged", b"original")
    put(s3, bucket, "changed", b"before")
    put(s3, bucket, "deleted", b"remove")
    base, delta1, delta2, unchanged = [
        tmp_path / key / "objects" for key in ("base", "delta1", "delta2", "unchanged")
    ]
    first = snapshot["capture"](base, copy_objects=True)
    assert first["copied_object_count"] == len(calls) == 3
    calls.clear()
    s3.delete_object(Bucket=bucket, Key="deleted")
    put(s3, bucket, "changed", b"after")
    put(s3, bucket, "added", b"new")
    second = snapshot["capture"](delta1, copy_objects=True, base=base)
    assert second["copied_object_count"] == len(calls) == 2
    assert second["reused_object_count"] == 1
    calls.clear()
    put(s3, bucket, "later", b"latest")
    third = snapshot["capture"](delta2, copy_objects=True, base=delta1)
    assert third["copied_object_count"] == len(calls) == 1
    assert third["reused_object_count"] == 3
    calls.clear()
    fourth = snapshot["capture"](unchanged, copy_objects=True, base=delta2)
    assert fourth["copied_object_count"] == fourth["downloaded_bytes"] == len(calls) == 0
    assert fourth["reused_object_count"] == fourth["object_count"] == 4
    assert list((unchanged / "objects").iterdir()) == []
    destination = "restore-" + uuid4().hex
    restored = snapshot["restore"](unchanged, bucket=destination, create_bucket=True)
    assert restored["restored_objects"] == 4
    assert {item["Key"] for item in s3.list_objects_v2(Bucket=destination)["Contents"]} == {
        "unchanged",
        "changed",
        "added",
        "later",
    }
    assert s3.get_object(Bucket=destination, Key="changed")["Body"].read() == b"after"
    assert (
        s3.head_object(Bucket=destination, Key="unchanged")["Metadata"]["purpose"] == "backup-test"
    )


@pytest.mark.parametrize(
    "corruption", ["base_bytes", "delta_bytes", "base_manifest", "missing_base", "escape"]
)
def test_corrupt_or_missing_chain_refuses_before_destination_write(
    pipeline, tmp_path, monkeypatch, corruption
):
    s3, _calls = configure(pipeline, monkeypatch)
    put(s3, pipeline.history.bucket, "original", b"original")
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    snapshot["capture"](base, copy_objects=True)
    put(s3, pipeline.history.bucket, "new", b"new")
    snapshot["capture"](delta, copy_objects=True, base=base)
    if corruption == "base_bytes":
        next((base / "objects").iterdir()).write_bytes(b"corrupt")
    elif corruption == "delta_bytes":
        next((delta / "objects").iterdir()).write_bytes(b"corrupt")
    elif corruption == "base_manifest":
        (base / "snapshot.json").write_text("{}")
    elif corruption == "missing_base":
        (base / "snapshot.json").unlink()
    else:
        metadata = json.loads((delta / "snapshot.json").read_text())
        metadata["base"]["path"] = "../../../../outside/objects"
        (delta / "snapshot.json").write_text(json.dumps(metadata))
    destination = "never-created-" + uuid4().hex
    with pytest.raises((ValueError, FileNotFoundError)):
        snapshot["restore"](delta, bucket=destination, create_bucket=True)
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=destination)


def test_legacy_full_snapshot_remains_a_restorable_incremental_base(
    pipeline, tmp_path, monkeypatch
):
    s3, calls = configure(pipeline, monkeypatch)
    put(s3, pipeline.history.bucket, "legacy", b"old")
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    metadata = snapshot["capture"](base, copy_objects=True)
    # The unchanged v1 inventory/file contract is exactly the old format.
    metadata["schema_version"] = 1
    for name in ("copied_object_count", "reused_object_count", "downloaded_bytes"):
        metadata.pop(name)
    (base / "snapshot.json").write_text(json.dumps(metadata))
    assert snapshot["verify"](base)["schema_version"] == 1
    calls.clear()
    result = snapshot["capture"](delta, copy_objects=True, base=base)
    assert result["copied_object_count"] == len(calls) == 0
    assert (
        snapshot["restore"](base, bucket="legacy-restore-" + uuid4().hex, create_bucket=True)[
            "restored_objects"
        ]
        == 1
    )
    assert (
        snapshot["restore"](delta, bucket="delta-restore-" + uuid4().hex, create_bucket=True)[
            "restored_objects"
        ]
        == 1
    )


@pytest.mark.parametrize("race", ["deleted", "changed", "same_size_wrong_bytes"])
def test_source_change_after_checkpoint_leaves_no_complete_backup(
    pipeline, tmp_path, monkeypatch, race
):
    s3, _calls = configure(pipeline, monkeypatch)
    put(s3, pipeline.history.bucket, "racing", b"expected")
    real_get = s3.get_object
    altered = False

    def racing(**kwargs):
        nonlocal altered
        if not altered:
            altered = True
            if race == "deleted":
                s3.delete_object(Bucket=pipeline.history.bucket, Key="racing")
            elif race == "changed":
                put(s3, pipeline.history.bucket, "racing", b"changed")
            else:
                response = real_get(**kwargs)
                from io import BytesIO

                response["Body"].close()
                response["Body"] = BytesIO(b"corrupt!")
                return response
        return real_get(**kwargs)

    monkeypatch.setattr(s3, "get_object", racing)
    output = tmp_path / "failed/objects"
    with pytest.raises((ValueError, ClientError)):
        snapshot["capture"](output, copy_objects=True)
    assert json.loads((output / "snapshot.json").read_text())["complete"] is False
    with pytest.raises(ValueError):
        snapshot["verify"](output)


def test_version_pin_copies_checkpoint_bytes_after_later_overwrite(pipeline, tmp_path, monkeypatch):
    s3, _calls = configure(pipeline, monkeypatch)
    bucket = pipeline.history.bucket
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    put(s3, bucket, "pinned", b"before")
    real_get = s3.get_object
    changed = False

    def overwriting(**kwargs):
        nonlocal changed
        if not changed:
            changed = True
            put(s3, bucket, "pinned", b"after")
        return real_get(**kwargs)

    monkeypatch.setattr(s3, "get_object", overwriting)
    output = tmp_path / "pinned/objects"
    snapshot["capture"](output, copy_objects=True)
    row = json.loads((output / "objects.jsonl").read_text())
    assert (output / "objects" / row["file"]).read_bytes() == b"before"


def test_checkpoint_fences_retention_then_releases_before_copy_and_ignores_retired_objects(
    pipeline, tmp_path, monkeypatch
):
    s3, _calls = configure(pipeline, monkeypatch)
    timestamp = now().isoformat()
    pipeline.import_records(
        [
            {
                "source": "backup_fixture",
                "entity_type": "video",
                "entity_id": "v",
                "synthetic": True,
                "observed_at": timestamp,
                "available_at": timestamp,
                "metrics": {},
            }
        ],
        synthetic=True,
    )
    put(s3, pipeline.history.bucket, "pending-deletion", b"retired")
    pipeline.catalog.execute(
        "INSERT INTO retention_objects(object_key,reason) VALUES('pending-deletion','test')"
    )
    ready, proceed = tmp_path / "ready.json", tmp_path / "continue"
    real_get = s3.get_object
    guard_pids = []
    guard = snapshot["capture"].__globals__["_checkpoint_guard"]

    @contextmanager
    def recorded_guard(enabled):
        with guard(enabled) as connection:
            guard_pids.append(connection.info.backend_pid)
            yield connection

    monkeypatch.setitem(snapshot["capture"].__globals__, "_checkpoint_guard", recorded_guard)
    released_during_copy = []

    def guard_is_held():
        with psycopg.connect(
            pipeline.settings.database_url.get_secret_value(), autocommit=True
        ) as connection:
            return connection.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s "
                "AND locktype='advisory' AND mode='ShareLock' AND granted)",
                (guard_pids[0],),
            ).fetchone()[0]

    def checked_get(**kwargs):
        released_during_copy.append(not guard_is_held())
        return real_get(**kwargs)

    monkeypatch.setattr(s3, "get_object", checked_get)
    with ThreadPoolExecutor(max_workers=1) as workers:
        task = workers.submit(
            snapshot["capture"],
            tmp_path / "guarded/objects",
            copy_objects=True,
            checkpoint_signal=ready,
            continue_signal=proceed,
            deadline_seconds=10,
        )
        import time

        for _ in range(100):
            if ready.exists() or task.done():
                break
            time.sleep(0.02)
        if task.done():
            task.result()
        assert ready.exists()
        assert guard_is_held()
        # Simulate the completed SQL dump and writer restart handshake.
        proceed.write_text("checkpoint complete")
        result = task.result(timeout=10)
    assert result["object_count"] == 3
    assert released_during_copy and all(released_during_copy)
    assert "pending-deletion" not in (tmp_path / "guarded/objects/objects.jsonl").read_text()


def test_guarded_copy_covers_assessments_frozen_decisions_and_staged_recovery(
    pipeline, tmp_path, monkeypatch
):
    from test_evaluation_integration import seed_product

    from sm_trending_products.pipeline import Pipeline

    s3, calls = configure(pipeline, monkeypatch)
    seed_product(pipeline, "before-backup")
    assessment = pipeline.assess(synthetic=True)
    frozen = pipeline.freeze(now(), synthetic=True)
    recovery = runpy.run_path(str(Path(__file__).parents[1] / "scripts/recovery_fixture.py"))
    staged = recovery["seed"](pipeline)
    before = pipeline.catalog.export_state()
    ready, proceed = tmp_path / "ready.json", tmp_path / "continue"
    proceed.write_text("checkpoint complete")
    output = tmp_path / "full/objects"
    result = snapshot["capture"](
        output, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
    )
    assert result["copied_object_count"] == result["object_count"]
    target = "checkpoint-restore-" + uuid4().hex
    snapshot["restore"](output, bucket=target, create_bucket=True)
    restored_settings = pipeline.settings.model_copy(
        update={"s3_bucket": target, "cache_dir": tmp_path / "restored-cache"}
    )
    with Pipeline(restored_settings) as restored:
        assert restored.verify()["ok"]
        assert restored.catalog.export_state() == before
        assert restored.evaluation(frozen["evaluation_id"], True)["payload"] == frozen["result"]
        assert canonical(restored.assessment_snapshot(assessment["run_id"], True)) == canonical(
            assessment
        )
    resumed = recovery["resume"](restored_settings)
    assert resumed["dataset_id"] == staged["dataset_id"]
    assert resumed["source_requests"] == resumed["restart_duplicate_observations"] == 0


def test_catalog_checksum_overrides_mutated_source_checksum_metadata(
    pipeline, tmp_path, monkeypatch
):
    s3, _calls = configure(pipeline, monkeypatch)
    timestamp = now().isoformat()
    dataset = pipeline.import_records(
        [
            {
                "source": "backup_fixture",
                "entity_type": "video",
                "entity_id": "v",
                "synthetic": True,
                "observed_at": timestamp,
                "available_at": timestamp,
                "metrics": {},
            }
        ],
        synthetic=True,
    )[0]
    manifest = pipeline.catalog.one(
        "SELECT manifest FROM datasets WHERE dataset_id=%s", (dataset,)
    )["manifest"]
    # A corrupted object and matching changed provider metadata do not redefine
    # the immutable checksum frozen in the authoritative dataset manifest.
    put(s3, pipeline.history.bucket, manifest["raw"]["key"], b"x" * manifest["raw"]["bytes"])
    ready, proceed = tmp_path / "ready.json", tmp_path / "continue"
    proceed.write_text("checkpoint complete")
    output = tmp_path / "invalid/objects"
    with pytest.raises(ValueError, match="content mismatch|checksum metadata mismatch"):
        snapshot["capture"](
            output, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
        )
    assert json.loads((output / "snapshot.json").read_text())["complete"] is False


def test_chain_limit_rejects_before_publishing_an_unrestorable_completed_delta(
    pipeline, tmp_path, monkeypatch
):
    s3, _calls = configure(pipeline, monkeypatch)
    put(s3, pipeline.history.bucket, "unchanged", b"same")
    base = None
    for index in range(snapshot["MAX_CHAIN_DEPTH"]):
        output = tmp_path / f"member-{index:02d}" / "objects"
        result = snapshot["capture"](output, copy_objects=True, base=base)
        assert result["complete"]
        base = output
    assert snapshot["verify"](base)["object_count"] == 1
    rejected = tmp_path / "excessive/objects"
    with pytest.raises(ValueError, match="chain limit"):
        snapshot["capture"](rejected, copy_objects=True, base=base)
    assert json.loads((rejected / "snapshot.json").read_text())["complete"] is False


@pytest.mark.parametrize("with_checksum", [False, True])
def test_committed_derived_dependencies_are_required_and_checked_on_old_and_new_catalogs(
    pipeline, tmp_path, monkeypatch, with_checksum
):
    s3, _calls = configure(pipeline, monkeypatch)
    key = "assessments/synthetic/provenance/required-node.json"
    pipeline.catalog.stage_derivation(key, [])
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (key,)
    )
    if with_checksum:
        pipeline.catalog.execute(
            "ALTER TABLE derived_publications ADD COLUMN IF NOT EXISTS object_sha256 text,"
            "ADD COLUMN IF NOT EXISTS object_bytes bigint"
        )
        pipeline.catalog.execute(
            "UPDATE derived_publications SET object_sha256=%s,object_bytes=%s WHERE object_key=%s",
            (hashlib.sha256(b"correct").hexdigest(), len(b"correct"), key),
        )
    ready, proceed = tmp_path / "ready.json", tmp_path / "continue"
    proceed.write_text("checkpoint complete")
    absent = tmp_path / "absent/objects"
    with pytest.raises(ValueError, match="absent from checkpoint"):
        snapshot["capture"](
            absent, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
        )
    assert not ready.exists()
    assert json.loads((absent / "snapshot.json").read_text())["complete"] is False
    put(s3, pipeline.history.bucket, key, b"correct")
    complete = tmp_path / "complete/objects"
    assert snapshot["capture"](
        complete, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
    )["complete"]
    if with_checksum:
        put(
            s3, pipeline.history.bucket, key, b"corrupt"
        )  # Same size and updated provider metadata.
        corrupt = tmp_path / "corrupt/objects"
        with pytest.raises(ValueError, match="content mismatch|checksum metadata mismatch"):
            snapshot["capture"](
                corrupt, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
            )
        assert json.loads((corrupt / "snapshot.json").read_text())["complete"] is False


def test_public_artifact_graph_and_local_windows_survive_incremental_object_restore(
    pipeline, tmp_path, monkeypatch
):
    from test_sharded_results import seed

    from sm_trending_products.history import History
    from sm_trending_products.provenance import Provenance

    cutoff, records, _ = seed(pipeline, videos=2, points=12)
    row = pipeline.assess(cutoff, True)["rows"][0]
    root = row["evidence_refs"]
    window = pipeline.catalog.one(
        "SELECT descriptor FROM feature_product_windows WHERE product_id='p0' AND state='ready'"
    )
    assert window
    local_key = window["descriptor"]["key"]
    pipeline.catalog.execute(
        "INSERT INTO retention_objects(object_key,state,local_only,reason) "
        "VALUES(%s,'deleted',true,'fixture_retirement')",
        ("local-only-retired-fixture",),
    )
    s3, calls = configure(pipeline, monkeypatch)
    keys = {item["Key"] for item in s3.list_objects_v2(Bucket=pipeline.history.bucket)["Contents"]}
    assert local_key not in keys
    leaf = pipeline.catalog.one(
        "SELECT p.object_key FROM derived_publications p WHERE p.state='committed' "
        "AND p.object_key LIKE 'provenance/%%' AND NOT EXISTS("
        "SELECT 1 FROM derived_artifact_edges e WHERE e.parent_id=p.object_id) LIMIT 1"
    )
    key = leaf["object_key"]
    original = s3.get_object(Bucket=pipeline.history.bucket, Key=key)["Body"].read()
    ready, proceed = tmp_path / "ready.json", tmp_path / "continue"
    proceed.write_text("checkpoint complete")
    s3.delete_object(Bucket=pipeline.history.bucket, Key=key)
    with pytest.raises(ValueError, match="absent from checkpoint"):
        snapshot["capture"](
            tmp_path / "missing/objects",
            copy_objects=True,
            checkpoint_signal=ready,
            continue_signal=proceed,
        )
    put(s3, pipeline.history.bucket, key, b"x" * len(original))
    with pytest.raises(ValueError, match="content mismatch|checksum metadata mismatch"):
        snapshot["capture"](
            tmp_path / "corrupt/objects",
            copy_objects=True,
            checkpoint_signal=ready,
            continue_signal=proceed,
        )
    put(s3, pipeline.history.bucket, key, original)
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    assert snapshot["capture"](
        base, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
    )["complete"]
    calls.clear()
    assert (
        snapshot["capture"](
            delta,
            copy_objects=True,
            base=base,
            checkpoint_signal=ready,
            continue_signal=proceed,
        )["downloaded_bytes"]
        == 0
    )
    assert not calls
    destination = "graph-restore-" + uuid4().hex
    snapshot["restore"](delta, bucket=destination, create_bucket=True)
    restored = History(
        pipeline.settings.model_copy(
            update={"s3_bucket": destination, "cache_dir": tmp_path / "restored-cache"}
        ),
        pipeline.catalog,
    )
    page = Provenance(pipeline.catalog, restored).page(root, limit=100)
    assert set(page["references"]) == {record["observation_id"] for record in records}
