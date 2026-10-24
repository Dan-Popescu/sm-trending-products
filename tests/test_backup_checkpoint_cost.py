"""Object inventories do not hold writer checkpoints across per-object HEADs."""

import hashlib
import json
from contextlib import contextmanager
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from test_object_backup import configure, put, snapshot
from test_sharded_results import seed

pytestmark = pytest.mark.integration


def checkpoint(pipeline, tmp_path, monkeypatch):
    s3, gets = configure(pipeline, monkeypatch)
    original = snapshot["capture"].__globals__["_checkpoint_guard"]
    state = {"checkpoint": False, "heads": 0}
    head = s3.head_object

    @contextmanager
    def guard(enabled):
        with original(enabled) as connection:
            state["checkpoint"] = True
            try:
                yield connection
            finally:
                state["checkpoint"] = False

    def counted_head(**kwargs):
        assert not state["checkpoint"], "Remote HEAD must follow writer checkpoint release"
        state["heads"] += 1
        assert "IfMatch" in kwargs
        return head(**kwargs)

    monkeypatch.setitem(snapshot["capture"].__globals__, "_checkpoint_guard", guard)
    monkeypatch.setattr(s3, "head_object", counted_head)
    proceed = tmp_path / "continue"
    proceed.write_text("writers resumed")
    options = {"checkpoint_signal": tmp_path / "ready", "continue_signal": proceed}
    return s3, gets, state, options


def test_managed_unchanged_inventory_reuses_verified_base_without_heads_or_gets(
    pipeline, tmp_path, monkeypatch
):
    cutoff, _, _ = seed(pipeline, videos=2, points=4)
    pipeline.assess(cutoff, True)
    s3, gets, state, options = checkpoint(pipeline, tmp_path, monkeypatch)
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    first = snapshot["capture"](base, copy_objects=True, **options)
    assert first["head_requests"] == state["heads"] == first["object_count"] > 0
    gets.clear()
    state["heads"] = 0
    second = snapshot["capture"](delta, copy_objects=True, base=base, **options)
    assert state["heads"] == second["head_requests"] == len(gets) == 0
    assert second["catalog_reused_object_count"] == second["object_count"]
    assert second["reused_object_count"] == first["object_count"]
    assert second["list_requests"] == 1
    assert snapshot["verify"](delta)["complete"]


def test_unmanaged_objects_still_inspect_after_checkpoint_and_reuse_only_verified_bytes(
    pipeline, tmp_path, monkeypatch
):
    seed(pipeline)
    s3, gets, state, options = checkpoint(pipeline, tmp_path, monkeypatch)
    put(s3, pipeline.history.bucket, "unmanaged", b"noncatalog bytes")
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    snapshot["capture"](base, copy_objects=True, **options)
    gets.clear()
    state["heads"] = 0
    second = snapshot["capture"](delta, copy_objects=True, base=base, **options)
    assert state["heads"] == second["head_requests"] == 1
    assert second["catalog_reused_object_count"] == second["object_count"] - 1
    assert len(gets) == second["copied_object_count"] == 0


@pytest.mark.parametrize("mutation", ["missing", "corrupt"])
def test_verified_base_does_not_mask_missing_or_changed_catalog_leaf(
    pipeline, tmp_path, monkeypatch, mutation
):
    cutoff, _, _ = seed(pipeline)
    pipeline.assess(cutoff, True)
    s3, _, _, options = checkpoint(pipeline, tmp_path, monkeypatch)
    base = tmp_path / "base/objects"
    snapshot["capture"](base, copy_objects=True, **options)
    leaf = pipeline.catalog.one(
        "SELECT object_key,object_bytes FROM derived_publications "
        "WHERE object_key LIKE 'provenance/%%' AND state='committed' LIMIT 1"
    )
    key = leaf["object_key"]
    if mutation == "missing":
        s3.delete_object(Bucket=pipeline.history.bucket, Key=key)
    else:
        put(s3, pipeline.history.bucket, key, b"x" * leaf["object_bytes"])
    failed = tmp_path / "failed/objects"
    with pytest.raises(ValueError, match="absent from checkpoint|checksum metadata mismatch"):
        snapshot["capture"](failed, copy_objects=True, base=base, **options)
    assert not json.loads((failed / "snapshot.json").read_text())["complete"]


def test_legacy_base_without_list_fingerprint_is_inspected_once_before_catalog_reuse(
    pipeline, tmp_path, monkeypatch
):
    seed(pipeline)
    _, gets, state, options = checkpoint(pipeline, tmp_path, monkeypatch)
    base, delta, latest = (tmp_path / name / "objects" for name in ("base", "delta", "latest"))
    snapshot["capture"](base, copy_objects=True, **options)
    inventory = base / "objects.jsonl"
    records = [json.loads(line) for line in inventory.read_text().splitlines()]
    for record in records:
        record.pop("last_modified")
    inventory.write_text("".join(json.dumps(record) + "\n" for record in records))
    metadata = json.loads((base / "snapshot.json").read_text())
    metadata["schema_version"] = 1
    metadata["inventory_sha256"] = hashlib.sha256(inventory.read_bytes()).hexdigest()
    (base / "snapshot.json").write_text(json.dumps(metadata))
    gets.clear()
    state["heads"] = 0
    first_delta = snapshot["capture"](delta, copy_objects=True, base=base, **options)
    assert state["heads"] == first_delta["object_count"]
    assert not gets
    state["heads"] = 0
    next_delta = snapshot["capture"](latest, copy_objects=True, base=delta, **options)
    assert not gets and state["heads"] == next_delta["head_requests"] == 0


@pytest.mark.parametrize("versioned", [False, True])
def test_source_change_before_deferred_head_refuses_instead_of_capturing_later_content(
    pipeline, tmp_path, monkeypatch, versioned
):
    s3, _ = configure(pipeline, monkeypatch)
    bucket = pipeline.history.bucket
    if versioned:
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    put(s3, bucket, "racing", b"checkpoint")
    original = s3.head_object
    changed = False

    def racing(**kwargs):
        nonlocal changed
        if not changed:
            changed = True
            put(s3, bucket, "racing", b"later bytes")
        return original(**kwargs)

    monkeypatch.setattr(s3, "head_object", racing)
    output = tmp_path / "failed/objects"
    with pytest.raises((ValueError, ClientError)):
        snapshot["capture"](output, copy_objects=True)
    assert not json.loads((output / "snapshot.json").read_text())["complete"]


def test_unmanaged_changed_checksum_metadata_cannot_inherit_a_verified_old_hash(
    pipeline, tmp_path, monkeypatch
):
    s3, calls = configure(pipeline, monkeypatch)
    bucket = pipeline.history.bucket
    put(s3, bucket, "legacy", b"same")
    base = tmp_path / "base/objects"
    snapshot["capture"](base, copy_objects=True)
    s3.put_object(Bucket=bucket, Key="legacy", Body=b"same", Metadata={"sha256": "0" * 64})
    calls.clear()
    output = tmp_path / "failed/objects"
    with pytest.raises(ValueError, match="content mismatch"):
        snapshot["capture"](output, copy_objects=True, base=base)
    assert len(calls) == 1
    assert not json.loads((output / "snapshot.json").read_text())["complete"]


@pytest.mark.parametrize("metadata", [{}, {"sha256": "0" * 64}])
def test_known_catalog_checksum_rejects_missing_or_wrong_restorable_metadata(
    pipeline, tmp_path, monkeypatch, metadata
):
    _, _, datasets = seed(pipeline)
    s3, _ = configure(pipeline, monkeypatch)
    raw = pipeline.catalog.one(
        "SELECT manifest->'raw' AS raw FROM datasets WHERE dataset_id=%s", (datasets[0],)
    )["raw"]
    original = s3.get_object(Bucket=pipeline.history.bucket, Key=raw["key"])["Body"].read()
    s3.put_object(Bucket=pipeline.history.bucket, Key=raw["key"], Body=original, Metadata=metadata)
    ready, proceed = tmp_path / "ready", tmp_path / "continue"
    proceed.write_text("writers resumed")
    output = tmp_path / "failed/objects"
    with pytest.raises(ValueError, match="checksum metadata mismatch"):
        snapshot["capture"](
            output, copy_objects=True, checkpoint_signal=ready, continue_signal=proceed
        )
    assert not json.loads((output / "snapshot.json").read_text())["complete"]


def test_bad_base_checksum_metadata_is_reinspected_and_repaired_before_reuse(
    pipeline, tmp_path, monkeypatch
):
    seed(pipeline)
    s3, gets, state, options = checkpoint(pipeline, tmp_path, monkeypatch)
    base, delta = tmp_path / "base/objects", tmp_path / "delta/objects"
    snapshot["capture"](base, copy_objects=True, **options)
    inventory = base / "objects.jsonl"
    records = [json.loads(line) for line in inventory.read_text().splitlines()]
    records[0]["metadata"]["sha256"] = "0" * 64
    inventory.write_text("".join(json.dumps(record) + "\n" for record in records))
    metadata = json.loads((base / "snapshot.json").read_text())
    metadata["inventory_sha256"] = hashlib.sha256(inventory.read_bytes()).hexdigest()
    (base / "snapshot.json").write_text(json.dumps(metadata))
    destination = "bad-metadata-restore-" + uuid4().hex
    with pytest.raises(ValueError, match="checksum metadata mismatch"):
        snapshot["restore"](base, bucket=destination, create_bucket=True)
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=destination)
    gets.clear()
    state["heads"] = 0
    result = snapshot["capture"](delta, copy_objects=True, base=base, **options)
    assert state["heads"] == result["head_requests"] == 1
    assert not gets
    repaired = [json.loads(line) for line in (delta / "objects.jsonl").read_text().splitlines()]
    assert all(record["metadata"]["sha256"] == record["sha256"] for record in repaired)
