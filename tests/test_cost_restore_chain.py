"""The real resolver requires expired parent-owned bytes before restoring a delta."""

import hashlib
import json
import runpy
from pathlib import Path

import pytest


def test_expired_parent_payload_and_each_inventory_are_required_by_the_real_resolver(tmp_path):
    snapshot = runpy.run_path(str(Path(__file__).parents[1] / "scripts/object_snapshot.py"))
    base, delta = (tmp_path / name / "objects" for name in ("base", "delta"))
    expired, live = b"expired parent payload", b"current live payload"

    def record(key, body, inherited=False):
        return {
            "key": key,
            "file": hashlib.sha256(key.encode()).hexdigest(),
            "sha256": hashlib.sha256(body).hexdigest(),
            "size": len(body),
            "from_base": inherited,
        }

    def write(path, records, parent=None):
        (path / "objects").mkdir(parents=True)
        inventory = b"".join((json.dumps(item) + "\n").encode() for item in records)
        (path / "objects.jsonl").write_bytes(inventory)
        metadata = {
            "schema_version": 2 if parent else 1,
            "complete": True,
            "copied_objects": True,
            "source_bucket": "fixture",
            "source_endpoint": "fixture",
            "namespace": "testing",
            "inventory_sha256": hashlib.sha256(inventory).hexdigest(),
            "object_count": len(records),
            "total_bytes": sum(item["size"] for item in records),
        }
        if parent:
            metadata["base"] = {
                "path": "../../base/objects",
                "snapshot_sha256": snapshot["file_hash"](parent / "snapshot.json"),
            }
        (path / "snapshot.json").write_text(json.dumps(metadata))

    expired_record, live_record = record("expired", expired), record("live", live)
    write(base, [expired_record, live_record])
    expired_file = base / "objects" / expired_record["file"]
    expired_file.write_bytes(expired)
    (base / "objects" / live_record["file"]).write_bytes(live)
    write(delta, [record("live", live, inherited=True)], parent=base)
    # A valid chain has no ancestor SQL dumps: only object lineage is traversed.
    with snapshot["ResolvedSnapshot"](delta) as resolved:
        assert resolved.depth == 2
        assert [item["key"] for item, _ in resolved.records()] == ["live"]
    expired_file.unlink()
    with pytest.raises(FileNotFoundError):
        with snapshot["ResolvedSnapshot"](delta):
            pass
    expired_file.write_bytes(expired)
    (base / "objects.jsonl").unlink()
    with pytest.raises(FileNotFoundError):
        with snapshot["ResolvedSnapshot"](delta):
            pass
