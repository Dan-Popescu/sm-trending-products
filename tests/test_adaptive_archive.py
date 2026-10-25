"""Pre-upload cost selection, exact template packing and bounded selected reads."""

from __future__ import annotations

import gzip
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from test_assessment_archive import logical_body, publish, stored_header
from test_object_backup import configure, snapshot
from test_sharded_results import seed

import sm_trending_products.assessment_archive as archive_module
import sm_trending_products.provenance as provenance_module
from sm_trending_products.assessment_archive import (
    PACKED_REPRESENTATION,
    REPRESENTATION,
    _pack,
    _split,
    _unpack,
)
from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.provenance import MAX_NODE_BYTES, Provenance


def test_template_retains_types_signed_zero_missing_fields_and_unicode():
    rows = [
        {"id": "été", "typed": 1, "zero": -0.0, "nested": {"same": [True, None]}},
        {"id": "ß", "typed": 1.0, "zero": 0.0, "nested": {"same": [True, None]}},
        {"id": "雪", "typed": True, "zero": 0, "nested": {"same": [True, None]}},
        {"id": "missing", "new": None},
    ]
    original = deepcopy(rows)
    packed = json.loads(canonical(_pack(rows)))
    assert canonical(_unpack(packed, [3, 0, 1, 2])) == canonical([rows[i] for i in (3, 0, 1, 2)])
    assert rows == original
    restored = _unpack(json.loads(canonical(_pack(rows[:3]))), [0, 1, 2])
    restored[0]["nested"]["same"].append("changed")
    assert restored[1]["nested"]["same"] == [True, None]
    assert rows == original


def test_template_preserves_complete_provenance_descriptors_for_retirement_edges():
    rows = [
        {
            "id": str(index),
            "evidence_refs": {
                "format": "sharded-provenance-v1",
                "root": {
                    "key": f"provenance/synthetic/{index}",
                    "sha256": str(index),
                    "bytes": 100,
                    "count": 1,
                },
                "count": 1,
            },
        }
        for index in range(3)
    ]
    assert Provenance.roots(_pack(rows)) == Provenance.roots(rows)


def test_packed_encoding_is_independent_of_dictionary_order_from_jsonb():
    rows = logical_body(100)["rows"]

    def reversed_keys(value):
        if isinstance(value, dict):
            return {key: reversed_keys(value[key]) for key in reversed(value)}
        if isinstance(value, list):
            return [reversed_keys(child) for child in value]
        return value

    assert canonical(_pack(rows)) == canonical(_pack(reversed_keys(rows)))


def test_numeric_key_maps_remain_opaque_and_reconstruct_after_json_round_trip(pipeline):
    body = logical_body(100)
    for index, row in enumerate(body["rows"]):
        row["unknown"] = {1: index}
    packed = json.loads(canonical(_pack(body["rows"])))
    assert canonical(_unpack(packed, [0, 10, 99])) == canonical(
        [body["rows"][index] for index in (0, 10, 99)]
    )
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == PACKED_REPRESENTATION
    assert canonical(pipeline.assessment_snapshot(publication.run_id, True)) == canonical(
        {"run_id": publication.run_id, **body}
    )


def test_numeric_key_types_and_canonical_default_values_are_not_conflated(pipeline):
    body = logical_body(100)
    keys = (1, 1.0, True)
    for index, row in enumerate(body["rows"]):
        row["unknown"] = {keys[index % 3]: "same value"}
        row["decimal"] = Decimal("1.0" if index % 2 else "1.00")
        row["timestamp"] = datetime(2026, 1, 1, tzinfo=UTC).astimezone(
            timezone(timedelta(hours=index % 2))
        )
    packed = json.loads(canonical(_pack(body["rows"])))
    assert canonical(_unpack(packed, list(range(100)))) == canonical(body["rows"])
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == PACKED_REPRESENTATION
    assert canonical(pipeline.assessment_snapshot(publication.run_id, True)) == canonical(
        {"run_id": publication.run_id, **body}
    )


def test_preupload_plan_publishes_only_selected_nodes_and_keeps_bridge_reads(pipeline, monkeypatch):
    body = logical_body(1000)
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=40")
    legacy = publish(pipeline, body)
    assert stored_header(pipeline, legacy)["representation"] == REPRESENTATION
    pipeline.catalog.execute("INSERT INTO schema_migrations(version) VALUES(40)")
    body = deepcopy(body)
    body["as_of"] += timedelta(hours=1)
    for index, row in enumerate(body["rows"]):
        if index % 20 == 0:
            row["unknown_future_field"] += "new information"
    plans = []
    snapshots = {}
    original = pipeline._assessment_archive.provenance.plan

    def capture_plan():
        plan = original()
        plans.append(plan)
        original_cost = plan.cost

        def cost():
            snapshots[id(plan)] = set(plan.nodes)
            return original_cost()

        plan.cost = cost
        return plan

    monkeypatch.setattr(pipeline._assessment_archive.provenance, "plan", capture_plan)
    before = {
        row["object_key"]
        for row in pipeline.catalog.all("SELECT object_key FROM derived_publications")
    }
    publication = publish(pipeline, body)
    header = stored_header(pipeline, publication)
    chosen = plans[1] if header["representation"] == PACKED_REPRESENTATION else plans[0]
    other = plans[0] if chosen is plans[1] else plans[1]
    after = {
        row["object_key"]
        for row in pipeline.catalog.all("SELECT object_key FROM derived_publications")
    }
    written = after - before - {publication.descriptor["key"]}
    assert written <= snapshots[id(chosen)]
    assert not written & (snapshots[id(other)] - snapshots[id(chosen)])
    assert all(plan.spool.closed and not plan.nodes for plan in plans)
    assert header["representation"] == PACKED_REPRESENTATION
    for saved in (legacy, publication):
        assert pipeline.assessment_snapshot(saved.run_id, True)["rows"] == saved.body["rows"]


def test_packed_pages_expand_only_selected_rows_and_split_large_variable_blocks(
    pipeline, monkeypatch
):
    body = logical_body(405)
    for index, row in enumerate(body["rows"]):
        row["variable"] = str(index) * 2500
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == PACKED_REPRESENTATION
    expanded = []
    original = archive_module._unpack

    def unpack(packed, positions):
        expanded.extend(positions)
        return original(packed, positions)

    monkeypatch.setattr(archive_module, "_unpack", unpack)
    with Pipeline(pipeline.settings) as restarted:
        page = restarted.assessment_run_page(publication.run_id, True, offset=199, limit=2)
        assert page["rows"] == body["rows"][199:201]
        assert len(expanded) == 2
    for row in pipeline.catalog.all(
        "SELECT object_key FROM derived_publications WHERE object_key LIKE 'provenance/%%'"
    ):
        response = pipeline.history.client.get_object(
            Bucket=pipeline.history.bucket, Key=row["object_key"]
        )
        with response["Body"] as stream:
            raw = gzip.decompress(stream.read())
        assert len(raw) <= MAX_NODE_BYTES
    assert canonical(pipeline.assessment_snapshot(publication.run_id, True)) == canonical(
        {"run_id": publication.run_id, **body}
    )


def test_adaptive_product_insertion_and_shallow_split_do_not_mutate_inputs(pipeline):
    body = logical_body(405)
    first = publish(pipeline, body)
    next_body = deepcopy(body)
    next_body["as_of"] += timedelta(hours=1)
    next_body["rows"].insert(20, {"product_id": "new-first", "name": "New"})
    expected = canonical(next_body)
    second = publish(pipeline, next_body)
    assert canonical(next_body) == expected
    assert canonical(pipeline.assessment_snapshot(second.run_id, True)) == canonical(
        {"run_id": second.run_id, **next_body}
    )
    assert pipeline.assessment_snapshot(first.run_id, True)["rows"] == body["rows"]


def test_packed_source_withdrawal_fences_staged_and_committed_archives(pipeline):
    cutoff, _records, datasets = seed(pipeline, products=100)
    first = pipeline.assess(cutoff, True)
    key = pipeline.catalog.one(
        "SELECT object_key FROM assessment_runs WHERE run_id=%s", (first["run_id"],)
    )["object_key"]
    response = pipeline.history.client.get_object(Bucket=pipeline.history.bucket, Key=key)
    with response["Body"] as stream:
        assert json.loads(gzip.decompress(stream.read()))["representation"] == PACKED_REPRESENTATION
    body = {key: value for key, value in first.items() if key != "run_id"}
    body["as_of"] += timedelta(hours=1)
    scope = pipeline.provenance.publication_scope([body["input_provenance"]], body["rows"], True)
    staged = pipeline._assessment_archive.stage(body, scope)
    pipeline.catalog.invalidate_dataset(datasets[0], "withdrawal")
    with (
        pipeline.catalog.transaction() as connection,
        pytest.raises(ValueError, match="invalidated"),
    ):
        pipeline._assessment_archive.commit(connection, staged)
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.assessment_run_page(first["run_id"], True)


def test_plan_cost_spools_without_publication_or_namespace_authority(pipeline):
    before = pipeline.history.metrics["put_requests"]
    with pipeline.provenance.plan() as plan:
        plan.records({str(i): {"value": str(i) * 100} for i in range(200)}, True)
        assert plan.cost()[1] > 0
        assert pipeline.history.metrics["put_requests"] == before
    assert pipeline.catalog.one("SELECT count(*) AS count FROM derived_publications")["count"] == 0


def test_near_limit_legacy_row_falls_back_without_uploading_an_invalid_candidate(pipeline):
    body = logical_body(32)
    body["rows"][0] = {"product_id": "near-limit", "large": "x" * (MAX_NODE_BYTES - 200)}
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == REPRESENTATION
    assert pipeline.assessment_snapshot(publication.run_id, True)["rows"] == body["rows"]


def test_candidate_header_overflow_preserves_valid_ordinary_header(pipeline):
    body = logical_body(100)
    body["padding"] = ""
    stable, ranks = {}, []
    for row in body["rows"]:
        stable[row["product_id"]], mobile = _split(row)
        ranks.append({"product_id": row["product_id"], "mobile": mobile})
    archive = pipeline._assessment_archive
    with archive.provenance.plan() as current, archive.provenance.plan() as packed:
        ordinary = archive._storage(body, "0" * 64, stable, ranks, True, current)
        candidate = archive._storage(body, "0" * 64, stable, ranks, True, packed, packed=True)
        ordinary_size = len(canonical(ordinary).encode())
        assert len(canonical(candidate).encode()) > ordinary_size
    body["padding"] = "x" * (MAX_NODE_BYTES - ordinary_size)
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == REPRESENTATION
    assert canonical(pipeline.assessment_snapshot(publication.run_id, True)) == canonical(
        {"run_id": publication.run_id, **body}
    )


def test_packed_archive_round_trips_through_object_backup_with_empty_cache(
    pipeline, tmp_path, monkeypatch
):
    configure(pipeline, monkeypatch)
    body = logical_body(405)
    publication = publish(pipeline, body)
    assert stored_header(pipeline, publication)["representation"] == PACKED_REPRESENTATION
    output = tmp_path / "objects"
    snapshot["capture"](output, copy_objects=True)
    target = "packed-restore-" + uuid4().hex
    snapshot["restore"](output, bucket=target, create_bucket=True)
    settings = pipeline.settings.model_copy(
        update={"s3_bucket": target, "cache_dir": tmp_path / "restored"}
    )
    with Pipeline(settings) as restored:
        assert canonical(restored.assessment_snapshot(publication.run_id, True)) == canonical(
            {"run_id": publication.run_id, **body}
        )


@pytest.mark.parametrize("byte_budget,node_budget", [(512, 1000), (4_000_000, 1)])
def test_planning_limit_closes_candidates_before_ordinary_fallback_and_keeps_source_guards(
    pipeline, monkeypatch, byte_budget, node_budget
):
    cutoff, _records, datasets = seed(pipeline, products=100)
    monkeypatch.setattr(provenance_module, "PUBLICATION_PLAN_BYTES", byte_budget)
    monkeypatch.setattr(provenance_module, "PUBLICATION_PLAN_NODES", node_budget)
    plans = []
    original_plan = pipeline._assessment_archive.provenance.plan
    original_publish = pipeline._assessment_archive.provenance._publish_node

    def capture_plan():
        plan = original_plan()
        plans.append(plan)
        return plan

    def publish_node(*args, **kwargs):
        assert plans and all(plan.spool.closed and not plan.nodes for plan in plans)
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(pipeline._assessment_archive.provenance, "plan", capture_plan)
    monkeypatch.setattr(pipeline._assessment_archive.provenance, "_publish_node", publish_node)
    first = pipeline.assess(cutoff, True)
    assert len(plans) == 2 and all(plan.spool.closed for plan in plans)
    if node_budget == 1:
        assert any(plan.bytes > 0 for plan in plans)
    key = pipeline.catalog.one(
        "SELECT object_key FROM assessment_runs WHERE run_id=%s", (first["run_id"],)
    )["object_key"]
    response = pipeline.history.client.get_object(Bucket=pipeline.history.bucket, Key=key)
    with response["Body"] as stream:
        assert json.loads(gzip.decompress(stream.read()))["representation"] == REPRESENTATION
    with Pipeline(pipeline.settings) as reopened:
        assert canonical(reopened.assessment_snapshot(first["run_id"], True)) == canonical(first)
    pipeline.catalog.invalidate_dataset(datasets[0], "withdrawal")
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.assessment_run_page(first["run_id"], True)
