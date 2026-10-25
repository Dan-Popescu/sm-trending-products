"""Lossless shared archives through real SQL, HTTP objects, and source fences."""

from __future__ import annotations

import gzip
import hashlib
import json
import runpy
import sqlite3
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from test_feature_projection import video
from test_sharded_results import seed

import sm_trending_products.assessment_archive as archive_module
import sm_trending_products.provenance as provenance_module
from sm_trending_products.analytical_json import read_json
from sm_trending_products.assessment_archive import AssessmentArchive, _join, _split
from sm_trending_products.contracts import canonical, digest, now
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.provenance import PublicationScope


def logical_body(count=405, *, cutoff=None):
    cutoff = cutoff or now()
    return {
        "schema_version": "2",
        "as_of": cutoff,
        "synthetic": True,
        "policy": {},
        "rows": [
            {
                "product_id": f"product-{index:06}",
                "name": f"Product {index}",
                "as_of": cutoff.isoformat(),
                "score": index / 7,
                "features": {"age_hours": index / 9, "cohort_growth_percentile": index / count},
                "attention": {
                    "score": index / 7,
                    "components": {
                        "cohort_growth_percentile": {"value": index / count, "weight": 0.5}
                    },
                },
                "unknown_future_field": "retained useful detail " * 130,
            }
            for index in reversed(range(count))
        ],
        "input_provenance": [],
    }


def publish(pipeline, body):
    publication = pipeline._assessment_archive.stage(body, PublicationScope([], ()))
    with pipeline.catalog.transaction() as connection:
        pipeline._assessment_archive.commit(connection, publication)
    return publication


def stored_header(pipeline, publication):
    response = pipeline.history.client.get_object(
        Bucket=pipeline.history.bucket, Key=publication.descriptor["key"]
    )
    with response["Body"] as stream:
        return json.loads(gzip.decompress(stream.read()))


@pytest.mark.parametrize(
    "row",
    [
        {"product_id": "empty"},
        {"product_id": "unknown", "features": None, "attention": {}},
        logical_body(1)["rows"][0],
        {"as_of": None, "score": None, "features": {}, "attention": {"score": None}},
    ],
)
def test_split_reconstructs_exact_fields_including_missing_and_null(row):
    original = deepcopy(row)
    assert _join(*_split(row)) == original
    assert row == original


def test_decompression_has_an_independent_decoded_byte_limit():
    encoded = gzip.compress(b'"' + b"a" * 256_000 + b'"', mtime=0)
    with pytest.raises(ValueError, match="decoded byte budget"):
        read_json(encoded, compressed=True, max_bytes=256_000)


def test_new_archive_is_exact_after_restart_and_pages_follow_rank_boundaries(pipeline, monkeypatch):
    body = logical_body()
    publication = publish(pipeline, body)
    assert publication.run_id == digest(body)
    assert publication.descriptor["sha256"] != publication.run_id
    assert publication.descriptor["sha256"] in publication.descriptor["key"]
    with Pipeline(pipeline.settings) as restarted:
        assert canonical(restarted.assessment_snapshot(publication.run_id, True)) == canonical(
            {"run_id": publication.run_id, **body}
        )
        selected = []
        original = provenance_module.pin_cached

        def pinned(history, descriptor):
            selected.append(descriptor["key"])
            return original(history, descriptor)

        monkeypatch.setattr(provenance_module, "pin_cached", pinned)
        page = restarted.assessment_run_page(publication.run_id, True, offset=199, limit=2)
        assert page["rows"] == body["rows"][199:201]
        assert page["count"] == 405 and page["next_offset"] == 201
        # Two requested products visit at most two selected stable leaf paths;
        # no full-universe deserialization or separate SQL transaction per row.
        assert len(selected) <= 8
        assert len(set(selected)) == len(selected)
        full_page = restarted.assessment_run_page(publication.run_id, True, offset=100, limit=200)
        assert full_page["rows"] == body["rows"][100:300]
        assert full_page["next_offset"] == 300
        assert restarted.assessment_run_page(publication.run_id, True, offset=405)["rows"] == []
        assert restarted.assessment_run_page(publication.run_id, False) is None
        with pytest.raises(ValueError, match="pages require"):
            restarted.assessment_run_page(publication.run_id, True, limit=201)
    for row in pipeline.catalog.all(
        "SELECT object_key,object_sha256,object_bytes FROM derived_publications"
    ):
        response = pipeline.history.client.get_object(
            Bucket=pipeline.history.bucket, Key=row["object_key"]
        )
        with response["Body"] as stream:
            encoded = stream.read()
        assert hashlib.sha256(encoded).hexdigest() == row["object_sha256"]
        assert len(encoded) == row["object_bytes"]
    assert pipeline.verify()["ok"]


def test_inventory_stream_bounds_python_memory_and_releases_source_snapshot(pipeline):
    tracemalloc.start()
    try:
        with closing(
            pipeline.catalog.stream(
                "SELECT n,repeat('x',1024) AS payload,pg_backend_pid() AS backend "
                "FROM generate_series(1,10000) AS n"
            )
        ) as inventory:
            first = next(inventory)
            assert first["n"] == 1
            backend = pipeline.catalog.one(
                "SELECT backend_xmin FROM pg_stat_activity WHERE pid=%s", (first["backend"],)
            )
            assert backend["backend_xmin"] is None
            assert sum(1 for _ in inventory) == 9999
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    # A full Python result is over 10 MB before dictionaries. This bound covers
    # batches and cursors without hiding the retained inventory in another list.
    assert peak < 4_000_000


def test_remote_verification_detects_a_lost_shared_archive_part(pipeline):
    publication = publish(pipeline, logical_body())
    header = stored_header(pipeline, publication)
    part = header["rows"]["stable"]["root"]["key"]
    pipeline.history.client.delete_object(Bucket=pipeline.history.bucket, Key=part)
    result = pipeline.verify()
    assert not result["ok"]
    assert result["failures"] == [
        {"object_key": part, "category": "missing_or_corrupt_analytical_artifact"}
    ]


def test_clock_only_runs_share_bases_and_sparse_updates_rewrite_selected_parts(pipeline):
    # Keep the original hash-partition sharing regression as a bridge-format
    # test. Adaptive layouts have separate cost-plan and insertion coverage.
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=40")
    body = logical_body(1000)
    first = publish(pipeline, body)
    first_header = stored_header(pipeline, first)
    later = deepcopy(body)
    later["as_of"] += timedelta(minutes=15)
    for row in later["rows"]:
        row["as_of"] = later["as_of"].isoformat()
        row["features"]["age_hours"] += 0.25
        row["score"] += 1 / 3
        row["attention"]["score"] = row["score"]
        row["features"]["cohort_growth_percentile"] = 0.123456789
        row["attention"]["components"]["cohort_growth_percentile"]["value"] = 0.123456789
    later["rows"].reverse()
    # Reopen the module so reuse must come from SQL, not its in-process memo.
    pipeline._assessment_archive = AssessmentArchive(pipeline.catalog, pipeline.history)
    second = publish(pipeline, later)
    second_header = stored_header(pipeline, second)
    assert first_header["rows"]["stable"] == second_header["rows"]["stable"]
    assert canonical(pipeline.assessment_snapshot(second.run_id, True)) == canonical(
        {"run_id": second.run_id, **later}
    )
    before = {
        row["object_key"]
        for row in pipeline.catalog.all("SELECT object_key FROM derived_publications")
    }
    sparse = deepcopy(later)
    sparse["as_of"] += timedelta(minutes=15)
    sparse["rows"][500]["unknown_future_field"] += "changed"
    sparse["rows"].append({"product_id": "additional", "name": "Additional"})
    third = publish(pipeline, sparse)
    new = {
        row["object_key"]
        for row in pipeline.catalog.all("SELECT object_key FROM derived_publications")
    } - before
    assert len(new) < 10
    assert canonical(pipeline.assessment_snapshot(third.run_id, True)) == canonical(
        {"run_id": third.run_id, **sparse}
    )


def test_bridge_retains_plain_writes_and_both_formats_remain_readable(pipeline):
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=38")
    body = logical_body(2)
    legacy = publish(pipeline, body)
    assert legacy.descriptor["key"].endswith(f"{legacy.run_id}.json")
    pipeline.catalog.execute("INSERT INTO schema_migrations(version) VALUES(38)")
    assert canonical(pipeline.assessment_snapshot(legacy.run_id, True)) == canonical(
        {"run_id": legacy.run_id, **body}
    )
    body["as_of"] += timedelta(minutes=15)
    modern = publish(pipeline, body)
    assert modern.descriptor["key"].endswith(".json.gz")
    assert pipeline.verify()["ok"]
    snapshot = runpy.run_path(str(Path(__file__).parents[1] / "scripts/object_snapshot.py"))
    with (
        psycopg.connect(pipeline.settings.database_url.get_secret_value()) as connection,
        sqlite3.connect(":memory:") as db,
    ):
        snapshot["_expected_objects"](connection, db)
        for publication in (legacy, modern):
            assert db.execute(
                "SELECT sha,bytes,required FROM expected WHERE key=?",
                (publication.descriptor["key"],),
            ).fetchone() == (publication.descriptor["sha256"], publication.descriptor["bytes"], 1)


def test_retry_and_concurrent_compression_variants_keep_the_registered_winner(
    pipeline, monkeypatch
):
    body = logical_body(2)
    first = pipeline._assessment_archive.stage(body, PublicationScope([], ()))
    original = archive_module.compress_json
    # A valid gzip header variation simulates a different compression runtime.
    monkeypatch.setattr(
        archive_module, "compress_json", lambda value: gzip.compress(value, mtime=1)
    )
    second = pipeline._assessment_archive.stage(body, PublicationScope([], ()))
    assert second.descriptor["key"] != first.descriptor["key"]
    with pipeline.catalog.transaction() as connection:
        pipeline._assessment_archive.commit(connection, first)
        assert pipeline._assessment_archive.commit(connection, second) == first.descriptor["key"]
    puts = pipeline.history.metrics["put_requests"]
    assert publish(pipeline, body).descriptor == first.descriptor
    assert pipeline.history.metrics["put_requests"] == puts
    monkeypatch.setattr(archive_module, "compress_json", original)
    newer = deepcopy(body)
    newer["as_of"] += timedelta(minutes=15)
    prepared = [
        pipeline._assessment_archive.stage(newer, PublicationScope([], ())) for _ in range(2)
    ]

    def commit(publication):
        with pipeline.catalog.transaction() as connection:
            return pipeline._assessment_archive.commit(connection, publication)

    with ThreadPoolExecutor(max_workers=2) as executor:
        winners = list(executor.map(commit, prepared))
    assert winners[0] == winners[1]
    assert pipeline.catalog.one("SELECT count(*) AS count FROM assessment_runs")["count"] == 2
    assert pipeline.assessment_snapshot(first.run_id, True)["rows"] == body["rows"]


def test_staged_upload_crash_can_retry_but_source_withdrawal_fences_commit_and_reads(
    pipeline, monkeypatch
):
    cutoff, _records, datasets = seed(pipeline, products=2)
    first = pipeline.assess(cutoff, True)
    body = {key: value for key, value in first.items() if key != "run_id"}
    body["as_of"] = cutoff + timedelta(minutes=15)
    scope = pipeline.provenance.publication_scope([body["input_provenance"]], body["rows"], True)
    original = pipeline.history.put

    def fail_header(key, *args, **kwargs):
        if key.startswith("assessments/"):
            raise OSError("upload interrupted")
        return original(key, *args, **kwargs)

    monkeypatch.setattr(pipeline.history, "put", fail_header)
    with pytest.raises(OSError, match="interrupted"):
        pipeline._assessment_archive.stage(body, scope)
    assert pipeline.catalog.one("SELECT count(*) AS count FROM assessment_runs")["count"] == 1
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS count FROM derived_publications WHERE state='staged'"
        )["count"]
        == 1
    )
    monkeypatch.setattr(pipeline.history, "put", original)
    recovered = pipeline._assessment_archive.stage(body, scope)
    with pipeline.catalog.transaction() as connection:
        pipeline._assessment_archive.commit(connection, recovered)
    body["as_of"] += timedelta(minutes=15)
    pending = pipeline._assessment_archive.stage(body, scope)
    pipeline.catalog.invalidate_dataset(datasets[0], "source-withdrawal")
    with (
        pipeline.catalog.transaction() as connection,
        pytest.raises(ValueError, match="invalidated"),
    ):
        pipeline._assessment_archive.commit(connection, pending)
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.assessment_snapshot(first["run_id"], True)
    assert pipeline.assessment("p0", True) is None


def test_real_time_cohort_changes_and_new_source_rows_reconstruct_exactly(pipeline):
    cutoff = now() + timedelta(minutes=1)
    records = []
    for product, age, growth in (("young", 23.9, 100), ("day", 24.1, 1000), ("older", 72, 400)):
        pipeline.create_product(product, product, synthetic=True)
        for hours, value in ((12, 100), (0, 100 + growth)):
            records.append(
                video(
                    product, cutoff - timedelta(hours=hours), value, cutoff - timedelta(hours=age)
                )
            )
    pipeline.import_records([record.model_dump(mode="json") for record in records], synthetic=True)
    for product in ("young", "day", "older"):
        pipeline.review_link(product, f"projection_fixture:video:{product}")
    first = pipeline.assess(cutoff, True)
    second = pipeline.assess(cutoff + timedelta(minutes=15), True)

    def by_id(result):
        return {row["product_id"]: row for row in result["rows"]}

    assert (
        by_id(first)["day"]["features"]["cohort_growth_percentile"]
        != by_id(second)["day"]["features"]["cohort_growth_percentile"]
    )
    later = cutoff + timedelta(hours=1)
    pipeline.import_records(
        [video("young", later, 5000, cutoff - timedelta(hours=23.9)).model_dump(mode="json")],
        synthetic=True,
    )
    third = pipeline.assess(later, True)
    for result in (first, second, third):
        assert canonical(pipeline.assessment_snapshot(result["run_id"], True)) == canonical(result)
