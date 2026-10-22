"""Real Rust source revisions remain immutable, time-scoped and recoverable."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
from threading import Barrier

import pytest
from test_pipeline_integration import ACCOUNT, controlled_source, video_observation

from sm_trending_products.cli import dispatch, parser
from sm_trending_products.contracts import digest, now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def collect_original(pipeline, entity="versioned-video", *, extra=None):
    row = {"id": entity, "view_count": 123, **(extra or {})}
    with controlled_source([(200, {"data": {"videos": [row], "has_more": False}}, {})]) as (
        endpoint,
        requests,
    ):
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {
                "endpoint_override": endpoint,
                "fixture_observed_at": (now() - timedelta(hours=1)).isoformat(),
            },
            synthetic=True,
        )
        result = pipeline.collect_batch(1, True)
        assert result["errors"] == [] and len(requests) == 1
        return result["datasets"][0]


def test_v2_changes_drift_reporting_without_rewriting_original_or_old_cutoff(pipeline, monkeypatch):
    original = collect_original(pipeline, extra={"undocumented_field": {"country_code": "FR"}})
    pipeline.create_product("versioned", "Versioned product", synthetic=True)
    pipeline.review_link("versioned", "tiktok_display:video:versioned-video")
    cutoff = now()
    before, _ = pipeline.history.observations(cutoff, True)
    decision = pipeline.freeze(cutoff, True)
    source = deepcopy(
        pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (original,))
    )
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    legacy = pipeline.replay(original)
    revised = pipeline.replay(original, "2")
    assert legacy["transform_version"] == "1" and revised["transform_version"] == "2"
    assert len({original, legacy["dataset_id"], revised["dataset_id"]}) == 3
    assert revised["records"] == 1 and revised["status"] == "quarantined"
    assert pipeline.replay(legacy["dataset_id"], "2")["dataset_id"] == revised["dataset_id"]
    assert pipeline.replay(revised["dataset_id"], "2")["dataset_id"] == revised["dataset_id"]
    assert pipeline.replay(original, "2") == revised
    assert (
        dispatch(pipeline, parser().parse_args(["replay", original, "--transform-version", "2"]))
        == revised
    )
    assert pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (original,)) == source
    assert pipeline.history.observations(cutoff, True)[0] == before
    assert pipeline.freeze(cutoff, True) == decision
    latest, _ = pipeline.history.observations(now(), True)
    assert len(latest) == 2
    version2 = next(row for row in latest if row.observation_id != before[0].observation_id)
    assert version2.observed_at == before[0].observed_at
    assert version2.available_at > cutoff
    assert version2.entity_key == before[0].entity_key
    assert version2.metrics == before[0].metrics and version2.market is None
    assert (
        version2.attributes["normalization"]["replaces_observation_id"] == before[0].observation_id
    )
    assert any(
        path.startswith("undocumented_field")
        for path in version2.attributes["unmapped_source_fields"]
    )
    current = now()
    page = pipeline.evidence_page("versioned", current, True)
    assert {row["observation_id"] for row in page["records"]} == {
        row.observation_id for row in latest
    }
    assert len(pipeline.evidence_page("versioned", cutoff, True)["records"]) == 1
    assert pipeline.catalog.one("SELECT count(*) AS count FROM replay_availability")["count"] == 1
    assert pipeline.verify()["ok"]


def test_v2_staged_retry_preserves_first_clock_and_exact_manifest(pipeline, monkeypatch):
    original = collect_original(pipeline)
    put = pipeline.history.put

    def interrupted(key, *args, **kwargs):
        if key.startswith("normalized/"):
            raise RuntimeError("controlled replay interruption")
        return put(key, *args, **kwargs)

    with monkeypatch.context() as failure:
        failure.setattr(pipeline.history, "put", interrupted)
        with pytest.raises(RuntimeError, match="controlled replay interruption"):
            pipeline.replay(original, "2")
    staged = pipeline.catalog.one("SELECT dataset_id,manifest FROM datasets WHERE state='staged'")
    clock = pipeline.catalog.one("SELECT available_at FROM replay_availability")["available_at"]
    with Pipeline(pipeline.settings) as restarted:
        result = restarted.replay(original, "2")
        assert result["dataset_id"] == staged["dataset_id"]
        assert (
            restarted.catalog.one(
                "SELECT dataset_id,manifest FROM datasets WHERE dataset_id=%s",
                (result["dataset_id"],),
            )
            == staged
        )
        rows, _ = restarted.history.observations(now(), True)
        assert (
            next(row for row in rows if row.raw_dataset_id == result["dataset_id"]).available_at
            == clock
        )
        assert restarted.verify()["ok"]


def test_independent_original_deliveries_get_distinct_v2_revision_contexts(pipeline):
    timestamp = (now() - timedelta(hours=1)).isoformat()
    response = {
        "data": {"videos": [{"id": "shared-original", "view_count": 123}], "has_more": False}
    }
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        for operation in ("list", "query"):
            payload = {"endpoint_override": endpoint, "fixture_observed_at": timestamp}
            if operation == "query":
                payload["filters"] = {"video_ids": ["shared-original"]}
            pipeline.enqueue("tiktok_display", ACCOUNT, operation, payload, synthetic=True)
        collected = pipeline.collect_batch(2, True)
        assert collected["errors"] == [] and len(requests) == 2
    originals = collected["datasets"]
    assert len(set(originals)) == 2
    assert len(pipeline.history.observations(now(), True)[0]) == 1
    pipeline.create_product("shared-original", "Shared source observation", synthetic=True)
    pipeline.review_link("shared-original", "tiktok_display:video:shared-original")
    before_replay = now()
    revisions = {pipeline.replay(original, "2")["dataset_id"] for original in originals}
    assert len(revisions) == 2
    records, _ = pipeline.history.observations(now(), True)
    assert len(records) == 3
    assert {
        row.attributes["normalization"]["source_dataset_id"]
        for row in records
        if "normalization" in row.attributes
    } == set(originals)
    assert len(pipeline.history.observations(before_replay, True)[0]) == 1
    assert len(pipeline.evidence_page("shared-original", now(), True)["records"]) == 3
    assert pipeline.verify()["ok"]


def test_concurrent_v2_aliases_publish_one_revision_without_clock_conflicts(pipeline):
    original = collect_original(pipeline)
    legacy = pipeline.replay(original)["dataset_id"]
    barrier = Barrier(4)

    def replay(index):
        with Pipeline(pipeline.settings) as worker:
            barrier.wait(timeout=10)
            return worker.replay(original if index % 2 else legacy, "2")["dataset_id"]

    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(replay, range(4)))
    assert len(set(results)) == 1
    assert pipeline.catalog.one("SELECT count(*) AS count FROM replay_availability")["count"] == 1
    assert len(pipeline.history.observations(now(), True)[0]) == 2
    assert pipeline.verify()["ok"]


def test_v2_compaction_retains_version_and_passthrough_cannot_invent_a_transformation(pipeline):
    for entity in ("compact-a", "compact-b"):
        original = collect_original(pipeline, entity)
        assert pipeline.replay(original, "2")["status"] == "complete"
    before, _ = pipeline.history.observations(now(), True)
    results = [pipeline.compact(True), pipeline.compact(True)]
    datasets = [
        pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (result["dataset_id"],))
        for result in results
    ]
    assert {dataset["transform_version"] for dataset in datasets} == {"1", "2"}
    revised = next(dataset for dataset in datasets if dataset["transform_version"] == "2")
    assert revised["manifest"]["transform_version"] == "2"
    assert revised["dataset_id"] == digest(
        {
            "kind": "compaction",
            "parents": sorted(revised["manifest"]["parent_dataset_ids"]),
            "version": "2",
        }
    )
    assert pipeline.replay(revised["dataset_id"], "2")["transform_version"] == "2"
    with pytest.raises(ValueError, match="archived source response"):
        pipeline.replay(revised["dataset_id"], "1")
    imported = pipeline.import_records([video_observation("import-only", now(), 1)], True)[0]
    with pytest.raises(ValueError, match="archived source response"):
        pipeline.replay(imported, "2")
    after, _ = pipeline.history.observations(now(), True)
    assert {row.observation_id for row in before} <= {row.observation_id for row in after}
    assert pipeline.verify()["ok"]


def test_retired_source_cannot_resurrect_a_v2_replay(pipeline):
    original = collect_original(pipeline)
    revised = pipeline.replay(original, "2")["dataset_id"]
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,verified,retention_days) "
        "VALUES('tiktok_display',true,1) ON CONFLICT(source) "
        "DO UPDATE SET verified=true,retention_days=1"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (original,),
    )
    assert pipeline.retention(apply=True)["applied"]
    for dataset in (original, revised):
        with pytest.raises(ValueError, match="No completed dataset"):
            pipeline.replay(dataset, "2")
    assert pipeline.catalog.one("SELECT count(*) AS count FROM replay_availability")["count"] == 1
