"""Offline replay preserves rejected evidence and immutable earlier publications."""

from __future__ import annotations

from copy import deepcopy

import pytest
from test_pipeline_integration import ACCOUNT, controlled_source

from sm_trending_products.contracts import digest, now

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "include_valid,legacy_replay", [(True, False), (False, False), (True, True)]
)
def test_replay_preserves_rejections_without_source_access_or_checkpoint_changes(
    pipeline, monkeypatch, include_valid, legacy_replay
):
    videos = [{"id": "bad", "view_count": -1}]
    if include_valid:
        videos.insert(0, {"id": "good", "view_count": 123})
    response = {"data": {"videos": videos, "has_more": True, "cursor": 100}}
    with controlled_source([(200, response, {})]) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(1, True)
        assert result["processed"] == 1 and len(requests) == 1
    parent_id = result["datasets"][0]
    parent = deepcopy(
        pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (parent_id,))
    )
    before = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    assert parent["quarantine_count"] == 1
    assert before["state"] == "pending" and before["page_index"] == 1
    original_rows, _ = pipeline.history.observations(now(), True)
    legacy_id = digest({"replay_of": parent_id, "version": "1"})
    if legacy_replay:
        pipeline.history.publish(
            original_rows,
            pipeline.history.raw(parent),
            source=parent["source"],
            synthetic=True,
            dataset_id=legacy_id,
            supersedes=parent_id,
        )
        legacy = deepcopy(
            pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (legacy_id,))
        )
        assert legacy["quarantine_count"] == 0
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    replay = pipeline.replay(parent_id)
    child = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (replay["dataset_id"],)
    )
    assert replay["records"] == int(include_valid)
    assert replay["quarantine_count"] == child["quarantine_count"] == 1
    assert replay["status"] == "quarantined" and replay["transform_version"] == "1"
    assert child["manifest"]["quarantine"] == parent["manifest"]["quarantine"]
    assert child["manifest"]["supersedes"] == parent_id
    assert pipeline.replay(parent_id) == replay
    assert (
        pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (parent_id,)) == parent
    )
    assert (
        pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        == before
    )
    if legacy_replay:
        assert replay["dataset_id"] != legacy_id
        assert (
            pipeline.catalog.one("SELECT * FROM datasets WHERE dataset_id=%s", (legacy_id,))
            == legacy
        )
    replayed, _ = pipeline.history.observations(now(), True)
    assert [row.model_dump() for row in replayed] == [row.model_dump() for row in original_rows]
    assert pipeline.verify()["ok"]


def test_replay_keeps_top_level_schema_failure_visible_without_advancing_collection(pipeline):
    with controlled_source([(200, {"unexpected": {"videos": []}}, {})]) as (endpoint, _):
        task_id = pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        result = pipeline.collect_batch(1, True)
    assert result["errors"][0]["category"] == "schema"
    parent_id = result["quarantine_datasets"][0]
    before = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
    replay = pipeline.replay(parent_id)
    assert replay["records"] == 0 and replay["quarantine_count"] == 1
    assert replay["status"] == "quarantined"
    child = pipeline.catalog.one(
        "SELECT * FROM datasets WHERE dataset_id=%s", (replay["dataset_id"],)
    )
    assert child["manifest"]["quarantine"] == [{"reason": "schema"}]
    assert (
        pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        == before
    )
    assert pipeline.replay(parent_id) == replay
    assert pipeline.verify()["ok"]
