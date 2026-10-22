"""Persisted planner decisions through bounded SQL reads and actual operator UI."""

from __future__ import annotations

import json

import pytest
from dagster import materialize
from psycopg.types.json import Jsonb
from streamlit.testing.v1 import AppTest
from test_pipeline_integration import ACCOUNT, controlled_source

from sm_trending_products import orchestration
from sm_trending_products.contracts import canonical, now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_collection_review_pages_bound_group_members_and_keep_workspaces_separate(
    pipeline, monkeypatch
):
    identifiers = {}
    for synthetic in (False, True):
        identifier = pipeline.catalog.enqueue(
            "tiktok_display",
            ACCOUNT,
            "query",
            {"access_token": "must-hide"},
            synthetic,
            due_at=now(),
            idempotency_key=f"review-{synthetic}",
        )
        identifiers[synthetic] = identifier
        reason = {
            "policy_version": "large-group-v1",
            "bucket": "priority_refresh",
            "entity_keys": [f"tiktok_display:video:{index}" for index in range(251)],
            "entity_reasons": [
                {"entity_key": f"tiktok_display:video:{index}", "position": index}
                for index in range(251)
            ],
        }
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET schedule_reason=%s WHERE task_id=%s",
            (Jsonb(reason), identifier),
        )
    before_upgrade = pipeline.catalog.all("SELECT * FROM collection_tasks ORDER BY task_id")
    pipeline.catalog.execute("DROP INDEX collection_review_page")
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=16")
    pipeline.catalog.initialize()
    pipeline.catalog.initialize()
    assert pipeline.catalog.all("SELECT * FROM collection_tasks ORDER BY task_id") == before_upgrade
    summaries = pipeline.collection_summaries(True, limit=1)
    assert len(summaries) == 1 and summaries[0]["task_id"] == identifiers[True]
    assert summaries[0]["entity_count"] == 251
    assert "schedule_reason" not in summaries[0] and "payload" not in summaries[0]
    assert pipeline.collection_summaries(True, limit=1, offset=1) == []
    detail = pipeline.collection_decision(identifiers[True], True, limit=2, offset=200)
    assert detail["member_count"] == 251 and len(detail["members"]) == 2
    assert [row["reason"]["position"] for row in detail["members"]] == [200, 201]
    assert "entity_reasons" not in detail["decision"] and "entity_keys" not in detail["decision"]
    assert "must-hide" not in canonical(detail)
    assert pipeline.collection_decision(identifiers[True], False) is None
    for reader in (
        pipeline.collection_summaries,
        lambda synthetic, **kwargs: pipeline.collection_decision(
            identifiers[True], synthetic, **kwargs
        ),
    ):
        with pytest.raises(ValueError, match="Review pages"):
            reader(True, limit=201)
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.radio(key="review_view").set_value("Collection plan").run(timeout=20)
    app.number_input(key="collection_member_page").set_value(2).run(timeout=20)
    assert not app.exception
    shown = next(json.loads(element.value) for element in app.json if '"members"' in element.value)
    assert len(shown["members"]) == 51 and shown["members"][0]["reason"]["position"] == 200
    app.toggle[0].set_value(False).run(timeout=20)
    assert not app.exception
    assert app.number_input(key="collection_member_page").value == 1
    shown = next(json.loads(element.value) for element in app.json if '"members"' in element.value)
    assert shown["task_id"] == identifiers[False] and shown["members"][0]["reason"]["position"] == 0


def test_recorded_exploration_and_refresh_reasons_survive_actual_collection_and_ui(
    pipeline, monkeypatch
):
    def response(videos):
        return (
            200,
            {
                "data": {
                    "videos": [{"id": key, "view_count": views} for key, views in videos.items()],
                    "has_more": False,
                }
            },
            {},
        )

    with controlled_source(
        [
            response({"hot": 100, "cold": 10}),
            response({"hot": 2000, "cold": 20}),
            response({"cold": 30}),
            response({"hot": 3000}),
        ]
    ) as (endpoint, requests):
        for key in ("baseline", "refresh"):
            pipeline.enqueue(
                "tiktok_display",
                ACCOUNT,
                "list",
                {"endpoint_override": endpoint},
                True,
                idempotency_key=key,
            )
        assert pipeline.collect_batch(2, True)["processed"] == 2
        pipeline.create_product("hot-product", "Hot product", synthetic=True)
        pipeline.review_link("hot-product", "tiktok_display:video:hot")
        published = pipeline.assess(now(), True)
        score = published["rows"][0]["score"]
        plan = pipeline.schedule(
            2,
            True,
            {
                "version": "inspectable-v1",
                "exploration_fraction": 0.5,
                "hot_threshold": 30,
                "monitoring_threshold": 20,
                "request_costs": {"tiktok_display": 2},
            },
        )
        assert plan["scheduled"] == plan["refresh_entities_scheduled"] == 2
        assert plan["exploration_scheduled"] == 1
        decisions = {task: pipeline.collection_decision(task, True) for task in plan["task_ids"]}
        assert {row["decision"]["tier"] for row in decisions.values()} == {"hot", "exploration"}
        assert {row["decision"]["planned_interval_hours"] for row in decisions.values()} == {1, 24}
        for row in decisions.values():
            member = row["members"][0]
            assert member["candidate"]["discovered_at"]
            assert member["candidate"]["origin_dataset_id"]
            inputs = member["reason"]["priority_inputs"]
            assert inputs["estimated_request_cost"] == 2
            assert inputs["assessment_score"] == (
                score if member["reason"]["entity_key"].endswith(":hot") else None
            )
        monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
        assets = [orchestration.collection_schedule, orchestration.observation_batch]
        executed = materialize(
            assets,
            run_config={
                "ops": {
                    asset.op.name: {
                        "config": {"synthetic": True, "request_budget": 0, "batch_size": 2}
                    }
                    for asset in assets
                }
            },
        )
        assert executed.success and executed.output_for_node("observation_batch")["processed"] == 2
        assert len(requests) == 4
        assert requests[2]["filters"]["video_ids"] == ["cold"]
        assert requests[3]["filters"]["video_ids"] == ["hot"]
    for identifier, before in decisions.items():
        after = pipeline.collection_decision(identifier, True)
        assert after["state"] == "done"
        assert after["decision"] == before["decision"]
        assert after["members"][0]["reason"] == before["members"][0]["reason"]
    latest = pipeline.assess(now(), True)["rows"][0]
    assert latest["features"]["interval_hours"][0] > 0
    unavailable = pipeline.schedule(
        1,
        False,
        {
            "version": "unverified-live",
            "discovery_queries": [
                {
                    "source": "tiktok_ads",
                    "account_id": ACCOUNT,
                    "operation": "query",
                    "payload": {
                        "filters": {
                            "ad_published_date_range": {"min": "20261001", "max": "20261022"}
                        }
                    },
                }
            ],
        },
    )
    assert unavailable["scheduled"] == 0
    assert any(row["category"] == "source_policy_unverified" for row in unavailable["warnings"])
    assert pipeline.capabilities() == []
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.radio(key="review_view").set_value("Collection plan").run(timeout=20)
    assert not app.exception
    shown = next(frame.value for frame in app.dataframe if "task_id" in frame.value)
    assert set(shown["task_id"]) == set(plan["task_ids"])
    selected = plan["task_ids"][0]
    app.selectbox(key="collection_task").select(selected).run(timeout=20)
    assert not app.exception
    detail = next(json.loads(element.value) for element in app.json if '"members"' in element.value)
    assert detail["decision"] == decisions[selected]["decision"]
    assert len(requests) == 4
    app.toggle[0].set_value(False).run(timeout=20)
    assert not app.exception and not any("task_id" in frame.value for frame in app.dataframe)
