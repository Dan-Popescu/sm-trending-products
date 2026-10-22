"""Controlled-source delivery through real Dagster, SQL, objects and review UI."""

from __future__ import annotations

from datetime import timedelta

import pytest
from dagster import materialize
from streamlit.testing.v1 import AppTest
from test_pipeline_integration import ACCOUNT, controlled_source

from sm_trending_products import orchestration
from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_dagster_collection_publishes_reviewable_evidence_and_a_frozen_decision(
    pipeline, monkeypatch
):
    def response(views):
        return (
            200,
            {
                "data": {
                    "videos": [{"id": "offline-video", "view_count": views}],
                    "has_more": False,
                },
                "error": {"code": "ok"},
            },
            {},
        )

    with controlled_source([response(100), response(500)]) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {
                "endpoint_override": endpoint,
                "fixture_observed_at": (now() - timedelta(hours=2)).isoformat(),
            },
            synthetic=True,
            idempotency_key="baseline",
        )
        assert pipeline.collect_batch(1, synthetic=True)["processed"] == 1
        pipeline.create_product("offline-product", "Controlled lamp", synthetic=True)
        pipeline.review_link("offline-product", "tiktok_display:video:offline-video")
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {
                "endpoint_override": endpoint,
                "fixture_observed_at": (now() - timedelta(hours=1)).isoformat(),
            },
            synthetic=True,
            idempotency_key="dagster-refresh",
        )
        monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
        assets = [
            orchestration.collection_schedule,
            orchestration.observation_batch,
            orchestration.opportunity_assessments,
            orchestration.prospective_decisions,
        ]
        result = materialize(
            assets,
            run_config={
                "ops": {
                    asset.op.name: {
                        "config": {
                            "synthetic": True,
                            "request_budget": 0,
                            "batch_size": 1,
                        }
                    }
                    for asset in assets
                }
            },
        )
        assert result.success
        assert len(requests) == 2

    assessments = pipeline.latest_assessments(synthetic=True)
    assert len(assessments) == 1 and assessments[0]["score"] is not None
    assert len(pipeline.evaluations(synthetic=True)) == 1
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    assert not app.exception
    assert app.dataframe[0].value.iloc[0]["Product"] == "Controlled lamp"
    assert app.selectbox(key="evidence_product").value == "offline-product"
    evidence_frame = next(frame.value for frame in app.dataframe if "observation_id" in frame.value)
    assert len(evidence_frame) == 2
    assert pipeline.verify()["ok"] is True
    assert pipeline.latest_assessments(synthetic=False) == []
    assert pipeline.capabilities() == []
