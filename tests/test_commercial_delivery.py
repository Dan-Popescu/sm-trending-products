"""Commercial source contracts through durable evidence and actual review."""

from __future__ import annotations

import json

import pytest
from streamlit.testing.v1 import AppTest
from test_pipeline_integration import ACCOUNT, controlled_source

from sm_trending_products.collector import CollectionError
from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("source", ["tiktok_ads", "tiktok_commercial"])
def test_commercial_pages_remain_separate_from_video_views_and_live_acceptance(
    pipeline, monkeypatch, source
):
    if source == "tiktok_ads":
        kind, record_key, date_filter = "ad", "ads", "ad_published_date_range"
        first = {
            "ad": {
                "id": "commercial-one",
                "first_shown_date": "20261001",
                "last_shown_date": "20261021",
                "reach": {"unique_users_seen": "10000-19999"},
                "videos": [{"id": "creative-one"}],
            },
            "advertiser": {"business_id": 42, "business_name": "Fixture advertiser"},
        }
        second = {"ad": {"id": "commercial-two", "reach": {"unique_user_seen": "11K"}}}
    else:
        kind, record_key, date_filter = (
            "commercial_content",
            "commercial_contents",
            "content_published_date_range",
        )
        first = {
            "id": "commercial-one",
            "create_timestamp": 1790812800,
            "create_date": "20261001",
            "creator": {"username": "fixture-maker"},
            "label": "Paid partnership",
            "brand_names": ["Fixture brand"],
            "videos": [{"id": "creative-one"}],
        }
        second = {"id": "commercial-two"}

    def response(record, more):
        return (
            200,
            {"data": {record_key: [record], "has_more": more, "search_id": "fixture-pages"}},
            {},
        )

    with controlled_source([response(first, True), response(second, False)]) as (
        endpoint,
        requests,
    ):
        pipeline.enqueue(
            source,
            ACCOUNT,
            "query",
            {
                "filters": {date_filter: {"min": "20261001", "max": "20261022"}},
                "endpoint_override": endpoint,
                "max_count": 2,
            },
            synthetic=True,
        )
        collected = pipeline.collect_batch(2, True)
        assert collected["processed"] == 2 and collected["errors"] == []
        assert len(requests) == 2
        assert "search_id" not in requests[0] and requests[1]["search_id"] == "fixture-pages"
        assert requests[0]["filters"] == requests[1]["filters"]
    observations, _ = pipeline.history.observations(now(), True)
    assert len(observations) == 2 and {row.entity_type for row in observations} == {kind}
    pipeline.create_product("commercial-product", "Commercial evidence", synthetic=True)
    for observation in observations:
        pipeline.review_link("commercial-product", observation.entity_key)
    published = pipeline.assess(now(), True)["rows"][0]
    assert published["attention"]["score"] is None and published["demand"]["score"] is None
    evidence = pipeline.evidence("commercial-product", now(), True)
    assert len(evidence) == 2
    assert all(row["source"] == source and row["synthetic"] is True for row in evidence)
    assert all("view_count" not in row["metrics"] for row in evidence)
    assert all(row["missing_fields"]["view_count"] == "not_exposed_by_source" for row in evidence)
    assert all(
        row["observed_at"] and row["available_at"] and row["raw_dataset_id"] for row in evidence
    )
    known = next(row for row in evidence if row["entity_id"] == "commercial-one")
    assert known["attributes"]["videos"] == [{"id": "creative-one"}]
    if source == "tiktok_ads":
        assert known["metrics"]["reach"]["lower"] == 10000
        assert known["metrics"]["reach"]["upper"] == 19999
        assert known["metrics"]["reach"]["precision"] == "bounded"
        assert known["attributes"]["advertiser_id"] == "42"
        assert known["attributes"]["first_shown_date"] == "20261001"
        assert known["attributes"]["last_shown_date"] == "20261021"
        assert known["attributes"]["distribution"] == "advertisement"
        rounded = next(row for row in evidence if row["entity_id"] == "commercial-two")
        assert "reach" not in rounded["metrics"] and rounded["attributes"]["reach_raw"] == "11K"
        assert rounded["missing_fields"]["reach"] == "rounding_bounds_not_documented"
    else:
        assert known["attributes"]["creator_id"] == "fixture-maker"
        assert known["attributes"]["label"] == "Paid partnership"
        assert known["attributes"]["distribution"] == "commercial_content"
        assert known["published_at"]
    assert pipeline.capabilities() == []
    with pytest.raises(CollectionError, match="source_access_unverified"):
        pipeline.enqueue(source, ACCOUNT, "query", {})
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
        "VALUES(%s,%s,'collect','denied','{\"category\":\"fixture_denial\"}')",
        (source, ACCOUNT),
    )
    with pytest.raises(CollectionError, match="source_access_unverified"):
        pipeline.enqueue(source, ACCOUNT, "query", {})
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    assert not app.exception
    displayed = next(
        value
        for element in app.json
        if isinstance(value := json.loads(element.value), list)
        and value
        and "observation_id" in value[0]
    )
    assert {row["entity_type"] for row in displayed} == {kind}
    assert all("view_count" not in row["metrics"] for row in displayed)
    app.radio(key="review_view").set_value("Health and coverage").run(timeout=20)
    assert not app.exception
    status = next(
        json.loads(element.value) for element in app.json if '"capabilities"' in element.value
    )
    assert status["pending_live_acceptance"] is True
    assert status["capabilities"][0]["state"] == "denied"
    assert len(requests) == 2
    assert (
        pipeline.latest_assessments(False) == []
        and pipeline.evidence("commercial-product", now(), False) == []
    )
    with Pipeline(pipeline.settings) as restarted:
        assert len(restarted.evidence("commercial-product", now(), True)) == 2
    assert pipeline.verify()["ok"]
