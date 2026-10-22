"""Published attention and immutable alerts over reviewed, stored timelines."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_pipeline_integration import video_observation

from sm_trending_products.contracts import AssessmentPolicy, now, utc
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_published_attention_and_identity_remain_auditable_after_resets_and_reassignment(
    pipeline, monkeypatch
):
    baseline = now()
    records = []
    for entity in ("broad-a", "broad-b", "solo", "older"):
        counts = (100, 200, 500) if entity.startswith("broad") else (100, 300, 900)
        for hour, count in zip((3, 2, 1), counts, strict=True):
            row = video_observation(entity, baseline - timedelta(hours=hour), count)
            age = timedelta(days=12) if entity == "older" else timedelta(hours=12)
            row["published_at"] = (baseline - age).isoformat()
            if entity == "older":
                row["metrics"]["view_count"] = {"lower": 100 * count, "upper": 100 * count}
            row["attributes"]["creator_id"] = entity
            records.append(row)
    records.extend(
        video_observation("stale", baseline - timedelta(hours=hour), count)
        for hour, count in ((100, 100), (99, 500))
    )
    records.append(video_observation("short", baseline - timedelta(hours=1), 100))
    missing = video_observation("missing", baseline - timedelta(hours=1), 100)
    missing["metrics"] = {}
    missing["missing_fields"] = {"view_count": "not_returned"}
    records.append(missing)
    pipeline.import_records(records, synthetic=True)
    for product in ("broad", "solo", "older", "unknown", "stale", "short", "missing"):
        pipeline.create_product(product, product.title(), synthetic=True)
    decisions = [
        pipeline.review_link(
            "broad" if entity.startswith("broad") else entity, f"fixture_import:video:{entity}"
        )
        for entity in ("broad-a", "broad-b", "solo", "older", "stale", "short", "missing")
    ]
    cutoff = now()
    policy = AssessmentPolicy(version="controlled-attention-v1", alert_threshold=0)
    first = pipeline.assess(cutoff, synthetic=True, policy=policy)
    by_id = {row["product_id"]: row for row in first["rows"]}
    broad = by_id["broad"]
    assert broad["features"]["growth_per_hour"] == pytest.approx(600)
    assert broad["features"]["acceleration_per_hour_squared"] == pytest.approx(400)
    assert broad["features"]["interval_hours"] == pytest.approx([1.0])
    assert broad["features"]["age_hours"] == pytest.approx(12, abs=0.01)
    assert broad["features"]["creator_concentration"] == pytest.approx(0.5)
    assert by_id["solo"]["features"]["growth_per_hour"] == pytest.approx(600)
    assert broad["score"] > by_id["solo"]["score"]
    assert by_id["older"]["features"]["age_hours"] == pytest.approx(288, abs=0.01)
    assert by_id["older"]["features"]["growth_per_hour"] == pytest.approx(60000)
    assert by_id["older"]["features"]["cohort_growth_percentile"] == pytest.approx(0.5)
    assert broad["features"]["cohort_growth_percentile"] == pytest.approx(0.5)
    assert by_id["unknown"]["score"] is None
    for product in ("stale", "short", "missing"):
        assert by_id[product]["score"] is None
        assert by_id[product]["features"]["growth_per_hour"] is None
        assert by_id[product]["attention"]["reasons"]
    assert by_id["stale"]["attention"]["status"] == "insufficient_history"
    assert by_id["short"]["features"]["view_count"] == 100
    assert by_id["missing"]["features"]["view_count"] is None
    assert broad["identity"]["decision_refs"] == sorted(
        row["decision_id"] for row in decisions if row["product_id"] == "broad"
    )
    evidence = pipeline.evidence("broad", cutoff, True)
    assert set(broad["evidence_refs"]) == {row["observation_id"] for row in evidence}
    assert set(broad["attention"]["evidence_refs"]) == set(broad["evidence_refs"])
    for row in first["rows"]:
        assert {"attention", "demand", "competition", "feasibility"} <= row.keys()
        assert row["demand"]["score"] is None and row["feasibility"]["score"] is None
    frozen = deepcopy(pipeline.alerts(True))
    assert {row["product_id"] for row in frozen} == {"broad", "solo", "older"}
    assert all(row["run_id"] == first["run_id"] for row in frozen)
    original_key = f"assessments/synthetic/{first['run_id']}.json"

    def stored_result(application):
        response = application.history.client.get_object(
            Bucket=application.history.bucket, Key=original_key
        )
        with response["Body"] as body:
            return body.read()

    original_bytes = stored_result(pipeline)

    # New evidence invalidates current growth; it cannot rewrite the old alert.
    later = cutoff + timedelta(hours=1)
    pipeline.import_records(
        [video_observation(entity, later, 20) for entity in ("broad-a", "broad-b")],
        synthetic=True,
    )
    revised = pipeline.assess(
        later, True, policy.model_copy(update={"version": "controlled-attention-v2"})
    )
    revised_broad = next(row for row in revised["rows"] if row["product_id"] == "broad")
    assert revised_broad["features"]["counter_corrections"] == 2
    assert revised_broad["features"]["growth_per_hour"] is None
    assert revised_broad["attention"]["status"] == "insufficient_history"
    assert revised_broad["score"] is None
    assert pipeline.alerts(True) == frozen  # Solo still qualifies; cooldown prevents repetition.

    pipeline.identity.reassign_entities(["fixture_import:video:broad-b"], "unknown")
    reassigned = pipeline.assess(later + timedelta(seconds=1), True, policy)
    reassigned_broad = next(row for row in reassigned["rows"] if row["product_id"] == "broad")
    assert reassigned_broad["identity"]["version"] != broad["identity"]["version"]
    assert pipeline.assess(cutoff, True, policy)["run_id"] == first["run_id"]
    assert stored_result(pipeline) == original_bytes
    assert pipeline.alerts(True) == frozen
    assert pipeline.latest_assessments(False) == [] and pipeline.alerts(False) == []

    # Actual review rendering reads the newly published version and the old alert.
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.selectbox(key="assessment_product").select("broad").run(timeout=20)
    assert not app.exception
    shown = [json.loads(element.value) for element in app.json]
    published = next(row for row in shown if isinstance(row, dict) and "features" in row)
    assert published == reassigned_broad
    assert utc(published["as_of"]) == later + timedelta(seconds=1)
    alert_table = next(frame.value for frame in app.dataframe if "alert_id" in frame.value)
    assert set(alert_table["alert_id"]) == {row["alert_id"] for row in frozen}
    expected_alert = next(row for row in frozen if row["product_id"] == "broad")
    app.selectbox(key="alert_id").select(expected_alert["alert_id"]).run(timeout=20)
    assert not app.exception
    shown_alert = next(
        row
        for element in app.json
        if isinstance(row := json.loads(element.value), dict)
        and row.get("alert_id") == expected_alert["alert_id"]
        and "payload" in row
    )
    assert shown_alert == json.loads(json.dumps(expected_alert, default=repr))
    assert shown_alert["payload"] == broad
    with Pipeline(pipeline.settings) as restarted:
        assert restarted.alerts(True) == frozen
        assert stored_result(restarted) == original_bytes
    assert pipeline.verify()["ok"]
