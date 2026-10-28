"""Product-scoped descriptive windows through native history and identity guards."""

from datetime import timedelta

import pytest
from test_evaluation_integration import record

from sm_trending_products.contracts import now
from sm_trending_products.history import RowBudgetExceeded
from sm_trending_products.trend_insights import TrendInsights

pytestmark = pytest.mark.integration


def test_future_only_capability_does_not_grant_historical_access(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('fixture_import','fixture-account','collect','available',%s,'{}')",
        (cutoff + timedelta(days=1),),
    )
    report = insights.describe("trend", cutoff, True, windows_hours=(24,))
    assert report["eligible_video_observation_count"] == 0
    assert report["windows"][0]["observed_growth_lower"] is None


def seed(pipeline, *, entities=("first",), hours=168, gaps=6, attributes=None, market=None):
    cutoff = now() + timedelta(days=8)
    pipeline.create_product("trend", "Trend", synthetic=True, market=market)
    rows = []
    for index, entity in enumerate(entities):
        for hour in range(0, hours + 1, gaps):
            row = record(entity, cutoff - timedelta(hours=hours - hour), 100 + hour * (index + 1))
            if attributes is not None:
                row["attributes"] = attributes[index]
            row["market"] = market
            rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    for entity in entities:
        pipeline.review_link("trend", f"fixture_import:video:{entity}")
    return TrendInsights(pipeline.catalog, pipeline.history), cutoff


def test_three_durations_reuse_one_product_read_without_temporal_spike_confusion(
    pipeline, monkeypatch
):
    insights, cutoff = seed(
        pipeline,
        entities=("first", "second", "third"),
        gaps=1,
        attributes=[
            {"creator_id": "a", "creative_fingerprint": "shared"},
            {"creator_id": "b", "creative_fingerprint": "shared"},
            {"creator_id": "c", "creative_fingerprint": "independent"},
        ],
    )
    reads = []
    original = pipeline.history.read

    def capture(*args, **kwargs):
        reads.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", capture)
    report = insights.describe("trend", cutoff, True)
    assert len(reads) == 1
    assert reads[0]["entity_keys"] == {
        "fixture_import:video:first",
        "fixture_import:video:second",
        "fixture_import:video:third",
    }
    assert [row["window_hours"] for row in report["windows"]] == [24, 72, 168]
    for window in report["windows"]:
        assert window["observed_growth_lower"] == window["window_hours"] * 6
        assert window["shape"] == "sustained_growth"
        assert window["evidence_quality"] == "complete"
        assert window["growth_weighted_temporal_peak_ratio"] == pytest.approx(1)
        assert window["known_creator_count"] == window["growing_creator_count"] == 3
        assert window["verified_creative_count"] == 2
        assert window["repeated_creative_entities"] == 1
        assert window["predictive_confidence"] is None
    assert insights.describe("trend", cutoff, True)["report_id"] == report["report_id"]
    longest = report["windows"][-1]
    assert longest["evidence_count"] == 507
    assert len(longest["evidence_preview"]) == 200
    assert longest["evidence_preview_truncated"] is True


def test_missing_entities_late_revisions_and_edge_gaps_remain_explicit(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    unknown = record("unobserved", cutoff, 0)
    unknown["metrics"] = {}
    pipeline.import_records([unknown], synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:unobserved")
    late = record("first", cutoff, 100000, available_at=cutoff + timedelta(seconds=1))
    pipeline.import_records([late], synthetic=True)
    report = insights.describe("trend", cutoff + timedelta(hours=1), True, windows_hours=(24,))
    # Late revision is known at this later cutoff, so use the original cutoff to
    # independently demonstrate availability filtering.
    original = insights.describe("trend", cutoff, True, windows_hours=(24,))["windows"][0]
    assert original["observed_growth_lower"] == 24
    assert original["missing_entity_count"] == 1
    assert original["evidence_quality"] == "partial"
    assert {row["reason"] for row in original["exclusions_preview"]} == {
        "no_eligible_metric_history"
    }
    window = report["windows"][0]
    assert window["minimum_entity_coverage_fraction"] == pytest.approx(18 / 24)
    assert window["evidence_quality"] == "partial"


def test_global_reassignment_does_not_resurrect_previous_owner(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    before = now()
    pipeline.create_product("new-owner", "New owner", synthetic=True)
    # Imported historical claims can coexist; avoid the write facade's automatic
    # rejection of the old claim so this exercises global read resolution itself.
    pipeline.catalog.execute(
        "INSERT INTO product_links(decision_id,product_id,entity_key,available_at,status,"
        "confidence,method) VALUES(%s,%s,%s,%s,'confirmed',0.1,'historical-import')",
        (
            "new-owner-claim",
            "new-owner",
            "fixture_import:video:first",
            before + timedelta(seconds=1),
        ),
    )
    assert (
        insights.describe("trend", before, True, windows_hours=(24,))["identity"]["decision_count"]
        == 1
    )
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))
    assert result["identity"]["decision_count"] == 0
    assert result["input_observation_count"] == 0
    assert result["windows"][0]["shape"] == "insufficient_history"


def test_source_scope_and_workspace_isolation(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    # A grant for another account must not make this source unrestricted.
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
        "VALUES('fixture_import','other-account','collect','available','{}')"
    )
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))
    assert result["eligible_video_observation_count"] == 0
    assert result["windows"][0]["missing_entity_count"] == 1
    with pytest.raises(ValueError, match="unavailable"):
        insights.describe("trend", cutoff, False)


def test_product_market_excludes_incomparable_market_evidence(pipeline):
    insights, cutoff = seed(pipeline, hours=24, market="FR")
    foreign = record("first", cutoff, 100000)
    foreign["observation_id"] = "zz-foreign-market"
    foreign["market"] = "DE"
    pipeline.import_records([foreign], synthetic=True)
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))
    assert result["market"] == "FR"
    assert result["windows"][0]["observed_growth_lower"] == 24


def test_resets_and_changed_counter_scope_are_excluded(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    reset = record("first", cutoff - timedelta(hours=12), 1, available_at=cutoff)
    scope_change = record("first", cutoff - timedelta(hours=6), 100, available_at=cutoff)
    scope_change["attributes"]["counter_scope"] = "new-scope"
    pipeline.import_records([reset, scope_change], synthetic=True)
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))["windows"][0]
    assert result["counter_correction_count"] == 1
    assert result["comparable_interval_count"] == 1
    assert result["observed_growth_lower"] == 6
    assert result["shape"] == "insufficient_history"
    assert result["evidence_quality"] == "partial"


def test_one_capture_interval_across_several_entities_is_not_persistence(pipeline):
    insights, cutoff = seed(pipeline, entities=("a", "b", "c"), hours=24, gaps=24)
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))["windows"][0]
    assert result["comparable_interval_count"] == 3
    assert result["shape"] == "insufficient_history"


def test_observed_concentration_is_distinct_from_independent_creative_spread(pipeline):
    cutoff = now() + timedelta(days=2)
    pipeline.create_product("trend", "Trend", synthetic=True)
    rows = []
    for hour, value in zip(range(0, 25, 6), (100, 100, 100, 100, 200), strict=True):
        row = record("spike", cutoff - timedelta(hours=24 - hour), value)
        row["attributes"] = {}
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:spike")
    result = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,)
    )["windows"][0]
    assert result["observed_growth_lower"] == 100
    assert result["shape"] == "concentrated_growth"
    assert result["growth_weighted_temporal_peak_ratio"] == pytest.approx(4)
    assert result["positive_observed_time_fraction"] == 0.25
    assert result["verified_creative_count"] == result["known_creator_count"] == 0
    assert result["unknown_fingerprint_entities"] == result["unknown_creator_entities"] == 1


def test_bounded_counters_and_incompatible_units_do_not_create_exact_growth(pipeline):
    insights, cutoff = seed(pipeline, entities=("bounded", "other-unit"), hours=24)
    revisions = []
    for entity in ("bounded", "other-unit"):
        for hours in range(0, 25, 6):
            row = record(entity, cutoff - timedelta(hours=hours), 0, available_at=cutoff)
            row["observation_id"] = f"zz-revised-{entity}-{hours}"
            row["metrics"]["view_count"] = (
                {"lower": 0, "upper": 100, "precision": "bounded"}
                if entity == "bounded"
                else {"lower": 100000, "upper": 100000, "unit": "EUR"}
            )
            revisions.append(row)
    pipeline.import_records(revisions, synthetic=True)
    result = insights.describe("trend", cutoff, True, windows_hours=(24,))["windows"][0]
    assert result["observed_growth_lower"] == 0
    assert result["observed_growth_upper"] == 400
    assert result["shape"] == "uncertain_growth"
    assert result["growth_precision"] == "bounded"
    assert result["measured_entity_count"] == result["missing_entity_count"] == 1


def test_row_budget_fails_instead_of_truncating_evidence(pipeline):
    insights, cutoff = seed(pipeline, hours=24)
    with pytest.raises(RowBudgetExceeded):
        insights.describe("trend", cutoff, True, max_rows=2)
    for windows in ((), (24, 24), (True,), (8761,), tuple(range(1, 10))):
        with pytest.raises(ValueError, match="Invalid bounded"):
            insights.describe("trend", cutoff, True, windows_hours=windows)
