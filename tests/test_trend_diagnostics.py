"""Disjoint trend comparisons through native catalog and history boundaries."""

from datetime import timedelta

import pytest
from test_evaluation_integration import record

from sm_trending_products.contracts import now
from sm_trending_products.history import RowBudgetExceeded
from sm_trending_products.trend_insights import TrendInsights

pytestmark = pytest.mark.integration


def add_series(
    pipeline, cutoff, entity, points, *, attributes=None, account_id=None, source="fixture_import"
):
    rows = []
    for hour, value in points:
        row = record(entity, cutoff + timedelta(hours=hour), value, account_id=account_id)
        row["source"] = source
        row["observation_id"] = source + ":" + row["observation_id"]
        row["published_at"] = (cutoff - timedelta(days=365)).isoformat()
        if attributes is not None:
            row["attributes"] = attributes
        rows.append(row)
    datasets = pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("trend", f"{source}:video:{entity}")
    return datasets


def test_newly_observed_old_video_does_not_manufacture_continuing_traction(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    add_series(pipeline, cutoff, "continuing", [(-48, 100), (-24, 124), (0, 148)])
    add_series(pipeline, cutoff, "discovered", [(-12, 100000), (0, 100120)])
    insights = TrendInsights(pipeline.catalog, pipeline.history)
    legacy = insights.describe("trend", cutoff, True, windows_hours=(24,))
    report = insights.describe("trend", cutoff, True, windows_hours=(24,), diagnostics=True)
    assert report["version"] == "descriptive-trends-v2"
    assert report["windows"] == legacy["windows"]
    comparison = report["diagnostics"]["comparisons"][0]
    assert comparison["previous"]["window_end"] == comparison["current"]["window_start"]
    assert comparison["previous"]["observed_growth_lower"] == 24
    assert comparison["current"]["observed_growth_lower"] == 144
    assert comparison["continuing_panel"]["entity_count"] == 1
    assert comparison["continuing_panel"]["pace_ratio_lower"] == 1
    assert comparison["continuing_panel"]["pace_ratio_upper"] == 1
    assert comparison["newly_observed"]["entity_count"] == 1
    assert comparison["newly_observed"]["observed_growth_lower"] == 120
    assert comparison["newly_observed"]["measured_entity_hours"] == 12
    assert insights.describe("trend", cutoff, True, windows_hours=(24,)) == legacy


def test_capture_gaps_use_measured_hours_and_missing_entities_stay_unknown(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    add_series(
        pipeline,
        cutoff,
        "gappy",
        [(-48, 100), (-42, 160), (-24, 160), (-6, 160), (0, 220)],
    )
    missing = record("missing", cutoff, 0)
    missing["metrics"] = {}
    late = record("gappy", cutoff, 100000, available_at=cutoff + timedelta(seconds=1))
    pipeline.import_records([missing, late], synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:missing")
    report = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), max_boundary_gap_hours=6, diagnostics=True
    )
    comparison = report["diagnostics"]["comparisons"][0]
    assert comparison["previous"]["observed_growth_lower"] == 60
    assert comparison["current"]["observed_growth_lower"] == 60
    assert comparison["current"]["measured_entity_hours"] == 6
    assert comparison["current"]["growth_per_measured_entity_hour_lower"] == 10
    assert comparison["current"]["mean_entity_coverage_fraction"] == 0.125
    assert comparison["current"]["missing_entity_count"] == 1
    assert comparison["continuing_panel"]["pace_ratio_lower"] == 1
    assert comparison["newly_observed"]["observed_growth_lower"] is None


def test_leave_largest_out_removes_copied_creatives_and_does_not_invent_people(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    for entity, final, attributes in (
        ("dominant", 2524, {"creator_id": "large", "creative_fingerprint": "copied"}),
        ("stable-a", 148, {"creator_id": "a", "creative_fingerprint": "copied"}),
        ("stable-b", 148, {"creator_id": "b", "creative_fingerprint": "other"}),
        ("uncertain", 148, {"creator_id": {"unverified": "somebody"}}),
    ):
        add_series(
            pipeline,
            cutoff,
            entity,
            [(-48, 100), (-24, 124), (0, final)],
            attributes=attributes,
        )
    comparison = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), diagnostics=True
    )["diagnostics"]["comparisons"][0]
    assert comparison["continuing_panel"]["pace_ratio_lower"] == 25.75
    creators = comparison["creator_robustness"]
    assert creators["known_identity_count"] == 3
    assert creators["unknown_identity_entity_count"] == 1
    assert creators["largest_identity"] == "fixture_import:creator:large"
    assert creators["largest_identity_growth_share_lower"] == pytest.approx(100 / 103)
    assert creators["unknown_identity_growth_share_lower"] == pytest.approx(1 / 103)
    assert creators["leave_largest_out"]["entity_count"] == 2
    assert creators["leave_largest_out"]["pace_ratio_lower"] == 1
    creatives = comparison["verified_creative_robustness"]
    assert creatives["known_identity_count"] == 2
    assert creatives["largest_identity"] == "copied"
    assert creatives["leave_largest_out"]["entity_count"] == 1
    assert creatives["leave_largest_out"]["pace_ratio_lower"] == 1


def test_bounded_growth_shares_do_not_claim_exact_attribution(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    add_series(pipeline, cutoff, "exact", [(-48, 100), (-24, 124), (0, 148)])
    rows = []
    for hour, bounds in ((-48, (100, 110)), (-24, (124, 134)), (0, (148, 158))):
        row = record("bounded", cutoff + timedelta(hours=hour), 0)
        row["metrics"]["view_count"] = {
            "lower": bounds[0],
            "upper": bounds[1],
            "precision": "bounded",
        }
        row["attributes"] = {}
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:bounded")
    comparison = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), diagnostics=True
    )["diagnostics"]["comparisons"][0]
    assert comparison["current"]["observed_growth_lower"] == 38
    assert comparison["current"]["observed_growth_upper"] == 58
    assert comparison["continuing_panel"]["pace_ratio_lower"] == pytest.approx(38 / 58)
    assert comparison["continuing_panel"]["pace_ratio_upper"] == pytest.approx(58 / 38)
    identities = comparison["creator_robustness"]
    assert identities["largest_identity_growth_share_lower"] == pytest.approx(24 / 58)
    assert identities["largest_identity_growth_share_upper"] == pytest.approx(24 / 38)
    assert identities["unknown_identity_growth_share_lower"] == pytest.approx(14 / 38)
    assert identities["unknown_identity_growth_share_upper"] == pytest.approx(34 / 58)


def test_distribution_and_reach_stay_separate_and_creator_ids_are_source_scoped(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    for source in ("tiktok_display", "explicit_public_import"):
        add_series(
            pipeline,
            cutoff,
            "same-id",
            [(-48, 100), (-24, 124), (0, 148)],
            attributes={"creator_id": "same-creator"},
            account_id="authorized",
            source=source,
        )
    ad = record("ad", cutoff, 0)
    ad["entity_type"] = "ad"
    ad["metrics"] = {"reach": {"lower": 10000, "upper": 20000, "precision": "bounded"}}
    pipeline.import_records([ad], synthetic=True)
    pipeline.review_link("trend", "fixture_import:ad:ad")
    diagnostics = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), diagnostics=True
    )["diagnostics"]
    comparison = diagnostics["comparisons"][0]
    assert comparison["current"]["observed_growth_lower"] == 48
    assert comparison["creator_robustness"]["known_identity_count"] == 2
    contexts = diagnostics["source_distribution"]["contexts"]
    assert len(contexts) == 3
    authorized = next(row for row in contexts if row["source"] == "tiktok_display")
    assert authorized["distribution_context"] == "authorized_account"
    assert authorized["organic_distribution"] == "not_established"
    reach = next(row for row in contexts if row["entity_type"] == "ad")
    assert reach["reach_measurements_preview"][0]["measurement"]["upper"] == 20000
    assert reach["reach_measurements_preview"][0]["observation_id"] == ad["observation_id"]
    assert {
        row["current"]["observed_growth_lower"]
        for row in comparison["source_distribution_contexts"]
    } == {24}


def test_resets_and_changed_counter_scopes_do_not_create_comparable_panels(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    add_series(pipeline, cutoff, "reset", [(-48, 100), (-24, 124), (-12, 1), (0, 13)])
    rows = []
    for hour, value in ((-48, 100), (-24, 124), (-12, 10), (0, 22)):
        row = record("scope-change", cutoff + timedelta(hours=hour), value)
        row["attributes"]["counter_scope"] = "new" if hour > -24 else "old"
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:scope-change")
    comparison = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), diagnostics=True
    )["diagnostics"]["comparisons"][0]
    assert comparison["current"]["observed_growth_lower"] == 24
    assert comparison["current"]["counter_correction_count"] == 2
    assert comparison["continuing_panel"]["entity_count"] == 1
    assert comparison["continuing_panel"]["pace_ratio_lower"] == 1
    assert comparison["other_current_evidence"]["entity_count"] == 1
    assert comparison["other_current_evidence"]["observed_growth_lower"] == 12


@pytest.mark.parametrize("guard", ["future_grant", "other_account", "retirement", "reassigned"])
def test_diagnostics_revalidate_source_retirement_and_global_ownership(pipeline, guard):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    datasets = add_series(pipeline, cutoff, "guarded", [(-48, 100), (-24, 124), (0, 148)])
    insights = TrendInsights(pipeline.catalog, pipeline.history)
    initial = insights.describe("trend", cutoff, True, windows_hours=(24,), diagnostics=True)
    assert initial["diagnostics"]["comparisons"][0]["current"]["observed_growth_lower"] == 24
    if guard in {"future_grant", "other_account"}:
        pipeline.catalog.execute(
            "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,"
            "details) "
            "VALUES('fixture_import',%s,'collect','available',%s,'{}')",
            (
                "other" if guard == "other_account" else "",
                cutoff + timedelta(days=1) if guard == "future_grant" else cutoff,
            ),
        )
    elif guard == "retirement":
        pipeline.catalog.invalidate_dataset(datasets[0], "source_retired")
    else:
        pipeline.create_product("other", "Other", synthetic=True)
        pipeline.catalog.execute(
            "INSERT INTO product_links(decision_id,product_id,entity_key,available_at,status,"
            "confidence,method) VALUES('new-owner','other','fixture_import:video:guarded',"
            "%s,'confirmed',0.1,'historical-import')",
            (cutoff - timedelta(hours=1),),
        )
    refreshed = insights.describe("trend", cutoff, True, windows_hours=(24,), diagnostics=True)
    comparison = refreshed["diagnostics"]["comparisons"][0]
    assert comparison["current"]["observed_growth_lower"] is None
    assert comparison["continuing_panel"]["pace_ratio_lower"] is None
    assert refreshed["report_id"] != initial["report_id"]
    assert comparison["creator_robustness"]["stress_check_status"] == "no_known_identity"


def test_diagnostics_have_one_selected_product_read_and_fail_closed_on_budgets(
    pipeline, monkeypatch
):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    add_series(pipeline, cutoff, "bounded", [(-48, 100), (-24, 124), (0, 148)])
    insights = TrendInsights(pipeline.catalog, pipeline.history)
    reads = []
    native_read = pipeline.history.read

    def read_partition(*args, **kwargs):
        reads.append(kwargs)
        return native_read(*args, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", read_partition)
    report = insights.describe("trend", cutoff, True, diagnostics=True)
    assert len(reads) == 1
    assert reads[0]["entity_keys"] == {"fixture_import:video:bounded"}
    assert reads[0]["since"] == cutoff - timedelta(hours=360)
    assert [row["window_hours"] for row in report["diagnostics"]["comparisons"]] == [24, 72, 168]
    with pytest.raises(RowBudgetExceeded):
        insights.describe("trend", cutoff, True, diagnostics=True, max_rows=2)
    with pytest.raises(ValueError, match="unavailable"):
        insights.describe("trend", cutoff, False, diagnostics=True)
    with pytest.raises(ValueError, match="Invalid bounded"):
        insights.describe("trend", cutoff, True, diagnostics="yes")


def test_distribution_changes_keep_measured_contexts_separate(pipeline):
    cutoff = now() + timedelta(days=3)
    pipeline.create_product("trend", "Trend", synthetic=True)
    rows = []
    for hour, value, distribution in (
        (-48, 100, "public_unknown"),
        (-24, 124, "public_unknown"),
        (-18, 130, "public_unknown"),
        (-12, 136, "paid"),
        (-6, 142, "paid"),
        (0, 148, "paid"),
    ):
        row = record("changed", cutoff + timedelta(hours=hour), value)
        row["attributes"]["distribution"] = distribution
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("trend", "fixture_import:video:changed")
    comparison = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "trend", cutoff, True, windows_hours=(24,), diagnostics=True
    )["diagnostics"]["comparisons"][0]
    assert comparison["current"]["observed_growth_lower"] == 18
    assert comparison["current"]["exclusion_counts"]["changed_distribution_context"] == 1
    assert comparison["continuing_panel"]["entity_count"] == 0
    contexts = {
        row["distribution_context"]: row for row in comparison["source_distribution_contexts"]
    }
    assert contexts["declared:public_unknown"]["current"]["observed_growth_lower"] == 6
    assert contexts["declared:paid"]["current"]["observed_growth_lower"] == 12
