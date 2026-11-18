"""Cutoff comparability through real disjoint diagnostic measurements."""

from copy import deepcopy
from datetime import timedelta

import pytest
from test_evaluation_integration import record

from sm_trending_products.contracts import Observation, canonical, utc
from sm_trending_products.opportunity_trends import compare_cutoff_period, describe_diagnostics

CUTOFF = utc("2026-01-04T00:00:00Z")
PREVIOUS_CUTOFF = CUTOFF - timedelta(hours=24)


def series(
    entity="continuing", *, distribution="public_video", correction=None, pace=1, previous_pace=1
):
    rows = []
    for hour in range(-72, 1, 6):
        views = (
            100 + (hour + 72) * previous_pace
            if hour <= -24
            else 100 + 48 * previous_pace + (hour + 24) * pace
        )
        if hour == correction:
            views = 1
        row = record(entity, CUTOFF + timedelta(hours=hour), views)
        row["attributes"]["distribution"] = distribution
        rows.append(Observation.model_validate(row))
    return rows


def diagnostics(rows, cutoff):
    selected = [row for row in rows if row.observed_at <= cutoff and row.available_at <= cutoff]
    return describe_diagnostics(
        selected,
        {row.entity_key for row in selected},
        cutoff,
        (24,),
        timedelta(hours=24),
    )["comparisons"][0]


def periods(rows):
    return diagnostics(rows, PREVIOUS_CUTOFF)["current"], diagnostics(rows, CUTOFF)


def compare(previous, current, cutoff=PREVIOUS_CUTOFF):
    original = canonical((previous, current))
    result = compare_cutoff_period(previous, current, previous_cutoff=cutoff.isoformat())
    assert canonical((previous, current)) == original
    return result


def assert_unavailable(result, reason):
    assert result == {
        "comparison_status": "unavailable",
        "reason": reason,
        "pace_ratio_lower": None,
        "pace_ratio_upper": None,
    }


def test_continuing_panel_exposes_only_admitted_kernel_ratio_bounds():
    previous, current = periods(series(pace=2))
    assert compare(previous, current) == {
        "comparison_status": "measured",
        "reason": None,
        "pace_ratio_lower": 2,
        "pace_ratio_upper": 2,
    }


def test_later_discovery_changes_coverage_without_changing_the_measured_baseline():
    rows = series(pace=2)
    discovered = series("discovered")[-3:]
    previous, current = periods([*rows, *discovered])
    assert previous["known_entity_count"] == 1
    assert current["previous"]["known_entity_count"] == 2
    assert previous["exclusion_counts"] != current["previous"]["exclusion_counts"]
    assert current["newly_observed"]["entity_count"] == 1
    assert compare(previous, current)["pace_ratio_lower"] == 2


def test_late_baseline_revision_is_not_new_growth():
    rows = series()
    previous, _ = periods(rows)
    revised = record("continuing", PREVIOUS_CUTOFF - timedelta(hours=6), 1)
    revised["attributes"]["distribution"] = "public_video"
    revised["available_at"] = (PREVIOUS_CUTOFF + timedelta(hours=1)).isoformat()
    current = diagnostics([*rows, Observation.model_validate(revised)], CUTOFF)
    assert_unavailable(compare(previous, current), "baseline_evidence_changed_or_newly_available")


def test_adjacent_cutoffs_are_required_before_any_later_measurement_checks():
    previous, current = periods(series(distribution="unknown", correction=-6))
    assert_unavailable(
        compare(previous, current, PREVIOUS_CUTOFF + timedelta(seconds=1)),
        "requested_cutoffs_do_not_match_adjacent_kernel_windows",
    )


def test_truncated_baseline_evidence_cannot_be_compared_from_its_preview():
    rows = [row for entity in range(41) for row in series(str(entity), correction=-6)]
    previous, current = periods(rows)
    assert previous["evidence_count"] > 200
    assert previous["evidence_preview_truncated"]
    assert_unavailable(compare(previous, current), "baseline_reference_preview_incomplete")
    # Either side's incomplete baseline is enough, even if the other side fits.
    complete_previous, complete_current = periods(series())
    assert_unavailable(compare(previous, complete_current), "baseline_reference_preview_incomplete")
    assert_unavailable(compare(complete_previous, current), "baseline_reference_preview_incomplete")


@pytest.mark.parametrize("correction", [-30, -6])
def test_corrections_are_refused_before_unknown_distribution_context(correction):
    previous, current = periods(series(distribution="unknown", correction=correction))
    assert (
        current["previous"]["counter_correction_count"]
        + current["current"]["counter_correction_count"]
        > 0
    )
    assert_unavailable(compare(previous, current), "counter_corrections_limit_comparison")


@pytest.mark.parametrize("distribution", [None, "unknown", " MIXED ", "not available"])
def test_unknown_source_distribution_does_not_make_a_comparable_growth_claim(distribution):
    previous, current = periods(series(distribution=distribution))
    assert current["continuing_panel"]["comparison_status"] == "measured"
    assert_unavailable(compare(previous, current), "source_context_unknown_or_incomplete")


def test_truncated_contexts_refuse_even_a_measured_known_continuing_panel():
    rows = series()
    for entity in range(200):
        rows.extend(series(str(entity), distribution=f"context-{entity}")[-3:])
    previous, current = periods(rows)
    assert not current["previous"]["evidence_preview_truncated"]
    assert current["source_distribution_contexts_truncated"]
    assert current["continuing_panel"]["comparison_status"] == "measured"
    assert_unavailable(compare(previous, current), "source_context_unknown_or_incomplete")


def test_unknown_context_without_continuing_members_does_not_hide_panel_refusal():
    rows = series(distribution="unknown")[-3:]
    previous, current = periods(rows)
    assert current["source_distribution_contexts"][0]["continuing_panel"]["entity_count"] == 0
    assert_unavailable(compare(previous, current), "insufficient_comparable_panel")


def test_zero_previous_pace_keeps_the_kernel_refusal_and_no_ratio():
    previous, current = periods(series(previous_pace=0))
    assert_unavailable(compare(previous, current), "zero_or_uncertain_previous_pace")


def test_measurement_changes_are_not_ignored_as_coverage():
    previous, current = periods(series())
    previous = deepcopy(previous)
    previous["measured_entity_hours"] += 1
    assert_unavailable(compare(previous, current), "baseline_evidence_changed_or_newly_available")
