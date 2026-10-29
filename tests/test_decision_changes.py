"""Two-cutoff comparisons through native guarded history and dossier interfaces."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta

import pytest
from test_competition_insights import observation
from test_evaluation_integration import record
from test_launch_dossiers import draft
from test_transcript_insights import import_selected, selected

from sm_trending_products.catalog import InvalidationPending
from sm_trending_products.commerce import LaunchError, LaunchService
from sm_trending_products.contracts import canonical, digest, now
from sm_trending_products.decision_brief import DecisionBrief
from sm_trending_products.decision_changes import DecisionChanges, explain_changes
from sm_trending_products.history import RowBudgetExceeded
from sm_trending_products.matching import MatchingProfile
from sm_trending_products.product_cohorts import CohortInsights

pytestmark = pytest.mark.integration


def changes(pipeline, previous, current, product="p1", **kwargs):
    return DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
        product, "FR", previous, current, True, **kwargs
    )


def seed(
    pipeline, *, distribution="public_video", unit="count", correction=False, context_change=False
):
    cutoff = now() + timedelta(days=4)
    pipeline.create_product("p1", "PRIVATE SOURCE TITLE", synthetic=True)
    rows = []
    for hour in range(0, 73, 6):
        value = 100 + hour if hour <= 48 else 148 + (hour - 48) * 2
        if correction and hour == 66:
            value = 1
        row = record("v1", cutoff - timedelta(hours=72 - hour), value)
        row["attributes"]["distribution"] = distribution
        if context_change and hour > 48:
            row["attributes"]["distribution"] = "advertisement"
        row["metrics"]["view_count"]["unit"] = unit
        rows.append(row)
    datasets = pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("p1", "fixture_import:video:v1")
    return cutoff - timedelta(hours=24), cutoff, datasets


def first_window(report):
    return report["trajectory"]["windows"][0]


def test_actual_continuing_panel_growth_uses_kernel_and_leaves_default_brief_unchanged(
    pipeline, monkeypatch
):
    previous, current, _ = seed(pipeline)
    brief = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    original = brief.describe("p1", "FR", current, True)
    baseline_registry = pipeline.catalog.one(
        "SELECT to_regclass('application_module_migrations') AS relation"
    )
    reads, active, peak = [], 0, 0
    original_read, original_connection = pipeline.history.read, pipeline.catalog.pool.connection

    def read(*args, **kwargs):
        reads.append(kwargs)
        return original_read(*args, **kwargs)

    @contextmanager
    def connection(*args, **kwargs):
        nonlocal active, peak
        with original_connection(*args, **kwargs) as value:
            active += 1
            peak = max(peak, active)
            try:
                yield value
            finally:
                active -= 1

    monkeypatch.setattr(pipeline.history, "read", read)
    monkeypatch.setattr(pipeline.catalog.pool, "connection", connection)
    report = changes(pipeline, previous, current)
    measured = first_window(report)
    assert measured["comparison_status"] == "measured"
    assert measured["pace_ratio_lower"] == measured["pace_ratio_upper"] == 2
    assert measured["continuing_entity_count"] == 1
    assert len(reads) == 2
    assert all(row["entity_keys"] == {"fixture_import:video:v1"} for row in reads)
    assert all(row["max_rows"] <= 50_000 for row in reads)
    assert peak <= 4
    assert report["review_state"]["launch_authorized"] is False
    assert report["observed_audience_country"] is None
    assert "PRIVATE SOURCE TITLE" not in canonical(report)
    assert report["report_id"] == digest({k: v for k, v in report.items() if k != "report_id"})
    assert len(canonical(report).encode()) <= report["budgets"]["maximum_output_bytes"]
    assert brief.describe("p1", "FR", current, True) == original
    assert (
        pipeline.catalog.one("SELECT to_regclass('application_module_migrations') AS relation")
        == baseline_registry
    )


@pytest.mark.parametrize(
    "options",
    [
        {"distribution": None},
        {"unit": "EUR"},
        {"correction": True},
        {"context_change": True},
    ],
)
def test_unknown_context_mixed_units_and_corrections_never_become_comparable_growth(
    pipeline, options
):
    previous, current, _ = seed(pipeline, **options)
    report = changes(pipeline, previous, current)
    assert first_window(report)["comparison_status"] == "unavailable"
    assert first_window(report)["pace_ratio_lower"] is None
    assert first_window(report)["pace_ratio_upper"] is None


def test_changed_selection_and_nonadjacent_cutoffs_do_not_turn_hashes_into_growth(pipeline):
    previous, current, _ = seed(pipeline)
    report = changes(pipeline, previous, current, current_selection={"query_scope": "changed"})
    assert report["selection_changes"]["query_scope"]["changed"] is True
    assert first_window(report)["reason"] == "changed_selection_or_configuration"
    assert first_window(report)["pace_ratio_lower"] is None
    nonadjacent = changes(pipeline, previous + timedelta(hours=1), current)
    assert first_window(nonadjacent)["reason"] == (
        "requested_cutoffs_do_not_match_adjacent_kernel_windows"
    )
    assert first_window(nonadjacent)["pace_ratio_lower"] is None


@pytest.mark.parametrize(
    "distribution",
    [
        "unknown",
        "mixed",
        "unspecified",
        "not_available",
        " UnKnOwN ",
        " MIXED ",
        " Unspecified\t",
        " NOT_AVAILABLE ",
        "unknown_distribution",
        "unavailable",
        "not available",
        " Unknown distribution ",
        " NOT--AVAILABLE ",
        "n/a",
        "None",
        "NULL",
        "not_applicable",
        "not applicable",
    ],
)
def test_declared_missing_distribution_retains_facts_without_a_growth_claim(pipeline, distribution):
    previous, current, _ = seed(pipeline, distribution=distribution)
    service = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    original = service.describe("p1", "FR", current, True)
    report = changes(pipeline, previous, current)
    row = first_window(report)
    assert row["comparison_status"] == "unavailable"
    assert row["reason"] == "source_context_unknown_or_incomplete"
    assert row["pace_ratio_lower"] is None and row["pace_ratio_upper"] is None
    assert row["previous_cutoff_measurement"]["observed_growth_lower"] == 24
    assert row["current_cutoff_measurement"]["observed_growth_lower"] == 48
    assert "multiplié par" not in "\n".join(explain_changes(report))
    assert service.describe("p1", "FR", current, True) == original


def test_late_revision_and_future_evidence_remain_distinct_from_trajectory(pipeline):
    previous, current, _ = seed(pipeline)
    before = changes(pipeline, previous, current)
    future = record("v1", current, 1_000_000, available_at=current + timedelta(hours=1))
    pipeline.import_records([future], synthetic=True)
    assert changes(pipeline, previous, current) == before
    late = record(
        "v1", previous - timedelta(hours=6), 1, available_at=previous + timedelta(hours=1)
    )
    pipeline.import_records([late], synthetic=True)
    report = changes(pipeline, previous, current)
    assert first_window(report)["reason"] == "baseline_evidence_changed_or_newly_available"
    assert first_window(report)["pace_ratio_lower"] is None


def test_newly_observed_videos_are_separate_from_continuing_growth(pipeline):
    previous, current, _ = seed(pipeline)
    rows = []
    for hours, views in ((12, 10000), (6, 20000), (0, 30000)):
        row = record("newly-sampled", current - timedelta(hours=hours), views)
        row["attributes"]["distribution"] = "public_video"
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("p1", "fixture_import:video:newly-sampled")
    report = changes(pipeline, previous, current)
    assert first_window(report)["newly_observed_entity_count"] == 1
    assert first_window(report)["continuing_entity_count"] == 1
    assert first_window(report)["pace_ratio_lower"] == 2
    assert report["competition"]["market_growth"] is None


def test_historical_dossier_replacement_is_unavailable_but_current_revision_is_exact(pipeline):
    previous, current, _ = seed(pipeline)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "first")
    profile = {"scenarios": [{"name": "declared", "ad_cost": "5"}]}
    selected_revision = {
        "dossier_id": "launch-p1-fr",
        "expected_revision": 1,
        "scenario_profile": profile,
    }
    report = changes(pipeline, previous, current, previous_selection=selected_revision)
    assert report["economics"]["status"] == "available"
    assert report["economics"]["facts_changed"] is False
    assert report["economics"]["assumptions_changed"] is False
    assert report["economics"]["previous"]["quote_readiness_evaluated_at"] != previous.isoformat()
    launches.save(draft(note="PRIVATE SUPPLIER CORRECTION"), 1, "replace")
    revised = {**selected_revision, "expected_revision": 2}
    report = changes(
        pipeline, previous, current, previous_selection=selected_revision, current_selection=revised
    )
    assert report["economics"]["status"] == "unavailable"
    assert report["economics"]["previous"] is None
    assert report["economics"]["current"]["revision"] == 2
    assert report["selections"]["previous"]["expected_revision"] == 1
    assert "PRIVATE SUPPLIER CORRECTION" not in canonical(report)
    assert report["economics"]["facts_changed"] is None
    assert (
        next(row for row in report["coverage"] if row["dimension"] == "economics")["previous"][
            "reason"
        ]
        == "selected_historical_dossier_revision_unavailable"
    )


def test_unsupported_dossier_scope_is_not_silently_unavailable(pipeline):
    previous, current, _ = seed(pipeline)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "first")
    service = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance)
    with pytest.raises(LaunchError) as error:
        service.compare(
            "p1",
            "DE",
            previous,
            current,
            True,
            previous_selection={
                "dossier_id": "launch-p1-fr",
                "expected_revision": 1,
                "scenario_profile": {"scenarios": [{"name": "one"}]},
            },
        )
    assert error.value.code == "brief_dossier_scope_mismatch"


def test_economic_assumptions_and_selected_facts_stay_distinct(pipeline):
    previous, current, _ = seed(pipeline)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create")
    launches.save(draft(dossier_id="alternate-p1-fr", currency="USD"), 0, "alternate")
    prior = {
        "dossier_id": "launch-p1-fr",
        "expected_revision": 1,
        "scenario_profile": {"scenarios": [{"name": "declared", "ad_cost": "5"}]},
    }
    assumptions = {
        **prior,
        "scenario_profile": {"scenarios": [{"name": "declared", "ad_cost": "10"}]},
    }
    report = changes(
        pipeline, previous, current, previous_selection=prior, current_selection=assumptions
    )
    assert report["economics"]["facts_changed"] is False
    assert report["economics"]["assumptions_changed"] is True
    assert report["economics"]["realized_profit_change"] is None
    facts = changes(
        pipeline,
        previous,
        current,
        previous_selection=prior,
        current_selection={**prior, "dossier_id": "alternate-p1-fr"},
    )
    assert facts["economics"]["facts_changed"] is True
    assert facts["economics"]["previous"]["currency"] == "EUR"
    assert facts["economics"]["current"]["currency"] == "USD"
    assert facts["economics"]["assumptions_changed"] is False


def test_all_optional_sections_and_competition_read_fit_default_pool(pipeline, monkeypatch):
    texts, value = selected(pipeline)
    import_selected(texts, value)
    cutoff = now() + timedelta(days=2)
    rows = [observation("offer", cutoff - timedelta(hours=hour)) for hour in (30, 24, 12, 0)]
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("p", "competition_fixture:offer:offer")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(product_id="p"), 0, "create")
    cohorts = CohortInsights(pipeline.catalog)
    cohorts.initialize()
    cohorts.save_taxonomy(
        "reviewed",
        [{"category_id": "item", "name": "Item"}],
        0,
        "create",
        reviewer="operator",
        rationale="Reviewed type",
    )
    cohorts.assign(
        "p",
        "reviewed",
        "item",
        0,
        "assign",
        taxonomy_revision=1,
        synthetic=True,
        reviewer="operator",
        rationale="Reviewed item",
    )
    active, peak = 0, 0
    original_connection = pipeline.catalog.pool.connection

    @contextmanager
    def connection(*args, **kwargs):
        nonlocal active, peak
        with original_connection(*args, **kwargs) as value:
            active += 1
            peak = max(peak, active)
            try:
                yield value
            finally:
                active -= 1

    monkeypatch.setattr(pipeline.catalog.pool, "connection", connection)
    report = changes(
        pipeline,
        cutoff - timedelta(hours=24),
        cutoff,
        product="p",
        previous_selection={
            "taxonomy_id": "reviewed",
            "category_id": "item",
            "dossier_id": "launch-p1-fr",
            "expected_revision": 1,
            "scenario_profile": {"scenarios": [{"name": "one"}]},
            "matching_profile": MatchingProfile(aliases=("Selected product",)),
        },
    )
    assert all(row["previous"]["status"] == "available" for row in report["coverage"])
    themes = next(row for row in report["coverage"] if row["dimension"] == "themes")
    assert themes["quantities"]["previous"]["distinct_text_count"] == 1
    assert report["economics"]["previous"]["revision"] == 1
    assert report["competition"]["scopes"][0]["facts"]["capture_count"]["changed"] is True
    assert peak == 4


def test_retirement_is_fenced_across_both_reads_and_refresh_has_no_first_text(
    pipeline, monkeypatch
):
    texts, value = selected(pipeline)
    import_selected(texts, value)
    previous, current = now(), now() + timedelta(seconds=1)
    original = DecisionBrief.describe
    calls = 0
    with ThreadPoolExecutor(max_workers=1) as executor:

        def describe(service, *args, **kwargs):
            nonlocal calls
            result = original(service, *args, **kwargs)
            calls += 1
            if calls == 1:
                future = executor.submit(
                    pipeline.catalog.invalidate_dataset, value.dataset_id, "source_removed"
                )
                with pytest.raises(InvalidationPending):
                    future.result(timeout=5)
            return result

        monkeypatch.setattr(DecisionBrief, "describe", describe)
        first = changes(pipeline, previous, current, product="p")
    assert "gagner du temps" not in canonical(first)
    monkeypatch.setattr(DecisionBrief, "describe", original)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    repeated = changes(pipeline, previous, current, product="p")
    assert repeated["report_id"] != first["report_id"]
    assert "gagner du temps" not in canonical(repeated)
    first_themes = next(row for row in first["coverage"] if row["dimension"] == "themes")
    last_themes = next(row for row in repeated["coverage"] if row["dimension"] == "themes")
    assert first_themes["current"]["reference"] != last_themes["current"]["reference"]
    assert last_themes["quantities"]["previous"]["distinct_text_count"] == 0
    assert last_themes["quantities"]["current"]["distinct_text_count"] == 0
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_namespace_future_product_changed_grants_and_row_refusal(pipeline):
    previous, current, _ = seed(pipeline)
    service = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance)
    with pytest.raises(ValueError, match="unavailable"):
        service.compare("p1", "FR", previous, current, False)
    with pytest.raises(ValueError, match="unavailable"):
        changes(pipeline, now() - timedelta(days=1), current)
    with pytest.raises(RowBudgetExceeded):
        changes(pipeline, previous, current, max_rows=1)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('fixture_import','other-account','collect','available',%s,'{}')",
        (current,),
    )
    result = changes(pipeline, previous, current)
    assert first_window(result)["comparison_status"] == "unavailable"
    assert first_window(result)["current_cutoff_measurement"]["observed_growth_lower"] is None


@pytest.mark.parametrize(
    "options",
    [
        {"max_rows": True},
        {"max_rows": 100001},
        {"text_limit": 201},
        {"max_products": 10001},
        {"previous_selection": {"expected_revision": True}},
        {"previous_selection": {"category_id": "category"}},
        {"current_selection": {"source": "PRIVATE\n"}},
    ],
)
def test_invalid_comparison_rejects_before_infrastructure(options):
    class Unavailable:
        def __getattr__(self, name):
            raise AssertionError("Invalid input reached infrastructure")

    infrastructure = Unavailable()
    with pytest.raises(ValueError):
        DecisionChanges(infrastructure, infrastructure, infrastructure).compare(
            "p1", "FR", now(), now() + timedelta(hours=1), True, **options
        )
