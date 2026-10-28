"""Private briefs through native product, history, transcript and dossier boundaries."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from threading import Event

import pytest
from test_evaluation_integration import seed_product
from test_launch_dossiers import draft
from test_transcript_insights import import_selected, selected

from sm_trending_products.catalog import InvalidationPending
from sm_trending_products.commerce import LaunchError, LaunchService
from sm_trending_products.content_insights import ContentInsights
from sm_trending_products.contracts import now
from sm_trending_products.decision_brief import DecisionBrief
from sm_trending_products.history import RowBudgetExceeded
from sm_trending_products.matching import MatchingProfile
from sm_trending_products.product_cohorts import CohortInsights
from sm_trending_products.trend_insights import TrendInsights

pytestmark = pytest.mark.integration


def brief(pipeline, product="p1", cutoff=None, **kwargs):
    return DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance).describe(
        product, "FR", cutoff or now(), True, **kwargs
    )


def codes(result, bucket):
    return {row["code"] for row in result[bucket]}


def test_partial_brief_never_initializes_modules_or_turns_absence_into_success(pipeline):
    pipeline.create_product("p1", "Selected physical product", synthetic=True)
    cutoff = now()
    assert (
        pipeline.catalog.one("SELECT to_regclass('application_module_migrations') AS relation")[
            "relation"
        ]
        is None
    )
    report = brief(pipeline, cutoff=cutoff, taxonomy_id="reviewed")
    assert report["status"] == "watch"
    assert report["sections"]["themes"]["status"] == "not_initialized"
    assert report["sections"]["comparables"]["status"] == "not_initialized"
    assert report["sections"]["economics"]["status"] == "not_requested"
    assert report["sections"]["competition"]["report"]["status"] == "not_comparable"
    assert "actual_purchases_and_product_market_fit_not_established" in codes(report, "missing")
    assert report["observed_audience_country"] is None
    assert report["audience_coverage"] == "unknown"
    assert (
        report["report_id"] == brief(pipeline, cutoff=cutoff, taxonomy_id="reviewed")["report_id"]
    )
    assert (
        pipeline.catalog.one("SELECT to_regclass('application_module_migrations') AS relation")[
            "relation"
        ]
        is None
    )


def test_unavailable_commerce_retains_exact_distinct_assumption_selections(pipeline):
    pipeline.create_product("p1", "Selected product", synthetic=True)
    cutoff = now()
    first = brief(
        pipeline,
        cutoff=cutoff,
        dossier_id="uninitialized",
        expected_revision=1,
        scenario_profile={"scenarios": [{"name": "declared", "ad_cost": "1"}]},
    )
    second = brief(
        pipeline,
        cutoff=cutoff,
        dossier_id="uninitialized",
        expected_revision=1,
        scenario_profile={"scenarios": [{"name": "declared", "ad_cost": "2"}]},
    )
    assert first["sections"]["economics"]["status"] == "not_initialized"
    assert first["selection"]["scenario_profile"]["scenarios"][0]["ad_cost"] == "1"
    assert (
        first["selection"]["scenario_profile_hash"] != second["selection"]["scenario_profile_hash"]
    )
    assert first["report_id"] != second["report_id"]


def test_composition_keeps_diagnostics_bounds_and_does_not_exhaust_default_pool(
    pipeline, monkeypatch
):
    seed_product(pipeline, "p1")
    seed_product(pipeline, "unrelated")
    cutoff = now()
    expected = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "p1", cutoff, True, diagnostics=True
    )
    reads, active, peak = [], 0, 0
    history_read = pipeline.history.read
    original_connection = pipeline.catalog.pool.connection

    def capture(*args, **kwargs):
        reads.append(kwargs)
        return history_read(*args, **kwargs)

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

    monkeypatch.setattr(pipeline.history, "read", capture)
    monkeypatch.setattr(pipeline.catalog.pool, "connection", connection)
    report = brief(pipeline, cutoff=cutoff)
    assert report["sections"]["trends"]["report"] == expected
    assert report["status"] == "investigate"
    assert len(reads) == 1
    assert reads[0]["entity_keys"] == {"fixture_import:video:video-p1"}
    assert reads[0]["max_rows"] <= 50000
    assert peak <= 3
    assert "positive_observed_attention" in codes(report, "support")
    assert report["status_policy"]["launch_authorized"] is False


def test_quoted_text_is_current_permissioned_evidence_not_cached_after_retirement(pipeline):
    texts, value = selected(pipeline)
    import_selected(texts, value)
    cutoff = now()
    first = brief(pipeline, "p", cutoff)
    assert first["sections"]["themes"]["report"]["distinct_text_count"] == 1
    assert any(
        mention["quote"]
        for text in first["sections"]["themes"]["report"]["texts"]
        for mention in text["mentions"]
    )
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    repeated = brief(pipeline, "p", cutoff)
    assert repeated["sections"]["themes"]["report"]["texts"] == []
    assert repeated["report_id"] != first["report_id"]
    assert "no_eligible_supplied_text_at_cutoff" in codes(repeated, "missing")
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_retirement_cannot_interleave_between_brief_sections(pipeline, monkeypatch):
    texts, value = selected(pipeline)
    import_selected(texts, value)
    cutoff = now()
    started = Event()
    original = ContentInsights.aggregate
    retiring = []

    def retire():
        started.set()
        try:
            pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
        except InvalidationPending:
            return "retry_after_reader"
        return "retired"

    with ThreadPoolExecutor(max_workers=1) as executor:

        def aggregate(service, *args, **kwargs):
            retiring.append(executor.submit(retire))
            assert started.wait(1)
            assert retiring[0].result(timeout=5) == "retry_after_reader"
            result = original(service, *args, **kwargs)
            return result

        monkeypatch.setattr(ContentInsights, "aggregate", aggregate)
        report = brief(pipeline, "p", cutoff)
        assert report["sections"]["themes"]["report"]["texts"]
    monkeypatch.setattr(ContentInsights, "aggregate", original)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert brief(pipeline, "p", cutoff)["sections"]["themes"]["report"]["texts"] == []


def test_row_budget_error_is_not_disguised_as_a_valid_empty_section(pipeline):
    seed_product(pipeline, "p1")
    with pytest.raises(RowBudgetExceeded):
        brief(pipeline, max_rows=1)


def test_exact_dossier_and_revision_pin_scenarios_and_keep_cash_unknown(pipeline):
    seed_product(pipeline, "p1")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create")
    profile = {"scenarios": [{"name": "declared-only", "ad_cost": "5"}]}
    cutoff = now()
    report = brief(
        pipeline,
        cutoff=cutoff,
        dossier_id="launch-p1-fr",
        expected_revision=1,
        scenario_profile=profile,
    )
    economics = report["sections"]["economics"]["report"]
    assert economics["revision"] == 1
    assert economics["country"] == report["target_country"] == "FR"
    assert economics["source_facts"]["draft"] == launches.dossier("launch-p1-fr", True)["draft"]
    assert report["status"] == "ready_to_review"
    assert "cash_volume_or_timing_unknown" in codes(report, "missing")
    assert "actual_purchases_and_product_market_fit_not_established" in codes(report, "missing")
    assert report["status_policy"]["predictive_probability"] is None
    launches.save(draft(note="New reviewed supplier fact"), 1, "revise")
    with pytest.raises(LaunchError) as error:
        brief(pipeline, dossier_id="launch-p1-fr", expected_revision=1, scenario_profile=profile)
    assert error.value.code == "revision_conflict"
    historical = brief(
        pipeline,
        cutoff=cutoff,
        dossier_id="launch-p1-fr",
        expected_revision=2,
        scenario_profile=profile,
    )
    assert historical["sections"]["economics"]["report"] is None
    assert historical["sections"]["economics"]["reason"] == (
        "selected_dossier_revision_not_available_at_cutoff"
    )


@pytest.mark.parametrize(
    ("known_ad_cost", "include_unknown", "expected_miss"),
    [("100", True, True), ("5", True, False), (None, True, False), ("100", False, True)],
)
def test_known_scenario_misses_remain_contrary_when_other_points_are_unknown(
    pipeline, known_ad_cost, include_unknown, expected_miss
):
    seed_product(pipeline, "p1")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create-mixed-scenarios")
    scenarios = []
    if known_ad_cost is not None:
        scenarios.append({"name": "known", "ad_cost": known_ad_cost})
    if include_unknown:
        scenarios.append({"name": "unknown-ad-cost"})
    report = brief(
        pipeline,
        dossier_id="launch-p1-fr",
        expected_revision=1,
        scenario_profile={"scenarios": scenarios},
    )
    economics = report["sections"]["economics"]["report"]
    assert ("declared_scenarios_miss_profit_target" in codes(report, "contrary")) is expected_miss
    assert ("scenario_costs_incomplete" in codes(report, "missing")) is include_unknown
    assert report["status"] == "investigate"
    assert "declared_scenarios_meet_target_not_actual_profit" not in codes(report, "support")
    if include_unknown:
        assert economics["summary"]["all_meet_target"] is None
    if expected_miss:
        assert any(point["economics"]["meets_target"] is False for point in economics["scenarios"])
        assert "declared_scenarios_miss_profit_target" in next(
            item["reasons"]
            for item in report["next_useful_facts"]
            if item["dimension"] == "economics"
        )


def test_dossier_cannot_cross_product_or_target_market(pipeline):
    seed_product(pipeline, "p1")
    seed_product(pipeline, "other")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create")
    service = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    for product, country in (("other", "FR"), ("p1", "DE")):
        with pytest.raises(LaunchError) as error:
            service.describe(
                product,
                country,
                now(),
                True,
                dossier_id="launch-p1-fr",
                expected_revision=1,
                scenario_profile={"scenarios": [{"name": "declared", "ad_cost": "5"}]},
            )
        assert error.value.code == "brief_dossier_scope_mismatch"


def test_namespace_and_future_product_are_rejected_and_source_capability_rechecked(pipeline):
    _, _, _ = seed_product(pipeline, "p1")
    service = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    with pytest.raises(ValueError, match="unavailable"):
        service.describe("p1", "FR", now(), False)
    with pytest.raises(ValueError, match="unavailable"):
        brief(pipeline, cutoff=now() - timedelta(days=1))
    cutoff = now()
    assert "positive_observed_attention" in codes(brief(pipeline, cutoff=cutoff), "support")
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('fixture_import','other-account','collect','available',%s,'{}')",
        (cutoff + timedelta(days=1),),
    )
    after = brief(pipeline, cutoff=cutoff)
    assert "positive_observed_attention" not in codes(after, "support")
    assert after["status"] == "watch"


def test_reviewed_taxonomy_selection_is_pinned_and_candidate_conflicts_are_not_identity(pipeline):
    pipeline.create_product("p1", "Steel bottle 500 ml", synthetic=True, market="FR")
    service = CohortInsights(pipeline.catalog)
    service.initialize()
    service.save_taxonomy(
        "types",
        [{"category_id": "bottle", "name": "Bottle"}],
        0,
        "create",
        reviewer="operator",
        rationale="Reviewed physical type",
    )
    service.assign(
        "p1",
        "types",
        "bottle",
        0,
        "assign",
        taxonomy_revision=1,
        synthetic=True,
        reviewer="operator",
        rationale="Reviewed bottle",
    )
    from test_matching import add_candidate

    add_candidate(
        pipeline.identity,
        "wrong-variant",
        "Steel bottle 900 ml",
        synthetic=True,
        attributes={"capacity": "900 ml"},
    )
    add_candidate(pipeline.identity, "missing-variant", "Steel bottle", synthetic=True)
    report = brief(
        pipeline,
        taxonomy_id="types",
        category_id="bottle",
        matching_profile=MatchingProfile(
            aliases=("Steel bottle",), variants={"capacity": "500 ml"}
        ),
    )
    comparable = report["sections"]["comparables"]["report"]
    assert comparable["taxonomy"]["revision"] == 1
    assert comparable["rows"][0]["product_id"] == "p1"
    assert comparable["rows"][0]["percentile"] is None
    assert "candidate_identity_or_variant_conflicts_require_review" in codes(report, "contrary")
    assert "variant_facts_incomplete" in codes(report, "missing")
    assert report["sections"]["variants"]["report"]["suggestions"][0]["already_linked"] is False
    assert pipeline.identity.product_links("p1", now(), True) == []


@pytest.mark.parametrize(
    "changes",
    [
        {"max_rows": True},
        {"max_rows": 100001},
        {"max_products": 10001},
        {"text_limit": 201},
        {"category_id": "no-taxonomy"},
        {"scenario_profile": {}},
        {"dossier_id": "only-dossier"},
        {"source": "x" * 129},
        {"query_scope": "bad\n"},
    ],
)
def test_invalid_requests_fail_before_database_or_history(changes):
    class Unavailable:
        def __getattr__(self, name):
            raise AssertionError("Invalid input reached infrastructure")

    unavailable = Unavailable()
    with pytest.raises(ValueError):
        DecisionBrief(unavailable, unavailable, unavailable).describe(
            "p1", "FR", now(), True, **changes
        )
