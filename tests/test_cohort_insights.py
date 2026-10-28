"""Reviewed taxonomy and honest compact panel reports on native SQL and HTTP S3."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from test_evaluation_integration import record

from sm_trending_products.contracts import AssessmentPolicy, now, utc
from sm_trending_products.product_cohorts import CohortInsights

pytestmark = pytest.mark.integration


def review(service, product_id, category="tools", *, expected=0, request=None, version=1):
    return service.assign(
        product_id,
        "physical",
        category,
        expected,
        request or product_id,
        taxonomy_revision=version,
        synthetic=True,
        reviewer="operator",
        rationale="Reviewed item",
    )


def seed(pipeline, count=4, *, markets=None, values=None, units=None):
    service = CohortInsights(pipeline.catalog)
    service.initialize()
    service.save_taxonomy(
        "physical",
        [{"category_id": "tools", "name": "Tools"}, {"category_id": "kitchen", "name": "Kitchen"}],
        0,
        "taxonomy-first",
        reviewer="operator",
        rationale="Reviewed category definitions",
    )
    cutoff = now() + timedelta(seconds=20)
    rows = []
    for index in range(count):
        product_id = f"product-{index}"
        market = markets[index] if markets else "FR"
        pipeline.create_product(product_id, product_id, synthetic=True, market=market)
        for hour, value in ((2, 100), (1, 100 + (values[index] if values else index + 1))):
            row = record(product_id, cutoff - timedelta(hours=hour), value)
            if units:
                row["metrics"]["view_count"]["unit"] = units[index]
            row["market"] = market
            row["published_at"] = (cutoff - timedelta(hours=12)).isoformat()
            rows.append(row)
        review(service, product_id)
    datasets = pipeline.import_records(rows, synthetic=True)
    for index in range(count):
        pipeline.review_link(f"product-{index}", f"fixture_import:video:product-{index}")
    pipeline.assess(cutoff, synthetic=True)
    return service, cutoff, datasets


def test_review_replay_conflicts_and_taxonomy_known_at_cutoff(pipeline):
    service, cutoff, _ = seed(pipeline)
    before = now()
    original = service.taxonomy("physical", before)
    args = ("physical", [{"category_id": "tools", "name": "Hand tools"}], 1, "taxonomy-second")
    changed = service.save_taxonomy(*args, reviewer="operator", rationale="Refined scope")
    assert service.save_taxonomy(*args, reviewer="operator", rationale="Refined scope") == changed
    assert service.taxonomy("physical", before) == original
    with pytest.raises(ValueError, match="request_conflict"):
        service.save_taxonomy(*args, reviewer="different", rationale="Refined scope")
    with pytest.raises(ValueError, match="revision_conflict"):
        service.save_taxonomy(*args[:3], "new-request", reviewer="operator", rationale="Scope")
    assert review(service, "product-0") == review(service, "product-0")
    with pytest.raises(ValueError, match="request_conflict"):
        review(service, "product-0", "kitchen")
    assert all(
        row["reason"] == "taxonomy_revision_requires_review"
        for row in service.compare(cutoff, True, taxonomy_id="physical")["rows"]
    )
    refreshed = review(service, "product-0", expected=1, request="refresh", version=2)
    assert refreshed["taxonomy_revision"] == 2
    assert utc(refreshed["available_at"]) > before


def test_concurrent_assignment_compare_and_swap_has_one_winner(pipeline):
    service, _, _ = seed(pipeline)

    def write(request):
        try:
            return review(service, "product-0", "kitchen", expected=1, request=request)
        except ValueError as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, ("competing-a", "competing-b")))
    assert sum(isinstance(row, dict) for row in results) == 1
    assert "revision_conflict" in results


def test_observed_panel_percentiles_are_bounded_honest_and_family_deduplicated(
    pipeline, monkeypatch
):
    service, cutoff, _ = seed(pipeline)

    def forbidden(*args, **kwargs):
        pytest.fail("Compact cohort reports must not scan raw history")

    monkeypatch.setattr(pipeline.history, "read", forbidden)
    report = service.compare(cutoff, True, taxonomy_id="physical")
    assert [row["percentile"] for row in report["rows"]] == [0, 100 / 3, 200 / 3, 100]
    assert all(row["peer_count"] == 3 for row in report["rows"])
    assert all(row["comparability"] == "limited" for row in report["rows"])
    assert all(row["country_basis"] == "product_target_market" for row in report["rows"])
    assert all(row["observed_audience_country"] is None for row in report["rows"])
    assert all(
        row["source_scope"] == "assessment_aggregate_mixed_or_unknown" for row in report["rows"]
    )
    assert report["report_id"] == service.compare(cutoff, True, taxonomy_id="physical")["report_id"]
    pipeline.set_family("product-0", "shared")
    pipeline.set_family("product-1", "shared")
    report = service.compare(cutoff, True, taxonomy_id="physical", min_peers=2)
    assert [row["peer_count"] for row in report["rows"]] == [2, 2, 2, 2]
    assert report["rows"][2]["percentile"] == 50
    assert service.compare(cutoff, False, taxonomy_id="physical")["rows"] == []
    with pytest.raises(ValueError, match="product budget"):
        service.compare(cutoff, True, taxonomy_id="physical", max_products=2)


def test_unknown_country_ties_stale_and_source_eligibility_are_explicit(pipeline):
    service, cutoff, _ = seed(
        pipeline, 5, markets=["FR", "FR", "FR", "FR", None], values=[5, 5, 5, 5, 5]
    )
    report = service.compare(cutoff, True, taxonomy_id="physical")
    assert [row["reason"] for row in report["rows"]] == ["all_peers_tied"] * 4 + [
        "unknown_target_country"
    ]
    assert all(row["percentile"] is None for row in report["rows"])
    stale = service.compare(cutoff + timedelta(days=4), True, taxonomy_id="physical")
    assert stale["rows"][0]["reason"] == "stale_assessment"
    # A target FR product with globally scoped video evidence never acquires
    # an audience-country label or a fabricated country growth measurement.
    global_row = record("global", cutoff - timedelta(minutes=30), 100000)
    global_row["attributes"]["language"] = "fr"
    pipeline.import_records([global_row], synthetic=True)
    pipeline.create_product("global-target", "French target", synthetic=True, market="FR")
    pipeline.review_link("global-target", "fixture_import:video:global")
    review(service, "global-target")
    pipeline.assess(cutoff, True)
    row = next(
        row
        for row in service.compare(cutoff, True, taxonomy_id="physical")["rows"]
        if row["product_id"] == "global-target"
    )
    assert row["value"] is None and row["percentile"] is None
    assert row["observed_audience_country"] is None and row["target_country"] == "FR"


def test_policy_changes_and_newer_pointers_do_not_mix_or_rewrite_past(pipeline):
    service, cutoff, _ = seed(pipeline)
    earlier = cutoff - timedelta(seconds=1)
    report = service.compare(earlier, True, taxonomy_id="physical")
    assert {row["reason"] for row in report["rows"]} == {"no_current_assessment_at_cutoff"}
    assert all(row["value"] is None for row in report["rows"])
    # A different immutable policy with the same version string must still
    # separate the comparison groups.
    second = pipeline.assess(cutoff, True, AssessmentPolicy(max_age_hours=48))
    pipeline.catalog.execute(
        "UPDATE current_assessments SET run_id=%s WHERE product_id='product-0'",
        (second["run_id"],),
    )
    # The publisher replaced every pointer; restore three original pointers to
    # construct the legitimate partial-publication cohort seen across workers.
    original = pipeline.catalog.one(
        "SELECT run_id FROM assessment_runs WHERE run_id<>%s ORDER BY created_at LIMIT 1",
        (second["run_id"],),
    )["run_id"]
    pipeline.catalog.execute(
        "UPDATE current_assessments SET run_id=%s WHERE product_id<>'product-0'", (original,)
    )
    report = service.compare(cutoff, True, taxonomy_id="physical")
    assert [row["peer_count"] for row in report["rows"]] == [0, 2, 2, 2]
    assert all(row["percentile"] is None for row in report["rows"])


def test_retired_sources_remove_metrics_but_keep_reviewed_category(pipeline):
    service, cutoff, datasets = seed(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('fixture_import',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' "
        "WHERE dataset_id=ANY(%s)",
        (datasets,),
    )
    pipeline.retention(apply=True)
    report = service.compare(cutoff, True, taxonomy_id="physical")
    assert all(row["value"] is None and row["percentile"] is None for row in report["rows"])
    assert all(row["category_id"] == "tools" for row in report["rows"])


def test_original_counter_units_are_uncertified_in_compact_assessments(pipeline):
    service, cutoff, _ = seed(pipeline, units=["count", "seconds", "USD", "count"])
    for metric in ("growth_per_hour", "view_count"):
        report = service.compare(cutoff, True, taxonomy_id="physical", metric=metric)
        assert all(row["percentile"] is None for row in report["rows"])
        assert all(row["reason"] == "uncertified_metric_unit" for row in report["rows"])
        assert all(row["metric_unit"] == "uncertified" for row in report["rows"])
    report = service.compare(cutoff, True, taxonomy_id="physical")
    assert report["metric"] == "attention_score"
    assert all(row["metric_unit"] == "score_points" for row in report["rows"])
    assert all(row["comparability"] == "limited" for row in report["rows"])
