"""Actual PostgreSQL/history behavior for bounded competition and price reports."""

from datetime import timedelta

import pytest

from sm_trending_products.competition_insights import CompetitionInsights
from sm_trending_products.contracts import now
from sm_trending_products.history import RowBudgetExceeded

pytestmark = pytest.mark.integration


def observation(
    entity,
    timestamp,
    *,
    price="20",
    seller="seller-a",
    kind="offer",
    market="FR",
    query="tools-fr-v1",
    coverage="first-two-pages-v1",
    **attributes,
):
    return {
        "observation_id": f"{entity}-{timestamp.isoformat()}-{price}",
        "source": "competition_fixture",
        "entity_type": kind,
        "entity_id": entity,
        "synthetic": True,
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "market": market,
        "metrics": {},
        "attributes": {
            "seller_id" if kind == "offer" else "advertiser_id": seller,
            "query_scope": query,
            "coverage_scope": coverage,
            "distribution": "offer_listing" if kind == "offer" else "advertisement",
            "price": price,
            "currency": "EUR",
            "variant_id": "blue-1pack",
            "price_basis": "per_item",
            "tax_basis": "tax_included",
            "shipping_basis": "shipping_excluded",
            **attributes,
        },
    }


def seed(pipeline, rows=None):
    cutoff = now() + timedelta(days=1)
    pipeline.create_product("competitive", "Competitive product", synthetic=True, market="FR")
    if rows is None:
        rows = []
        for hours, price in ((24, "20"), (12, "18"), (1, "17")):
            rows.append(observation("offer-a", cutoff - timedelta(hours=hours), price=price))
        for hours, price in ((12, "22"), (1, "21")):
            rows.append(
                observation(
                    "offer-b", cutoff - timedelta(hours=hours), price=price, seller="seller-b"
                )
            )
        for entity, seller, hours in (
            ("ad-a", "advertiser-a", 24),
            ("ad-a", "advertiser-a", 12),
            ("ad-b", "advertiser-b", 12),
            ("ad-b", "advertiser-b", 1),
        ):
            rows.append(
                observation(entity, cutoff - timedelta(hours=hours), kind="ad", seller=seller)
            )
    datasets = pipeline.import_records(rows, synthetic=True)
    for row in rows:
        pipeline.review_link(
            "competitive", f"competition_fixture:{row['entity_type']}:{row['entity_id']}"
        )
    return CompetitionInsights(pipeline.catalog, pipeline.history), cutoff, datasets


def test_single_bounded_read_reused_for_offers_ads_and_exact_decimal_prices(pipeline, monkeypatch):
    insights, cutoff, _ = seed(pipeline)
    reads = []
    original = pipeline.history.read

    def captured(*args, **kwargs):
        reads.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", captured)
    report = insights.describe("competitive", cutoff, True)
    assert len(reads) == 1
    assert len(reads[0]["entity_keys"]) == 4
    assert report["status"] == "observed"
    assert report["competition_growth"] is report["market_saturation_percent"] is None
    offers = next(row for row in report["scopes"] if row["distribution"] == "offer_listing")
    assert offers["observed_seller_count"] == 2
    assert [row["newly_observed_seller_count"] for row in offers["timeline_preview"]] == [1, 1, 0]
    assert [row["continuing_observed_seller_count"] for row in offers["timeline_preview"]] == [
        0,
        1,
        2,
    ]
    assert offers["price_interval_count"] == offers["observed_price_decrease_count"] == 3
    first = offers["price_offers_preview"][0]
    assert first["intervals_preview"][0]["change_lower"] == "-2"
    assert first["price_context"]["currency"] == "EUR"
    assert insights.describe("competitive", cutoff, True)["report_id"] == report["report_id"]
    with pytest.raises(RowBudgetExceeded):
        insights.describe("competitive", cutoff, True, max_rows=2)


def test_unknown_country_query_and_coverage_never_become_zero_competition(pipeline):
    base = now()
    rows = [
        observation("global", base + timedelta(hours=hour), market=None, language="fr")
        for hour in (0, 1)
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    report = insights.describe("competitive", cutoff, True)
    assert report["status"] == "not_comparable"
    assert report["scopes"][0]["reason"] == "unknown_observed_country"
    assert report["scopes"][0]["observed_seller_count"] is None
    filtered = insights.describe("competitive", cutoff, True, country="FR")
    assert filtered["scopes"] == []
    assert filtered["excluded_observations"]["unknown_observed_country"] == 2
    more = [
        observation("undeclared", base + timedelta(hours=hour), query=None, coverage=None)
        for hour in (2, 3)
    ]
    pipeline.import_records(more, synthetic=True)
    pipeline.review_link("competitive", "competition_fixture:offer:undeclared")
    filtered = insights.describe("competitive", cutoff, True, query_scope="tools-fr-v1")
    assert filtered["status"] == "not_comparable"
    assert filtered["excluded_observations"]["undeclared_query_scope"] == 2
    unfiltered = insights.describe("competitive", cutoff, True)
    undeclared = next(row for row in unfiltered["scopes"] if row["observed_country"] == "FR")
    assert undeclared["reason"] == "undeclared_query_scope"
    assert undeclared["price_interval_count"] == 0


def test_changed_queries_coverage_and_returning_scope_break_comparison(pipeline):
    base = now()
    rows = [
        observation(
            "changing",
            base + timedelta(hours=index),
            price=str(20 - index),
            query="same" if index != 1 else "broader",
            coverage="page1" if index != 1 else "pages1to10",
        )
        for index in range(3)
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    report = insights.describe("competitive", cutoff, True)
    assert report["scope_change_count"] == 2
    assert all(row["price_interval_count"] == 0 for row in report["scopes"])
    same = next(row for row in report["scopes"] if row["query_scope"] == "same")
    assert same["status"] == "not_comparable"
    assert same["timeline_preview"][-1]["interval_reason"] == "coverage_context_interrupted"
    assert report["competition_growth"] is None


@pytest.mark.parametrize(
    "field,changed",
    [
        ("currency", "USD"),
        ("variant_id", "red-2pack"),
        ("tax_basis", "tax_excluded"),
        ("shipping_basis", "shipping_included"),
        ("price_basis", "per_pack"),
    ],
)
def test_currency_variant_tax_shipping_and_basis_changes_do_not_make_price_pressure(
    pipeline, field, changed
):
    base = now()
    rows = [
        observation("quote", base, price="20"),
        observation("quote", base + timedelta(hours=1), price="10", **{field: changed}),
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    scope = insights.describe("competitive", cutoff, True)["scopes"][0]
    assert scope["price_interval_count"] == scope["observed_price_decrease_count"] == 0
    assert scope["price_offers_preview"][0]["excluded_quotes_or_intervals"] == {
        "changed_variant_currency_tax_shipping_or_price_basis": 1
    }


def test_price_intervals_retain_their_currency_before_the_latest_currency_switch(pipeline):
    base = now()
    rows = [
        observation("quote", base + timedelta(hours=index), price=price, currency=currency)
        for index, (price, currency) in enumerate(
            (("20", "EUR"), ("18", "EUR"), ("30", "USD"), ("25", "USD"))
        )
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    scope = insights.describe("competitive", cutoff, True)["scopes"][0]
    offer = scope["price_offers_preview"][0]
    assert offer["price_context"]["currency"] == "USD"
    assert offer["observed_decrease_count"] == 2
    assert [row["change_lower"] for row in offer["intervals_preview"]] == ["-2", "-5"]
    assert [row["price_context"]["currency"] for row in offer["intervals_preview"]] == [
        "EUR",
        "USD",
    ]


@pytest.mark.parametrize(
    "field,initial,changed",
    [
        ("currency", "EUR", "USD"),
        ("variant_id", "blue-1pack", "red-2pack"),
        ("tax_basis", "tax_included", "tax_excluded"),
        ("shipping_basis", "shipping_excluded", "shipping_included"),
        ("price_basis", "per_item", "per_pack"),
    ],
)
def test_price_context_round_trips_keep_each_interval_and_never_bridge_transitions(
    pipeline, field, initial, changed
):
    base = now()
    rows = [
        observation("quote", base + timedelta(hours=index), price=price, **{field: context})
        for index, (price, context) in enumerate(
            (
                ("20", initial),
                ("18", initial),
                ("30", changed),
                ("25", changed),
                ("20", initial),
                ("19", initial),
            )
        )
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    scope = insights.describe("competitive", cutoff, True)["scopes"][0]
    offer = scope["price_offers_preview"][0]
    intervals = offer["intervals_preview"]
    assert [row["change_lower"] for row in intervals] == ["-2", "-5", "-1"]
    assert [row["price_context"][field] for row in intervals] == [initial, changed, initial]
    assert all(row["collection_scope_id"] == scope["scope_id"] for row in intervals)
    assert all(row["interval_hours"] == 1 for row in intervals)
    assert [row["evidence_refs"] for row in intervals] == [
        [rows[index]["observation_id"], rows[index + 1]["observation_id"]] for index in (0, 2, 4)
    ]
    assert offer["comparable_interval_count"] == offer["observed_decrease_count"] == 3
    assert offer["excluded_quotes_or_intervals"] == {
        "changed_variant_currency_tax_shipping_or_price_basis": 2
    }
    assert offer["price_context_scope"] == "latest_valid_quote"


def test_unknown_price_context_bounded_quotes_and_revisions_stay_explicit(pipeline):
    base = now()
    rows = [
        observation("quote", base, price="20"),
        observation("quote", base + timedelta(hours=1), price="15"),
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    revision = observation(
        "quote",
        base + timedelta(hours=1),
        price="14",
        price_lower="12",
        price_upper="16",
        price_precision="bounded",
    )
    revision["available_at"] = cutoff.isoformat()
    pipeline.import_records([revision], synthetic=True)
    report = insights.describe("competitive", cutoff, True)
    assert report["revision_count"] == 1
    quote = report["scopes"][0]["price_offers_preview"][0]
    assert quote["comparable_interval_count"] == 1
    assert quote["intervals_preview"][0]["change_precision"] == "bounded"
    assert quote["intervals_preview"][0]["change_lower"] == "-8"
    assert quote["intervals_preview"][0]["change_upper"] == "-4"
    future = observation("quote", base + timedelta(hours=1), price="1")
    future["available_at"] = (cutoff + timedelta(seconds=1)).isoformat()
    pipeline.import_records([future], synthetic=True)
    assert insights.describe("competitive", cutoff, True)["report_id"] == report["report_id"]
    unknown = observation("quote", base + timedelta(hours=2), price="1", tax_basis=None)
    pipeline.import_records([unknown], synthetic=True)
    quote = insights.describe("competitive", cutoff, True)["scopes"][0]["price_offers_preview"][0]
    assert quote["excluded_quotes_or_intervals"]["unknown_tax_basis"] == 1
    assert quote["comparable_interval_count"] == 1


def test_absent_ads_remain_stale_not_departed_and_gaps_are_not_continuous(pipeline):
    insights, cutoff, _ = seed(pipeline)
    report = insights.describe("competitive", cutoff, True, max_age_hours=6)
    ads = next(row for row in report["scopes"] if row["distribution"] == "advertisement")
    assert ads["fresh_entity_count"] == ads["stale_entity_count"] == 1
    assert ads["observed_advertiser_count"] == 2
    assert ads["status"] == "not_comparable"  # All actual intervals exceed the gap limit.
    assert "departure_count" not in ads
    stale = insights.describe("competitive", cutoff + timedelta(days=4), True)
    assert stale["status"] == "not_comparable"
    assert {row["reason"] for row in stale["scopes"]} == {"stale_latest_capture"}


def test_future_grant_other_account_and_withdrawal_deny_historical_evidence(pipeline):
    insights, cutoff, _ = seed(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('competition_fixture','other-account','collect','available',%s,'{}')",
        (cutoff + timedelta(days=1),),
    )
    denied = insights.describe("competitive", cutoff, True)
    assert denied["eligible_observation_count"] == 0 and denied["scopes"] == []
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('competition_fixture','','collect','available',%s,'{}')",
        (cutoff - timedelta(days=2),),
    )
    assert insights.describe("competitive", cutoff, True)["status"] == "observed"
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('competition_fixture','','collect','denied',%s,'{}')",
        (cutoff,),
    )
    assert insights.describe("competitive", cutoff, True)["eligible_observation_count"] == 0
    with pytest.raises(ValueError, match="unavailable"):
        insights.describe("competitive", cutoff, False)


def test_family_scope_and_global_owner_resolution_preserve_identity_cutoff(pipeline):
    insights, cutoff, _ = seed(pipeline)
    pipeline.create_product("related", "Related product", synthetic=True)
    pipeline.set_family("competitive", "family")
    pipeline.set_family("related", "family")
    row = observation("related", cutoff - timedelta(hours=1), seller="related-seller")
    pipeline.import_records([row], synthetic=True)
    pipeline.review_link("related", "competition_fixture:offer:related")
    report = insights.describe("competitive", cutoff, True, family=True)
    assert report["product_ids"] == ["competitive", "related"]
    assert report["input_observation_count"] == 10
    pipeline.create_product("other-owner", "Other owner", synthetic=True)
    pipeline.catalog.execute(
        "INSERT INTO product_links(decision_id,product_id,entity_key,available_at,status,"
        "confidence,method) VALUES('reassign','other-owner','competition_fixture:offer:offer-a',"
        "%s,'confirmed',0.1,'review')",
        (cutoff - timedelta(minutes=1),),
    )
    assert insights.describe("competitive", cutoff, True)["input_observation_count"] == 6
    unknown = insights.describe("other-owner", cutoff, True, family=True)
    assert unknown["family_id"] is None
    assert "Unknown family" in unknown["limitations"][-1]


def test_retirement_and_simultaneous_revision_conflicts_do_not_leak_price_claims(pipeline):
    insights, cutoff, datasets = seed(pipeline)
    conflicting = observation("offer-a", cutoff - timedelta(hours=12), price="1")
    # The same availability instant with a distinct observation ID is ambiguous,
    # rather than an observation-ID lexical winner.
    pipeline.import_records([conflicting], synthetic=True)
    report = insights.describe("competitive", cutoff, True)
    assert report["conflicting_entity_count"] == 1
    quote = next(
        row
        for scope in report["scopes"]
        for row in scope["price_offers_preview"]
        if row["entity_key"].endswith(":offer-a")
    )
    assert quote["comparable_interval_count"] == 0
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('competition_fixture',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' "
        "WHERE source='competition_fixture'"
    )
    pipeline.retention(apply=True)
    report = insights.describe("competitive", cutoff, True)
    assert report["scopes"] == [] and report["input_observation_count"] == 0
    assert datasets


def test_malformed_imported_price_context_is_reported_as_unknown(pipeline, monkeypatch):
    base = now()
    rows = [
        observation("malformed", base, tax_basis=["tax_included"]),
        observation("malformed", base + timedelta(hours=1), shipping_basis={"included": True}),
    ]
    insights, cutoff, _ = seed(pipeline, rows)
    quote = insights.describe("competitive", cutoff, True)["scopes"][0]["price_offers_preview"][0]
    assert quote["status"] == "not_comparable"
    assert quote["excluded_quotes_or_intervals"] == {
        "unknown_shipping_basis": 1,
        "unknown_tax_basis": 1,
    }

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid public scope must fail before source reads")

    monkeypatch.setattr(pipeline.history, "read", forbidden)
    for configuration in (
        {"max_age_hours": True},
        {"country": 42},
        {"min_link_confidence": "high"},
        {"max_age_hours": float("nan")},
    ):
        with pytest.raises(ValueError, match="Invalid bounded competition"):
            insights.describe("competitive", cutoff, True, **configuration)


def test_distribution_requires_explicit_bounded_metadata_and_accounts_remain_unknown(pipeline):
    base = now()
    rows = []
    malformed = (None, {}, [], "", "x" * 129, "paid\ntraffic", "unknown")
    for index, distribution in enumerate(malformed):
        for hour in (0, 1):
            row = observation(
                f"unknown-{index}",
                base + timedelta(hours=hour),
                price=str(20 - hour),
                distribution=distribution,
            )
            if index == 0:
                row["attributes"].pop("distribution")
            rows.append(row)
    for hour in (0, 1):
        rows.append(observation("declared", base + timedelta(hours=hour), price=str(20 - hour)))
    insights, cutoff, _ = seed(pipeline, rows)
    report = insights.describe("competitive", cutoff, True)
    unknown = next(scope for scope in report["scopes"] if scope["distribution"] is None)
    assert unknown["status"] == "not_comparable"
    assert unknown["reason"] == "unknown_distribution_scope"
    assert unknown["observed_entity_count"] == len(malformed)
    assert unknown["comparable_interval_count"] == unknown["price_interval_count"] == 0
    assert unknown["observed_seller_count"] is None
    assert all(
        offer["excluded_quotes_or_intervals"] == {"unknown_distribution_scope": 2}
        for offer in unknown["price_offers_preview"]
    )
    assert all(
        point["newly_observed_seller_count"] is None for point in unknown["timeline_preview"]
    )
    declared = next(scope for scope in report["scopes"] if scope["distribution"] == "offer_listing")
    assert declared["status"] == "observed"
    assert declared["comparable_interval_count"] == declared["price_interval_count"] == 1
    assert declared["source_account"] is None
    assert declared["source_account_scope"] == "unknown"
    assert declared["source_account_independence"] == "not_established"
