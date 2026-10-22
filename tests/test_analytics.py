from datetime import UTC, datetime, timedelta

import pytest

from sm_trending_products.contracts import Measurement, Observation, Product, ProductLink

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def video(entity_id: str, hour: int, views: int, **kwargs: object) -> Observation:
    return Observation(
        observation_id=f"{entity_id}-{hour}",
        source="tiktok_display",
        entity_type="video",
        entity_id=entity_id,
        observed_at=BASE + timedelta(hours=hour),
        available_at=BASE + timedelta(hours=hour),
        published_at=BASE,
        metrics={"view_count": Measurement(lower=views, upper=views)},
        attributes={"creator_id": entity_id},
        **kwargs,
    )


def test_attention_uses_interval_growth_and_keeps_sales_unknown() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="tiktok_display:video:v1", available_at=BASE)
    rows = assess(
        [video("v1", 1, 100), video("v1", 3, 500)], [product], [link], BASE + timedelta(hours=3)
    )
    assert len(rows) == 1
    assert rows[0]["features"]["growth_per_hour"] == pytest.approx(200)
    assert rows[0]["features"]["view_count"] == 500
    assert rows[0]["attention"]["score"] is not None
    assert rows[0]["demand"]["score"] is None
    assert rows[0]["demand"]["status"] == "unknown"


def offer(entity_id: str, hour: int, lower: int, upper: int, **attributes: object) -> Observation:
    return Observation(
        observation_id=f"offer-{entity_id}-{hour}",
        source="offer_import",
        entity_type="offer",
        entity_id=entity_id,
        observed_at=BASE + timedelta(hours=hour),
        available_at=BASE + timedelta(hours=hour),
        metrics={
            "units_sold": Measurement(
                lower=lower, upper=upper, precision="exact" if lower == upper else "bounded"
            )
        },
        attributes={
            "counter_scope": "lifetime",
            "currency": "EUR",
            "price": "20.00",
            "seller_id": entity_id,
            **attributes,
        },
    )


def test_bounded_sold_counters_preserve_bounds_and_interval() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    row = assess(
        [offer("o1", 1, 100, 199), offer("o1", 3, 200, 299)],
        [product],
        [link],
        BASE + timedelta(hours=3),
    )[0]
    assert row["demand"]["status"] == "bounded"
    assert row["features"]["units_sold"] is None
    assert row["features"]["sold_delta_lower"] == 1
    assert row["features"]["sold_delta_upper"] == 199
    assert row["features"]["sold_period_seconds"] == 7200
    assert row["demand"]["intervals"][0]["lower"] == 1


def test_late_evidence_and_future_link_do_not_rewrite_past() -> None:
    from sm_trending_products.analytics import assess

    cutoff = BASE + timedelta(hours=3)
    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    other = Product(product_id="p2", name="Different lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="tiktok_display:video:v1", available_at=BASE)
    future_link = ProductLink(
        product_id="p2", entity_key=link.entity_key, available_at=cutoff + timedelta(hours=1)
    )
    delayed = video("v1", 2, 99999).model_copy(update={"available_at": cutoff + timedelta(days=1)})
    rows = assess(
        [video("v1", 1, 100), video("v1", 3, 500), delayed],
        [product, other],
        [link, future_link],
        cutoff,
    )
    by_id = {row["product_id"]: row for row in rows}
    assert by_id["p1"]["features"]["growth_per_hour"] == pytest.approx(200)
    assert by_id["p2"]["features"]["growth_per_hour"] is None
    assert delayed.observation_id not in by_id["p1"]["evidence_refs"]
    later = assess(
        [video("v1", 1, 100), video("v1", 3, 500)],
        [product, other],
        [link, future_link],
        cutoff + timedelta(hours=1),
    )
    by_id = {row["product_id"]: row for row in later}
    assert by_id["p1"]["features"]["growth_per_hour"] is None
    assert by_id["p2"]["features"]["growth_per_hour"] == pytest.approx(200)


def test_real_and_synthetic_identity_namespaces_are_isolated() -> None:
    from sm_trending_products.analytics import assess

    real = Product(product_id="real", name="Real lamp", created_at=BASE)
    fake = Product(product_id="fake", name="Fixture lamp", created_at=BASE, synthetic=True)
    links = [
        ProductLink(product_id="real", entity_key="tiktok_display:video:v1", available_at=BASE),
        ProductLink(
            product_id="fake",
            entity_key="tiktok_display:video:v1",
            available_at=BASE + timedelta(hours=1),
        ),
    ]
    observations = [video("v1", 1, 100), video("v1", 3, 500), video("v1", 2, 99999, synthetic=True)]
    rows = assess(observations, [real, fake], links, BASE + timedelta(hours=3))
    assert [row["product_id"] for row in rows] == ["real"]
    assert rows[0]["features"]["growth_per_hour"] == pytest.approx(200)


def test_known_market_does_not_attribute_unknown_audience_geography() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE, market="FR")
    link = ProductLink(product_id="p1", entity_key="tiktok_display:video:v1", available_at=BASE)
    rows = assess(
        [video("v1", 1, 100), video("v1", 3, 500, market="US")],
        [product],
        [link],
        BASE + timedelta(hours=3),
    )
    assert rows[0]["attention"]["score"] is None
    assert rows[0]["coverage"]["market_excluded_observations"] == 2


def test_counter_reset_is_reported_and_not_negative_consumption() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="tiktok_display:video:v1", available_at=BASE)
    row = assess(
        [video("v1", 1, 1000), video("v1", 3, 50)], [product], [link], BASE + timedelta(hours=3)
    )[0]
    assert row["features"]["growth_per_hour"] is None
    assert row["features"]["counter_corrections"] == 1
    assert row["attention"]["status"] == "insufficient_history"


def test_global_normalization_is_independent_of_product_batching() -> None:
    from sm_trending_products.analytics import assess, normalize_assessments

    cutoff = BASE + timedelta(hours=3)
    products = [Product(product_id=f"p{i}", name=f"Lamp {i}", created_at=BASE) for i in range(2)]
    links = [
        ProductLink(product_id=f"p{i}", entity_key=f"tiktok_display:video:v{i}", available_at=BASE)
        for i in range(2)
    ]
    observations = [
        video("v0", 1, 100),
        video("v0", 3, 500),
        video("v1", 1, 100),
        video("v1", 3, 2100),
    ]
    together = assess(observations, products, links, cutoff)
    shards = [
        row
        for product in products
        for row in assess(observations, [product], links, cutoff, normalize=False)
    ]
    assert normalize_assessments(shards) == together
    assert together[0]["product_id"] == "p1"


def test_creator_spread_distinguishes_product_diffusion_from_one_creative() -> None:
    from sm_trending_products.analytics import assess

    cutoff = BASE + timedelta(hours=3)
    products = [
        Product(product_id="solo", name="Single creator", created_at=BASE),
        Product(product_id="spread", name="Multiple creators", created_at=BASE),
    ]
    observations = [
        video("solo", 1, 0),
        video("solo", 3, 1000),
        video("a", 1, 0),
        video("a", 3, 500),
        video("b", 1, 0),
        video("b", 3, 500),
    ]
    links = [
        ProductLink(
            product_id="solo" if entity == "solo" else "spread",
            entity_key=f"tiktok_display:video:{entity}",
            available_at=BASE,
        )
        for entity in ["solo", "a", "b"]
    ]
    rows = {row["product_id"]: row for row in assess(observations, products, links, cutoff)}
    assert rows["spread"]["attention"]["score"] > rows["solo"]["attention"]["score"]
    assert rows["spread"]["features"]["creator_concentration"] == pytest.approx(0.5)


def test_feasibility_requires_complete_currency_consistent_cost_scenario() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="cost_import:cost:c1", available_at=BASE)
    observation = Observation(
        observation_id="cost-1",
        source="cost_import",
        entity_type="cost",
        entity_id="c1",
        observed_at=BASE,
        available_at=BASE,
        attributes={
            "currency": "EUR",
            "sale_price": "20.00",
            "unit_cost": "4.00",
            "shipping_cost": "2.00",
            "payment_fee": "1.00",
            "ad_cost": "3.00",
            "tax_cost": "2.00",
            "return_rate": "0.1",
            "return_cost": "5.00",
        },
    )
    row = assess([observation], [product], [link], BASE + timedelta(hours=1))[0]
    assert row["feasibility"]["status"] == "scenario"
    assert row["feasibility"]["scenarios"][0]["contribution_margin"] == "7.500"
    assert row["feasibility"]["score"] == pytest.approx(37.5)
    mixed = observation.model_copy(
        update={"attributes": {**observation.attributes, "unit_cost_currency": "USD"}}
    )
    invalid = assess([mixed], [product], [link], BASE + timedelta(hours=1))[0]
    assert invalid["feasibility"]["score"] is None
    assert "mixed currencies" in " ".join(invalid["feasibility"]["reasons"])
    incomplete = observation.model_copy(
        update={
            "attributes": {
                key: value for key, value in observation.attributes.items() if key != "return_rate"
            }
        }
    )
    assert (
        assess([incomplete], [product], [link], BASE + timedelta(hours=1))[0]["feasibility"][
            "score"
        ]
        is None
    )


def test_missing_required_dimensions_are_not_silently_zero_or_renormalized() -> None:
    from sm_trending_products.analytics import assess
    from sm_trending_products.contracts import AssessmentPolicy

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="tiktok_display:video:v1", available_at=BASE)
    observations = [video("v1", 1, 100), video("v1", 3, 500)]
    policy = AssessmentPolicy(weights={"attention": 0.5, "demand": 0.5})
    row = assess(observations, [product], [link], BASE + timedelta(hours=3), policy=policy)[0]
    assert row["score"] is None
    explicit = policy.model_copy(update={"missing_data": "renormalize"})
    row = assess(observations, [product], [link], BASE + timedelta(hours=3), policy=explicit)[0]
    assert row["score"] == row["attention"]["score"]
    assert row["demand"]["score"] is None


def test_repeated_creatives_do_not_inflate_independent_diffusion() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    observations = [
        video(entity, hour, count).model_copy(
            update={
                "attributes": {
                    "creator_id": entity,
                    "creative_fingerprint": "verified-media-sha256",
                }
            }
        )
        for entity in ["a", "b"]
        for hour, count in [(1, 0), (3, 500)]
    ]
    links = [
        ProductLink(product_id="p1", entity_key=f"tiktok_display:video:{entity}", available_at=BASE)
        for entity in ["a", "b"]
    ]
    row = assess(observations, [product], links, BASE + timedelta(hours=3))[0]
    assert row["features"]["creator_count"] == 2
    assert row["features"]["growth_per_hour"] == pytest.approx(500)
    assert row["features"]["view_count"] == 1000
    assert row["features"]["independent_spread_concentration"] == 1
    assert row["features"]["verified_creative_count"] == 1
    assert row["attention"]["components"]["creator_spread"]["value"] == 0


def test_product_transfer_remains_global_across_product_batches() -> None:
    from sm_trending_products.analytics import assess, normalize_assessments

    products = [Product(product_id=key, name=key, created_at=BASE) for key in ["old", "new"]]
    links = [
        ProductLink(product_id="old", entity_key="tiktok_display:video:v1", available_at=BASE),
        ProductLink(
            product_id="new",
            entity_key="tiktok_display:video:v1",
            available_at=BASE + timedelta(hours=2),
        ),
    ]
    observations = [video("v1", 1, 100), video("v1", 3, 500)]
    cutoff = BASE + timedelta(hours=3)
    batched = [
        row
        for product in products
        for row in assess(observations, [product], links, cutoff, normalize=False)
    ]
    assert normalize_assessments(batched) == assess(observations, products, links, cutoff)


def test_rejected_alternative_hypothesis_does_not_erase_confirmed_owner() -> None:
    from sm_trending_products.analytics import assess

    products = [
        Product(product_id=key, name=key, created_at=BASE) for key in ["confirmed", "hypothesis"]
    ]
    key = "tiktok_display:video:v1"
    links = [
        ProductLink(product_id="confirmed", entity_key=key, available_at=BASE),
        ProductLink(
            product_id="hypothesis",
            entity_key=key,
            available_at=BASE + timedelta(hours=1),
            status="rejected",
        ),
    ]
    rows = {
        row["product_id"]: row
        for row in assess(
            [video("v1", 1, 100), video("v1", 3, 500)], products, links, BASE + timedelta(hours=3)
        )
    }
    assert rows["confirmed"]["features"]["growth_per_hour"] == pytest.approx(200)
    assert rows["hypothesis"]["features"]["growth_per_hour"] is None


def test_price_refresh_does_not_make_old_sold_counter_fresh() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    observations = [offer("o1", 1, 100, 100), offer("o1", 3, 200, 200)]
    observations.append(offer("o1", 200, 200, 200).model_copy(update={"metrics": {}}))
    row = assess(observations, [product], [link], BASE + timedelta(hours=200))[0]
    assert row["demand"]["status"] == "unknown"
    assert row["features"]["units_sold"] is None


def test_documented_interval_sales_are_not_treated_as_lifetime_counters() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    record = offer(
        "o1",
        24,
        24,
        24,
        period_start=BASE.isoformat(),
        period_end=(BASE + timedelta(hours=24)).isoformat(),
    ).model_copy(
        update={"metrics": {"units_sold": Measurement(lower=24, upper=24, semantics="interval")}}
    )
    row = assess([record], [product], [link], BASE + timedelta(hours=24))[0]
    assert row["demand"]["status"] == "observed"
    assert row["features"]["units_sold"] is None
    assert row["features"]["sold_delta_lower"] == 24
    assert row["features"]["sold_period_seconds"] == 86400
    assert row["features"]["sold_growth_per_hour"] == pytest.approx(1)


@pytest.mark.parametrize("period_field", ["units_sold_period", "sold_period"])
def test_changed_sold_reporting_period_is_not_sales_growth(period_field: str) -> None:
    from sm_trending_products.analytics import assess

    start = datetime(2026, 10, 21, 23, tzinfo=UTC)
    product = Product(product_id="p1", name="Desk lamp", created_at=start)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=start)
    observations = [
        Observation(
            observation_id=f"sold-{index}",
            source="offer_import",
            entity_type="offer",
            entity_id="o1",
            observed_at=at,
            available_at=at,
            metrics={"units_sold": Measurement(lower=value, upper=value)},
            attributes={period_field: period},
        )
        for index, (at, value, period) in enumerate(
            [(start, 100, "2026-09"), (start + timedelta(hours=1), 200, "2026-10")]
        )
    ]
    row = assess(observations, [product], [link], start + timedelta(hours=1))[0]
    assert row["demand"]["score"] is None
    assert row["features"]["sold_delta_lower"] is None
    assert row["features"]["sold_growth_per_hour"] is None
    assert row["demand"]["intervals"] == []


def test_undocumented_sold_period_is_not_assumed_lifetime() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    records = [
        offer("o1", hour, value, value).model_copy(update={"attributes": {}})
        for hour, value in [(1, 100), (3, 200)]
    ]
    row = assess(records, [product], [link], BASE + timedelta(hours=3))[0]
    assert row["demand"]["score"] is None
    assert row["features"]["sold_delta_lower"] is None
    assert row["features"]["sold_growth_per_hour"] is None
    assert row["features"]["units_sold"] is None


@pytest.mark.parametrize("period", ["lifetime", "2026-01"])
def test_documented_matching_sold_period_remains_comparable(period: str) -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    records = [
        offer("o1", hour, value, value).model_copy(update={"attributes": {"sold_period": period}})
        for hour, value in [(1, 100), (3, 200)]
    ]
    row = assess(records, [product], [link], BASE + timedelta(hours=3))[0]
    assert row["demand"]["score"] is not None
    assert row["features"]["sold_delta_lower"] == 100
    assert row["features"]["sold_growth_per_hour"] == pytest.approx(50)
    assert row["features"]["sold_counter_periods"] == {"offer_import:offer:o1": period}


@pytest.mark.parametrize("period", ["unknown", "last_30_days", "30d"])
def test_unknown_or_rolling_period_overrides_generic_lifetime_hint(period: str) -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    records = [
        offer("o1", hour, value, value, units_sold_period=period)
        for hour, value in [(1, 100), (3, 200)]
    ]
    row = assess(records, [product], [link], BASE + timedelta(hours=3))[0]
    assert row["demand"]["score"] is None
    assert row["features"]["sold_delta_lower"] is None
    assert row["features"]["units_sold"] is None


def test_competition_retains_price_evolution_not_just_latest_price() -> None:
    from sm_trending_products.analytics import assess

    products = [
        Product(product_id=key, name=key, created_at=BASE, market="US")
        for key in ["falling", "stable"]
    ]
    links = [
        ProductLink(product_id=key, entity_key=f"offer_import:offer:{key}", available_at=BASE)
        for key in ["falling", "stable"]
    ]
    records = [
        offer(key, hour, 0, 0, currency="USD", price=price).model_copy(update={"market": "US"})
        for key, prices in [
            ("falling", [(1, "20.00"), (3, "10.00")]),
            ("stable", [(1, "10.00"), (3, "10.00")]),
        ]
        for hour, price in prices
    ]
    rows = {
        row["product_id"]: row
        for row in assess(records, products, links, BASE + timedelta(hours=3))
    }
    falling = rows["falling"]["competition"]["price_evolution"]["offers"][0]
    stable = rows["stable"]["competition"]["price_evolution"]["offers"][0]
    assert falling["direction"] == "decrease"
    assert falling["change_lower"] == "-10.00"
    assert falling["change_upper"] == "-10.00"
    assert falling["currency"] == "USD"
    assert falling["market"] == "US"
    assert falling["source"] == "offer_import"
    assert falling["evidence_refs"] == ["offer-falling-1", "offer-falling-3"]
    assert stable["direction"] == "unchanged"
    assert stable["change_lower"] == "0.00"
    assert rows["falling"]["competition"]["score"] == rows["stable"]["competition"]["score"]


def test_competition_deduplicates_only_verified_creative_fingerprints() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    records = [
        Observation(
            observation_id=f"ad-{index}",
            source="tiktok_ads",
            entity_type="ad",
            entity_id=str(index),
            observed_at=BASE,
            available_at=BASE,
            attributes=attributes,
        )
        for index, attributes in enumerate(
            [
                {"creative_fingerprint": "sha256-same", "advertiser_id": "a"},
                {"creative_fingerprint": "sha256-same", "advertiser_id": "a"},
                {"advertiser_id": "b"},
            ]
        )
    ]
    links = [
        ProductLink(product_id="p1", entity_key=record.entity_key, available_at=BASE)
        for record in records
    ]
    competition = assess(records, [product], links, BASE + timedelta(hours=1))[0]["competition"]
    assert competition["observed_ad_count"] == 3
    assert competition["observed_creative_count"] == 1
    assert competition["creative_coverage"]["unknown_fingerprint_entities"] == 1
    assert competition["creative_coverage"]["fingerprinted_entities"] == 2


def test_price_evolution_preserves_bounded_quote_uncertainty() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE, market="US")
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    records = [
        offer("o1", 1, 0, 0, price="20.00", currency="USD").model_copy(update={"market": "US"}),
        offer(
            "o1",
            3,
            0,
            0,
            price=None,
            price_lower="9.00",
            price_upper="11.00",
            price_precision="bounded",
            currency="USD",
        ).model_copy(update={"market": "US"}),
    ]
    summary = assess(records, [product], [link], BASE + timedelta(hours=3))[0]["competition"][
        "price_evolution"
    ]["offers"][0]
    assert summary["status"] == "bounded"
    assert summary["change_precision"] == "bounded"
    assert summary["change_lower"] == "-11.00"
    assert summary["change_upper"] == "-9.00"
    assert summary["direction"] == "decrease"
    assert summary["relative_change_lower"] == "-0.55"
    assert summary["relative_change_upper"] == "-0.45"


@pytest.mark.parametrize("changed_scope", ["market", "currency", "source"])
def test_price_evolution_does_not_compare_different_contexts(changed_scope: str) -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    first = offer("o1", 1, 0, 0, price="20.00", currency="USD").model_copy(update={"market": "US"})
    last = offer("o1", 3, 0, 0, price="10.00", currency="USD").model_copy(update={"market": "US"})
    if changed_scope == "currency":
        last = last.model_copy(update={"attributes": {**last.attributes, "currency": "EUR"}})
    else:
        last = last.model_copy(
            update={changed_scope: "FR" if changed_scope == "market" else "different_source"}
        )
    links = [
        ProductLink(product_id="p1", entity_key=key, available_at=BASE)
        for key in sorted({first.entity_key, last.entity_key})
    ]
    summaries = assess([first, last], [product], links, BASE + timedelta(hours=3))[0][
        "competition"
    ]["price_evolution"]["offers"]
    assert all(summary["status"] == "unknown" for summary in summaries)
    assert all("change_lower" not in summary for summary in summaries)


def test_price_evolution_excludes_revision_changes_and_stale_quotes() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    first = offer("o1", 1, 0, 0, price="20.00", currency="USD").model_copy(update={"market": "US"})
    correction = first.model_copy(
        update={
            "observation_id": "price-correction",
            "available_at": BASE + timedelta(hours=2),
            "attributes": {**first.attributes, "price": "10.00"},
        }
    )
    last = offer("o1", 3, 0, 0, price="10.00", currency="USD").model_copy(update={"market": "US"})
    summary = assess([first, correction, last], [product], [link], BASE + timedelta(hours=3))[0][
        "competition"
    ]["price_evolution"]["offers"][0]
    assert summary["direction"] == "unchanged"
    assert summary["revision_count"] == 1
    assert "price-correction" in summary["evidence_refs"]
    assert first.observation_id not in summary["evidence_refs"]
    stale = assess([first, last], [product], [link], BASE + timedelta(hours=200))[0]["competition"][
        "price_evolution"
    ]["offers"][0]
    assert stale["status"] == "unknown"
    assert stale["reason"] == "stale_latest_quote"


@pytest.mark.parametrize(
    "attributes",
    [{"price_currency": None}, {"price_precision": ["exact"]}, {"price_precision": "rounded"}],
)
def test_unusable_price_precision_or_currency_stays_unknown(attributes: dict) -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    link = ProductLink(product_id="p1", entity_key="offer_import:offer:o1", available_at=BASE)
    records = [
        offer("o1", 1, 0, 0, price="20.00", currency="USD").model_copy(update={"market": "US"}),
        offer("o1", 3, 0, 0, price="10.00", currency="USD", **attributes).model_copy(
            update={"market": "US"}
        ),
    ]
    summary = assess(records, [product], [link], BASE + timedelta(hours=3))[0]["competition"][
        "price_evolution"
    ]["offers"][0]
    assert summary["status"] == "unknown"
    assert "change_lower" not in summary


def test_unknown_creative_fingerprints_do_not_become_distinct_ad_creatives() -> None:
    from sm_trending_products.analytics import assess

    product = Product(product_id="p1", name="Desk lamp", created_at=BASE)
    records = [
        Observation(
            observation_id=f"ad-{index}",
            source="tiktok_ads",
            entity_type="ad",
            entity_id=str(index),
            observed_at=BASE,
            available_at=BASE,
            attributes={"advertiser_id": "a"},
        )
        for index in range(2)
    ]
    links = [
        ProductLink(product_id="p1", entity_key=record.entity_key, available_at=BASE)
        for record in records
    ]
    result = assess(records, [product], links, BASE + timedelta(hours=1))[0]["competition"]
    assert result["observed_ad_count"] == 2
    assert result["observed_creative_count"] is None
    assert result["creative_coverage"]["unknown_fingerprint_entities"] == 2
