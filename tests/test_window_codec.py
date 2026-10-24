"""Native cache round trips preserve full observations and original price timestamps."""

from datetime import timedelta, timezone

from analytical_results import snapshot_semantics
from test_feature_windows import count_source_pins, rolling_case

from sm_trending_products.analytics import assess
from sm_trending_products.contracts import (
    AssessmentPolicy,
    Product,
    ProductLink,
    now,
)


def test_previous_columnar_cache_is_read_without_source_rebuild(pipeline, monkeypatch):
    from psycopg.types.json import Jsonb

    from sm_trending_products.cache import store_local_cached

    cutoff, _product, _links, scope, cache, initial, _audit = rolling_case(pipeline)
    # Normalized history uses the previous cache's columnar observation schema,
    # canonical nested JSON and original timestamp strings.
    previous_body = pipeline.history.encode(initial.observations)
    descriptor = store_local_cached(pipeline.history, previous_body)
    assert descriptor is not None
    pipeline.catalog.execute(
        "UPDATE feature_product_windows SET descriptor=%s WHERE product_id='p'",
        (Jsonb(descriptor),),
    )
    pins = count_source_pins(monkeypatch)
    loaded = cache.load_window(pipeline.history, scope, cutoff, AssessmentPolicy())
    assert loaded.mode == "delta" and not pins
    assert [row.model_dump(mode="json") for row in loaded.observations] == [
        row.model_dump(mode="json") for row in initial.observations
    ]


def test_local_columnar_roundtrip_preserves_original_offsets_and_complete_price_history(
    pipeline, monkeypatch
):
    cutoff = now() + timedelta(minutes=1)
    pipeline.create_product("p", "Product", synthetic=True, market="US")
    source_rows = []
    for index, offset in enumerate((2, -4)):
        observed = (cutoff - timedelta(hours=12 * (1 - index))).astimezone(
            timezone(timedelta(hours=offset))
        )
        source_rows.append(
            {
                "observation_id": f"offer-{index}",
                "source": "codec_fixture",
                "entity_type": "offer",
                "entity_id": "offer",
                "observed_at": observed.isoformat(),
                "available_at": observed.isoformat(),
                "synthetic": True,
                "market": "US",
                "attributes": {
                    "price": str(20 - index),
                    "currency": "USD",
                    "unicode": "été ♜",
                    "nested": {"items": [None, False, 0.125, "value"]},
                },
                "missing_fields": {"units_sold": "not provided"},
            }
        )
    pipeline.import_records(source_rows, synthetic=True)
    pipeline.review_link("p", "codec_fixture:offer:offer")
    product = pipeline.identity.all_products(cutoff, synthetic=True)
    links = [ProductLink.model_validate(row) for row in pipeline.links(cutoff, True)]
    # First publication admits the optimized columnar window; a forced feature miss then
    # exercises that raw state instead of the SQL feature-row cache.
    pipeline.assess(cutoff, True)
    source, _ = pipeline.history.observations(cutoff, True)
    pipeline.catalog.execute("DELETE FROM feature_product_projections WHERE product_id='p'")
    pins = count_source_pins(monkeypatch)
    later = cutoff + timedelta(hours=1)
    actual = pipeline.assess(later, True)
    expected = assess(source, [Product.model_validate(row) for row in product], links, later, True)
    assert snapshot_semantics(pipeline, {"rows": actual["rows"]})["rows"] == expected
    actual_prices = actual["rows"][0]["competition"]["price_evolution"]
    assert actual_prices["offers"][0]["latest_price"]["observed_at"].endswith("-04:00")
    assert not pins
