"""Imported transaction/cost evidence stays explicit through published review."""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal

import pytest

from sm_trending_products.contracts import AssessmentPolicy, now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_commercial_dimensions_keep_bounds_proxies_scenarios_and_missingness_visible(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    captured = now()

    def observation(source, kind, identifier, timestamp, metrics, attributes):
        return {
            "source": source,
            "entity_type": kind,
            "entity_id": identifier,
            "observed_at": timestamp.isoformat(),
            "available_at": timestamp.isoformat(),
            "published_at": (captured - timedelta(hours=12)).isoformat()
            if kind == "video"
            else None,
            "synthetic": True,
            "market": "FR",
            "metrics": metrics,
            "attributes": attributes,
        }

    records = []
    for product in ("full", "proxy", "unknown", "estimated"):
        for hour, views in ((2, 100), (1, 500 if product == "full" else 200)):
            records.append(
                observation(
                    "fixture_import",
                    "video",
                    f"{product}-video",
                    captured - timedelta(hours=hour),
                    {"view_count": {"lower": views, "upper": views}},
                    {"creator_id": product},
                )
            )
    for hour, lower, upper in ((2, 100, 199), (1, 200, 299)):
        records.append(
            observation(
                "offer_import",
                "offer",
                "full-offer",
                captured - timedelta(hours=hour),
                {
                    "units_sold": {"lower": lower, "upper": upper, "precision": "bounded"},
                    "price_minor": {"lower": 1999, "upper": 1999, "unit": "EUR_minor"},
                },
                {
                    "currency": "EUR",
                    "price": "19.99",
                    "seller_id": "seller-one",
                    "stock_status": "in_stock",
                    "counter_scope": "lifetime",
                    "units_sold_period": "lifetime",
                },
            )
        )
    costs = {
        "currency": "EUR",
        "sale_price": "20.00",
        "unit_cost": "4.00",
        "shipping_cost": "2.00",
        "payment_fee": "1.00",
        "ad_cost": "3.00",
        "tax_cost": "2.00",
        "return_rate": "0.1",
        "return_cost": "5.00",
        "delivery_days": 7,
        "quote_ref": "fixture-supplier-quote",
    }
    records.append(
        observation("cost_import", "cost", "full-cost", captured - timedelta(hours=1), {}, costs)
    )
    records.append(
        observation(
            "intent_import",
            "video",
            "proxy-intent",
            captured - timedelta(hours=1),
            {"purchase_intent_count": {"lower": 12, "upper": 12}},
            {"label_method": "manual-intent-v1"},
        )
    )
    records.append(
        observation(
            "estimate_import",
            "offer",
            "estimated-offer",
            captured - timedelta(hours=1),
            {
                "units_sold": {
                    "lower": 10,
                    "upper": 20,
                    "precision": "estimated",
                    "model_version": "manual-estimate-v1",
                }
            },
            {"units_sold_period": "lifetime", "model_assumptions": {"conversion_fraction": 0.01}},
        )
    )
    pipeline.import_records(records, True, origin="controlled-commercial-import")
    for product in ("full", "proxy", "unknown", "estimated"):
        pipeline.create_product(product, product.title(), True, market="FR")
    for key in {f"{row['source']}:{row['entity_type']}:{row['entity_id']}" for row in records}:
        pipeline.review_link(key.rsplit(":", 1)[1].split("-", 1)[0], key)
    cutoff = now()
    policy = AssessmentPolicy(
        version="explicit-commercial-v1",
        weights={"attention": 0.5, "demand": 0.25, "feasibility": 0.25},
    )
    first = pipeline.assess(cutoff, True, policy)
    rows = {row["product_id"]: row for row in first["rows"]}
    full = rows["full"]
    assert full["demand"]["status"] == "bounded" and full["features"]["units_sold"] is None
    assert full["features"]["sold_delta_lower"] == 1 and full["features"]["sold_delta_upper"] == 199
    assert full["features"]["sold_period_seconds"] == pytest.approx(3600)
    assert full["competition"]["observed_seller_count"] == 1
    assert full["competition"]["market_coverage"] == "unknown"
    assert full["feasibility"]["status"] == "scenario"
    assert Decimal(full["feasibility"]["scenarios"][0]["contribution_margin"]) == Decimal("7.5")
    assert (
        rows["proxy"]["demand"]["status"] == "proxy"
        and rows["proxy"]["demand"]["intent_lower"] == 12
    )
    assert (
        rows["estimated"]["demand"]["status"] == "estimated"
        and rows["estimated"]["demand"]["score"] is None
    )
    assert rows["unknown"]["competition"]["score"] is None
    assert rows["unknown"]["feasibility"]["score"] is None
    assert all(rows[product]["score"] is None for product in ("proxy", "unknown", "estimated"))
    evidence = pipeline.evidence("full", cutoff, True)
    offer = next(row for row in evidence if row["entity_type"] == "offer")
    cost = next(row for row in evidence if row["entity_type"] == "cost")
    assert (
        offer["attributes"]["currency"] == "EUR"
        and offer["attributes"]["stock_status"] == "in_stock"
    )
    assert offer["metrics"]["units_sold"]["precision"] == "bounded" and offer["available_at"]
    assert (
        cost["attributes"]["delivery_days"] == 7
        and cost["attributes"]["quote_ref"] == "fixture-supplier-quote"
    )
    assert cost["attributes"]["acquisition_context"] == "controlled-commercial-import"
    assert all(row["raw_dataset_id"] for row in evidence)

    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.selectbox(key="assessment_product").select("full").run(timeout=20)
    assert not app.exception

    def displayed():
        return next(
            value
            for element in app.json
            if isinstance(value := json.loads(element.value), dict) and "features" in value
        )

    assert displayed() == full
    app.selectbox(key="assessment_product").select("proxy").run(timeout=20)
    assert displayed()["score"] is None and displayed()["demand"]["status"] == "proxy"
    app.selectbox(key="evidence_product").select("estimated").run(timeout=20)
    modeled = next(
        row
        for element in app.json
        if isinstance(value := json.loads(element.value), list)
        and value
        and "observation_id" in value[0]
        for row in value
        if row["entity_type"] == "offer"
    )
    assert modeled["metrics"]["units_sold"]["model_version"] == "manual-estimate-v1"
    assert modeled["attributes"]["model_assumptions"] == {"conversion_fraction": 0.01}
    partial = policy.model_copy(
        update={"version": "explicit-partial-v1", "missing_data": "renormalize"}
    )
    pipeline.assess(cutoff + timedelta(seconds=1), True, partial)
    app.run(timeout=20)
    assert not app.exception
    assert displayed()["score"] == displayed()["attention"]["score"]
    assert displayed()["demand"]["score"] is None and displayed()["feasibility"]["score"] is None
    assert displayed()["ranking_policy"] == {
        "weights": partial.weights,
        "missing_data": "renormalize",
    }

    later = cutoff + timedelta(hours=1)
    incomplete = {key: value for key, value in costs.items() if key != "tax_cost"}
    pipeline.import_records(
        [observation("cost_import", "cost", "full-cost", later, {}, incomplete)], True
    )
    revised = pipeline.assess(later, True, policy)
    updated = next(row for row in revised["rows"] if row["product_id"] == "full")
    assert updated["feasibility"]["score"] is None and updated["score"] is None
    assert "tax_cost" in " ".join(updated["feasibility"]["reasons"])
    app.selectbox(key="assessment_product").select("full").run(timeout=20)
    assert not app.exception and displayed() == updated
    assert pipeline.evidence("full", cutoff, True) == evidence
    assert pipeline.latest_assessments(False) == [] and pipeline.capabilities() == []
    assert pipeline.verify()["ok"]
