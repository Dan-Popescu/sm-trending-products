"""Commercial outcome contracts through immutable persistence and operator review."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_evaluation_integration import outcome_config, record, seed_product

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def offer(
    identifier, product, observed, lower=None, upper=None, *, estimated=False, available=None
):
    metric = {}
    if lower is not None:
        metric = {
            "units_sold": {
                "lower": lower,
                "upper": upper,
                "precision": "estimated" if estimated else "exact" if lower == upper else "bounded",
                "model_version": "fixture-demand-v1" if estimated else None,
            }
        }
    return {
        "observation_id": identifier,
        "source": "offer_import",
        "entity_type": "offer",
        "entity_id": product,
        "synthetic": True,
        "observed_at": observed.isoformat(),
        "available_at": (available or observed).isoformat(),
        "metrics": metric,
        "attributes": {
            "sold_period": "lifetime",
            "currency": "EUR",
            "price_minor": 2000,
            "acquisition_method": "controlled_import",
            **({"estimate_assumptions": {"conversion_rate": 0.01}} if estimated else {}),
        },
    }


def test_sales_bounds_missingness_estimates_late_inputs_and_baseline_eligibility(pipeline):
    decision = now() + timedelta(minutes=1)
    end = decision + timedelta(days=3)
    for product in ("bounded", "missing", "estimated"):
        seed_product(pipeline, product, base=decision)
    pipeline.import_records(
        [
            offer("bounded-before", "bounded", decision - timedelta(hours=1), 100, 110),
            offer("bounded-start", "bounded", decision, 150, 160),
            offer("bounded-end", "bounded", end, 170, 210),
            offer("missing-price", "missing", decision),
            offer("estimated-start", "estimated", decision, 1000, 1000, estimated=True),
            offer("estimated-end", "estimated", end, 2000, 3000, estimated=True),
        ],
        synthetic=True,
    )
    for product in ("bounded", "missing", "estimated"):
        pipeline.review_link(product, f"offer_import:offer:{product}")
    config = outcome_config(target="sales", threshold=30, top_k=3)
    frozen = pipeline.freeze(decision, True, config)
    assert set(frozen["result"]["selected"]) == {"bounded", "missing", "estimated"}
    assert "sales_popularity" not in frozen["result"]["baselines"]
    original = pipeline.mature(frozen["evaluation_id"], end)
    result = original["result"]["scenarios"][0]["methods"]["policy"]
    outcomes = {row["product_id"]: row for row in result["outcomes"]}
    assert {key: row["status"] for key, row in outcomes.items()} == {
        "bounded": "ambiguous",
        "missing": "censored",
        "estimated": "estimated",
    }
    assert (outcomes["bounded"]["lower"], outcomes["bounded"]["upper"]) == (10, 60)
    assert outcomes["estimated"]["precision"] == "estimated"
    assert result["selected_count"] == 3 and result["observed_count"] == 0
    assert result["bounded_count"] == 1 and result["estimated_count"] == 1
    assert result["precision_at_k"] is None and result["precision_bounds"] == [0, 1]

    late = end + timedelta(days=1)
    pipeline.import_records(
        [
            offer("late-start", "missing", decision, 100, 100, available=late),
            offer("late-end", "missing", end, 150, 150, available=late),
        ],
        synthetic=True,
    )
    revised = pipeline.mature(frozen["evaluation_id"], late)
    summary = revised["result"]["scenarios"][0]["methods"]["policy"]
    assert summary["status_counts"] == {"ambiguous": 1, "estimated": 1, "success": 1}
    assert summary["observed_count"] == 1 and summary["coverage"] == pytest.approx(1 / 3)
    assert summary["precision_at_k"] is None
    assert pipeline.mature(frozen["evaluation_id"], end) == original
    assert pipeline.evaluation(original["evaluation_id"], True)["payload"] == original["result"]
    historical = pipeline.backtest([decision], late, True, config)["result"]
    assert historical["snapshots"] == [frozen["result"]]
    assert historical["reports"] == [revised["result"]]

    next_decision = decision + timedelta(days=5)
    rows = []
    for product, units in (("bounded", 200), ("missing", 300), ("estimated", 400)):
        rows.extend(
            [
                record(f"video-{product}", next_decision - timedelta(hours=2), 1000),
                record(f"video-{product}", next_decision - timedelta(hours=1), 2000),
                offer(
                    f"exact-{product}", product, next_decision - timedelta(hours=1), units, units
                ),
            ]
        )
    pipeline.import_records(rows, synthetic=True)
    newer = pipeline.freeze(next_decision, True, {**config, "top_k": 1})
    assert newer["result"]["baselines"]["sales_popularity"] == ["estimated"]
    assert (
        "sales_popularity"
        not in pipeline.evaluation(frozen["evaluation_id"], True)["payload"]["baselines"]
    )
    assert pipeline.evaluations(False) == []


def test_attention_success_requires_separate_actual_profit_and_visible_target(
    pipeline, monkeypatch
):
    decision = now() + timedelta(minutes=1)
    end = decision + timedelta(days=3)
    entity, _, _ = seed_product(pipeline, "separate-targets", base=decision)
    pipeline.import_records(
        [record(entity, decision, 300), record(entity, end, 1000000)], synthetic=True
    )
    attention = pipeline.freeze(decision, True, outcome_config())
    profit = pipeline.freeze(
        decision, True, outcome_config(target="profit", currency="EUR", threshold=30)
    )
    attention_result = pipeline.mature(attention["evaluation_id"], end)
    profit_result = pipeline.mature(profit["evaluation_id"], end)
    assert attention_result["result"]["scenarios"][0]["methods"]["policy"]["precision_at_k"] == 1
    summary = profit_result["result"]["scenarios"][0]["methods"]["policy"]
    assert summary["status_counts"] == {"censored": 1}
    assert summary["precision_at_k"] is None
    assert profit_result["result"]["target"] == "profit"
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.radio(key="review_view").set_value("Datasets and evaluations").run(timeout=20)
    assert not app.exception
    summaries = next(frame.value for frame in app.dataframe if "evaluation_id" in frame.value)
    assert set(summaries["target"]) == {"attention", "profit"}
    app.selectbox[0].select_index(
        app.selectbox[0].options.index(profit_result["evaluation_id"])
    ).run(timeout=20)
    displayed = next(
        json.loads(value.value) for value in app.json if "evaluation_id" in value.value
    )
    assert displayed["payload"] == profit_result["result"]
    assert pipeline.evaluations(False) == []
