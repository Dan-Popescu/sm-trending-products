"""Counter-period meaning through frozen decisions and observable outcomes."""

from datetime import timedelta

import pytest
from test_evaluation import catalog, observation
from test_evaluation_integration import outcome_config, seed_product

from sm_trending_products.contracts import Measurement, ProductLink, canonical, now, utc
from sm_trending_products.evaluation import EvaluationConfig, freeze_decision, mature_decision


def sales_case(attributes):
    product, video_link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    offer_link = ProductLink(
        product_id=product.product_id,
        entity_key="fixture:offer:o1",
        available_at=video_link.available_at,
    )
    for timestamp, value in (
        ("2026-01-01T00:00:00Z", 90),
        ("2026-01-02T00:00:00Z", 100),
        ("2026-01-05T00:00:00Z", 150),
    ):
        evidence.append(
            observation(
                "o1",
                timestamp,
                0,
                entity_type="offer",
                metrics={"units_sold": Measurement(lower=value, upper=value)},
                attributes=attributes,
            )
        )
    return product, [video_link, offer_link], evidence


def test_documented_counter_contract_does_not_report_rolling_difference_as_exact_sales():
    product, links, evidence = sales_case(
        {"units_sold_period": "rolling_30d", "counter_scope": "lifetime"}
    )
    config = EvaluationConfig(
        target="sales",
        threshold=40,
        top_k=1,
        horizon_days=(3,),
        launch_delay_days=(0,),
        counter_period_contract="documented-counter-v1",
    )
    snapshot = freeze_decision(
        evidence, [product], links, utc("2026-01-02T00:00:00Z"), config, synthetic=True
    )
    report = mature_decision(snapshot, evidence, utc("2026-01-05T00:00:00Z"))
    outcome = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert snapshot["config"]["counter_period_contract"] == "documented-counter-v1"
    assert outcome["status"] == "censored"
    assert outcome["entities"][0]["reason"] == "unknown_or_changed_counter_period"


@pytest.mark.parametrize(
    ("attributes", "measurable"),
    [
        ({"units_sold_period": "lifetime"}, True),
        ({"sold_period": "2026-01"}, True),
        ({"units_sold_period": " Lifetime ", "sold_period": "lifetime"}, True),
        ({"counter_scope": "lifetime"}, True),
        ({"units_sold_period": "unknown", "counter_scope": "lifetime"}, False),
        ({"units_sold_period": "lifetime", "sold_period": "rolling_30d"}, False),
        ({"units_sold_period": 30}, False),
        ({}, False),
    ],
)
def test_versioned_outcomes_keep_documented_periods_and_censor_ambiguous_sales(
    attributes, measurable
):
    product, links, evidence = sales_case(attributes)
    config = EvaluationConfig(
        target="sales",
        threshold=40,
        top_k=1,
        horizon_days=(3,),
        launch_delay_days=(0,),
        counter_period_contract="documented-counter-v1",
    )
    snapshot = freeze_decision(
        evidence, [product], links, utc("2026-01-02T00:00:00Z"), config, synthetic=True
    )
    outcome = mature_decision(snapshot, evidence, utc("2026-01-05T00:00:00Z"))["scenarios"][0][
        "methods"
    ]["policy"]["outcomes"][0]
    if measurable:
        assert outcome["status"] == "success"
        assert outcome["lower"] == outcome["upper"] == 50
        assert outcome["precision"] == "exact"
    else:
        assert outcome["status"] == "censored"
        assert outcome["entities"][0]["reason"] == "unknown_or_changed_counter_period"


def test_unversioned_frozen_outcomes_retain_their_original_counter_meaning_and_json():
    product, links, evidence = sales_case({"units_sold_period": "rolling_30d"})
    config = EvaluationConfig(
        target="sales", threshold=40, top_k=1, horizon_days=(3,), launch_delay_days=(0,)
    )
    snapshot = freeze_decision(
        evidence, [product], links, utc("2026-01-02T00:00:00Z"), config, synthetic=True
    )
    original = canonical(snapshot)
    assert "counter_period_contract" not in snapshot["config"]
    report = mature_decision(snapshot, evidence, utc("2026-01-05T00:00:00Z"))
    outcome = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "success"
    assert outcome["lower"] == outcome["upper"] == 50
    assert report["config"] == snapshot["config"]
    assert canonical(snapshot) == original


def test_new_decision_configuration_rejects_a_non_object_with_a_validation_error():
    with pytest.raises(ValueError, match="valid dictionary"):
        EvaluationConfig.for_creation([{"target": "sales"}])


@pytest.mark.integration
def test_new_persisted_forward_and_backtest_decisions_freeze_documented_counter_rules(pipeline):
    seed_product(pipeline, "period-product")
    cutoff = now() + timedelta(minutes=1)
    records = []
    for timestamp, value in ((cutoff, 100), (cutoff + timedelta(days=3), 150)):
        records.append(
            {
                "observation_id": f"rolling-offer-{value}",
                "source": "fixture_import",
                "entity_type": "offer",
                "entity_id": "rolling-offer",
                "observed_at": timestamp.isoformat(),
                "available_at": timestamp.isoformat(),
                "synthetic": True,
                "metrics": {"units_sold": {"lower": value, "upper": value}},
                "attributes": {"units_sold_period": "rolling_30d"},
            }
        )
    pipeline.import_records(records, synthetic=True)
    pipeline.review_link("period-product", "fixture_import:offer:rolling-offer")
    frozen = pipeline.freeze(cutoff, True, outcome_config(target="sales", threshold=40))
    assert frozen["result"]["config"]["counter_period_contract"] == "documented-counter-v1"
    matured = pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(days=3))
    outcome = matured["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "censored"
    assert outcome["entities"][0]["reason"] == "unknown_or_changed_counter_period"
    replayed = pipeline.backtest(
        [cutoff], cutoff + timedelta(days=3), True, outcome_config(target="sales", threshold=40)
    )
    assert replayed["result"]["config"]["counter_period_contract"] == "documented-counter-v1"


@pytest.mark.integration
def test_new_persisted_decisions_reject_an_explicit_null_counter_contract(pipeline):
    seed_product(pipeline, "declared-period-product")
    cutoff = now() + timedelta(minutes=1)
    config = outcome_config(counter_period_contract=None)
    with pytest.raises(ValueError, match="counter_period_contract"):
        pipeline.freeze(cutoff, True, config)
    with pytest.raises(ValueError, match="counter_period_contract"):
        pipeline.backtest([], cutoff, True, config)
    assert pipeline.evaluations(True) == []
