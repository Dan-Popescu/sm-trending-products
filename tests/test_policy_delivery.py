"""Configured ranking policies through persisted replay and forward evaluation."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from test_evaluation_integration import outcome_config, record

from sm_trending_products.cli import dispatch, parser
from sm_trending_products.contracts import AssessmentPolicy, now

pytestmark = pytest.mark.integration


def test_persisted_ranking_policy_changes_new_decisions_and_keeps_prior_outcomes_frozen(
    pipeline, tmp_path
):
    cutoff = now() + timedelta(minutes=1)
    records = []
    for product, history in (
        ("fast", ((2, 100), (1, 1000))),
        ("steady", ((3, 100), (2, 200), (1, 400))),
    ):
        for hours, views in history:
            records.append(record(product, cutoff - timedelta(hours=hours), views))
        for days in (0, 2, 3, 7, 10, 14):
            observed = cutoff + timedelta(days=days)
            records.append(
                record(
                    product,
                    observed,
                    3000 + 1000 * days,
                    available_at=observed + timedelta(seconds=1),
                )
            )
    pipeline.import_records(records, synthetic=True)
    for product in ("fast", "steady"):
        pipeline.create_product(product, product.title(), synthetic=True)
        pipeline.review_link(product, f"fixture_import:video:{product}")
    config = outcome_config(horizon_days=[3, 7, 14], launch_delay_days=[0, 2, 10])
    original = pipeline.freeze(cutoff, True, config)
    frozen_before = deepcopy(original["result"])
    assert original["result"]["selected"] == ["fast"]
    policy = AssessmentPolicy(version="three-samples-v1", min_observations=3)
    revised = pipeline.freeze(cutoff, True, config, policy=policy)
    assert revised["evaluation_id"] != original["evaluation_id"]
    assert revised["result"]["selected"] == ["steady"]
    assert revised["result"]["policy"] == policy.model_dump(mode="json")
    assert {row["product_id"] for row in revised["result"]["universe"]} == {"steady"}
    assert all(selection == ["steady"] for selection in revised["result"]["baselines"].values())
    assert revised["result"]["coverage"]["known_products"] == 2
    published = pipeline.assess(cutoff, True, policy)
    assert next(row for row in published["rows"] if row["product_id"] == "steady") == {
        key: value for key, value in revised["result"]["universe"][0].items() if key != "family_id"
    }
    assert pipeline.freeze(cutoff, True, config, policy=policy) == revised

    config_file, policy_file = tmp_path / "evaluation.json", tmp_path / "policy.json"
    config_file.write_text(json.dumps(config))
    policy_file.write_text(policy.model_dump_json())
    arguments = [
        "freeze",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
        "--config-file",
        str(config_file),
    ]
    assert dispatch(pipeline, parser().parse_args(arguments)) == original
    assert (
        dispatch(pipeline, parser().parse_args([*arguments, "--policy-file", str(policy_file)]))
        == revised
    )
    with pytest.raises(ValueError, match="Policy was not available"):
        pipeline.freeze(
            cutoff,
            True,
            {**config, "policy_available_at": (cutoff + timedelta(days=1)).isoformat()},
            policy=policy,
        )

    pending = pipeline.mature(original["evaluation_id"], cutoff + timedelta(days=2))["result"]
    assert all(
        row["status"]
        == ("not_actionable" if row["launch_delay_days"] >= row["horizon_days"] else "pending")
        for row in pending["scenarios"]
    )
    for day in (3, 7, 14):
        report = pipeline.mature(
            original["evaluation_id"], cutoff + timedelta(days=day, seconds=1)
        )["result"]
        for scenario in report["scenarios"]:
            expected = (
                "not_actionable"
                if scenario["launch_delay_days"] >= scenario["horizon_days"]
                else "matured"
                if scenario["horizon_days"] <= day
                else "pending"
            )
            assert scenario["status"] == expected
            if expected == "matured":
                method = scenario["methods"]["policy"]
                assert method["precision_at_k"] == 1 and method["observed_count"] == 1
                assert method["wilson_interval_observed"] is not None
    final_time = cutoff + timedelta(days=14, seconds=1)
    original_outcome = pipeline.mature(original["evaluation_id"], final_time)["result"]
    revised_outcome = pipeline.mature(revised["evaluation_id"], final_time)["result"]
    replayed = pipeline.backtest([cutoff], final_time, True, config, policy=policy)["result"]
    assert replayed["snapshots"] == [revised["result"]]
    assert replayed["reports"] == [revised_outcome]
    assert (
        pipeline.backtest([cutoff], final_time, True, config, policy=policy)["result"] == replayed
    )
    dates_file = tmp_path / "decisions.json"
    dates_file.write_text(json.dumps([cutoff.isoformat()]))
    cli_replay = dispatch(
        pipeline,
        parser().parse_args(
            [
                "backtest",
                "--synthetic",
                "--as-of",
                final_time.isoformat(),
                "--config-file",
                str(config_file),
                "--policy-file",
                str(policy_file),
                "--decision-times-file",
                str(dates_file),
            ]
        ),
    )
    assert cli_replay["result"] == replayed
    assert pipeline.mature(original["evaluation_id"], final_time)["result"] == original_outcome
    saved = next(
        row
        for row in pipeline.evaluations(True)
        if row["evaluation_id"] == original["evaluation_id"]
    )
    assert saved["payload"] == frozen_before
    assert saved["payload"]["policy"]["min_observations"] == 2
    assert pipeline.evaluations(False) == []
