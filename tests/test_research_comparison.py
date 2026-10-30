"""Actual governed development attempts through native SQL/history boundaries."""

import json
from datetime import timedelta

import pytest
from test_research_validation import seed
from test_research_validation_operator import connect_fixture, run

from sm_trending_products.contracts import AssessmentPolicy
from sm_trending_products.research_validation import ResearchError, ValidationProtocol
from sm_trending_products.research_validation._candidates import candidate_bundle
from sm_trending_products.research_validation_cli import main


def two_runs(pipeline, *, outcomes=True, strict=False, measurement_sample_size=0):
    policies = {
        "attention": AssessmentPolicy(),
        "other": AssessmentPolicy(
            version="unvalidated-demand",
            weights={"attention": 0.7, "demand": 0.3},
            missing_data="exclude" if strict else "renormalize",
        ),
    }
    service, _, identifier, base, datasets = seed(
        pipeline,
        outcomes=outcomes,
        policies=policies,
        measurement_sample_size=measurement_sample_size,
    )
    attempts = [
        service.run(
            identifier,
            "development",
            policy,
            policy,
            synthetic=True,
            as_of=base + timedelta(days=1),
        )
        for policy in policies
    ]
    assert all(row["state"] == "completed" for row in attempts), attempts
    return service, identifier, base, datasets, tuple(row["attempt_id"] for row in attempts)


def test_candidates_are_editable_explicitly_unvalidated_and_configuration_free(monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    def forbidden(*args, **kwargs):
        raise AssertionError("candidate generation must not construct a pipeline")

    monkeypatch.setattr(Pipeline, "from_env", forbidden)
    bundle = run(capsys, ["candidates"])
    assert bundle["status"] == "UNVALIDATED"
    profile = run(capsys, ["candidates", "--format", "protocol"])
    parsed = ValidationProtocol.model_validate(profile)
    assert set(policy.lookback_days for policy in parsed.policies.values()) == {7, 14, 30}
    assert parsed.config.launch_delay_hours == (12, 24, 48)
    assert {policy.missing_data for policy in parsed.policies.values()} == {
        "exclude",
        "renormalize",
    }
    assert "not modeled" in bundle["criterion_dictionary"]["delivery"]["limit"]
    assert candidate_bundle()["protocol"] == profile
    first = candidate_bundle()
    first["criterion_dictionary"]["delivery"]["limit"] = "edited"
    first["protocol"]["policies"].clear()
    assert candidate_bundle()["protocol"] == profile
    assert candidate_bundle()["criterion_dictionary"]["delivery"]["limit"] != "edited"
    assert run(capsys, ["compare", "--attempt-id", "x"], 2)["error"] == "invalid_comparison"
    assert (
        run(capsys, ["compare", "--attempt-id", "x", "--attempt-id", "x"], 2)["error"]
        == "invalid_comparison"
    )


@pytest.mark.integration
def test_comparison_uses_existing_selected_identities_and_actual_outcome_summaries(pipeline):
    service, identifier, _, _, ids = two_runs(pipeline)
    count_before = len(service.attempts(identifier, True))
    comparison = service.compare(ids, True)
    assert comparison["status"] == "comparable"
    assert comparison["winner"] is None and comparison["candidate_status"] == "UNVALIDATED"
    assert len(service.attempts(identifier, True)) == count_before
    for candidate in comparison["candidates"]:
        attempt = service.attempt(candidate["attempt_id"], True)
        assert candidate["decisions"][0]["selected"] == ["dev"]
        assert candidate["decisions"][0]["coverage"]["counts"]["eligible"] == 1
        for actual, stored in zip(
            candidate["outcome_summaries"], attempt["result"]["comparisons"], strict=True
        ):
            assert actual == {key: value for key, value in stored.items() if key != "outcomes"}
        policy = next(row for row in candidate["outcome_summaries"] if row["method"] == "policy")
        assert policy["observed_count"] == 1 and policy["status_counts"] == {"success": 1}


@pytest.mark.integration
def test_strict_missingness_flags_effects_without_hiding_selections_or_denominators(pipeline):
    service, _, _, _, ids = two_runs(pipeline, strict=True)
    comparison = service.compare(ids, True)
    assert comparison["status"] == "flagged"
    assert "different_coverage_or_exclusions" in comparison["flags"]
    first, strict = comparison["candidates"]
    assert first["decisions"][0]["selected"] == ["dev"]
    assert strict["decisions"][0]["selected"] == []
    assert strict["decisions"][0]["coverage"]["reason_counts"]["score_unavailable"] == 1
    policy = next(row for row in strict["outcome_summaries"] if row["method"] == "policy")
    assert policy["selected_count"] == 0 and policy["precision_at_k"] is None


@pytest.mark.integration
def test_unknown_and_maturing_outcomes_are_explicit(pipeline):
    service, identifier, base, _, ids = two_runs(pipeline, outcomes=False)
    comparison = service.compare(ids, True)
    policy = next(
        row for row in comparison["candidates"][0]["outcome_summaries"] if row["method"] == "policy"
    )
    assert policy["observed_count"] == 0 and policy["precision_at_k"] is None
    assert policy["status_counts"] and policy["coverage"] == 0
    early = [
        service.run(identifier, "development", name, "early-" + name, synthetic=True, as_of=base)[
            "attempt_id"
        ]
        for name in ("attention", "other")
    ]
    immature = service.compare(tuple(early), True)
    policy = next(
        row for row in immature["candidates"][0]["outcome_summaries"] if row["method"] == "policy"
    )
    assert policy["status_counts"] == {"pending": 1}


@pytest.mark.integration
def test_unequal_cutoffs_refused_and_heldout_rejected_before_attempt_hydration(
    pipeline, monkeypatch
):
    service, identifier, base, _, ids = two_runs(pipeline)
    early = service.run(identifier, "development", "attention", "early", synthetic=True, as_of=base)
    with pytest.raises(ResearchError, match="comparison_cutoff_mismatch"):
        service.compare((ids[0], early["attempt_id"]), True)
    service.lock(identifier, "attention", "lock", True, as_of=base + timedelta(days=1))
    heldout = service.run(
        identifier,
        "holdout",
        "attention",
        "holdout",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert heldout["state"] == "completed"

    def forbidden(*args, **kwargs):
        raise AssertionError("heldout attempt must not hydrate")

    monkeypatch.setattr(service, "attempt", forbidden)
    with pytest.raises(ResearchError, match="comparison_development_only"):
        service.compare((ids[0], heldout["attempt_id"]), True)


@pytest.mark.integration
def test_source_withdrawal_removes_comparison_and_namespaces_are_guarded(pipeline):
    service, _, _, datasets, ids = two_runs(pipeline)
    assert service.compare(ids, True)["status"] == "comparable"
    with pytest.raises(ResearchError, match="attempt_not_found"):
        service.compare(ids, False)
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    result = service.compare(ids, True)
    assert result["status"] == "unavailable"
    assert "candidates" not in result
    assert all(row["result_unavailable"] == "evidence_invalidated" for row in result["attempts"])


@pytest.mark.integration
def test_compare_read_never_installs_optional_tables(pipeline):
    from sm_trending_products.research_validation import ResearchValidation

    service = ResearchValidation(pipeline.catalog, pipeline.history, pipeline.identity)
    with pytest.raises(ValueError):
        service.compare(("a", "b"), True)
    assert (
        pipeline.catalog.one("SELECT to_regclass('research_protocols') AS value")["value"] is None
    )


@pytest.mark.integration
def test_cli_compare_machine_and_french_summary_guard_current_secrets(
    pipeline, monkeypatch, capsys
):
    service, _, _, _, ids = two_runs(pipeline)
    connect_fixture(monkeypatch, pipeline)
    argv = ["compare", "--synthetic", "--attempt-id", ids[0], "--attempt-id", ids[1]]
    comparison = run(capsys, argv)
    assert comparison["status"] == "comparable"
    monkeypatch.setenv("OPERATOR_SECRET", "attention")
    assert main([*argv, "--format", "summary"]) == 0
    text = capsys.readouterr().out
    assert "NON VALIDÉS" in text and "1/1" in text and "attention" not in text
    assert "attention" not in json.dumps(run(capsys, argv))


@pytest.mark.integration
def test_sample_coverage_and_limits_are_retained(pipeline, monkeypatch):
    from sm_trending_products.research_validation import _comparison

    service, _, _, _, ids = two_runs(pipeline, measurement_sample_size=2)
    comparison = service.compare(ids, True)
    sample = comparison["candidates"][0]["decisions"][0]["measurement_sample"]
    assert sample["methodology"]["sample_size"] == 2
    assert sample["methodology"]["sampling_population"] == 2
    assert sample["methodology"]["inclusion_probability"] == 1
    assert "outcomes" not in sample["scenarios"][0]["methods"]["policy"]
    monkeypatch.setattr(_comparison, "MAX_RECORDS", 1)
    with pytest.raises(ResearchError, match="comparison_capacity_exceeded"):
        service.compare(ids, True)
    monkeypatch.setattr(_comparison, "MAX_RECORDS", 10000)
    monkeypatch.setattr(_comparison, "MAX_BYTES", 100)
    with pytest.raises(ResearchError, match="comparison_capacity_exceeded"):
        service.compare(ids, True)


@pytest.mark.integration
def test_different_protocols_refused_before_result_read(pipeline, monkeypatch):
    from test_research_validation import protocol_at

    from sm_trending_products.contracts import now

    service, _, base, _, ids = two_runs(pipeline)
    second = service.register(
        protocol_at(base, name="other-protocol"), "register-other", synthetic=True, as_of=now()
    )
    attempt = service.run(
        second["protocol_id"],
        "development",
        "attention",
        "other-run",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert attempt["state"] == "completed"

    def forbidden(*args, **kwargs):
        raise AssertionError("different scope must not read result")

    monkeypatch.setattr(service, "attempt", forbidden)
    with pytest.raises(ResearchError, match="comparison_protocol_mismatch"):
        service.compare((ids[0], attempt["attempt_id"]), True)


@pytest.mark.integration
def test_private_comparison_refreshes_source_guards_without_raw_session_cache(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    service, identifier, _, datasets, ids = two_runs(pipeline, strict=True)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("CACHE_DIR", str(pipeline.settings.cache_dir))
    app = AppTest.from_string(
        """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.research_validation_dashboard import render_validation
with Pipeline.from_env() as pipeline:
    render_validation(pipeline, True)
""",
        default_timeout=30,
    ).run(timeout=30)
    app.text_input(key="research-protocol-widget:True").set_value(identifier)
    app.button(key="research-inspect:True").click().run()
    scope = f"True:{identifier}"
    app.text_input(key=f"research-compare-widget:{scope}").set_value(",".join(ids))
    app.button(key=f"research-compare:{scope}").click().run()
    assert not app.exception and not app.error, [item.value for item in app.error]
    comparison = json.loads(app.json[-1].value)
    assert comparison["status"] == "flagged"
    assert any("NON VALIDÉS" in item.value for item in app.text)
    assert not any(
        isinstance(value, dict) and "candidates" in value for value in app.session_state.values()
    )
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    app.run()
    assert not app.exception
    comparison = json.loads(app.json[-1].value)
    assert comparison["status"] == "unavailable" and "candidates" not in comparison


@pytest.mark.parametrize(
    "unsafe",
    [float("nan"), object(), "x" * 4_000_001],
    ids=("non-finite", "unsupported-object", "oversized-string"),
)
def test_compare_cli_refuses_unsafe_final_display_input(monkeypatch, capsys, unsafe):
    from contextlib import contextmanager
    from types import SimpleNamespace

    import sm_trending_products.research_validation_cli as operator
    from sm_trending_products.pipeline import Pipeline

    @contextmanager
    def connected():
        yield object()

    monkeypatch.setattr(Pipeline, "from_env", connected)
    monkeypatch.setattr(
        operator,
        "service_for",
        lambda pipeline: SimpleNamespace(
            compare=lambda *args: {"status": "comparable", "details": unsafe}
        ),
    )
    result = run(capsys, ["compare", "--attempt-id", "a", "--attempt-id", "b"], 2)
    assert result["error"] == "research_validation_failed"


@pytest.mark.integration
@pytest.mark.parametrize("withdraw_at", ["attempt", "page"])
def test_withdrawal_during_hydration_refuses_stale_results_with_default_pool(
    pipeline, monkeypatch, withdraw_at
):
    from concurrent.futures import ThreadPoolExecutor

    service, _, _, datasets, ids = two_runs(pipeline)
    target = service if withdraw_at == "attempt" else service.evaluations.provenance
    name = "attempt" if withdraw_at == "attempt" else "page"
    ordinary_read = getattr(target, name)
    withdrawn = False
    with ThreadPoolExecutor(max_workers=1) as workers:

        def read_and_withdraw(*args, **kwargs):
            nonlocal withdrawn
            result = ordinary_read(*args, **kwargs)
            if not withdrawn:
                withdrawn = True
                workers.submit(
                    pipeline.catalog.invalidate_dataset, datasets[0], "source_retention_expired"
                ).result(timeout=10)
            return result

        monkeypatch.setattr(target, name, read_and_withdraw)
        try:
            response = service.compare(ids, True)
        except (ResearchError, ValueError):
            pass
        else:
            assert response["status"] == "unavailable" and "candidates" not in response
    assert withdrawn
    assert service.compare(ids, True)["status"] == "unavailable"


@pytest.mark.integration
def test_multiple_dates_preserve_frozen_chronology_in_hashed_record_tree(pipeline):
    from sm_trending_products.contracts import now, utc

    service, original, _, base, _ = seed(pipeline)
    payload = original.model_dump(mode="json")
    payload["name"] = "multi-date"
    payload["partitions"]["development"]["decision_times"] = [base, base + timedelta(days=2)]
    payload["partitions"]["test"]["decision_times"] = [base + timedelta(days=4)]
    payload["partitions"]["holdout"]["decision_times"] = [base + timedelta(days=6)]
    protocol = ValidationProtocol.model_validate(payload)
    identifier = service.register(protocol, "multi", synthetic=True, as_of=now())["protocol_id"]
    ids = tuple(
        service.run(
            identifier,
            "development",
            "attention",
            request,
            synthetic=True,
            as_of=base + timedelta(days=3),
        )["attempt_id"]
        for request in ("one", "two")
    )
    result = service.compare(ids, True)
    assert [utc(row["decision_time"]) for row in result["candidates"][0]["decisions"]] == [
        base,
        base + timedelta(days=2),
    ]


@pytest.mark.integration
def test_equal_aggregate_counts_flag_different_exclusions_but_not_normal_selection_changes(
    pipeline,
):
    from test_evaluation_integration import record

    policies = {
        "demand": AssessmentPolicy(version="demand-only", weights={"demand": 1}),
        "feasibility": AssessmentPolicy(version="feasibility-only", weights={"feasibility": 1}),
    }
    service, _, identifier, base, _ = seed(pipeline, policies=policies)
    pipeline.import_records(
        [
            record("thin", base - timedelta(hours=1), 2),
            record("thin", base + timedelta(days=1), 305),
        ],
        synthetic=True,
    )

    def sold(product_id, before, after):
        rows = []
        for timestamp, value in ((base - timedelta(hours=1), before), (base, after)):
            row = record(product_id + "-offer", timestamp, value)
            row["entity_type"] = "offer"
            row["metrics"] = {
                "units_sold": {"lower": value, "upper": value + 1, "precision": "bounded"}
            }
            row["attributes"] = {"sold_period": "lifetime"}
            rows.append(row)
        pipeline.import_records(rows, synthetic=True)
        pipeline.review_link(product_id, f"fixture_import:offer:{product_id}-offer")

    def cost(product_id, unit_cost):
        row = record(product_id + "-cost", base, 0)
        row["entity_type"] = "cost"
        row["metrics"] = {}
        row["attributes"] = {
            "currency": "EUR",
            "sale_price": "20",
            "unit_cost": unit_cost,
            "shipping_cost": "2",
            "payment_fee": "1",
            "ad_cost": "3",
            "tax_cost": "2",
            "return_rate": "0.1",
            "return_cost": "5",
        }
        pipeline.import_records([row], synthetic=True)
        pipeline.review_link(product_id, f"fixture_import:cost:{product_id}-cost")

    def compare_attempts(request_prefix):
        return service.compare(
            tuple(
                service.run(
                    identifier,
                    "development",
                    policy,
                    request_prefix + policy,
                    synthetic=True,
                    as_of=base + timedelta(days=1),
                )["attempt_id"]
                for policy in policies
            ),
            True,
        )

    sold("dev", 100, 200)
    cost("thin", "4")
    comparison = compare_attempts("missing-")
    demand, feasibility = comparison["candidates"]
    demand_audit = demand["decisions"][0]["coverage"]
    feasibility_audit = feasibility["decisions"][0]["coverage"]
    assert demand_audit["counts"] == feasibility_audit["counts"]
    assert demand_audit["reason_counts"] == feasibility_audit["reason_counts"]
    assert demand_audit["reason_counts"]["score_unavailable"] == 1
    assert [
        (row["selected_count"], row["observed_count"], row["status_counts"])
        for row in demand["outcome_summaries"]
    ] == [
        (row["selected_count"], row["observed_count"], row["status_counts"])
        for row in feasibility["outcome_summaries"]
    ]
    assert comparison["flags"] == ["different_coverage_or_exclusions"]
    assert comparison["status"] == "flagged"
    assert comparison["coverage_differences"] == [
        {
            "decision_time": base.isoformat(),
            "product_ids": ["dev", "thin"],
        }
    ]
    demand_products = {row["product_id"]: row for row in demand_audit["products"]}
    feasibility_products = {row["product_id"]: row for row in feasibility_audit["products"]}
    assert demand_products["thin"]["reason"] == "score_unavailable"
    assert feasibility_products["dev"]["reason"] == "score_unavailable"
    assert demand_products["dev"]["stages"]["ranked"]
    assert feasibility_products["thin"]["stages"]["ranked"]
    assert demand_products["thin"]["assessment_reasons"]
    assert feasibility_products["dev"]["assessment_reasons"]

    # Both products now have both dimensions, and opposite policy preferences.
    # Different selections alone must not masquerade as an exclusion/coverage gap.
    sold("thin", 100, 101)
    cost("dev", "18")
    complete = compare_attempts("complete-")
    assert [row["decisions"][0]["selected"] for row in complete["candidates"]] == [
        ["dev"],
        ["thin"],
    ]
    assert complete["status"] == "comparable"
    assert complete["coverage_differences"] == []
    assert "different_coverage_or_exclusions" not in complete["flags"]
    assert all(
        candidate["decisions"][0]["coverage"]["counts"]["ranked"] == 2
        for candidate in complete["candidates"]
    )
    assert complete["winner"] is None
