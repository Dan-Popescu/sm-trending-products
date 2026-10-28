"""Own-commerce learning through native SQL, durable object delivery and evaluation."""

import gzip
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from test_evaluation_integration import seed_product
from test_launch_dossiers import draft

from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_learning import (
    ACTUAL_COSTS,
    ActualCostContract,
    CommercialLearning,
    FinancialCoverage,
    LearningError,
    ReviewedPeriod,
)
from sm_trending_products.commerce_learning import (
    TestPlan as Plan,
)
from sm_trending_products.contracts import now, utc


def plan(**changes):
    return Plan.model_validate(
        {
            "selection": "test",
            "reason": "Reviewed multi-day attention and supplier feasibility",
            "selection_policy": "Manual pilot policy v1; compare actuals without training",
            "cost_contract": {
                "required_costs": list(ACTUAL_COSTS),
                "revenue_basis": "settled_payments_less_executed_refunds_excluding_output_taxes",
                "coverage_policy": "Review all settlements, refunds and invoices for this period",
            },
            **changes,
        }
    )


def period(start, end, **changes):
    return ReviewedPeriod.model_validate(
        {
            "period_start": start,
            "period_end": end,
            "country": "FR",
            "currency": "EUR",
            "realized_revenue": "120.00",
            "realized_costs": {
                **dict.fromkeys(ACTUAL_COSTS, "0.00"),
                "goods": "50.00",
                "advertising": "20.00",
                "payment_fees": "10.00",
            },
            "evidence_refs": [
                "order-snapshot:" + "a" * 64,
                "processor-report:fixture-1",
                "supplier-invoice:fixture-2",
            ],
            "coverage": {name: True for name in FinancialCoverage.model_fields},
            "reviewer_note": "Reviewed actual statements and all declared component amounts",
            **changes,
        }
    )


@pytest.fixture
def learning(pipeline):
    seed_product(pipeline, "p1")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create-launch")
    service = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    service.initialize()
    return service


def planned(learning, **changes):
    return learning.plan_test("launch-p1-fr", 1, "plan-first", True, plan=plan(**changes))


def test_test_exposes_declaration_delivery_without_mutating_or_repairing_it(learning, pipeline):
    original = planned(learning)
    publication = original["declaration_publication"]
    assert publication["state"] == "published"
    assert pipeline.verify(publication["dataset_id"])["ok"]
    pipeline.catalog.execute(
        "UPDATE commercial_publications SET state='pending' WHERE dataset_id=%s",
        (publication["dataset_id"],),
    )
    assert learning.test(original["test_id"], True)["declaration_publication"] == {
        "dataset_id": publication["dataset_id"],
        "state": "pending",
    }
    pipeline.catalog.execute(
        "DELETE FROM commercial_publications WHERE dataset_id=%s", (publication["dataset_id"],)
    )
    assert learning.test(original["test_id"], True)["declaration_publication"] is None
    watch = learning.plan_test("launch-p1-fr", 1, "watch-test", True, plan=plan(selection="watch"))
    assert watch["declaration_publication"] is None


@pytest.mark.parametrize("value", [1.25, True, "NaN", "Infinity", "1.1234567"])
def test_money_rejects_inexact_nonfinite_or_overprecise_values(value):
    with pytest.raises(ValueError):
        period(now(), now() + timedelta(days=1), realized_revenue=value)


def test_contract_requires_explicit_actual_components_and_coverage_policy():
    definition = plan().cost_contract.model_dump(mode="json")
    for costs in ([], list(ACTUAL_COSTS[:-1]), [*ACTUAL_COSTS, "goods"], ["returns_allowance"]):
        with pytest.raises(ValueError):
            ActualCostContract.model_validate({**definition, "required_costs": costs})
    with pytest.raises(ValueError):
        ActualCostContract.model_validate({**definition, "coverage_policy": " "})
    with pytest.raises(ValueError):
        period(now(), now() + timedelta(days=1), amount_kind="scenario")
    with pytest.raises(ValueError):
        period(now(), now() + timedelta(days=1), evidence_refs=["customer@example.com"])
    with pytest.raises(ValueError):
        period(now(), now() + timedelta(days=1), realized_costs={"goods": "-1"})


@pytest.mark.integration
def test_plan_freezes_dossier_and_selection_without_turning_quotes_into_actuals(learning, pipeline):
    original = planned(learning)
    assert original["state"] == "ready"
    assert original["dossier_revision"] == original["revision"] == 1
    key = original["ledger_key"]
    assert key.startswith("commerce_actuals:cost:")
    assert original["profit_contract"]["ledgers"] == {key: list(ACTUAL_COSTS)}
    rows, _ = pipeline.history.observations(now() + timedelta(minutes=1), True, entity_keys={key})
    assert len(rows) == 1 and rows[0].attributes["realized"] is False
    assert "realized_revenue" not in rows[0].attributes
    assert "realized_costs" not in rows[0].attributes
    launches = LaunchService(pipeline.catalog)
    launches.save(draft(note="Later changed quote", retail_price="50"), 1, "revise-launch")
    assert planned(learning) == original
    assert original["dossier_snapshot"]["draft"]["retail_price"] == "36.00"
    with pytest.raises(LearningError, match="request conflict"):
        learning.plan_test(
            "launch-p1-fr", 1, "plan-first", True, plan=plan(reason="Different reason")
        )
    with pytest.raises(LearningError, match="dossier revision conflict"):
        learning.plan_test("launch-p1-fr", 1, "another-request", True, plan=plan())
    assert learning.tests(False) == []
    with pytest.raises(LearningError, match="test not found"):
        learning.test(original["test_id"], False)


@pytest.mark.integration
def test_watch_and_skip_reasons_are_recorded_without_creating_financial_entities(
    learning, pipeline
):
    count = len(pipeline.datasets())
    for selection in ("watch", "skip"):
        choice = learning.plan_test(
            "launch-p1-fr", 1, selection, True, plan=plan(selection=selection)
        )
        assert choice["state"] == "recorded" and choice["ledger_key"] is None
        assert choice["plan"]["reason"]
        with pytest.raises(LearningError, match="no ready ledger"):
            learning.review_period(
                choice["test_id"],
                period(now(), now() + timedelta(days=1)),
                0,
                "invalid-review",
                True,
                as_of=now() + timedelta(days=2),
            )
    assert len(pipeline.datasets()) == count
    assert {row["plan"]["selection"] for row in learning.tests(True)} == {"watch", "skip"}
    LaunchService(pipeline.catalog).save(draft(stock_status="unknown"), 1, "incomplete-quote")
    with pytest.raises(LearningError, match="dossier not ready"):
        learning.plan_test("launch-p1-fr", 2, "not-ready", True, plan=plan())
    watched = learning.plan_test(
        "launch-p1-fr", 2, "watch-not-ready", True, plan=plan(selection="watch")
    )
    assert watched["state"] == "recorded"


@pytest.mark.integration
def test_complete_reviewed_period_connects_to_the_predeclared_frozen_profit_contract(
    learning,
    pipeline,
):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    end, cutoff = start + timedelta(days=3), start + timedelta(days=3, minutes=1)
    config = {
        "target": "profit",
        "currency": "EUR",
        "top_k": 1,
        "threshold": 30,
        "horizon_days": [3],
        "launch_delay_days": [0],
        "profit_contract": test["profit_contract"],
    }
    frozen = pipeline.freeze(start, True, config)
    assert frozen["result"]["selected"] == ["p1"]
    reviewed = learning.review_period(
        test["test_id"], period(start, end), 0, "actual", True, as_of=cutoff
    )
    assert reviewed["complete"] is True and reviewed["realized_profit"] == "40.00"
    assert reviewed["publication_state"] == "pending"
    before = pipeline.mature(frozen["evaluation_id"], cutoff)
    assert (
        before["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"] == "censored"
    )
    published = learning.publish(test["test_id"], reviewed["period_id"], 1, True)
    assert published["publication_state"] == "published"
    result = pipeline.mature(frozen["evaluation_id"], cutoff)
    outcome = result["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "success" and outcome["lower"] == "40.00"
    assert (
        pipeline.mature(frozen["evaluation_id"], end)["result"]["scenarios"][0]["methods"][
            "policy"
        ]["outcomes"][0]["status"]
        == "censored"
    )
    assert learning.publish(test["test_id"], reviewed["period_id"], 1, True) == published
    assert learning.periods(test["test_id"], True) == [published]
    evidence, _ = pipeline.history.observations(cutoff, True, entity_keys={test["ledger_key"]})
    actual = next(row for row in evidence if row.attributes.get("realized"))
    assert actual.available_at == cutoff
    assert actual.attributes["dossier_revision"] == 1
    assert "reviewer_note" not in actual.attributes


@pytest.mark.integration
@pytest.mark.parametrize("missing", ["cost", "payment", "refund", "fee", "evidence"])
def test_incomplete_actuals_remain_unknown_and_cannot_publish(learning, missing):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    values = period(start, start + timedelta(days=1)).model_dump(mode="json")
    if missing == "cost":
        del values["realized_costs"]["advertising"]
    elif missing == "evidence":
        values["evidence_refs"] = []
    else:
        values["coverage"][
            {"payment": "payments_complete", "refund": "refunds_complete", "fee": "fees_complete"}[
                missing
            ]
        ] = False
    reviewed = learning.review_period(
        test["test_id"],
        ReviewedPeriod.model_validate(values),
        0,
        "incomplete",
        True,
        as_of=start + timedelta(days=2),
    )
    assert reviewed["complete"] is False
    assert reviewed["realized_profit"] is None and reviewed["reasons"]
    with pytest.raises(LearningError, match="incomplete period"):
        learning.publish(test["test_id"], reviewed["period_id"], 1, True)


@pytest.mark.integration
def test_late_incomplete_correction_retracts_profit_without_rewriting_old_cutoffs(
    learning, pipeline
):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    end = start + timedelta(days=3)
    first, later = end + timedelta(minutes=1), end + timedelta(hours=1)
    frozen = pipeline.freeze(
        start,
        True,
        {
            "target": "profit",
            "currency": "EUR",
            "threshold": 30,
            "horizon_days": [3],
            "launch_delay_days": [0],
            "profit_contract": test["profit_contract"],
        },
    )
    original = learning.review_period(
        test["test_id"], period(start, end), 0, "first", True, as_of=first
    )
    learning.publish(test["test_id"], original["period_id"], 1, True)
    original_outcome = pipeline.mature(frozen["evaluation_id"], first)
    changed = period(
        start,
        end,
        coverage={
            "payments_complete": True,
            "refunds_complete": False,
            "fees_complete": True,
            "costs_complete": True,
        },
    )
    correction = learning.review_period(
        test["test_id"], changed, 1, "correction", True, as_of=later
    )
    assert correction["complete"] is False and correction["publication_kind"] == "withdrawal"
    assert correction["publication_state"] == "published"
    assert (
        learning.review_period(test["test_id"], changed, 1, "correction", True, as_of=later)
        == correction
    )
    current = pipeline.mature(frozen["evaluation_id"], later)
    assert (
        current["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"]
        == "censored"
    )
    assert pipeline.mature(frozen["evaluation_id"], first) == original_outcome
    assert len(learning.periods(test["test_id"], True, include_history=True)) == 2
    evidence, _ = pipeline.history.observations(later, True, entity_keys={test["ledger_key"]})
    withdrawal = next(row for row in evidence if row.available_at == later)
    assert withdrawal.attributes["realized"] is False
    assert "realized_revenue" not in withdrawal.attributes


@pytest.mark.integration
@pytest.mark.parametrize(
    "failure", ["before_upload", "object_ack_loss", "sql_commit", "sql_ack_loss"]
)
def test_historical_withdrawal_replays_after_a_newer_complete_revision(
    learning,
    pipeline,
    monkeypatch,
    failure,
):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    end = start + timedelta(days=3)
    first, retract, newer = (end + timedelta(hours=value) for value in (1, 2, 3))
    frozen = pipeline.freeze(
        start,
        True,
        {
            "target": "profit",
            "currency": "EUR",
            "threshold": 30,
            "horizon_days": [3],
            "launch_delay_days": [0],
            "profit_contract": test["profit_contract"],
        },
    )
    original = learning.review_period(
        test["test_id"], period(start, end), 0, "r1", True, as_of=first
    )
    learning.publish(test["test_id"], original["period_id"], 1, True)
    first_outcome = pipeline.mature(frozen["evaluation_id"], first)
    review = period(
        start,
        end,
        coverage={
            "payments_complete": True,
            "refunds_complete": False,
            "fees_complete": True,
            "costs_complete": True,
        },
    )
    with monkeypatch.context() as fault:
        if failure == "before_upload":

            def unavailable(*args, **kwargs):
                raise RuntimeError("withdrawal unavailable before upload")

            fault.setattr(pipeline.history, "publish", unavailable)
        elif failure == "object_ack_loss":
            put = pipeline.history._put

            def lost_object_ack(key, body, **kwargs):
                result = put(key, body, **kwargs)
                if key.startswith("raw/"):
                    source = json.loads(gzip.decompress(body))
                    if source["records"][0]["attributes"].get("publication_kind") == "withdrawal":
                        raise RuntimeError("withdrawal object acknowledgement lost")
                return result

            fault.setattr(pipeline.history, "_put", lost_object_ack)
        else:
            commit = pipeline.catalog.commit_dataset

            def failed_sql(*args, **kwargs):
                if failure == "sql_ack_loss":
                    commit(*args, **kwargs)
                raise RuntimeError("withdrawal SQL completion failed")

            fault.setattr(pipeline.catalog, "commit_dataset", failed_sql)
        with pytest.raises(RuntimeError, match="withdrawal"):
            learning.review_period(test["test_id"], review, 1, "r2", True, as_of=retract)
    pending = learning.periods(test["test_id"], True)[0]
    assert pending["revision"] == 2 and pending["publication_state"] == "pending"
    intent = pipeline.catalog.one(
        "SELECT observation FROM commercial_publications WHERE dataset_id=%s",
        (pending["dataset_id"],),
    )["observation"]
    learning.review_period(test["test_id"], period(start, end), 2, "r3", True, as_of=newer)
    learning.publish(test["test_id"], original["period_id"], 3, True)
    historical = learning.periods(test["test_id"], True, include_history=True)[1]
    assert historical["superseded"] is True
    assert historical["publication_state"] == "pending"  # still recoverable
    recovered = learning.publish(test["test_id"], original["period_id"], 2, True)
    assert recovered["publication_state"] == "published"
    assert recovered["dataset_id"] == pending["dataset_id"]
    assert recovered["available_at"] == retract.isoformat()
    assert learning.publish(test["test_id"], original["period_id"], 2, True) == recovered
    assert (
        learning.review_period(test["test_id"], review, 1, "r2", True, as_of=retract) == recovered
    )
    assert pipeline.mature(frozen["evaluation_id"], first) == first_outcome
    intermediate = pipeline.mature(frozen["evaluation_id"], retract)
    latest = pipeline.mature(frozen["evaluation_id"], newer)
    assert (
        intermediate["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"]
        == "censored"
    )
    last_outcome = latest["result"]["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert last_outcome["status"] == "success" and last_outcome["lower"] == "40.00"
    rows, _ = pipeline.history.observations(newer, True, entity_keys={test["ledger_key"]})
    withdrawals = [row for row in rows if row.attributes.get("publication_kind") == "withdrawal"]
    assert len(withdrawals) == 1 and withdrawals[0].observation_id == intent["observation_id"]
    assert withdrawals[0].available_at == retract
    assert "realized_revenue" not in withdrawals[0].attributes
    assert "realized_costs" not in withdrawals[0].attributes


@pytest.mark.integration
@pytest.mark.parametrize("after_commit", [False, True])
def test_declaration_replays_after_s3_success_and_sql_failure(
    learning, pipeline, monkeypatch, after_commit
):
    original = pipeline.catalog.commit_dataset
    failures = 0

    def fail_once(*args, **kwargs):
        nonlocal failures
        if failures == 0:
            failures += 1
            if after_commit:
                original(*args, **kwargs)
            raise RuntimeError("Injected SQL commit response loss")
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline.catalog, "commit_dataset", fail_once)
    with pytest.raises(RuntimeError, match="Injected SQL"):
        planned(learning)
    pending = learning.tests(True)[0]
    assert pending["state"] == "pending_ledger"
    intent = pipeline.catalog.one(
        "SELECT * FROM commercial_publications WHERE test_id=%s", (pending["test_id"],)
    )
    object_keys = {
        row["Key"]
        for row in pipeline.history.client.list_objects_v2(Bucket=pipeline.history.bucket)[
            "Contents"
        ]
    }
    recovered = planned(learning)
    assert recovered["state"] == "ready" and recovered["test_id"] == pending["test_id"]
    assert recovered["planned_at"] == pending["planned_at"]
    assert (
        pipeline.catalog.one(
            "SELECT state FROM datasets WHERE dataset_id=%s", (intent["dataset_id"],)
        )["state"]
        == "committed"
    )
    assert {
        row["Key"]
        for row in pipeline.history.client.list_objects_v2(Bucket=pipeline.history.bucket)[
            "Contents"
        ]
    } == object_keys
    assert planned(learning) == recovered


@pytest.mark.integration
def test_currency_revision_overlap_and_superseded_publication_guards(learning, pipeline):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    end, cutoff = start + timedelta(days=1), start + timedelta(days=2)
    frozen = pipeline.freeze(
        start,
        True,
        {
            "target": "profit",
            "currency": "EUR",
            "horizon_days": [1],
            "launch_delay_days": [0],
            "profit_contract": test["profit_contract"],
        },
    )
    for changes in ({"country": "DE"}, {"currency": "USD"}):
        with pytest.raises(LearningError, match="immutable country currency"):
            learning.review_period(
                test["test_id"], period(start, end, **changes), 0, "wrong-scope", True, as_of=cutoff
            )
    first = learning.review_period(
        test["test_id"], period(start, end), 0, "first", True, as_of=cutoff
    )
    with pytest.raises(LearningError, match="overlapping period"):
        learning.review_period(
            test["test_id"],
            period(start + timedelta(hours=1), end),
            0,
            "overlap",
            True,
            as_of=cutoff + timedelta(hours=1),
        )
    with pytest.raises(LearningError, match="non monotonic"):
        learning.review_period(
            test["test_id"], period(start, end), 1, "backdated", True, as_of=cutoff
        )
    corrected = learning.review_period(
        test["test_id"],
        period(start, end, realized_revenue="-20"),
        1,
        "second",
        True,
        as_of=cutoff + timedelta(hours=1),
    )
    assert corrected["realized_profit"] == "-100.00"
    with pytest.raises(LearningError, match="superseded review"):
        learning.publish(test["test_id"], first["period_id"], 1, True)
    history = learning.periods(test["test_id"], True, include_history=True)
    assert history[1]["superseded"] is True and history[1]["publication_state"] == "superseded"
    assert (
        learning.publish(test["test_id"], corrected["period_id"], 2, True)["publication_state"]
        == "published"
    )
    outcome = pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(hours=1))["result"][
        "scenarios"
    ][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "failure" and outcome["lower"] == "-100.00"
    with pytest.raises(LearningError, match="test not found"):
        learning.publish(test["test_id"], corrected["period_id"], 2, False)


@pytest.mark.integration
def test_concurrent_replay_has_one_test_and_one_ledger_declaration(learning, pipeline):
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: planned(learning), range(2)))
    assert results[0] == results[1]
    assert len(learning.tests(True)) == 1
    assert (
        pipeline.catalog.one("SELECT count(*) AS count FROM commercial_publications")["count"] == 1
    )


@pytest.mark.integration
def test_optional_original_policy_references_are_validated_and_frozen(learning, pipeline):
    decision = now() + timedelta(minutes=1)
    frozen = pipeline.freeze(decision, True, {"horizon_days": [3], "launch_delay_days": [0]})
    selected = plan(forward_evaluation_id=frozen["evaluation_id"])
    test = learning.plan_test(
        "launch-p1-fr", 1, "from-frozen", True, plan=selected, as_of=decision + timedelta(minutes=1)
    )
    reference = test["policy_references"]["forward_decision"]
    assert reference["snapshot_id"] == frozen["result"]["snapshot_id"]
    assert reference["policy"] == frozen["result"]["policy"]
    assert reference["selected"] is True
    with pytest.raises(LearningError, match="evaluation reference unavailable"):
        learning.plan_test(
            "launch-p1-fr",
            1,
            "unknown-reference",
            True,
            plan=plan(forward_evaluation_id="unrecorded"),
            as_of=decision + timedelta(minutes=1),
        )
    with pytest.raises(LearningError, match="evaluation reference unavailable"):
        learning.plan_test("launch-p1-fr", 1, "future-reference", True, plan=selected)


@pytest.mark.integration
def test_actual_publication_recovery_retains_review_time_and_observation_identity(
    learning,
    pipeline,
    monkeypatch,
):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    cutoff = start + timedelta(days=2)
    reviewed = learning.review_period(
        test["test_id"], period(start, start + timedelta(days=1)), 0, "actual", True, as_of=cutoff
    )
    original = pipeline.catalog.commit_dataset

    def fail(*args, **kwargs):
        raise RuntimeError("Actual ledger SQL failure after object upload")

    monkeypatch.setattr(pipeline.catalog, "commit_dataset", fail)
    with pytest.raises(RuntimeError, match="SQL failure"):
        learning.publish(test["test_id"], reviewed["period_id"], 1, True)
    intent = pipeline.catalog.one(
        "SELECT observation FROM commercial_publications WHERE dataset_id=%s",
        (reviewed["dataset_id"],),
    )["observation"]
    monkeypatch.setattr(pipeline.catalog, "commit_dataset", original)
    restarted = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    recovered = restarted.publish(test["test_id"], reviewed["period_id"], 1, True)
    assert recovered["dataset_id"] == reviewed["dataset_id"]
    assert recovered["available_at"] == cutoff.isoformat()
    stored, _ = pipeline.history.observations(cutoff, True, entity_keys={test["ledger_key"]})
    actuals = [row for row in stored if row.attributes.get("realized")]
    assert len(actuals) == 1 and actuals[0].observation_id == intent["observation_id"]


@pytest.mark.integration
def test_concurrent_period_revisions_cannot_overwrite_each_other(learning):
    test = planned(learning)
    start = now() + timedelta(minutes=1)
    report = period(start, start + timedelta(days=1))

    def review(index):
        try:
            return learning.review_period(
                test["test_id"], report, 0, f"review-{index}", True, as_of=start + timedelta(days=2)
            )
        except LearningError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(review, range(2)))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert "period_revision_conflict" in results
    assert len(learning.periods(test["test_id"], True, include_history=True)) == 1


@pytest.mark.integration
def test_real_clock_cannot_be_supplied_and_namespace_never_changes(pipeline):
    pipeline.create_product("real", "Own product", synthetic=False)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(dossier_id="real-fr", product_id="real", synthetic=False), 0, "real-create")
    learning = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    learning.initialize()
    with pytest.raises(LearningError, match="database time"):
        learning.plan_test("real-fr", 1, "real", False, plan=plan(), as_of=now())
    before = now()
    test = learning.plan_test("real-fr", 1, "real", False, plan=plan())
    assert before <= utc(test["planned_at"]) <= now()
    assert test["synthetic"] is False
    assert learning.tests(True) == []
    with pytest.raises(LearningError, match="database time"):
        learning.review_period(
            test["test_id"],
            period(now(), now() + timedelta(days=1)),
            0,
            "backdate-real",
            False,
            as_of=now() + timedelta(days=2),
        )
