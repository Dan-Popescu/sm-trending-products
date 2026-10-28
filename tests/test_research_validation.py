"""Real PostgreSQL/history enforcement, not relabeled free-form evaluations."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from analytical_results import backtest_reports, backtest_snapshots, record_values
from pydantic import ValidationError
from test_evaluation_integration import record

from sm_trending_products.contracts import AssessmentPolicy, now, utc
from sm_trending_products.evaluation import EvaluationConfig
from sm_trending_products.research_validation import (
    ResearchError,
    ResearchValidation,
    ValidationPartition,
    ValidationProtocol,
)


def protocol_at(base, **changes):
    values = {
        "name": "research-test",
        "config": EvaluationConfig(
            top_k=1,
            horizon_days=(1,),
            launch_delay_days=(0,),
            max_boundary_gap_hours=1,
            threshold=100,
        ),
        "policies": {"attention": AssessmentPolicy()},
        "partitions": {
            "development": ValidationPartition(
                product_ids=("dev", "empty", "thin"), decision_times=(base,)
            ),
            "test": ValidationPartition(
                product_ids=("test",), decision_times=(base + timedelta(days=2),)
            ),
            "holdout": ValidationPartition(
                product_ids=("hold",), decision_times=(base + timedelta(days=4),)
            ),
        },
        **changes,
    }
    return ValidationProtocol(**values)


def seed(pipeline, *, outcomes=True, **changes):
    service = ResearchValidation(pipeline.catalog, pipeline.history, pipeline.identity)
    service.initialize()
    base = now() + timedelta(hours=1)
    datasets = []
    for product, day in (("dev", 0), ("test", 2), ("hold", 4), ("outside", 0)):
        pipeline.create_product(product, product, synthetic=True)
        decision = base + timedelta(days=day)
        multiplier = 10000 if product == "outside" else 1
        rows = [
            record(product, decision - timedelta(hours=1), 100 * multiplier),
            record(product, decision, 200 * multiplier),
        ]
        if outcomes:
            rows.append(record(product, decision + timedelta(days=1), 500 * multiplier))
        datasets.extend(pipeline.import_records(rows, synthetic=True))
        pipeline.review_link(product, f"fixture_import:video:{product}")
    for product in ("empty", "thin"):
        pipeline.create_product(product, product, synthetic=True)
    pipeline.import_records([record("thin", base, 5)], synthetic=True)
    pipeline.review_link("thin", "fixture_import:video:thin")
    protocol = protocol_at(base, **changes)
    registration = service.register(protocol, "register", synthetic=True, as_of=now())
    return service, protocol, registration["protocol_id"], base, datasets


def test_protocol_rejects_overlapping_horizons_and_product_leakage():
    base = now()
    payload = protocol_at(base).model_dump(mode="json")
    payload["partitions"]["test"]["decision_times"] = [base + timedelta(hours=24)]
    with pytest.raises(ValidationError, match="must not overlap"):
        ValidationProtocol.model_validate(payload)
    payload = protocol_at(base).model_dump(mode="json")
    payload["partitions"]["holdout"]["product_ids"] = ["dev"]
    with pytest.raises(ValidationError, match="isolated"):
        ValidationProtocol.model_validate(payload)
    payload = protocol_at(base).model_dump(mode="json")
    payload["partitions"]["test"]["decision_times"] = [base - timedelta(days=1)]
    with pytest.raises(ValidationError, match="strictly chronological"):
        ValidationProtocol.model_validate(payload)


def test_protocol_caps_policies_scenarios_dates_and_profit_sampling():
    base = now()
    with pytest.raises(ValidationError):
        protocol_at(base, policies={str(index): AssessmentPolicy() for index in range(9)})
    with pytest.raises(ValidationError, match="budget exceeded"):
        protocol_at(base, config=EvaluationConfig(horizon_days=(367,)))
    with pytest.raises(ValidationError):
        protocol_at(base, measurement_sample_size=26)
    with pytest.raises(ValidationError, match="attention target"):
        protocol_at(base, config=EvaluationConfig(target="sales"), measurement_sample_size=1)
    with pytest.raises(ValidationError, match="previous protocol"):
        protocol_at(base, revision=2)


@pytest.mark.integration
def test_scoped_execution_excludes_future_data_and_records_pre_universe_funnel(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    pipeline.import_records(
        [record("dev", base, 999999, available_at=base + timedelta(seconds=1))], synthetic=True
    )
    attempt = service.run(
        identifier,
        "development",
        "attention",
        "dev",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert attempt["state"] == "completed", attempt
    snapshot = backtest_snapshots(pipeline, attempt["result"])[0]
    assert snapshot["selected"] == ["dev"]
    assert snapshot["universe"][0]["features"]["view_count"] == 200
    assert set(snapshot["entity_bindings"].values()) == {"dev"}
    audit = snapshot["selection_audit"]
    assert audit["counts"] == {
        "discovered": 3,
        "linked": 2,
        "eligible": 1,
        "ranked": 1,
        "selected": 1,
        "reviewed": None,
        "planned_test": None,
        "tested": None,
    }
    assert audit["reason_counts"]["insufficient_distinct_history"] == 1
    assert audit["reason_counts"]["no_eligible_reviewed_links"] == 1
    assert {row["product_id"] for row in audit["products"]} == {"dev", "empty", "thin"}
    assert all(row["future_outcome"] == "unknown_until_measured" for row in audit["products"])


@pytest.mark.integration
def test_holdout_requires_prior_lock_and_preserves_failed_attempts(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    denied = service.run(
        identifier,
        "test",
        "attention",
        "unlocked",
        synthetic=True,
        as_of=base + timedelta(days=3),
    )
    assert denied["state"] == "failed"
    assert denied["error_code"] == "heldout_requires_prior_selected_policy_lock"
    locked = service.lock(identifier, "attention", "lock", True, as_of=base + timedelta(days=1))
    assert locked["heldout_timing"]["test"] == "prospective_predeclared"
    assert (
        service.lock(identifier, "attention", "lock", True, as_of=base + timedelta(days=1))
        == locked
    )
    with pytest.raises(ResearchError, match="already_locked"):
        service.lock(identifier, "attention", "another-lock", True)
    result = service.run(
        identifier,
        "test",
        "attention",
        "test",
        synthetic=True,
        as_of=base + timedelta(days=3),
    )
    assert result["state"] == "completed", result
    assert backtest_snapshots(pipeline, result["result"])[0]["selected"] == ["test"]
    assert [row["state"] for row in service.attempts(identifier, True)] == ["failed", "completed"]


@pytest.mark.integration
def test_replay_and_concurrent_attempt_reservation_cannot_duplicate_execution(
    pipeline, monkeypatch
):
    service, _, identifier, base, _ = seed(pipeline)
    started, release = Event(), Event()
    original = service.evaluations.scoped_backtest
    calls = []

    def blocked(*args, **kwargs):
        calls.append(1)
        started.set()
        assert release.wait(15)
        return original(*args, **kwargs)

    monkeypatch.setattr(service.evaluations, "scoped_backtest", blocked)
    args = (identifier, "development", "attention", "same-request")
    kwargs = {"synthetic": True, "as_of": base + timedelta(days=1)}
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.run, *args, **kwargs)
        assert started.wait(15)
        replay = service.run(*args, **kwargs)
        assert replay["state"] == "running"
        release.set()
        complete = first.result(timeout=30)
    assert complete["state"] == "completed", complete
    assert service.run(*args, **kwargs) == complete
    assert calls == [1]
    assert len(service.attempts(identifier, True)) == 1
    with pytest.raises(ResearchError, match="request_conflict"):
        service.run(identifier, "test", "attention", "same-request", **kwargs)


@pytest.mark.integration
def test_family_transfers_after_registration_reject_cross_partition_execution(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    pipeline.set_family("dev", "common")
    pipeline.set_family("test", "common")
    # Even a later transfer away must not hide the known historical overlap.
    pipeline.set_family("test", "new-family")
    attempt = service.run(
        identifier,
        "development",
        "attention",
        "leaking",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert attempt["state"] == "failed"
    assert attempt["error_code"] == "family_or_entity_crosses_partitions"
    assert attempt["evaluation_id"] is None


@pytest.mark.integration
def test_family_ties_reject_and_unknown_families_are_not_pooled(pipeline):
    service, protocol, _, base, _ = seed(pipeline)
    timestamp = now()
    for family in ("one", "two"):
        pipeline.catalog.execute(
            "INSERT INTO family_memberships(product_id,family_id,available_at) VALUES(%s,%s,%s)",
            ("dev", family, timestamp),
        )
    with pytest.raises(ResearchError, match="ambiguous_family"):
        service.register(
            protocol.model_copy(update={"name": "tied"}), "tied", synthetic=True, as_of=base
        )


@pytest.mark.integration
def test_global_owner_transfer_outside_partition_never_revives_old_owner(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    pipeline.review_link("outside", "fixture_import:video:dev")
    result = service.run(
        identifier,
        "development",
        "attention",
        "transferred",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert result["state"] == "completed", result
    assert backtest_snapshots(pipeline, result["result"])[0]["selected"] == []


@pytest.mark.integration
def test_missing_outcomes_and_excluded_samples_remain_unknown(pipeline):
    service, _, identifier, base, _ = seed(pipeline, outcomes=False, measurement_sample_size=2)
    result = service.run(
        identifier,
        "development",
        "attention",
        "censored",
        synthetic=True,
        as_of=base + timedelta(days=1, hours=2),
    )
    assert result["state"] == "completed", result
    report = backtest_reports(pipeline, result["result"])[0]
    policy = report["scenarios"][0]["methods"]["policy"]
    assert policy["precision_at_k"] is None
    assert policy["observed_precision"] is None
    assert policy["precision_bounds"] == [0, 1]
    assert policy["status_counts"] == {"censored": 1}
    audit = backtest_snapshots(pipeline, result["result"])[0]["selection_audit"]
    sample = audit["measurement_sample"]
    assert set(sample["product_ids"]) == {"empty", "thin"}
    outcomes = sample["report"]["scenarios"][0]["methods"]["policy"]["outcomes"]
    assert all(row["status"] == "censored" for row in record_values(pipeline, outcomes))
    assert sample["measurement_only"] is True


@pytest.mark.integration
def test_retired_evidence_cannot_be_exposed_through_saved_attempt_or_replay(pipeline):
    service, _, identifier, base, datasets = seed(pipeline)
    args = (identifier, "development", "attention", "dev")
    kwargs = {"synthetic": True, "as_of": base + timedelta(days=1)}
    result = service.run(*args, **kwargs)
    assert result["state"] == "completed", result
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    replay = service.run(*args, **kwargs)
    assert "result" not in replay
    assert replay["result_unavailable"] == "evidence_invalidated"


@pytest.mark.integration
def test_registration_revalidates_bypassed_models_revisions_and_namespace(pipeline):
    service, protocol, identifier, base, _ = seed(pipeline)
    timestamp = utc(service.protocol(identifier, True)["registered_at"])
    replay = service.register(protocol, "register", synthetic=True, as_of=timestamp)
    assert replay["protocol_id"] == identifier
    with pytest.raises(ResearchError, match="request_conflict"):
        service.register(
            protocol, "register", synthetic=True, as_of=timestamp + timedelta(seconds=1)
        )
    revised = protocol.model_copy(update={"revision": 2, "previous_protocol_id": identifier})
    revision = service.register(revised, "revision", synthetic=True, as_of=base)
    assert revision["protocol"]["revision"] == 2
    assert service.protocol(identifier, True)["protocol"]["revision"] == 1


@pytest.mark.integration
def test_retrospective_development_cannot_use_outcome_corrections_from_test_period(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    pipeline.import_records(
        [
            record(
                "dev",
                base + timedelta(days=1),
                50000,
                available_at=base + timedelta(days=3),
            )
        ],
        synthetic=True,
    )
    result = service.run(
        identifier,
        "development",
        "attention",
        "late-outcomes",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert result["state"] == "completed", result.get("error_code")
    assert utc(result["evaluation_cutoff"]) < base + timedelta(days=2)
    outcome = backtest_reports(pipeline, result["result"])[0]["scenarios"][0]["methods"]["policy"]
    assert outcome["outcomes"][0]["lower"] == 300
    with pytest.raises(ResearchError, match="precedes_development_evidence"):
        service.lock(identifier, "attention", "backdated", True, as_of=base + timedelta(days=1))
    locked = service.lock(
        identifier, "attention", "historical-lock", True, as_of=base + timedelta(days=5)
    )
    assert locked["heldout_timing"]["test"] == "retrospective_governed"


@pytest.mark.integration
def test_registered_candidates_cannot_replace_locked_policy_or_revise_opened_holdout(pipeline):
    policies = {"attention": AssessmentPolicy(), "alternative": AssessmentPolicy(version="other")}
    service, protocol, identifier, base, _ = seed(pipeline, policies=policies)
    service.lock(identifier, "attention", "lock", True, as_of=base)
    denied = service.run(
        identifier,
        "holdout",
        "alternative",
        "alternative",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert denied["error_code"] == "heldout_requires_prior_selected_policy_lock"
    with pytest.raises(ResearchError, match="already_locked"):
        service.lock(identifier, "attention", "lock", True, as_of=base + timedelta(seconds=1))
    revision = protocol.model_copy(update={"revision": 2, "previous_protocol_id": identifier})
    with pytest.raises(ResearchError, match="opened_protocol_cannot_be_revised"):
        service.register(revision, "revise-after-opening", synthetic=True, as_of=base)


@pytest.mark.integration
def test_mutable_nested_scope_bypass_and_attempt_capacity(pipeline):
    service, protocol, identifier, base, _ = seed(pipeline, max_attempts=1)
    bypassed = protocol.model_copy(deep=True)
    bypassed.partitions["test"] = bypassed.partitions["development"]
    with pytest.raises(ValidationError, match="isolated"):
        service.register(bypassed, "bypass", synthetic=True)
    wrong = service.run(
        identifier, "development", "unregistered", "wrong", synthetic=True, as_of=base
    )
    assert wrong["state"] == "failed" and wrong["error_code"] == "policy_not_registered"
    with pytest.raises(ResearchError, match="capacity_exhausted"):
        service.run(identifier, "development", "attention", "overflow", synthetic=True, as_of=base)
    with pytest.raises(ResearchError, match="protocol_not_found"):
        service.protocol(identifier, False)
    with pytest.raises(ResearchError, match="real_clock"):
        service.register(protocol.model_copy(update={"name": "real"}), "real", as_of=base)


@pytest.mark.integration
def test_commercial_funnel_distinguishes_review_plans_and_actual_observations(pipeline):
    from test_commerce_learning import period, plan
    from test_launch_dossiers import draft

    from sm_trending_products.commerce import LaunchService
    from sm_trending_products.commerce_learning import CommercialLearning

    service, _, identifier, base, _ = seed(pipeline)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(dossier_id="audit-dev", product_id="dev"), 0, "create-audit")
    learning = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    learning.initialize()
    timestamp = now() + timedelta(minutes=1)
    learning.plan_test("audit-dev", 1, "watch", True, plan=plan(selection="watch"), as_of=timestamp)
    chosen = learning.plan_test("audit-dev", 1, "test", True, plan=plan(), as_of=timestamp)
    learning.review_period(
        chosen["test_id"],
        period(timestamp, timestamp + timedelta(minutes=1)),
        0,
        "review-actual",
        True,
        as_of=timestamp + timedelta(minutes=2),
    )
    result = service.run(
        identifier,
        "development",
        "attention",
        "audit",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert result["state"] == "completed", result.get("error_code")
    audit = backtest_snapshots(pipeline, result["result"])[0]["selection_audit"]
    assert audit["counts"]["reviewed"] == 1
    assert audit["counts"]["planned_test"] == 1
    assert audit["counts"]["tested"] == 1
    dev = next(row for row in audit["products"] if row["product_id"] == "dev")
    assert dev["stages"]["tested"] is True


@pytest.mark.integration
def test_optional_reads_never_initialize_and_freeform_replay_stays_unlocked(pipeline):
    service = ResearchValidation(pipeline.catalog, pipeline.history, pipeline.identity)
    with pytest.raises(ValueError, match="not initialized"):
        service.protocol("unknown", True)
    assert (
        pipeline.catalog.one("SELECT to_regclass('research_protocols') AS value")["value"] is None
    )
    result = pipeline.backtest([], now(), synthetic=True)["result"]
    assert result["kind"] == "backtest"
    assert "protocol_id" not in result


@pytest.mark.integration
def test_retrospective_audit_retains_declared_ids_without_future_product_facts(pipeline):
    service = ResearchValidation(pipeline.catalog, pipeline.history, pipeline.identity)
    service.initialize()
    base = now() - timedelta(days=10)
    for product in ("dev", "empty", "thin", "test", "hold"):
        pipeline.create_product(product, "Future name must not enter the past", synthetic=True)
    identifier = service.register(protocol_at(base), "retrospective", synthetic=True)["protocol_id"]
    result = service.run(identifier, "development", "attention", "past", synthetic=True)
    assert result["state"] == "completed", result.get("error_code")
    snapshot = backtest_snapshots(pipeline, result["result"])[0]
    audit = snapshot["selection_audit"]
    assert audit["declared_product_count"] == 3
    assert audit["counts"]["discovered"] == 0
    assert audit["reason_counts"] == {"not_discovered_at_cutoff": 3}
    assert {row["product_id"] for row in audit["products"]} == {"dev", "empty", "thin"}
    assert all(not row["stages"]["ranked"] for row in audit["products"])
    assert snapshot["universe"] == [] and snapshot["entity_bindings"] == {}
    assert "Future name" not in str(snapshot)


@pytest.mark.integration
def test_future_only_source_control_never_grants_ranking_or_excluded_measurements(pipeline):
    service, _, identifier, base, _ = seed(pipeline, measurement_sample_size=3)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('fixture_import','','collect','available',%s,'{}')",
        (base + timedelta(days=10),),
    )
    result = service.run(
        identifier,
        "development",
        "attention",
        "future-control",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    assert result["state"] == "completed", result.get("error_code")
    snapshot = backtest_snapshots(pipeline, result["result"])[0]
    assert snapshot["selected"] == []
    assert snapshot["controlled_sources"] == ["fixture_import"]
    assert all(row["source"] != "fixture_import" for row in snapshot["capabilities"])
    assert snapshot["selection_audit"]["counts"]["eligible"] == 0
    sample = snapshot["selection_audit"]["measurement_sample"]
    assert set(sample["product_ids"]) == {"dev", "empty", "thin"}
    outcomes = sample["report"]["scenarios"][0]["methods"]["policy"]["outcomes"]
    assert {row["status"] for row in record_values(pipeline, outcomes)} == {"censored"}


@pytest.mark.integration
def test_any_opened_ancestor_freezes_preexisting_descendants_and_future_revisions(pipeline):
    policies = {"attention": AssessmentPolicy(), "alternative": AssessmentPolicy(version="other")}
    service, protocol, first_id, base, _ = seed(pipeline, policies=policies)
    second = protocol.model_copy(update={"revision": 2, "previous_protocol_id": first_id})
    second_id = service.register(second, "pre-open-revision", synthetic=True)["protocol_id"]
    service.lock(first_id, "attention", "first-lock", True, as_of=base)
    service.lock(second_id, "alternative", "second-lock", True, as_of=base)
    opened = service.run(
        first_id,
        "holdout",
        "attention",
        "open",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert opened["state"] == "completed", opened.get("error_code")
    third = second.model_copy(update={"revision": 3, "previous_protocol_id": second_id})
    with pytest.raises(ResearchError, match="opened_protocol_cannot_be_revised"):
        service.register(third, "late-fork", synthetic=True)
    blocked = service.run(
        second_id,
        "holdout",
        "alternative",
        "reopen",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert blocked["state"] == "failed"
    assert blocked["error_code"] == "lineage_heldout_already_opened"
    assert blocked["evaluation_id"] is None
    assert (
        service.run(
            second_id,
            "holdout",
            "alternative",
            "reopen",
            synthetic=True,
            as_of=base + timedelta(days=5),
        )
        == blocked
    )
    assert len(service.attempts(second_id, True)) == 1


@pytest.mark.integration
def test_concurrent_cross_revision_holdouts_admit_only_one_lineage_owner(pipeline):
    policies = {"attention": AssessmentPolicy(), "alternative": AssessmentPolicy(version="other")}
    service, protocol, first_id, base, _ = seed(pipeline, policies=policies)
    second = protocol.model_copy(update={"revision": 2, "previous_protocol_id": first_id})
    second_id = service.register(second, "pre-open-revision", synthetic=True)["protocol_id"]
    service.lock(first_id, "attention", "first-lock", True, as_of=base)
    service.lock(second_id, "alternative", "second-lock", True, as_of=base)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                service.run,
                identifier,
                "holdout",
                policy,
                "open",
                synthetic=True,
                as_of=base + timedelta(days=5),
            )
            for identifier, policy in ((first_id, "attention"), (second_id, "alternative"))
        ]
        results = [future.result(timeout=30) for future in futures]
    assert sorted(row["state"] for row in results) == ["completed", "failed"], results
    failed = next(row for row in results if row["state"] == "failed")
    assert failed["error_code"] == "lineage_heldout_already_opened"
    assert (
        pipeline.catalog.one("SELECT count(*) AS value FROM research_lineage_freezes")["value"] == 1
    )


@pytest.mark.integration
def test_optional_upgrade_backfills_existing_lineage_openings_without_rewriting_runs(pipeline):
    service, _, identifier, base, _ = seed(pipeline)
    service.lock(identifier, "attention", "lock", True, as_of=base)
    result = service.run(
        identifier,
        "holdout",
        "attention",
        "open",
        synthetic=True,
        as_of=base + timedelta(days=5),
    )
    assert result["state"] == "completed", result.get("error_code")
    # Simulate the exact version-one application tables in an isolated fixture.
    pipeline.catalog.execute("DROP TABLE research_lineage_freezes")
    pipeline.catalog.execute(
        "DELETE FROM application_module_migrations WHERE module='research_validation' AND version=2"
    )
    with pytest.raises(ValueError, match="not initialized"):
        service.protocol(identifier, True)
    assert service.initialize()["version"] == 2
    freeze = pipeline.catalog.one("SELECT * FROM research_lineage_freezes")
    assert freeze["protocol_id"] == identifier and freeze["policy_id"] == "attention"
    assert service.attempt(result["attempt_id"], True) == result
