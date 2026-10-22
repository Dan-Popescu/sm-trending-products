"""Evaluation behavior at frozen-decision and outcome-report boundaries."""

import pytest

from sm_trending_products.contracts import (
    Capability,
    FamilyMembership,
    Measurement,
    Observation,
    Product,
    ProductLink,
    utc,
)
from sm_trending_products.evaluation import (
    EvaluationConfig,
    backtest,
    freeze_decision,
    mature_decision,
)


def observation(entity: str, timestamp: str, value: int, **changes: object) -> Observation:
    values = {
        "observation_id": f"{entity}-{timestamp}-{value}",
        "source": "fixture",
        "entity_type": "video",
        "entity_id": entity,
        "observed_at": utc(timestamp),
        "available_at": utc(timestamp),
        "published_at": utc("2026-01-01T00:00:00Z"),
        "synthetic": True,
        "metrics": {"view_count": Measurement(lower=value, upper=value)},
        "attributes": {"creator_id": entity},
    }
    values.update(changes)
    return Observation.model_validate(values)


def catalog(entity: str = "v1", product_id: str = "p1") -> tuple[Product, ProductLink]:
    created_at = utc("2026-01-01T00:00:00Z")
    return (
        Product(product_id=product_id, name=product_id, created_at=created_at, synthetic=True),
        ProductLink(
            product_id=product_id,
            entity_key=f"fixture:video:{entity}",
            available_at=created_at,
        ),
    )


def test_frozen_decision_excludes_late_observations_and_future_identity() -> None:
    first, first_link = catalog()
    second, second_link = catalog("v2", "p2")
    second_link = second_link.model_copy(update={"available_at": utc("2026-01-03T00:00:00Z")})
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v2", "2026-01-01T00:00:00Z", 1),
        observation("v2", "2026-01-02T00:00:00Z", 10000),
        observation("v1", "2026-01-02T00:00:00Z", 100000, available_at=utc("2026-01-04T00:00:00Z")),
    ]
    snapshot = freeze_decision(
        evidence,
        [first, second],
        [first_link, second_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(top_k=1, horizon_days=(3,), launch_delay_days=(0,)),
        synthetic=True,
    )
    assert [row["product_id"] for row in snapshot["universe"]] == ["p1"]
    assert evidence[-1].observation_id not in snapshot["evidence_refs"]
    assert snapshot["baselines"]["popularity"] == ["p1"]


def test_forward_decision_waits_for_horizon_and_rejects_edits() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v1", "2026-01-04T00:00:00Z", 1500),
        observation("v1", "2026-01-05T00:00:00Z", 1700),
    ]
    snapshot = freeze_decision(
        evidence,
        [product],
        [link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(top_k=1, horizon_days=(3,), launch_delay_days=(0,), threshold=1000),
        synthetic=True,
    )
    pending = mature_decision(snapshot, evidence, utc("2026-01-04T00:00:00Z"))
    assert pending["scenarios"][0]["status"] == "pending"
    report = mature_decision(snapshot, evidence, utc("2026-01-05T00:00:00Z"))
    result = report["scenarios"][0]["methods"]["policy"]
    assert result["precision_at_k"] == 1
    assert result["outcomes"][0]["lower"] == 1500
    assert result["outcomes"][0]["lead_time_hours"] == 48
    edited = {**snapshot, "selected": []}
    with pytest.raises(ValueError, match="integrity"):
        mature_decision(edited, evidence, utc("2026-01-05T00:00:00Z"))


def test_family_grouping_uses_only_membership_known_at_decision() -> None:
    first, first_link = catalog()
    second, second_link = catalog("v2", "p2")
    evidence = [
        observation(entity, timestamp, value)
        for entity in ("v1", "v2")
        for timestamp, value in (("2026-01-01T00:00:00Z", 100), ("2026-01-02T00:00:00Z", 200))
    ]
    families = [
        FamilyMembership(
            product_id=product_id, family_id="family-a", available_at=utc("2026-01-01T00:00:00Z")
        )
        for product_id in ("p1", "p2")
    ]
    options = {"synthetic": True, "families": families}
    snapshot = freeze_decision(
        evidence,
        [first, second],
        [first_link, second_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(top_k=2),
        **options,
    )
    assert len(snapshot["selected"]) == 1
    assert all(len(ids) == 1 for ids in snapshot["baselines"].values())
    later_families = [
        row.model_copy(update={"available_at": utc("2026-01-03T00:00:00Z")}) for row in families
    ]
    earlier = freeze_decision(
        evidence,
        [first, second],
        [first_link, second_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(top_k=2),
        synthetic=True,
        families=later_families,
    )
    assert len(earlier["selected"]) == 2


def test_bounded_sales_remain_ambiguous_and_counter_reset_is_censored() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    offer_link = ProductLink(
        product_id="p1", entity_key="fixture:offer:o1", available_at=utc("2026-01-01T00:00:00Z")
    )
    for timestamp, lower, upper in [
        ("2026-01-01T00:00:00Z", 100, 100),
        ("2026-01-02T00:00:00Z", 150, 160),
        ("2026-01-05T00:00:00Z", 170, 210),
    ]:
        evidence.append(
            observation(
                "o1",
                timestamp,
                0,
                entity_type="offer",
                metrics={"units_sold": Measurement(lower=lower, upper=upper, precision="bounded")},
                attributes={"sold_period": "lifetime"},
            )
        )
    snapshot = freeze_decision(
        evidence,
        [product],
        [link, offer_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(
            target="sales", threshold=30, top_k=1, horizon_days=(3,), launch_delay_days=(0,)
        ),
        synthetic=True,
    )
    report = mature_decision(snapshot, evidence, utc("2026-01-05T00:00:00Z"))
    result = report["scenarios"][0]["methods"]["policy"]
    assert result["outcomes"][0]["lower"] == 10
    assert result["outcomes"][0]["upper"] == 60
    assert result["outcomes"][0]["status"] == "ambiguous"
    assert result["bounded_count"] == 1
    assert result["precision_at_k"] is None
    assert "sales_popularity" not in snapshot["baselines"]
    reset = observation(
        "o1",
        "2026-01-04T00:00:00Z",
        0,
        entity_type="offer",
        metrics={"units_sold": Measurement(lower=0, upper=0)},
        attributes={"sold_period": "lifetime"},
    )
    corrected = mature_decision(snapshot, [*evidence, reset], utc("2026-01-05T00:00:00Z"))
    outcome = corrected["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "censored"
    assert outcome["entities"][0]["reason"] == "counter_correction_or_reset"


def test_realized_profit_requires_complete_actual_costs_and_currency() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    cost_link = ProductLink(
        product_id="p1", entity_key="fixture:cost:c1", available_at=utc("2026-01-01T00:00:00Z")
    )
    realized = observation(
        "c1",
        "2026-01-06T00:00:00Z",
        0,
        entity_type="cost",
        metrics={},
        attributes={
            "period_start": "2026-01-02T00:00:00Z",
            "period_end": "2026-01-05T00:00:00Z",
            "currency": "EUR",
            "realized": True,
            "costs_complete": True,
            "realized_revenue": "100.10",
            "realized_costs": {"goods": "40.10", "other": "20"},
        },
    )
    snapshot = freeze_decision(
        evidence,
        [product],
        [link, cost_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(
            target="profit", currency="EUR", threshold=30, horizon_days=(3,), launch_delay_days=(0,)
        ),
        synthetic=True,
    )
    report = mature_decision(snapshot, [*evidence, realized], utc("2026-01-06T00:00:00Z"))
    outcome = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert outcome["status"] == "success"
    assert outcome["lower"] == "40.00"
    incomplete = realized.model_copy(
        update={"attributes": {**realized.attributes, "costs_complete": False}}
    )
    censored = mature_decision(snapshot, [*evidence, incomplete], utc("2026-01-06T00:00:00Z"))
    assert censored["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"] == "censored"


def test_capability_granted_later_cannot_enable_historical_selection() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100, account_id="account-a"),
        observation("v1", "2026-01-02T00:00:00Z", 200, account_id="account-a"),
    ]
    capability = Capability(
        source="fixture",
        account_id="account-a",
        capability="collect",
        state="available",
        available_at=utc("2026-01-03T00:00:00Z"),
    )
    snapshot = freeze_decision(
        evidence,
        [product],
        [link],
        utc("2026-01-02T00:00:00Z"),
        synthetic=True,
        capabilities=[capability],
    )
    assert snapshot["status"] == "insufficient_history"
    assert snapshot["selected"] == []
    earlier = capability.model_copy(update={"available_at": utc("2026-01-01T00:00:00Z")})
    ready = freeze_decision(
        evidence,
        [product],
        [link],
        utc("2026-01-02T00:00:00Z"),
        synthetic=True,
        capabilities=[earlier],
    )
    assert ready["selected"] == ["p1"]


def test_backtest_is_reproducible_and_reports_unactionable_delays_and_missing_data() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    config = EvaluationConfig(top_k=1, horizon_days=(3,), launch_delay_days=(0, 10))
    first = backtest(
        evidence,
        [product],
        [link],
        [utc("2026-01-02T00:00:00Z")],
        utc("2026-01-06T00:00:00Z"),
        config,
        synthetic=True,
    )
    second = backtest(
        list(reversed(evidence)),
        [product],
        [link],
        [utc("2026-01-02T00:00:00Z")],
        utc("2026-01-06T00:00:00Z"),
        config,
        synthetic=True,
    )
    assert first["evaluation_id"] == second["evaluation_id"]
    scenarios = first["reports"][0]["scenarios"]
    assert scenarios[0]["methods"]["policy"]["outcomes"][0]["status"] == "censored"
    assert scenarios[1]["status"] == "not_actionable"
    assert scenarios[1]["methods"]["policy"]["precision_at_k"] is None
    assert first["methodology"]["fitting"] == "none"
    snapshot = first["snapshots"][0]
    pending = mature_decision(snapshot, evidence, utc("2026-01-03T00:00:00Z"))
    assert pending["status"] == "pending"


def test_late_interval_sales_are_measured_by_source_period_without_inventing_availability() -> None:
    product, link = catalog()
    offer_link = ProductLink(
        product_id="p1", entity_key="fixture:offer:o1", available_at=utc("2026-01-01T00:00:00Z")
    )
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    snapshot = freeze_decision(
        evidence,
        [product],
        [link, offer_link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(target="sales", threshold=35, horizon_days=(3,), launch_delay_days=(0,)),
        synthetic=True,
    )
    intervals = []
    for day, lower, upper in [(2, 10, 10), (3, 10, 20), (4, 10, 10)]:
        intervals.append(
            observation(
                "o1",
                "2026-01-06T00:00:00Z",
                0,
                observation_id=f"interval-{day}",
                entity_type="offer",
                metrics={
                    "units_sold": Measurement(
                        lower=lower, upper=upper, precision="bounded", semantics="interval"
                    )
                },
                attributes={
                    "period_start": f"2026-01-0{day}T00:00:00Z",
                    "period_end": f"2026-01-0{day + 1}T00:00:00Z",
                },
            )
        )
    waiting_for_publication = mature_decision(
        snapshot,
        [*evidence, *intervals],
        utc("2026-01-05T00:00:00Z"),
    )
    assert (
        waiting_for_publication["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"]
        == "censored"
    )
    report = mature_decision(snapshot, [*evidence, *intervals], utc("2026-01-06T00:00:00Z"))
    outcome = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert (outcome["lower"], outcome["upper"], outcome["status"]) == (30, 40, "ambiguous")
    gap = mature_decision(
        snapshot, [*evidence, intervals[0], intervals[2]], utc("2026-01-06T00:00:00Z")
    )
    assert gap["scenarios"][0]["methods"]["policy"]["outcomes"][0]["status"] == "censored"


def test_retention_invalidation_and_synthetic_isolation() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v1", "2026-01-05T00:00:00Z", 1700),
    ]
    config = EvaluationConfig(horizon_days=(3,), launch_delay_days=(0,))
    snapshot = freeze_decision(
        evidence, [product], [link], utc("2026-01-02T00:00:00Z"), config, synthetic=True
    )
    report = mature_decision(snapshot, evidence[1:], utc("2026-01-05T00:00:00Z"))
    assert report["status"] == "invalidated"
    assert report["missing_evidence_refs"] == [evidence[0].observation_id]
    live_product = product.model_copy(update={"synthetic": False})
    live = freeze_decision(
        evidence, [live_product], [link], utc("2026-01-02T00:00:00Z"), config, synthetic=False
    )
    assert live["selected"] == []
    assert live["evidence_refs"] == []


def test_source_capability_name_and_policy_availability_are_enforced() -> None:
    product, link = catalog()
    evidence = [
        observation(
            "v1",
            "2026-01-01T00:00:00Z",
            100,
            account_id="account-a",
            attributes={"required_capability": "video.list"},
        ),
        observation(
            "v1",
            "2026-01-02T00:00:00Z",
            200,
            account_id="account-a",
            attributes={"required_capability": "video.list"},
        ),
    ]
    wrong_scope = Capability(
        source="fixture",
        account_id="account-a",
        capability="user.info",
        state="available",
        available_at=utc("2026-01-01T00:00:00Z"),
    )
    snapshot = freeze_decision(
        evidence,
        [product],
        [link],
        utc("2026-01-02T00:00:00Z"),
        synthetic=True,
        capabilities=[wrong_scope],
    )
    assert snapshot["selected"] == []
    with pytest.raises(ValueError, match="Policy was not available"):
        freeze_decision(
            evidence,
            [product],
            [link],
            utc("2026-01-02T00:00:00Z"),
            EvaluationConfig(policy_available_at=utc("2026-01-03T00:00:00Z")),
            synthetic=True,
        )


def test_polling_gaps_bound_the_window_instead_of_reporting_exact_growth() -> None:
    product, link = catalog()
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
        observation("v1", "2026-01-03T12:00:00Z", 500),
        observation("v1", "2026-01-04T12:00:00Z", 1000),
        observation("v1", "2026-01-05T12:00:00Z", 1500),
    ]
    snapshot = freeze_decision(
        evidence,
        [product],
        [link],
        utc("2026-01-02T00:00:00Z"),
        EvaluationConfig(
            target="attention",
            threshold=600,
            horizon_days=(3,),
            launch_delay_days=(1,),
            max_boundary_gap_hours=24,
        ),
        synthetic=True,
    )
    report = mature_decision(snapshot, evidence, utc("2026-01-06T00:00:00Z"))
    outcome = report["scenarios"][0]["methods"]["policy"]["outcomes"][0]
    assert (outcome["lower"], outcome["upper"]) == (500, 1300)
    assert outcome["precision"] == "bounded"
    assert outcome["status"] == "ambiguous"


def test_rejecting_another_product_hypothesis_preserves_the_confirmed_owner() -> None:
    product, link = catalog()
    alternative, _ = catalog("v1", "alternative")
    rejection = ProductLink(
        product_id="alternative",
        entity_key=link.entity_key,
        available_at=utc("2026-01-02T00:00:00Z"),
        status="rejected",
    )
    evidence = [
        observation("v1", "2026-01-01T00:00:00Z", 100),
        observation("v1", "2026-01-02T00:00:00Z", 200),
    ]
    snapshot = freeze_decision(
        evidence,
        [product, alternative],
        [link, rejection],
        utc("2026-01-02T00:00:00Z"),
        synthetic=True,
    )
    assert snapshot["selected"] == ["p1"]


@pytest.mark.parametrize("multiple_scenarios", [False, True])
def test_precomputed_partitions_and_lazy_outcomes_preserve_evaluation_results(
    multiple_scenarios: bool,
) -> None:
    from collections import Counter, defaultdict

    from sm_trending_products.analytics import assess, normalize_assessments

    first, first_link = catalog()
    second, second_link = catalog("v2", "p2")
    products, links = [first, second], [first_link, second_link]
    cutoff = utc("2026-01-02T00:00:00Z")
    before = [
        observation(entity, timestamp, value * multiplier)
        for entity, multiplier in [("v1", 1), ("v2", 2)]
        for timestamp, value in [("2026-01-01T00:00:00Z", 100), ("2026-01-02T00:00:00Z", 200)]
    ]
    rows = []
    for product in products:
        rows.extend(assess(before, [product], links, cutoff, True, normalize=False))
    rows = normalize_assessments(rows)
    counts = Counter(row.entity_key for row in before)
    refs = defaultdict(list)
    for row in before:
        refs[row.entity_key].append(row.observation_id)
    config = (
        EvaluationConfig(top_k=1)
        if multiple_scenarios
        else EvaluationConfig(top_k=1, horizon_days=(3,), launch_delay_days=(0,))
    )
    raw = freeze_decision(before, products, links, cutoff, config, synthetic=True)
    prepared = freeze_decision(
        [],
        products,
        links,
        cutoff,
        config,
        synthetic=True,
        precomputed_assessments=rows,
        observation_counts=counts,
        evidence_refs_by_entity=refs,
    )
    assert prepared == raw
    observations = [
        *before,
        observation("v1", "2026-01-05T00:00:00Z", 1700),
        observation("v2", "2026-01-05T00:00:00Z", 2000),
    ]
    as_of = utc("2026-01-16T00:00:00Z" if multiple_scenarios else "2026-01-05T00:00:00Z")
    loaded = []

    def load_product(product_id):
        loaded.append(product_id)
        return [
            row
            for row in observations
            if prepared["entity_bindings"].get(row.entity_key) == product_id
        ]

    lazy = mature_decision(
        prepared,
        [],
        as_of,
        product_evidence_loader=load_product,
        verified_evidence_refs=prepared["evidence_refs"],
    )
    assert lazy == mature_decision(raw, observations, as_of)
    assert len(loaded) == len(set(loaded))
    assert set(loaded) == set(prepared["selected"]) | {
        product for selected in prepared["baselines"].values() for product in selected
    }


def test_capability_scope_respects_latest_denials_required_grants_and_unscoped_sources():
    from sm_trending_products.evaluation import eligible_evidence

    cutoff = utc("2026-01-02T00:00:00Z")
    rows = [
        observation("allowed", cutoff.isoformat(), 100, account_id="allowed"),
        observation("denied", cutoff.isoformat(), 100, account_id="denied"),
        observation(
            "wrong-grant",
            cutoff.isoformat(),
            100,
            account_id="allowed",
            attributes={"required_capability": "discovery"},
        ),
        *[
            observation(
                f"malformed-{index}",
                cutoff.isoformat(),
                100,
                account_id="allowed",
                attributes={"required_capability": value},
            )
            for index, value in enumerate((["collect"], {"capability": "collect"}, 1))
        ],
        observation("future", cutoff.isoformat(), 100, source="future", account_id="allowed"),
        observation("unscoped", cutoff.isoformat(), 100, source="unscoped"),
    ]
    grants = [
        Capability(
            source="fixture",
            account_id=account,
            capability="collect",
            state=state,
            available_at=utc(timestamp),
        )
        for account, state, timestamp in [
            ("allowed", "available", "2026-01-01T00:00:00Z"),
            ("denied", "available", "2026-01-01T00:00:00Z"),
            ("denied", "denied", "2026-01-02T00:00:00Z"),
            ("allowed", "denied", "2026-01-03T00:00:00Z"),
        ]
    ]
    grants.append(
        Capability(
            source="future",
            account_id="allowed",
            capability="collect",
            state="available",
            available_at=utc("2026-01-03T00:00:00Z"),
        )
    )
    assert [
        row.entity_id for row in eligible_evidence(rows, cutoff, True, capabilities=grants)
    ] == [
        "allowed",
        "unscoped",
    ]
