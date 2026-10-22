"""Point-in-time policy comparisons and immutable prospective decisions.

Outcomes measure changes on the entities linked at the decision time. They do not
retroactively attribute newly discovered creatives to old decisions. Missing
boundary observations are censored rather than interpolated from current totals.
"""

from __future__ import annotations

import math
import random
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, DecimalException, InvalidOperation
from typing import Any, Literal

from pydantic import Field, model_validator

from .contracts import (
    AssessmentPolicy,
    Capability,
    Contract,
    FamilyMembership,
    Observation,
    Product,
    ProductLink,
    digest,
    utc,
)
from .identity_rules import resolve_links
from .outcome_timeline import EntityTimeline, _period


class ProfitContract(Contract):
    """Predeclared financial ledgers and their required actual-cost components."""

    version: Literal["realized-profit-v1"] = "realized-profit-v1"
    ledgers: dict[str, tuple[str, ...]] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_ledgers(self) -> ProfitContract:
        for key, components in self.ledgers.items():
            parts = key.split(":", 2)
            if len(parts) != 3 or not parts[0] or parts[1] not in {"cost", "offer"} or not parts[2]:
                raise ValueError("Profit ledgers require a source:cost|offer:entity key")
            if (
                not components
                or len(set(components)) != len(components)
                or any(not component or component != component.strip() for component in components)
            ):
                raise ValueError("Profit ledgers require nonempty, unique cost component names")
        return self


class EvaluationConfig(Contract):
    """A predeclared outcome contract, serialized into each decision snapshot."""

    version: str = "evaluation-v1"
    target: Literal["attention", "sales", "profit"] = "attention"
    threshold: float = 1000.0
    top_k: int = Field(default=10, ge=1, le=10000)
    horizon_days: tuple[int, ...] = (3, 7, 14)
    launch_delay_days: tuple[int, ...] = (2, 10)
    market: str | None = None
    currency: str | None = None
    seed: int = 42
    max_boundary_gap_hours: float = Field(default=24, gt=0)
    min_link_confidence: float = Field(default=0.9, ge=0, le=1)
    minimum_history_observations: int = Field(default=2, ge=2)
    minimum_decision_time: datetime | None = None
    policy_available_at: datetime | None = None
    split: Literal["validation", "test", "forward"] = "test"
    missing_outcomes: Literal["censor"] = "censor"
    outcome_entity_scope: Literal["frozen_links"] = "frozen_links"
    horizon_anchor: Literal["decision"] = "decision"
    profit_contract: ProfitContract | None = None

    @model_validator(mode="after")
    def validate_configuration(self) -> EvaluationConfig:
        if not self.horizon_days or any(day <= 0 for day in self.horizon_days):
            raise ValueError("Outcome horizons must be positive days")
        if not self.launch_delay_days or any(day < 0 for day in self.launch_delay_days):
            raise ValueError("Launch delays cannot be negative")
        if len(set(self.horizon_days)) != len(self.horizon_days):
            raise ValueError("Outcome horizons must be unique")
        if len(set(self.launch_delay_days)) != len(self.launch_delay_days):
            raise ValueError("Launch delays must be unique")
        if not math.isfinite(self.threshold):
            raise ValueError("Outcome threshold must be finite")
        if self.target == "profit" and not self.currency:
            raise ValueError("Profit evaluation requires an explicit currency")
        for value in (self.minimum_decision_time, self.policy_available_at):
            if value is not None:
                utc(value)
        return self

    def validate_creation(self) -> None:
        """Require explicit new financial definitions without rejecting old snapshots."""
        if self.target == "profit" and self.profit_contract is None:
            raise ValueError("New profit decisions require a declared profit_contract")
        if self.target != "profit" and self.profit_contract is not None:
            raise ValueError("A profit_contract requires the profit target")

    def validate_ledger_scope(self, links: Sequence[ProductLink]) -> None:
        """Validate against already resolved, workspace/market-scoped reviewed links."""
        if self.profit_contract is not None:
            unknown = set(self.profit_contract.ledgers).difference(
                link.entity_key for link in links
            )
            if unknown:
                raise ValueError(
                    "Profit ledger keys must have reviewed links in the decision scope: "
                    + ", ".join(sorted(unknown))
                )


def outcome_entity_keys(bindings: Mapping[str, str], config: EvaluationConfig) -> set[str]:
    """Scope declared ledger reads while preserving legacy outcome/lead-time context."""
    if config.target == "profit" and config.profit_contract is not None:
        return set(bindings).intersection(config.profit_contract.ledgers)
    return set(bindings)


def _entity_timeline(
    key: str, rows: Sequence[Observation], config: EvaluationConfig
) -> EntityTimeline:
    return EntityTimeline(
        rows,
        config.target,
        config.currency,
        required_cost_components=(
            config.profit_contract.ledgers[key]
            if config.target == "profit" and config.profit_contract is not None
            else None
        ),
    )


def _eligible_observations(
    observations: Sequence[Observation],
    cutoff: datetime,
    synthetic: bool,
    market: str | None,
    *,
    since: datetime | None = None,
) -> list[Observation]:
    result: dict[str, Observation] = {}
    for row in observations:
        if row.synthetic != synthetic or row.observed_at > cutoff or row.available_at > cutoff:
            continue
        if since is not None and row.observed_at < since:
            continue
        if market is not None and row.market != market:
            continue
        previous = result.get(row.observation_id)
        if previous is not None and previous != row:
            raise ValueError("Conflicting observations share an observation identifier")
        result[row.observation_id] = row
    return sorted(result.values(), key=lambda row: (row.observed_at, row.observation_id))


def _source_scope(
    rows: Sequence[Observation],
    capabilities: Sequence[Capability],
    cutoff: datetime,
) -> list[Observation]:
    if not capabilities:
        return list(rows)
    latest: dict[tuple[str, str, str], Capability] = {}
    scoped_sources = {capability.source for capability in capabilities}
    for capability in capabilities:
        if capability.available_at <= cutoff:
            key = (capability.source, capability.account_id, capability.capability)
            if key not in latest or latest[key].available_at < capability.available_at:
                latest[key] = capability
    grants = {key for key, capability in latest.items() if capability.state == "available"}
    accounts = {(source, account) for source, account, _ in grants}
    allowed = []
    for row in rows:
        if row.source not in scoped_sources:
            allowed.append(row)
            continue
        required = row.attributes.get("required_capability")
        account = (row.source, row.account_id or "")
        if (required is None and account in accounts) or (
            isinstance(required, str) and (*account, required) in grants
        ):
            allowed.append(row)
    return allowed


def eligible_evidence(
    observations: Sequence[Observation],
    cutoff: datetime,
    synthetic: bool,
    market: str | None = None,
    capabilities: Sequence[Capability] = (),
    *,
    since: datetime | None = None,
) -> list[Observation]:
    """Apply the same knowledge and capability gates to a bounded evidence partition."""
    return _source_scope(
        _eligible_observations(
            observations,
            utc(cutoff),
            synthetic,
            market,
            since=utc(since) if since is not None else None,
        ),
        capabilities,
        utc(cutoff),
    )


def reviewed_links(
    links: Sequence[ProductLink],
    cutoff: datetime,
    confidence: float,
) -> list[ProductLink]:
    """Resolve reviewed ownership using the shared temporal identity rules."""
    return list(resolve_links(links, cutoff, min_confidence=confidence).values())


def _family_map(
    families: Sequence[FamilyMembership],
    cutoff: datetime,
) -> dict[str, str]:
    memberships: dict[str, FamilyMembership] = {}
    for membership in sorted(families, key=lambda row: (row.available_at, row.family_id)):
        if membership.available_at <= cutoff:
            memberships[membership.product_id] = membership
    return {product_id: row.family_id for product_id, row in memberships.items()}


def _select_families(rows: Sequence[dict[str, Any]], top_k: int) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if row["family_id"] not in seen:
            selected.append(str(row["product_id"]))
            seen.add(str(row["family_id"]))
            if len(selected) >= top_k:
                break
    return selected


def _baseline_order(
    rows: Sequence[dict[str, Any]],
    feature: str,
) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            row["features"].get(feature) is None,
            -float(row["features"].get(feature) or 0),
            row["product_id"],
        ),
    )


def freeze_decision(
    observations: Sequence[Observation],
    products: Sequence[Product],
    links: Sequence[ProductLink],
    decision_time: datetime,
    config: EvaluationConfig | None = None,
    *,
    synthetic: bool = False,
    policy: AssessmentPolicy | None = None,
    families: Sequence[FamilyMembership] = (),
    capabilities: Sequence[Capability] = (),
    precomputed_assessments: Sequence[dict[str, Any]] | None = None,
    observation_counts: Mapping[str, int] | None = None,
    evidence_refs_by_entity: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Freeze policy and baseline selections using knowledge available at cutoff.

    The returned JSON value contains no reference to mutable caller-owned objects.
    Snapshot integrity is checked again before maturation.
    History adequacy counts distinct observation times per entity, while audit
    references retain all eligible revisions of those observations. Prepared
    observation counts must use the same distinct-time definition.
    """
    from .analytics import assess

    cutoff = utc(decision_time)
    config = config or EvaluationConfig()
    config.validate_creation()
    policy = policy or AssessmentPolicy()
    if config.minimum_decision_time and cutoff < config.minimum_decision_time:
        raise ValueError("Decision precedes the declared evaluation period")
    if config.policy_available_at and cutoff < config.policy_available_at:
        raise ValueError("Policy was not available at the decision time")
    eligible = eligible_evidence(
        observations,
        cutoff,
        synthetic,
        config.market,
        capabilities,
        since=cutoff - timedelta(days=policy.lookback_days),
    )
    scoped_products = [
        product
        for product in products
        if product.created_at <= cutoff
        and product.synthetic == synthetic
        and (config.market is None or product.market == config.market)
    ]
    product_ids = {product.product_id for product in scoped_products}
    known_links = [
        link
        for link in reviewed_links(
            links,
            cutoff,
            max(config.min_link_confidence, policy.min_link_confidence),
        )
        if link.product_id in product_ids
    ]
    entities = {link.entity_key for link in known_links}
    config.validate_ledger_scope(known_links)
    eligible = [row for row in eligible if row.entity_key in entities]
    prepared = precomputed_assessments is not None
    if prepared != (observation_counts is not None) or prepared != (
        evidence_refs_by_entity is not None
    ):
        raise ValueError("Precomputed assessments require counts and evidence references together")
    if prepared and observations:
        raise ValueError("Supply raw evidence or precomputed partitions, not both")
    counts = (
        observation_counts
        if prepared
        else Counter(
            entity_key
            for entity_key, _observed_at in {
                (row.entity_key, utc(row.observed_at)) for row in eligible
            }
        )
    )
    assert counts is not None
    adequate_entities = {
        key for key, count in counts.items() if count >= config.minimum_history_observations
    }
    candidates = {link.product_id for link in known_links if link.entity_key in adequate_entities}
    rows = (
        list(precomputed_assessments or ())
        if prepared
        else assess(
            eligible,
            scoped_products,
            known_links,
            cutoff,
            synthetic,
            policy,
        )
    )
    families_at_cutoff = _family_map(families, cutoff)
    universe = []
    for row in rows:
        if row["product_id"] not in candidates or row.get("score") is None:
            continue
        copied = dict(row)
        copied["family_id"] = families_at_cutoff.get(row["product_id"], row["product_id"])
        universe.append(copied)
    universe.sort(key=lambda row: (-float(row["score"]), row["product_id"]))
    population = {row["product_id"] for row in universe}
    bindings = {
        link.entity_key: link.product_id for link in known_links if link.product_id in population
    }
    selected = _select_families(universe, config.top_k)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in universe:
        grouped[row["family_id"]].append(row)
    randomizer = random.Random(digest({"seed": config.seed, "decision_time": cutoff}))
    random_groups = sorted(grouped)
    randomizer.shuffle(random_groups)
    random_selection = [
        randomizer.choice(sorted(grouped[family], key=lambda row: row["product_id"]))["product_id"]
        for family in random_groups[: config.top_k]
    ]
    baselines = {
        "popularity": _select_families(_baseline_order(universe, "view_count"), config.top_k),
        "growth": _select_families(_baseline_order(universe, "growth_per_hour"), config.top_k),
        "random": random_selection,
    }
    sales_rows = [row for row in universe if row["features"].get("units_sold") is not None]
    sales_periods = [row["features"].get("sold_counter_periods", {}) for row in universe]
    common_periods = {period for periods in sales_periods for period in periods.values()}
    exclusions = {
        row["features"].get("sales_baseline_exclusion_reason", "missing_exact_transaction_counts")
        for row in universe
    }
    sales_baseline_reason = (
        "incomplete_exact_transaction_coverage"
        if "incomplete_exact_transaction_coverage" in exclusions
        else "missing_exact_transaction_counts"
        if not sales_rows
        or len(sales_rows) != len(universe)
        or "missing_exact_transaction_counts" in exclusions
        else "incompatible_sales_counter_periods"
        if not all(sales_periods)
        or len(common_periods) != 1
        or None in common_periods
        or "incompatible_sales_counter_periods" in exclusions
        else None
    )
    if sales_baseline_reason is None:
        baselines["sales_popularity"] = _select_families(
            _baseline_order(sales_rows, "units_sold"),
            config.top_k,
        )
    evidence_refs = sorted(
        {
            reference
            for key in bindings
            for reference in (evidence_refs_by_entity or {}).get(key, ())
        }
        if prepared
        else {row.observation_id for row in eligible if row.entity_key in bindings}
    )
    snapshot: dict[str, Any] = {
        "schema_version": "1",
        "decision_time": cutoff.isoformat(),
        "synthetic": synthetic,
        "config": config.model_dump(mode="json"),
        "policy": policy.model_dump(mode="json"),
        "universe": universe,
        "selected": selected,
        "baselines": baselines,
        "baseline_eligibility": {
            "sales_popularity": {
                "status": "included" if sales_baseline_reason is None else "excluded",
                "reason": sales_baseline_reason,
            }
        },
        "entity_bindings": bindings,
        "evidence_refs": evidence_refs,
        "identity_decisions": [
            link.model_dump(mode="json") for link in known_links if link.entity_key in bindings
        ],
        "family_memberships": {
            key: value for key, value in families_at_cutoff.items() if key in population
        },
        "capabilities": [
            capability.model_dump(mode="json")
            for capability in capabilities
            if capability.available_at <= cutoff
        ],
        "status": "frozen" if universe else "insufficient_history",
        "coverage": {
            "eligible_products": len(population),
            "known_products": len(product_ids),
            "family_count": len(grouped),
            "selected_products": len(selected),
            "observed_entities": len(bindings),
            "observations": len(evidence_refs),
        },
        "methodology": {
            "outcome_entity_scope": "frozen_links",
            "horizon_anchor": "decision",
            "fitting": "none",
            "policy_availability": "verified" if config.policy_available_at else "not_supplied",
            "capability_history": "provided" if capabilities else "not_supplied",
            "interpretation": "Observable target comparison; no causal or profit claim from views",
        },
    }
    # JSON serialization breaks mutable references and makes persisted hashing portable.
    import json

    from .contracts import canonical

    snapshot = json.loads(canonical(snapshot))
    snapshot["snapshot_id"] = digest(snapshot)
    return snapshot


def _product_outcome(
    product_id: str,
    evidence: dict[str, list[Observation]],
    bindings: dict[str, str],
    start: datetime,
    end: datetime,
    config: EvaluationConfig,
    *,
    timelines: Mapping[str, EntityTimeline] | None = None,
    include_references: bool = True,
) -> dict[str, Any]:
    kinds = (
        {"video", "commercial_content"}
        if config.target == "attention"
        else ({"offer"} if config.target == "sales" else {"cost", "offer"})
    )
    keys = outcome_entity_keys(
        {key: owner for key, owner in bindings.items() if owner == product_id}, config
    )
    results = []
    for key in sorted(keys):
        if len(key.split(":", 2)) != 3 or key.split(":", 2)[1] not in kinds:
            continue
        timeline = (
            timelines[key]
            if timelines is not None
            else _entity_timeline(key, evidence.get(key, []), config)
        )
        result = timeline.outcome(
            start,
            end,
            timedelta(hours=config.max_boundary_gap_hours),
            include_references=include_references,
        )
        results.append({"entity_key": key, **result})
    if not results or any(result["status"] != "measured" for result in results):
        return {
            "product_id": product_id,
            "status": "censored",
            "entities": results,
            "reason": "no_supported_entities" if not results else "incomplete_entity_outcomes",
        }
    units = {result["unit"] for result in results}
    if len(units) != 1:
        return {
            "product_id": product_id,
            "status": "censored",
            "entities": results,
            "reason": "incompatible_outcome_units",
        }
    strict_financial = config.target == "profit" and config.profit_contract is not None
    try:
        lower = sum((Decimal(str(result["lower"])) for result in results), Decimal(0))
        upper = sum((Decimal(str(result["upper"])) for result in results), Decimal(0))
        if strict_financial and (not lower.is_finite() or not upper.is_finite()):
            raise InvalidOperation("Nonfinite product financial result")
    except DecimalException:
        if not strict_financial:
            raise
        return {
            "product_id": product_id,
            "status": "censored",
            "entities": results,
            "reason": "invalid_realized_product_amount",
        }
    threshold = Decimal(str(config.threshold))
    precisions = {result["precision"] for result in results}
    precision = (
        "estimated"
        if "estimated" in precisions
        else ("bounded" if "bounded" in precisions else "exact")
    )
    status = "success" if lower >= threshold else ("failure" if upper < threshold else "ambiguous")
    if precision == "estimated":
        status = "estimated"
    return {
        "product_id": product_id,
        "status": status,
        "lower": str(lower) if config.target == "profit" else int(lower),
        "upper": str(upper) if config.target == "profit" else int(upper),
        "precision": precision,
        "unit": next(iter(units)),
        "entities": results,
        "evidence_refs": sorted(
            {reference for result in results for reference in result.get("evidence_refs", [])}
        ),
    }


def _wilson(successes: int, count: int) -> list[float] | None:
    if count == 0:
        return None
    proportion, z = successes / count, 1.959963984540054
    denominator = 1 + z * z / count
    center = (proportion + z * z / (2 * count)) / denominator
    margin = z * math.sqrt(proportion * (1 - proportion) / count + z * z / (4 * count * count))
    return [max(0, center - margin / denominator), min(1, center + margin / denominator)]


def _lead_time(
    product_id: str,
    evidence: dict[str, list[Observation]],
    bindings: dict[str, str],
    start: datetime,
    end: datetime,
    decision_time: datetime,
    config: EvaluationConfig,
    *,
    timelines: Mapping[str, EntityTimeline] | None = None,
) -> dict[str, Any]:
    if timelines is None:
        timelines = {
            key: _entity_timeline(key, evidence.get(key, []), config)
            for key in outcome_entity_keys(bindings, config)
            if bindings[key] == product_id
        }
    rows = [
        row
        for key, values in evidence.items()
        if key in timelines and bindings.get(key) == product_id
        for row in values
    ]
    boundaries = set()
    for row in rows:
        period = _period(row)
        boundary = period[1] if period is not None else row.observed_at
        if start < boundary <= end:
            boundaries.add(boundary)
    by_id = {row.observation_id: row for row in rows}
    for boundary in sorted(boundaries):
        candidate = _product_outcome(
            product_id,
            evidence,
            bindings,
            start,
            boundary,
            config,
            timelines=timelines,
            include_references=False,
        )
        if candidate["status"] == "success":
            candidate = _product_outcome(
                product_id,
                evidence,
                bindings,
                start,
                boundary,
                config,
                timelines=timelines,
            )
            confirmation = max(
                [
                    boundary,
                    *[
                        by_id[reference].available_at
                        for reference in candidate.get("evidence_refs", [])
                    ],
                ]
            )
            return {
                "observed_crossing_at": boundary.isoformat(),
                "confirmed_crossing_at": confirmation.isoformat(),
                "lead_time_hours": (confirmation - decision_time).total_seconds() / 3600,
            }
    return {"lead_time_hours": None, "lead_time_reason": "crossing_not_observed"}


def _lead_distribution(outcomes: Sequence[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(
        float(outcome["lead_time_hours"])
        for outcome in outcomes
        if outcome.get("lead_time_hours") is not None
    )
    count = len(values)
    median = (
        None
        if not values
        else (
            values[count // 2] if count % 2 else (values[count // 2 - 1] + values[count // 2]) / 2
        )
    )
    return {
        "sample_size": count,
        "min_hours": min(values) if values else None,
        "median_hours": median,
        "max_hours": max(values) if values else None,
        "values_hours": values,
    }


def _summarize(outcomes: Sequence[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(outcome["status"] for outcome in outcomes)
    count = len(outcomes)
    successes = counts["success"]
    observed = successes + counts["failure"]
    missing = count - observed
    all_known = count > 0 and missing == 0
    return {
        "selected_count": count,
        "observed_count": observed,
        "status_counts": dict(counts),
        "coverage": observed / count if count else 0,
        "precision_at_k": successes / count if all_known else None,
        "observed_precision": successes / observed if observed else None,
        "precision_bounds": [successes / count, (successes + missing) / count] if count else None,
        "wilson_interval_observed": _wilson(successes, observed),
        "uncertainty_note": "Wilson interval is descriptive; repeated decisions are dependent",
        "exact_count": sum(outcome.get("precision") == "exact" for outcome in outcomes),
        "bounded_count": sum(outcome.get("precision") == "bounded" for outcome in outcomes),
        "estimated_count": counts["estimated"],
        "lead_time": _lead_distribution(outcomes),
        "outcomes": list(outcomes),
    }


def mature_decision(
    snapshot: dict[str, Any],
    observations: Sequence[Observation],
    as_of: datetime,
    *,
    product_evidence_loader: Callable[[str], Sequence[Observation]] | None = None,
    verified_evidence_refs: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Measure matured horizons, optionally loading one selected product at a time.

    A partitioned caller must validate the frozen dataset dependencies before
    supplying verified_evidence_refs and again atomically when persisting results.
    """
    snapshot_body = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    if snapshot.get("snapshot_id") != digest(snapshot_body):
        raise ValueError("Frozen decision integrity check failed")
    if snapshot.get("schema_version") != "1":
        raise ValueError("Unsupported decision schema")
    config = EvaluationConfig.model_validate(snapshot["config"])
    cutoff, decision_time = utc(as_of), utc(snapshot["decision_time"])
    if cutoff < decision_time:
        raise ValueError("Evaluation cannot precede the decision")
    if product_evidence_loader is not None and (observations or verified_evidence_refs is None):
        raise ValueError("Partitioned maturation requires validated references and no raw history")
    eligible = _eligible_observations(
        observations, cutoff, bool(snapshot["synthetic"]), config.market
    )
    evidence: dict[str, list[Observation]] = defaultdict(list)
    for row in eligible:
        evidence[row.entity_key].append(row)
    available_ids = (
        set(verified_evidence_refs or ())
        if product_evidence_loader is not None
        else {row.observation_id for row in eligible}
    )
    missing_references = sorted(set(snapshot["evidence_refs"]) - available_ids)
    methods = {"policy": snapshot["selected"], **snapshot["baselines"]}
    scenarios: list[dict[str, Any]] = []
    windows: list[tuple[datetime, datetime, str]] = []
    measured: list[dict[str, dict[str, Any]]] = []
    for horizon in config.horizon_days:
        end = decision_time + timedelta(days=horizon)
        for delay in config.launch_delay_days:
            start = decision_time + timedelta(days=delay)
            state = (
                "not_actionable"
                if start >= end
                else "pending"
                if cutoff < end
                else "invalidated"
                if missing_references
                else "matured"
            )
            windows.append((start, end, state))
            measured.append({})
            scenarios.append(
                {
                    "horizon_days": horizon,
                    "launch_delay_days": delay,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "status": state,
                }
            )

    # Keep at most one product's raw evidence alive. Only compact outcomes survive
    # across products; all scenarios and methods reuse that product's one read.
    if any(state == "matured" for _, _, state in windows):
        product_ids = sorted({product for selected in methods.values() for product in selected})
        bindings_by_product: dict[str, set[str]] = defaultdict(set)
        for key in outcome_entity_keys(snapshot["entity_bindings"], config):
            bindings_by_product[snapshot["entity_bindings"][key]].add(key)
        for product_id in product_ids:
            keys = bindings_by_product[product_id]
            product_evidence: dict[str, list[Observation]] = defaultdict(list)
            if product_evidence_loader is None:
                product_evidence.update({key: evidence[key] for key in keys if key in evidence})
            else:
                for item in _eligible_observations(
                    product_evidence_loader(product_id),
                    cutoff,
                    bool(snapshot["synthetic"]),
                    config.market,
                ):
                    if item.entity_key in keys:
                        product_evidence[item.entity_key].append(item)
            product_bindings = {key: product_id for key in keys}
            timelines = {
                key: _entity_timeline(key, product_evidence.get(key, []), config) for key in keys
            }
            for index, (start, end, state) in sorted(
                enumerate(windows), key=lambda item: (item[1][0], item[1][1])
            ):
                if state != "matured":
                    continue
                outcome = _product_outcome(
                    product_id,
                    product_evidence,
                    product_bindings,
                    start,
                    end,
                    config,
                    timelines=timelines,
                )
                if outcome["status"] == "success":
                    outcome.update(
                        _lead_time(
                            product_id,
                            product_evidence,
                            product_bindings,
                            start,
                            end,
                            decision_time,
                            config,
                            timelines=timelines,
                        )
                    )
                measured[index][product_id] = outcome
            del product_evidence, timelines

    for index, scenario in enumerate(scenarios):
        state = scenario["status"]
        scenario["methods"] = {
            method: _summarize(
                [
                    measured[index][product_id]
                    if state == "matured"
                    else {"product_id": product_id, "status": state}
                    for product_id in selected_ids
                ]
            )
            for method, selected_ids in methods.items()
        }
    report: dict[str, Any] = {
        "schema_version": "1",
        "snapshot_id": snapshot["snapshot_id"],
        "decision_time": decision_time.isoformat(),
        "as_of": cutoff.isoformat(),
        "synthetic": snapshot["synthetic"],
        "target": config.target,
        "config": snapshot["config"],
        "scenarios": scenarios,
        "missing_evidence_refs": missing_references,
        "status": "insufficient_history"
        if not snapshot["universe"]
        else (
            "invalidated"
            if missing_references
            else "pending"
            if any(row["status"] == "pending" for row in scenarios)
            and not any(row["status"] == "matured" for row in scenarios)
            else "evaluated"
        ),
        "coverage": snapshot["coverage"],
        "lead_time": {
            "definition": "Hours from decision to first evidence-confirmed threshold crossing",
            "scenarios": [
                {
                    "horizon_days": row["horizon_days"],
                    "launch_delay_days": row["launch_delay_days"],
                    **row["methods"]["policy"]["lead_time"],
                }
                for row in scenarios
            ],
        },
    }
    report["report_id"] = digest(report)
    return report


def backtest(
    observations: Sequence[Observation],
    products: Sequence[Product],
    links: Sequence[ProductLink],
    decision_times: Sequence[datetime],
    as_of: datetime,
    config: EvaluationConfig | None = None,
    *,
    synthetic: bool = False,
    policy: AssessmentPolicy | None = None,
    families: Sequence[FamilyMembership] = (),
    capabilities: Sequence[Capability] = (),
) -> dict[str, Any]:
    """Replay deterministic policy and baseline decisions over recorded knowledge."""
    config = config or EvaluationConfig()
    config.validate_creation()
    cutoff = utc(as_of)
    dates = sorted({utc(timestamp) for timestamp in decision_times})
    if any(timestamp > cutoff for timestamp in dates):
        raise ValueError("A backtest cannot contain a future decision")
    snapshots = [
        freeze_decision(
            observations,
            products,
            links,
            timestamp,
            config,
            synthetic=synthetic,
            policy=policy,
            families=families,
            capabilities=capabilities,
        )
        for timestamp in dates
    ]
    reports = [mature_decision(snapshot, observations, cutoff) for snapshot in snapshots]
    return summarize_backtest(snapshots, reports, cutoff, config, synthetic=synthetic)


def summarize_backtest(
    snapshots: Sequence[dict[str, Any]],
    reports: Sequence[dict[str, Any]],
    as_of: datetime,
    config: EvaluationConfig,
    *,
    synthetic: bool = False,
) -> dict[str, Any]:
    """Aggregate completed decision partitions without materializing their raw history."""
    cutoff = utc(as_of)
    if len(snapshots) != len(reports) or any(
        report["snapshot_id"] != snapshot["snapshot_id"]
        for snapshot, report in zip(snapshots, reports, strict=True)
    ):
        raise ValueError("Backtest reports must match their frozen decision partitions")
    selected = [product_id for snapshot in snapshots for product_id in snapshot["selected"]]
    family_alerts = [
        row["family_id"]
        for snapshot in snapshots
        for row in snapshot["universe"]
        if row["product_id"] in snapshot["selected"]
    ]
    groups: dict[tuple[int, int, str], list[dict[str, Any]]] = defaultdict(list)
    for report in reports:
        for scenario in report["scenarios"]:
            for method, method_result in scenario["methods"].items():
                groups[(scenario["horizon_days"], scenario["launch_delay_days"], method)].extend(
                    method_result["outcomes"],
                )
    comparisons = [
        {
            "horizon_days": horizon,
            "launch_delay_days": delay,
            "method": method,
            **_summarize(outcomes),
        }
        for (horizon, delay, method), outcomes in sorted(groups.items())
    ]
    measured_financial_outcomes = (
        sum(
            outcome.get("precision") == "exact" and outcome["status"] in {"success", "failure"}
            for report in reports
            for scenario in report["scenarios"]
            for outcome in scenario["methods"]["policy"]["outcomes"]
        )
        if config.target == "profit"
        else 0
    )
    result: dict[str, Any] = {
        "schema_version": "1",
        "kind": "backtest",
        "as_of": cutoff.isoformat(),
        "synthetic": synthetic,
        "config": config.model_dump(mode="json"),
        "snapshots": list(snapshots),
        "reports": list(reports),
        "comparisons": comparisons,
        "status": "evaluated"
        if any(snapshot["universe"] for snapshot in snapshots)
        else "insufficient_history",
        "dependence": {
            "decisions": len(snapshots),
            "selected_product_decisions": len(selected),
            "distinct_selected_products": len(set(selected)),
            "distinct_selected_families": len(set(family_alerts)),
            "repeated_product_decisions": len(selected) - len(set(selected)),
            "repeated_family_decisions": len(family_alerts) - len(set(family_alerts)),
        },
        "methodology": {
            "fitting": "none",
            "split": config.split,
            "family_grouping": "as_of_each_decision",
            "missing_outcomes": "censor",
            "target": config.target,
            "measured_financial_outcome_count": measured_financial_outcomes,
            "profit_contract_version": (
                config.profit_contract.version if config.profit_contract is not None else None
            ),
            "profit_claim": False,
        },
    }
    result["evaluation_id"] = digest(result)
    return result
