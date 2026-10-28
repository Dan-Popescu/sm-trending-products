"""Selection accounting and measurement sampling, without new counter formulas."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from typing import Any

from ..catalog import Catalog
from ..contracts import Product, ProductLink, digest
from ..evaluation import EvaluationConfig, reviewed_links


def selection_funnel(
    catalog: Catalog,
    products: Sequence[Product],
    links: Sequence[ProductLink],
    rows: Sequence[dict[str, Any]],
    observation_counts: Mapping[str, int],
    snapshot: dict[str, Any],
    config: EvaluationConfig,
    cutoff: datetime,
    synthetic: bool,
    *,
    declared_product_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    known_products = {product.product_id: product for product in products}
    product_ids = (
        tuple(declared_product_ids) if declared_product_ids is not None else tuple(known_products)
    )
    by_product = {row["product_id"]: row for row in rows}
    keys: dict[str, list[str]] = {}
    for link in links:
        keys.setdefault(link.product_id, []).append(link.entity_key)
    ranked = {row["product_id"] for row in snapshot["universe"]}
    selected = set(snapshot["selected"])
    commercial: dict[str, dict] = {}
    installed = catalog.one("SELECT to_regclass('commercial_tests') AS relation") or {
        "relation": None
    }
    if installed and installed["relation"]:
        commercial = {
            row["product_id"]: row
            for row in catalog.all(
                "SELECT t.product_id,bool_or(t.plan->>'selection'='test') AS planned_test,"
                "bool_or(EXISTS(SELECT 1 FROM commercial_period_reviews r "
                "WHERE r.test_id=t.test_id AND r.available_at<=%s "
                "AND jsonb_array_length(r.payload->'evidence_refs')>0)) AS tested "
                "FROM commercial_tests t WHERE t.synthetic=%s AND t.planned_at<=%s "
                "AND t.product_id=ANY(%s) GROUP BY t.product_id",
                (cutoff, synthetic, cutoff, list(product_ids)),
            )
        }
    audit: list[dict] = []
    counts: Counter[str] = Counter()
    for product_id in product_ids:
        product = known_products.get(product_id)
        discovered = product is not None and product.created_at <= cutoff
        in_market = product is not None and (
            config.market is None or product.market == config.market
        )
        linked = bool(keys.get(product_id))
        eligible = any(
            observation_counts.get(key, 0) >= config.minimum_history_observations
            for key in keys.get(product_id, ())
        )
        is_ranked = product_id in ranked
        reason = (
            "not_discovered_at_cutoff"
            if not discovered
            else "outside_declared_market"
            if not in_market
            else "no_eligible_reviewed_links"
            if not linked
            else "insufficient_distinct_history"
            if not eligible
            else "score_unavailable"
            if not is_ranked
            else "selected"
            if product_id in selected
            else "outside_top_k_or_family_duplicate"
        )
        state = {
            "discovered": discovered,
            "linked": linked,
            "eligible": eligible,
            "ranked": is_ranked,
            "selected": product_id in selected,
            "reviewed": product_id in commercial if installed["relation"] else None,
            "planned_test": (
                bool(commercial.get(product_id, {}).get("planned_test"))
                if installed["relation"]
                else None
            ),
            "tested": (
                bool(commercial.get(product_id, {}).get("tested"))
                if installed["relation"]
                else None
            ),
        }
        counts.update(key for key, value in state.items() if value is True)
        audit.append(
            {
                "product_id": product_id,
                "stages": state,
                "reason": reason,
                "assessment_reasons": by_product.get(product_id, {}).get("reasons", []),
                "future_outcome": "unknown_until_measured",
            }
        )
    return {
        "scope": "declared_observed_product_panel_not_global_market",
        "declared_product_count": len(product_ids),
        "cutoff": cutoff.isoformat(),
        "counts": {
            key: counts[key] for key in ("discovered", "linked", "eligible", "ranked", "selected")
        }
        | {
            key: counts[key] if installed["relation"] else None
            for key in ("reviewed", "planned_test", "tested")
        },
        "commercial_stage_note": (
            "Tested means an available reviewed actual period with evidence, not a test plan"
            if installed["relation"]
            else "Commercial module unavailable; reviewed/tested stages are unknown"
        ),
        "reason_counts": dict(Counter(row["reason"] for row in audit)),
        "products": audit,
    }


def measurement_snapshot(
    snapshot: dict[str, Any], metadata: dict[str, Any], sample_size: int, seed: int
) -> dict[str, Any]:
    """Freeze excluded candidates' existing links; missing history stays censored."""
    candidates = [
        row["product_id"]
        for row in snapshot["selection_audit"]["products"]
        if row["stages"]["discovered"]
        and not row["stages"]["selected"]
        and row["reason"] != "outside_declared_market"
    ]
    selected = sorted(
        candidates,
        key=lambda product_id: digest(
            {"seed": seed, "decision_time": snapshot["decision_time"], "product_id": product_id}
        ),
    )[:sample_size]
    links = [
        link
        for link in reviewed_links(
            metadata["links"],
            datetime.fromisoformat(snapshot["decision_time"]),
            max(
                snapshot["config"]["min_link_confidence"],
                snapshot["policy"]["min_link_confidence"],
            ),
        )
        if link.product_id in selected
    ]
    sample = deepcopy(snapshot)
    sample.pop("selection_audit")
    sample["selected"], sample["baselines"] = selected, {}
    sample["entity_bindings"] = {link.entity_key: link.product_id for link in links}
    sample["identity_decisions"] = [link.model_dump(mode="json") for link in links]
    sample["evidence_refs"] = sample["input_provenance"]
    sample["universe"] = [{"product_id": value, "family_id": value} for value in selected]
    sample["status"] = "frozen" if selected else "insufficient_history"
    sample["methodology"] = {
        **sample["methodology"],
        "measurement_only": True,
        "sampling_population": len(candidates),
        "sample_size": len(selected),
        "inclusion_probability": len(selected) / len(candidates) if candidates else None,
        "profit_claim": False,
    }
    sample.pop("snapshot_id")
    sample["snapshot_id"] = digest(sample)
    return sample
