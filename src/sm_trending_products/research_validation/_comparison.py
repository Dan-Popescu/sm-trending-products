"""Bounded descriptive reads of already governed development results."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..contracts import canonical, utc
from .models import ResearchError

if TYPE_CHECKING:
    from .service import ResearchValidation

MAX_RECORDS = 10000
MAX_BYTES = 4_000_000


def compare(service: ResearchValidation, attempt_ids: tuple[str, ...], synthetic: bool) -> dict:
    from .service import _request

    if not isinstance(attempt_ids, tuple) or not 2 <= len(attempt_ids) <= 8:
        raise ResearchError("invalid_comparison")
    for identifier in attempt_ids:
        _request(identifier, synthetic)
    if len(set(attempt_ids)) != len(attempt_ids):
        raise ResearchError("invalid_comparison")
    service._require()
    provenance = service.evaluations.provenance
    records_read = 0
    bytes_read = 0

    def values(descriptor: Any) -> list:
        nonlocal records_read, bytes_read
        offset = 0
        collected: list[dict[str, Any]] = []
        while True:
            page = provenance.page(descriptor, limit=100, offset=offset)
            records = page.get("records", [])
            # Preserve frozen selection/date order across hash-sharded pages.
            items = (
                records
                if records
                else [
                    {"position": offset + index, "value": value}
                    for index, value in enumerate(page.get("references", []))
                ]
            )
            records_read += len(items)
            bytes_read += len(canonical(items).encode())
            if records_read > MAX_RECORDS or bytes_read > MAX_BYTES:
                raise ResearchError("comparison_capacity_exceeded")
            collected.extend(items)
            if page["next_offset"] is None:
                return [
                    item["value"] for item in sorted(collected, key=lambda row: row["position"])
                ]
            offset = page["next_offset"]

    # Preflight identity metadata before opening any result, especially heldout.
    with service.catalog.transaction() as connection:
        service.catalog.guard_namespace(connection, synthetic)
        headers = connection.execute(
            "SELECT a.attempt_id,a.protocol_id,a.partition_name FROM research_attempts a "
            "JOIN research_protocols p USING(protocol_id) WHERE a.attempt_id=ANY(%s) "
            "AND p.synthetic=%s",
            (list(attempt_ids), synthetic),
        ).fetchall()
        if len(headers) != len(attempt_ids):
            raise ResearchError("attempt_not_found")
        if any(row["partition_name"] != "development" for row in headers):
            raise ResearchError("comparison_development_only")
        if len({row["protocol_id"] for row in headers}) != 1:
            raise ResearchError("comparison_protocol_mismatch")
    protocol = service.protocol(headers[0]["protocol_id"], synthetic)
    attempts = [service.attempt(identifier, synthetic) for identifier in attempt_ids]
    if any(row["state"] != "completed" or "result" not in row for row in attempts):
        return {
            "status": "unavailable",
            "reasons": ["attempt_incomplete_or_evidence_unavailable"],
            "attempts": [
                {
                    key: row.get(key)
                    for key in (
                        "attempt_id",
                        "policy_id",
                        "state",
                        "error_code",
                        "result_unavailable",
                    )
                }
                for row in attempts
            ],
        }
    if len({utc(row["evaluation_cutoff"]) for row in attempts}) != 1:
        raise ResearchError("comparison_cutoff_mismatch")
    candidates = []
    roots = []
    for attempt in attempts:
        result = attempt["result"]
        roots.extend(provenance.roots(result))
        decisions = []
        for snapshot in values(result["snapshots"]):
            audit = snapshot["selection_audit"]
            selected = values(snapshot["selected"])
            sample = audit.get("measurement_sample")
            decisions.append(
                {
                    "decision_time": snapshot["decision_time"],
                    "selected": selected,
                    "coverage": {
                        key: audit[key]
                        for key in (
                            "scope",
                            "declared_product_count",
                            "counts",
                            "reason_counts",
                            "commercial_stage_note",
                            "products",
                        )
                    },
                    "measurement_sample": None
                    if sample is None
                    else {
                        "product_ids": sample["product_ids"],
                        "methodology": sample["methodology"],
                        "scenarios": [
                            {
                                **{
                                    key: value
                                    for key, value in scenario.items()
                                    if key != "methods"
                                },
                                "methods": {
                                    method: {
                                        key: value
                                        for key, value in summary.items()
                                        if key != "outcomes"
                                    }
                                    for method, summary in scenario["methods"].items()
                                },
                            }
                            for scenario in sample["report"]["scenarios"]
                        ],
                    },
                }
            )
        declared_dates = protocol["protocol"]["partitions"]["development"]["decision_times"]
        if [utc(row["decision_time"]) for row in decisions] != [
            utc(date) for date in declared_dates
        ]:
            raise ResearchError("comparison_scope_mismatch")
        candidates.append(
            {
                "attempt_id": attempt["attempt_id"],
                "evaluation_id": attempt["evaluation_id"],
                "policy_id": attempt["policy_id"],
                "policy": protocol["protocol"]["policies"][attempt["policy_id"]],
                "as_of": attempt["as_of"],
                "evaluation_cutoff": attempt["evaluation_cutoff"],
                "decisions": decisions,
                "outcome_summaries": [
                    {key: value for key, value in summary.items() if key != "outcomes"}
                    for summary in result["comparisons"]
                ],
                "dependence": result["dependence"],
                "methodology": result["methodology"],
            }
        )
    # Compare the actual preselection population, not aggregate counts or which
    # fully observed products each policy selected. Commercial review stages also
    # do not establish whether a product was observable/eligible for ranking.
    coverage = [
        [
            {
                product["product_id"]: {
                    "stages": {
                        key: product["stages"][key]
                        for key in ("discovered", "linked", "eligible", "ranked")
                    },
                    "exclusion_reason": None if product["stages"]["ranked"] else product["reason"],
                }
                for product in decision["coverage"]["products"]
            }
            for decision in candidate["decisions"]
        ]
        for candidate in candidates
    ]
    coverage_differences = []
    for index, decision in enumerate(candidates[0]["decisions"]):
        products = set().union(*(candidate[index] for candidate in coverage))
        changed = sorted(
            product_id
            for product_id in products
            if any(
                candidate[index].get(product_id) != coverage[0][index].get(product_id)
                for candidate in coverage[1:]
            )
        )
        if changed:
            coverage_differences.append(
                {
                    "decision_time": decision["decision_time"],
                    "product_ids": changed,
                }
            )
    flags = []
    if coverage_differences:
        flags.append("different_coverage_or_exclusions")
    outcome_coverage = [
        [
            (
                row["horizon_days"],
                row.get("launch_delay_hours", row.get("launch_delay_days")),
                row["method"],
                row["selected_count"],
                row["observed_count"],
                row["status_counts"],
            )
            for row in candidate["outcome_summaries"]
        ]
        for candidate in candidates
    ]
    if any(value != outcome_coverage[0] for value in outcome_coverage[1:]):
        flags.append("different_outcome_coverage_or_status")
    # No nested reads under this final fence: a queued exclusive retirement must
    # never wait on an outer lock while a nested reader waits behind that writer.
    # Recheck every collected root after hydration, then finalize bounded output.
    with service.catalog.transaction() as connection:
        service.catalog.guard_retention(connection)
        service.catalog.guard_namespace(connection, synthetic)
        evaluations = connection.execute(
            "SELECT evaluation_id,invalidated FROM evaluation_runs WHERE evaluation_id=ANY(%s)",
            (list({row["evaluation_id"] for row in attempts}),),
        ).fetchall()
        if len(evaluations) != len({row["evaluation_id"] for row in attempts}) or any(
            row["invalidated"] for row in evaluations
        ):
            raise ResearchError("evaluation_invalidated")
        provenance.guard(connection, roots)
        response = {
            "status": "flagged" if flags else "comparable",
            "flags": flags,
            "coverage_differences": coverage_differences,
            "candidate_status": "UNVALIDATED",
            "winner": None,
            "protocol_id": protocol["protocol_id"],
            "synthetic": synthetic,
            "partition": "development",
            "scope": protocol["protocol"]["partitions"]["development"],
            "config": protocol["protocol"]["config"],
            "registration_timing": protocol["registration_timing"],
            "locked_policy_id": protocol["locked_policy_id"],
            "candidates": candidates,
            "limits": {"max_attempts": 8, "max_records": MAX_RECORDS, "max_bytes": MAX_BYTES},
            "note": "Descriptive governed development evidence; no automatic winner. "
            "Different coverage cannot support a rate league. Missing outcomes are unknown.",
        }
        if len(canonical(response).encode()) > MAX_BYTES:
            raise ResearchError("comparison_capacity_exceeded")
        return response


def french_summary(result: dict) -> str:
    """Short bounded operator prose; adapters still guard the final serialization."""
    if result["status"] == "unavailable":
        return "Comparaison indisponible : tentative incomplète ou preuves retirées."
    lines = ["Critères NON VALIDÉS — comparaison descriptive du développement. Aucun gagnant."]
    if result["flags"]:
        lines.append(
            "Couvertures ou exclusions différentes : les taux ne classent pas les stratégies."
        )
    for candidate in result["candidates"]:
        selected = candidate["dependence"]["selected_product_decisions"]
        lines.append(f"{candidate['policy_id']} : {selected} sélections produit/date.")
        summaries = [row for row in candidate["outcome_summaries"] if row["method"] == "policy"]
        for summary in summaries[:6]:
            delay = summary.get("launch_delay_hours")
            delay_label = f"{delay} h" if delay is not None else f"{summary['launch_delay_days']} j"
            lines.append(
                f"  Horizon {summary['horizon_days']} j, délai {delay_label} : "
                f"{summary['observed_count']}/{summary['selected_count']} résultats observés ; "
                f"états {canonical(summary['status_counts'])}."
            )
        if len(summaries) > 6:
            lines.append(f"  {len(summaries) - 6} autres scénarios dans les données JSON.")
    lines.append(
        "Résultats absents ou en maturation : inconnus, jamais zéro. "
        "L’attention ne démontre ni ventes ni profit."
    )
    return "\n".join(lines)
