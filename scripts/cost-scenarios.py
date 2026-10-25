#!/usr/bin/env python3
"""Generate inspectable massive-collection scenarios without network or credentials."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from sm_trending_products.costs import Coefficients, Machine, Scenario, Store, Workload, estimate


def main() -> int:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument(
        "--prices", type=Path, required=True, help="Dated machine/storage catalog JSON"
    )
    root.add_argument("--coefficients", type=Path, required=True, help="Explicit coefficient JSON")
    root.add_argument("--products", nargs="+", type=int, default=[5000, 10000, 50000])
    root.add_argument("--history-days", nargs="+", type=int, default=[30, 90])
    root.add_argument("--videos-per-product", type=int, default=20)
    root.add_argument("--refreshes-per-day", type=Decimal, default=Decimal(4))
    root.add_argument("--assessments-per-day", type=Decimal, default=Decimal(24))
    root.add_argument("--free-tier-available", action="store_true", help="Account quota is unused")
    root.add_argument(
        "--sensitivity", nargs="+", type=Decimal, default=[Decimal("0.5"), Decimal(1), Decimal(2)]
    )
    root.add_argument("--output", type=Path, required=True)
    root.add_argument(
        "--compact",
        action="store_true",
        help="Group sensitivity results in a compact review artifact",
    )
    args = root.parse_args()
    if any(value <= 0 or not value.is_finite() for value in args.sensitivity):
        root.error("Sensitivity factors must be finite and positive")
    catalog = json.loads(args.prices.read_text())
    machines = [Machine.model_validate(item) for item in catalog["machines"]]
    stores = [Store.model_validate(item) for item in catalog["stores"]]
    coefficients = Coefficients.model_validate_json(args.coefficients.read_text())
    if (
        len(args.sensitivity)
        * len(args.products)
        * len(args.history_days)
        * len(machines)
        * len(stores)
        > 10000
    ):
        root.error("At most 10,000 explicit scenarios may be generated in one bounded report")
    reports = []
    # These physical-byte terms expose storage/index/dump uncertainty. Request
    # counts remain unchanged, so the report does not conflate byte and call costs.
    variable_bytes = {
        "raw_bytes_per_observation",
        "normalized_bytes_per_observation",
        "manifest_bytes_per_dataset",
        "provenance_bytes_per_product_refresh",
        "assessment_bytes_per_product_run",
        "sql_bytes_per_observation",
        "sql_bytes_per_compaction_delivery",
        "sql_dump_bytes_per_compaction_delivery",
        "wal_bytes_per_compaction_delivery",
        "sql_bytes_per_provenance_node",
        "sql_bytes_per_assessment_run",
        "wal_bytes_per_new_observation",
        "sql_dump_bytes",
        "sql_dump_bytes_per_retained_observation",
        "source_download_bytes_per_product_refresh",
        "upstream_response_bytes_per_observation",
        "backup_inventory_bytes_per_object",
        "restore_ancestor_snapshot_metadata_bytes",
        "sql_bytes_per_retired_artifact",
        "sql_bytes_per_retired_dataset",
        "sql_dump_bytes_per_retired_marker",
    }
    for factor in args.sensitivity:
        changed = coefficients.model_dump()
        for key in variable_bytes:
            if changed[key] is not None:
                changed[key] *= factor
                if factor != 1 and key in changed["evidence"]:
                    changed["evidence"][key]["classification"] = "assumed"
                    changed["evidence"][key]["scope"] += f"; sensitivity scaled by {factor}"
        changed["source"] += f"; physical-byte sensitivity factor={factor}"
        selected = Coefficients.model_validate(changed)
        for products in args.products:
            for days in args.history_days:
                for machine in machines:
                    for store in stores:
                        scenario = Scenario(
                            workload=Workload(
                                products=products,
                                videos_per_product=args.videos_per_product,
                                refreshes_per_day=args.refreshes_per_day,
                                history_days=days,
                                assessment_runs_per_day=args.assessments_per_day,
                                assessment_retention_days=days,
                            ),
                            coefficients=selected,
                            machine=machine,
                            store=store,
                            backup_store=store,
                            account_free_tier_available=args.free_tier_available,
                        )
                        reports.append(
                            {
                                "machine": machine.name,
                                "storage": store.name,
                                "byte_sensitivity": str(factor),
                                "configuration": scenario.model_dump(mode="json"),
                                "report": estimate(scenario),
                            }
                        )
    output = {
        "schema_version": "1",
        "price_sources": catalog["sources"],
        "scope": "Illustrative steady-state planning, no capacity certificate or purchasing action",
        "scenarios": reports,
    }
    if args.compact:
        groups = {}
        for item in reports:
            report = item["report"]
            workload = report["workload"]
            key = (workload["products"], workload["history_days"], item["machine"], item["storage"])
            group = groups.setdefault(
                key,
                {
                    "products": workload["products"],
                    "history_days": workload["history_days"],
                    "operating_age_days": workload["operating_age_days"],
                    "machine": item["machine"],
                    "store": item["storage"],
                    "byte_sensitivity": {},
                },
            )
            group["byte_sensitivity"][item["byte_sensitivity"]] = {
                name: report[name]
                for name in (
                    "modeled_infrastructure_excluding_tax",
                    "known_subtotal_excluding_tax",
                    "monthly_total_excluding_tax",
                    "unknown_costs",
                    "working_disk_fits_advertised_capacity",
                )
            }
            if Decimal(item["byte_sensitivity"]) == 1:
                group["base_physical_quantities"] = {
                    name: report["quantities"][name]
                    for name in (
                        "retained_observations",
                        "retained_original_delivery_rows",
                        "retained_compaction_delivery_rows",
                        "retained_total_delivery_rows",
                        "compaction_delivery_rows_written_per_day",
                        "compaction_delivery_rows_written_per_month",
                        "sql_base_bytes",
                        "sql_compaction_delivery_bytes",
                        "sql_dump_compaction_delivery_component_bytes",
                        "wal_compaction_bytes_per_day",
                        "wal_compaction_retained_bytes",
                        "source_store_bytes",
                        "backup_store_bytes",
                        "source_objects",
                        "source_write_requests",
                        "source_read_requests",
                        "backup_inventory_head_requests",
                        "backup_inventory_list_requests",
                        "vps_working_disk_bytes",
                        "vps_local_backup_family_bytes",
                        "vps_restore_copy_bytes",
                        "retired_marker_rows",
                        "retired_marker_sql_bytes",
                        "vps_inbound_bytes",
                        "backup_captures_per_month",
                        "backup_full_captures_per_month",
                        "backup_effective_full_period_days",
                        "backup_max_family_snapshots",
                        "backup_reserved_families",
                        "backup_reserved_captures",
                        "restore_chain_captures",
                        "restore_chain_owned_objects",
                        "restore_chain_payload_bytes",
                        "restore_chain_inventory_bytes",
                        "restore_chain_ancestor_snapshot_metadata_bytes",
                        "restore_chain_target_sql_dump_bytes",
                        "restore_chain_metadata_requests",
                        "backup_read_requests",
                        "backup_store_egress_bytes",
                    )
                }
                group["base_monthly_charges"] = report["monthly_charges_excluding_tax"]
        output = {
            "schema_version": "1",
            "price_catalog": str(args.prices),
            "coefficient_file": str(args.coefficients),
            "scope": output["scope"],
            "price_sources": catalog["sources"],
            "cases": list(groups.values()),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(
        json.dumps(
            {"scenarios": len(reports), "output": str(args.output), "capacity_verified": False}
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
