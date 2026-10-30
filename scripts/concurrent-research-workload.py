"""Matched bounded synthetic Rust capture with concurrent public research reports."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

from sm_trending_products.concurrent_research_workload import Campaign, worker
from sm_trending_products.research_workload import Workload

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "research_runner", ROOT / "scripts/research-workload.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def run_campaign(
    campaign: Campaign,
    workload: Workload,
    *,
    output: Path,
    timeout: float = 120,
    max_disk_bytes: int = 256 * runner.MIB,
) -> dict:
    """Two sequential isolated cases share inputs and limits, never state or caches."""
    campaign = Campaign.model_validate(campaign)
    if workload.case != "research" or (not campaign.larger_profile and workload.rows > 1000):
        raise ValueError("Require bounded public research profile; larger history needs opt-in")
    cases = {}
    for name, concurrent in (("collection_only", False), ("concurrent", True)):
        cases[name] = runner.run_case(
            workload,
            output=output.with_name(output.name + f".{name}.json"),
            timeout=timeout,
            max_disk_bytes=max_disk_bytes,
            campaign=campaign.model_copy(update={"concurrent": concurrent}).model_dump(mode="json"),
        )
        if not cases[name]["ok"]:
            break
    control, treatment = cases.get("collection_only", {}), cases.get("concurrent", {})
    baseline_seconds = control.get("collection_seconds")
    treatment_seconds = treatment.get("collection_seconds")
    report = {
        "version": "concurrent-research-comparison-v1",
        "synthetic": True,
        "ok": len(cases) == 2 and all(case["ok"] for case in cases.values()),
        "cases": cases,
        "comparison": {
            "collection_seconds_ratio": treatment_seconds / baseline_seconds
            if treatment_seconds is not None and baseline_seconds
            else None,
            "scope": "Same retained fixtures, arrivals, source latency and bounds; "
            "sequential fresh "
            "schemas/Moto/workers. Control runs historical checks outside collection. "
            "No host-cache isolation or causal statistical claim.",
        },
        "measurement_limits": [
            "Retained days describe logical synthetic history, not campaign endurance.",
            "Report thread CPU excludes collection thread and Rust; worker CPU includes both; "
            "reaped-child CPU includes Rust. Supervisor CPU includes its HTTP Moto server.",
            "PostgreSQL CPU/RSS, Moto RSS, WAL/temp disk and whole-host peak memory are unknown. "
            "Sampled discovered processes can miss short-lived Rust children.",
            "Baseline report primes PostgreSQL/Moto/OS caches; concurrent-cache begins absent, "
            "then subsequent report iterations reuse its application cache.",
            "No real source access, provider invoice, paid inference or target capacity "
            "is established.",
        ],
    }
    runner.existing.write_report(output, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--collector", default=str(ROOT / "collector/target/debug/sm-collector"))
    parser.add_argument("--profile", choices=("small", "retained"), default="small")
    parser.add_argument("--allow-larger-profile", action="store_true")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--max-disk-mib", type=int, default=256)
    args = parser.parse_args()
    if args.profile == "retained" and not args.allow_larger_profile:
        parser.error("retained profile requires --allow-larger-profile")
    retained = args.profile == "retained"
    campaign = Campaign(
        collector_binary=args.collector,
        larger_profile=retained,
        arrivals=48 if retained else 12,
        arrival_interval_seconds=0.10 if retained else 0.02,
        source_latency_seconds=0.08,
        retained_days=30 if retained else 7,
        max_report_iterations=32 if retained else 16,
    )
    workload = Workload(
        case="research",
        products=4 if retained else 1,
        entities=8,
        texts=8,
        samples=121 if retained else 8,
    )
    report = run_campaign(
        campaign,
        workload,
        output=args.output,
        timeout=args.timeout,
        max_disk_bytes=args.max_disk_mib * runner.MIB,
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        raise SystemExit(
            worker(
                json.load(sys.stdin),
                write_report=runner.existing.write_report,
                sql_counts=runner.sql_counts,
            )
        )
    raise SystemExit(main())
