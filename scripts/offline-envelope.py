"""Run a bounded sequential synthetic native matrix; no production capacity claim."""

from __future__ import annotations

import argparse
import runpy
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

HELPER = runpy.run_path(str(Path(__file__).with_name("offline-benchmark.py")))
GIB = HELPER["GIB"]


def matrix_cases() -> list[tuple[str, list[str], bool]]:
    return [
        (
            "baseline",
            ["--records", "1000", "--product-count", "50", "--workers", "1", "--compact"],
            False,
        ),
        (
            "parallel_skew",
            [
                "--records",
                "4000",
                "--product-count",
                "100",
                "--workers",
                "4",
                "--hot-fraction",
                "0.9",
                "--source-latency-ms",
                "50",
                "--compact",
            ],
            False,
        ),
        (
            "paced_one",
            [
                "--records",
                "1200",
                "--product-count",
                "50",
                "--workers",
                "1",
                "--arrival-rate",
                "2",
                "--drain-seconds",
                "30",
            ],
            False,
        ),
        (
            "paced_four",
            [
                "--records",
                "1200",
                "--product-count",
                "50",
                "--workers",
                "4",
                "--arrival-rate",
                "2",
                "--drain-seconds",
                "30",
            ],
            False,
        ),
        (
            "paced_higher",
            [
                "--records",
                "4800",
                "--product-count",
                "100",
                "--workers",
                "4",
                "--arrival-rate",
                "8",
                "--drain-seconds",
                "30",
            ],
            False,
        ),
        (
            "quota_recovery",
            [
                "--records",
                "1600",
                "--product-count",
                "50",
                "--workers",
                "4",
                "--arrival-rate",
                "4",
                "--drain-seconds",
                "100",
                "--request-capacity",
                "1",
                "--requests-per-second",
                "1",
            ],
            False,
        ),
        ("interruption", ["--records", "60", "--product-count", "10", "--workers", "2"], True),
    ]


def run_matrix(
    output: Path,
    collector: Path,
    profile: str,
    suite_timeout: float = 900,
    start_memory_headroom_gib: int = 4,
) -> dict:
    if not 0 < suite_timeout <= 900:
        raise ValueError("Suite deadline must be positive and at most 900 seconds")
    if start_memory_headroom_gib not in (3, 4):
        raise ValueError("Start memory headroom must be 3 or 4 GiB; the stop floor remains 2 GiB")
    output.mkdir(parents=True, exist_ok=True)
    start, rows, current_case = time.monotonic(), [], None
    planned_cases = matrix_cases()
    summary = {
        "suite_id": uuid4().hex,
        "started_at": datetime.now(UTC).isoformat(),
        "ok": False,
        "complete": False,
        "status": "running",
        "active_case": None,
        "synthetic": True,
        "operating_envelope": "controlled_finite_local_matrix",
        "suite_deadline_seconds": suite_timeout,
        "elapsed_seconds": time.monotonic() - start,
        "stop_reason": None,
        "cases": rows,
        "bounds": {
            "max_workers": 4,
            "max_records_per_case": 10000,
            "max_tasks_per_case": 500,
            "max_case_seconds": 180,
            "max_owned_disk_bytes": GIB,
            "start_memory_headroom_bytes": start_memory_headroom_gib * GIB,
            "stop_memory_headroom_bytes": 2 * GIB,
            "start_disk_headroom_bytes": 8 * GIB,
        },
        "measurement_limits": [
            "Finite controlled arrivals and loopback storage do not establish production capacity",
            "Resources exclude PostgreSQL, Dagster and dashboard; shared-host contention remains",
            "Deadline stops case admission/execution; bounded cleanup can extend elapsed time",
            "Provider prices, real upstream freshness and target VPS headroom remain unknown",
        ],
    }

    def checkpoint():
        summary["elapsed_seconds"] = time.monotonic() - start
        HELPER["write_report"](output / "summary.json", summary)

    # Replace a previous successful report before admitting any new work.
    checkpoint()
    try:
        for name, arguments, interruption in planned_cases:
            current_case = name
            facts = HELPER["hardware_facts"](output)
            remaining = suite_timeout - (time.monotonic() - start)
            if remaining <= 15:
                summary["stop_reason"] = "suite_deadline"
            elif facts["memory_headroom_bytes"] is None:
                summary["stop_reason"] = "memory_headroom_unknown"
            elif facts["memory_headroom_bytes"] < start_memory_headroom_gib * GIB:
                summary["stop_reason"] = "insufficient_memory_headroom"
            elif facts["disk_free_bytes"] < 8 * GIB:
                summary["stop_reason"] = "insufficient_disk_headroom"
            if summary["stop_reason"]:
                break
            report_path = output / f"{name}.json"
            summary["active_case"] = name
            checkpoint()
            result = HELPER["run_case"](
                [*arguments, "--output", str(report_path.resolve())],
                collector_binary=collector,
                collector_profile=profile,
                case_timeout=min(180, remaining - 10),
                interruption=interruption,
            )
            measurements, runner = result.get("measurements", {}), result.get("runner", {})
            rows.append(
                {
                    "name": name,
                    "ok": result["ok"],
                    "status": result.get("status"),
                    "report": str(report_path),
                    "elapsed_seconds": runner.get("elapsed_seconds"),
                    "collection_records_per_second": measurements.get(
                        "collection_records_per_second"
                    ),
                    "task_latency_seconds": measurements.get("task_latency_seconds"),
                    "sampled_peak_session_rss_bytes": runner.get("sampled_peak_session_rss_bytes"),
                    "storage_traffic": measurements.get("storage_traffic"),
                }
            )
            summary["active_case"] = None
            checkpoint()
            if not result["ok"]:
                summary["stop_reason"] = "case_failed"
                break
    except BaseException as error:
        interrupted = isinstance(error, (KeyboardInterrupt, InterruptedError))
        summary.update(
            ok=False,
            complete=False,
            status="interrupted" if interrupted else "failed",
            stop_reason="suite_interrupted" if interrupted else "suite_failed",
            active_case=None,
            failed_case=current_case,
            terminal_error={"type": type(error).__name__},
        )
        checkpoint()
        raise
    summary["complete"] = len(rows) == len(planned_cases)
    summary["ok"] = summary["complete"] and all(row["ok"] for row in rows)
    if summary["ok"]:
        summary["status"] = "completed"
    elif rows and rows[-1]["status"] == "runner_interrupted":
        summary["status"] = "interrupted"
    elif rows and not rows[-1]["ok"]:
        summary["status"] = "failed"
    else:
        summary["status"] = "stopped"
    checkpoint()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output/envelope"))
    parser.add_argument("--collector-binary", type=Path, required=True)
    parser.add_argument("--collector-profile", choices=("debug", "release"), required=True)
    parser.add_argument("--suite-timeout", type=float, default=900)
    parser.add_argument("--start-memory-headroom-gib", type=int, choices=(3, 4), default=4)
    args = parser.parse_args()
    result = run_matrix(
        args.output_dir,
        args.collector_binary,
        args.collector_profile,
        args.suite_timeout,
        args.start_memory_headroom_gib,
    )
    print(HELPER["canonical"](result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
