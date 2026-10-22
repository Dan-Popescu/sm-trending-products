"""Non-interactive operator commands; stdout is JSON and secrets are never echoed."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from .contracts import AssessmentPolicy, canonical, now, utc
from .source_contracts import canonical_operation


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="sm-trends",
        description="Collect evidence, rank opportunities, and validate decisions.",
        epilog="Examples: sm-trends init --create-bucket; sm-trends worker --batch-size 20; "
        "sm-trends assess --synthetic; sm-trends verify --json. See docs/operations/.",
    )
    root.add_argument("--version", action="version", version="sm-trends 0.1.0")
    commands = root.add_subparsers(dest="command", required=True)

    def command(name: str, help_text: str, *, synthetic: bool = False):
        child = commands.add_parser(name, help=help_text, description=help_text)
        child.add_argument("--json", action="store_true", help="Emit JSON (the default)")
        if synthetic:
            child.add_argument(
                "--synthetic", action="store_true", help="Use the isolated fixture workspace"
            )
        return child

    init = command("init", "Apply migrations and verify object-store access")
    init.add_argument(
        "--create-bucket", action="store_true", help="Create a missing bucket explicitly"
    )
    command("status", "Inspect health, freshness, queues, capabilities, and storage traffic")
    tasks = command(
        "enqueue", "Queue a bounded source request with an idempotency key", synthetic=True
    )
    tasks.add_argument(
        "--source", required=True, choices=["tiktok_display", "tiktok_ads", "tiktok_commercial"]
    )
    tasks.add_argument("--account-id", required=True)
    tasks.add_argument(
        "--operation",
        choices=["list", "query"],
        help="Defaults to list for Display and query for commercial sources",
    )
    tasks.add_argument("--payload-file", type=Path, required=True)
    tasks.add_argument("--idempotency-key", required=True)
    ingest = command("import", "Import provenance-preserving observation JSONL", synthetic=True)
    ingest.add_argument("file", type=Path)
    ingest.add_argument("--batch-size", type=int, default=500)
    worker = command(
        "worker", "Process bounded collection batches; --loop keeps polling", synthetic=True
    )
    worker.add_argument("--batch-size", type=int, default=20)
    worker.add_argument("--loop", action="store_true")
    worker.add_argument("--poll-seconds", type=float, default=10)
    schedule = command(
        "schedule", "Allocate exploration and refresh tasks within quotas", synthetic=True
    )
    schedule.add_argument("--budget", type=int, default=100)
    schedule.add_argument("--policy-file", type=Path)
    assessment = command("assess", "Publish reproducible opportunity assessments", synthetic=True)
    assessment.add_argument("--as-of", default=None)
    assessment.add_argument("--policy-file", type=Path)
    product = command("product", "Create a canonical product", synthetic=True)
    product.add_argument("--id", required=True)
    product.add_argument("--name", required=True)
    product.add_argument("--market")
    link = command("link", "Review a product/evidence link without backdating knowledge")
    link.add_argument("--product-id", required=True)
    link.add_argument("--entity-key", required=True)
    link.add_argument(
        "--status", choices=["confirmed", "proposed", "rejected"], default="confirmed"
    )
    link.add_argument("--confidence", type=float, default=1)
    family = command("family", "Record product-family membership")
    family.add_argument("--product-id", required=True)
    family.add_argument("--family-id", required=True)
    suggestions = command(
        "suggest", "Suggest bounded text matches for manual review", synthetic=True
    )
    suggestions.add_argument("query")
    command("capabilities", "Inspect actual source access and pending acceptance")
    budget = command("budget", "Configure shared per-source/account token-bucket limits")
    budget.add_argument("--source", required=True)
    budget.add_argument("--account-id", required=True)
    budget.add_argument("--capacity", type=float, required=True)
    budget.add_argument("--requests-per-minute", type=float, required=True)
    live = command("verify-live", "Verify real API access only after intended use is approved")
    live.add_argument(
        "--source", required=True, choices=["tiktok_display", "tiktok_ads", "tiktok_commercial"]
    )
    live.add_argument("--account-id", required=True)
    live.add_argument(
        "--operation",
        choices=["list", "query"],
        help="Defaults to list for Display and query for commercial sources",
    )
    live.add_argument("--payload-file", type=Path, required=True)
    live.add_argument("--approve-use", action="store_true", required=True)
    live.add_argument(
        "--evidence", required=True, help="Approval/scope evidence reference; never a token"
    )
    live.add_argument("--retention-days", type=int, required=True)
    replay = command("replay", "Rebuild normalized evidence from immutable raw history")
    replay.add_argument("dataset_id")
    replay.add_argument(
        "--transform-version",
        default="1",
        help="Source normalizer: 1 (legacy default), 2 (field drift and revision provenance)",
    )
    verify = command("verify", "Read-only verification of committed objects and catalog references")
    verify.add_argument("--dataset-id")
    compact = command(
        "compact", "Compact completed normalized files without deleting raw history", synthetic=True
    )
    compact.add_argument("--max-datasets", type=int, default=100)
    retention = command(
        "retention", "Inspect or apply source-policy retention and dependent invalidation"
    )
    retention.add_argument("--apply", action="store_true", help="Default is a non-mutating preview")
    for name, help_text in (
        ("backtest", "Replay policies with point-in-time eligibility"),
        ("freeze", "Freeze a prospective decision and its evaluation configuration"),
    ):
        evaluation = command(name, help_text, synthetic=True)
        evaluation.add_argument("--as-of")
        evaluation.add_argument("--config-file", type=Path)
        evaluation.add_argument(
            "--policy-file", type=Path, help="AssessmentPolicy JSON; frozen with the decision"
        )
        if name == "backtest":
            evaluation.add_argument(
                "--decision-times-file",
                type=Path,
                required=True,
                help="JSON array of UTC timestamps",
            )
    mature = command("mature", "Measure pending or mature outcomes of a frozen decision")
    mature.add_argument("evaluation_id")
    mature.add_argument("--as-of")
    due = command(
        "mature-due", "Advance a fair bounded batch of due forward decisions", synthetic=True
    )
    due.add_argument("--limit", type=int, default=100)
    due.add_argument("--as-of")
    benchmark = command("benchmark", "Measure an isolated synthetic full-pipeline workload")
    benchmark.add_argument("--records", type=int, default=1000)
    benchmark.add_argument("--output", type=Path, required=True)
    benchmark.add_argument("--product-count", type=int, default=100)
    benchmark.add_argument("--workers", type=int, default=1)
    benchmark.add_argument("--source-latency-ms", type=float, default=0)
    benchmark.add_argument("--hot-fraction", type=float, default=0)
    benchmark.add_argument("--compact", action="store_true")
    benchmark.add_argument("--request-capacity", type=float, default=10000)
    benchmark.add_argument("--requests-per-second", type=float, default=10000)
    benchmark.add_argument("--arrival-rate", type=float, default=0)
    benchmark.add_argument("--drain-seconds", type=float, default=30)
    query = command(
        "query", "Run a bounded read-only DuckDB query on completed history", synthetic=True
    )
    query.add_argument("--sql-file", type=Path, required=True)
    query.add_argument("--dataset-id", action="append", default=[])
    query.add_argument("--max-rows", type=int, default=1000)
    return root


def read_json(path: Path | None, default=None):
    return json.loads(path.read_text()) if path else default


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            result = dispatch(pipeline, arguments)
        print(canonical(result), flush=True)
        return 1 if isinstance(result, dict) and result.get("ok") is False else 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from pydantic import ValidationError

        from .collector import CollectionError

        if isinstance(error, ValidationError):
            body: dict[str, Any] = {
                "error": "invalid_configuration_or_record",
                "fields": [list(item["loc"]) for item in error.errors(include_input=False)],
            }
        elif isinstance(error, CollectionError):
            body = {"error": error.category, "retry_after_seconds": error.retry_after_seconds}
        else:
            body = {
                "error": type(error).__name__,
                "hint": "Check command inputs, DATABASE_URL, S3 settings, and operator status; "
                "see docs/operations. Raw exception messages are suppressed to protect secrets.",
            }
        print(canonical(body), file=sys.stderr)
        return 2


def dispatch(pipeline, args):
    synthetic = getattr(args, "synthetic", False)
    cutoff = utc(args.as_of) if getattr(args, "as_of", None) else now()
    match args.command:
        case "init":
            return pipeline.initialize(create_bucket=args.create_bucket)
        case "status":
            return pipeline.status()
        case "enqueue":
            return {
                "task_id": pipeline.enqueue(
                    args.source,
                    args.account_id,
                    canonical_operation(args.source, args.operation),
                    read_json(args.payload_file),
                    synthetic=synthetic,
                    idempotency_key=args.idempotency_key,
                )
            }
        case "import":
            return pipeline.import_file(args.file, synthetic=synthetic, batch_size=args.batch_size)
        case "worker":
            stop = threading.Event()
            if args.loop:
                signal.signal(signal.SIGTERM, lambda *_: stop.set())
                signal.signal(signal.SIGINT, lambda *_: stop.set())
            if not 0 < args.poll_seconds <= 60:
                raise ValueError("Poll interval must be positive and at most 60 seconds")
            while not stop.is_set():
                result = pipeline.collect_batch(args.batch_size, synthetic=synthetic)
                if not args.loop:
                    return result
                print(canonical(result), flush=True)
                stop.wait(args.poll_seconds)
            return {"status": "stopped"}
        case "schedule":
            return pipeline.schedule(
                args.budget, synthetic=synthetic, policy=read_json(args.policy_file)
            )
        case "assess":
            return pipeline.assess(
                cutoff,
                synthetic=synthetic,
                policy=AssessmentPolicy.model_validate(read_json(args.policy_file, {})),
            )
        case "product":
            return pipeline.create_product(
                args.id, args.name, synthetic=synthetic, market=args.market
            )
        case "link":
            return pipeline.review_link(
                args.product_id, args.entity_key, status=args.status, confidence=args.confidence
            )
        case "family":
            return pipeline.set_family(args.product_id, args.family_id)
        case "suggest":
            return pipeline.suggest_links(args.query, synthetic=synthetic)
        case "capabilities":
            return pipeline.capabilities()
        case "budget":
            pipeline.catalog.configure_budget(
                args.source, args.account_id, args.capacity, args.requests_per_minute / 60
            )
            return {"ok": True}
        case "verify-live":
            return pipeline.verify_live(
                args.source,
                args.account_id,
                canonical_operation(args.source, args.operation),
                read_json(args.payload_file),
                evidence=args.evidence,
                retention_days=args.retention_days,
            )
        case "replay":
            return pipeline.replay(args.dataset_id, args.transform_version)
        case "verify":
            return pipeline.verify(args.dataset_id)
        case "compact":
            return pipeline.compact(synthetic=synthetic, max_datasets=args.max_datasets)
        case "retention":
            return pipeline.retention(apply=args.apply)
        case "backtest":
            return pipeline.backtest(
                [utc(item) for item in read_json(args.decision_times_file)],
                cutoff,
                synthetic=synthetic,
                config=read_json(args.config_file),
                policy=AssessmentPolicy.model_validate(read_json(args.policy_file, {})),
            )
        case "freeze":
            return pipeline.freeze(
                cutoff,
                synthetic=synthetic,
                config=read_json(args.config_file),
                policy=AssessmentPolicy.model_validate(read_json(args.policy_file, {})),
            )
        case "mature":
            return pipeline.mature(args.evaluation_id, cutoff)
        case "mature-due":
            return pipeline.mature_due(limit=args.limit, synthetic=synthetic, as_of=cutoff)
        case "benchmark":
            from .benchmark import benchmark

            return benchmark(
                pipeline,
                args.records,
                args.output,
                product_count=args.product_count,
                workers=args.workers,
                source_latency_ms=args.source_latency_ms,
                hot_fraction=args.hot_fraction,
                compaction=args.compact,
                request_capacity=args.request_capacity,
                requests_per_second=args.requests_per_second,
                arrival_rate=args.arrival_rate,
                drain_seconds=args.drain_seconds,
            )
        case "query":
            return pipeline.explore(
                args.sql_file.read_text(),
                synthetic=synthetic,
                dataset_ids=args.dataset_id,
                max_rows=args.max_rows,
            )
    raise ValueError("Unknown command")
