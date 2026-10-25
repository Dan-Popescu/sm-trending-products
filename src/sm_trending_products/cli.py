"""Non-interactive operator commands; stdout is JSON and secrets are never echoed."""

from __future__ import annotations

import argparse
import json
import sys
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
    capacity = command("capacity", "Compare an explicit video workload with planning budgets")
    capacity.add_argument("--products", type=int, required=True)
    capacity.add_argument("--videos-per-product", type=int, required=True)
    capacity.add_argument("--refreshes-per-day", type=float, required=True)
    capacity.add_argument("--videos-per-request", type=int, default=20)
    capacity.add_argument("--history-days", type=int, default=90)
    capacity.add_argument("--source-requests-per-day", type=int)
    cost = command("cost", "Price an explicit scenario without database or cloud credentials")
    cost.add_argument(
        "--config", type=Path, required=True, help="Scenario JSON including dated prices"
    )
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
    for name, description in (
        ("worker", "Process legacy combined collection batches"),
        ("capture", "Durably archive source pages independently of normalization"),
        ("publish-captures", "Normalize committed captures without contacting the source"),
    ):
        worker = command(name, description, synthetic=True)
        worker.add_argument("--batch-size", type=int, default=20)
        worker.add_argument("--loop", action="store_true")
        worker.add_argument("--poll-seconds", type=float, default=10)
        worker.epilog = f"Example: sm-trends {name} --loop --batch-size 20 --poll-seconds 2"
    policy = command(
        "capture-policy", "Register an immutable source recovery contract", synthetic=True
    )
    policy.add_argument("--file", type=Path, required=True)
    policy.epilog = "Example: sm-trends capture-policy --file /private/source-policy.json"
    capture_status = command(
        "capture-status", "Inspect original capture windows and receipts", synthetic=True
    )
    capture_status.add_argument("--as-of")
    capture_status.add_argument("--limit", type=int, default=200)
    capture_status.epilog = "Example: sm-trends capture-status --limit 100"
    spool = command("capture-spool", "Inspect or apply authorized local receipt maintenance")
    spool.add_argument("--limit", type=int, default=20)
    spool.add_argument("--apply", action="store_true", help="Default is a read-only preview")
    spool.epilog = "Example: sm-trends capture-spool --limit 20; sm-trends capture-spool --apply"
    revoke = command(
        "capture-spool-revoke",
        "Record scoped authorization to remove retained local inputs",
        synthetic=True,
    )
    revoke.add_argument("--source", required=True)
    revoke.add_argument("--account-id", required=True)
    revoke.add_argument("--reason", required=True)
    revoke.add_argument("--captured-before")
    revoke.add_argument("--apply", action="store_true", help="Default previews without revoking")
    revoke.epilog = (
        "Example: sm-trends capture-spool-revoke --source tiktok_display "
        "--account-id ACCOUNT --reason source_removed --apply"
    )
    pause = command(
        "capture-pause",
        "Assess a bounded blackout against verified recovery windows",
        synthetic=True,
    )
    pause.add_argument("--as-of")
    pause.add_argument("--pause-seconds", required=True, type=float)
    pause.add_argument("--rollback-seconds", required=True, type=float)
    pause.add_argument("--safety-seconds", required=True, type=float)
    pause.add_argument("--catchup-requests-per-second", required=True, type=float)
    pause.add_argument("--request-budget", required=True, type=int)
    pause.epilog = (
        "Example: sm-trends capture-pause --pause-seconds 30 --rollback-seconds 30 "
        "--safety-seconds 60 --catchup-requests-per-second 1 --request-budget 100"
    )
    for name, description in (
        ("worker-status", "Inspect registered worker generation, drain state and owned leases"),
        ("worker-drain", "Block new claims; the owning worker acknowledges settled work"),
        ("worker-resume", "Resume an acknowledged drained worker generation"),
    ):
        control = command(name, description)
        control.add_argument("--worker-id", required=True)
        if name in {"worker-drain", "worker-resume"}:
            control.add_argument("--generation", required=True, type=int)
        if name == "worker-drain":
            control.add_argument(
                "--timeout-seconds", type=float, default=5, help="Maximum admission-lock wait"
            )
            control.add_argument(
                "--wait-seconds",
                type=float,
                help="Also wait for acknowledgement, or report blocked",
            )
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
    retention_policy = live.add_mutually_exclusive_group(required=True)
    retention_policy.add_argument("--retention-days", type=int)
    retention_policy.add_argument(
        "--source-retention-unlimited",
        action="store_true",
        help="Explicitly verified absence of a source expiry cap; requires SQL 039",
    )
    live.add_argument(
        "--source-retention-evidence",
        help="Separate source-wide evidence; required for unlimited retention or a relaxed cap",
    )
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
    for maintenance_command in (compact, retention):
        maintenance_command.add_argument("--drain", action="store_true")
        maintenance_command.add_argument("--max-batches", type=int)
        maintenance_command.add_argument("--max-seconds", type=float)
    retention.add_argument("--max-objects", type=int)
    retention.add_argument("--max-operations", type=int)
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
        if arguments.command == "capacity":
            from .capacity import WorkloadProfile
            from .settings import PlanningSettings

            result = WorkloadProfile(
                products=arguments.products,
                videos_per_product=arguments.videos_per_product,
                refreshes_per_day=arguments.refreshes_per_day,
                videos_per_request=arguments.videos_per_request,
                history_days=arguments.history_days,
                source_requests_per_day=arguments.source_requests_per_day,
            ).report(PlanningSettings())
        elif arguments.command == "cost":
            from .costs import Scenario, estimate

            result = estimate(Scenario.model_validate_json(arguments.config.read_text()))
        else:
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
        case "worker" | "capture" | "publish-captures":
            from .worker_runtime import run_worker

            return run_worker(
                pipeline,
                mode=args.command,
                batch_size=args.batch_size,
                synthetic=synthetic,
                loop=args.loop,
                poll_seconds=args.poll_seconds,
            )
        case "worker-status":
            state = pipeline.worker_control.status(args.worker_id)
            if state is None:
                raise ValueError("Unknown worker")
            return state
        case "capture-policy":
            from .capture_contracts import CapturePolicy

            identity = pipeline.capture_contracts.register(
                CapturePolicy.model_validate(read_json(args.file)), synthetic=synthetic
            )
            return {"contract_id": identity, "synthetic": synthetic}
        case "capture-spool":
            return pipeline.maintain_capture_spool(args.limit, apply=args.apply)
        case "capture-spool-revoke":
            before = utc(args.captured_before) if args.captured_before else cutoff
            if not args.apply:
                return {
                    "preview": True,
                    "source": args.source,
                    "account_id": args.account_id,
                    "synthetic": synthetic,
                    "reason": args.reason,
                    "captured_before": before,
                }
            return pipeline.revoke_capture_spool(
                args.source, args.account_id, synthetic, args.reason, captured_before=before
            )
        case "capture-status":
            return pipeline.capture_contracts.status(
                as_of=cutoff, synthetic=synthetic, limit=args.limit
            )
        case "capture-pause":
            return pipeline.capture_contracts.assess_pause(
                as_of=cutoff,
                synthetic=synthetic,
                pause_seconds=args.pause_seconds,
                rollback_seconds=args.rollback_seconds,
                safety_seconds=args.safety_seconds,
                catchup_requests_per_second=args.catchup_requests_per_second,
                request_budget=args.request_budget,
            )
        case "worker-drain":
            if args.wait_seconds is not None and not 0 < args.wait_seconds <= 3600:
                raise ValueError("Drain wait must be positive and at most 3600 seconds")
            control = pipeline.worker_control
            state = control.request_drain(
                args.worker_id, args.generation, timeout_seconds=args.timeout_seconds
            )
            if state.get("ok") is False or args.wait_seconds is None:
                return state
            return control.wait_drained(
                args.worker_id, args.generation, timeout_seconds=args.wait_seconds
            )
        case "worker-resume":
            return pipeline.worker_control.resume(args.worker_id, args.generation)
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
                source_retention_unlimited=args.source_retention_unlimited,
                source_retention_evidence=args.source_retention_evidence,
            )
        case "replay":
            return pipeline.replay(args.dataset_id, args.transform_version)
        case "verify":
            return pipeline.verify(args.dataset_id)
        case "compact":
            if not args.drain and any(
                value is not None for value in (args.max_batches, args.max_seconds)
            ):
                raise ValueError("Drain limits require --drain")
            if args.drain:
                return pipeline.compact_drain(
                    synthetic=synthetic,
                    max_datasets=args.max_datasets,
                    max_batches=args.max_batches,
                    max_seconds=args.max_seconds,
                )
            return pipeline.compact(synthetic=synthetic, max_datasets=args.max_datasets)
        case "retention":
            if not args.drain and any(
                value is not None
                for value in (
                    args.max_batches,
                    args.max_seconds,
                    args.max_objects,
                    args.max_operations,
                )
            ):
                raise ValueError("Drain limits require --drain")
            if args.drain:
                if not args.apply:
                    raise ValueError("Retention drain requires explicit --apply")
                return pipeline.retention_drain(
                    max_batches=args.max_batches,
                    max_objects=args.max_objects,
                    max_seconds=args.max_seconds,
                    max_operations=args.max_operations,
                )
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
