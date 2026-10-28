"""Private order reconciliation and reviewed financial feedback; no paid effects."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import ExitStack
from pathlib import Path

from .contracts import canonical

LEARNING_ACTIONS = {
    "learning-init",
    "plan-test",
    "review-period",
    "publish-period",
    "test",
    "tests",
    "periods",
}


def _read(path: Path, limit: int = 262_144):
    with path.open("rb") as file:
        body = file.read(limit + 1)
    if len(body) > limit:
        raise ValueError("Operator input exceeds its size limit")
    return json.loads(body)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="sm-trends commerce operations",
        description="Reconcile orders and record reviewed actuals. "
        "No payment, shipment or mail is sent.",
        epilog="Examples: sm-trends commerce operations order-init; "
        "sm-trends commerce operations reconcile-order --connection store.json --order-id ORDER; "
        "sm-trends commerce operations plan-template",
    )
    commands = root.add_subparsers(dest="action", required=True)
    for name in ("order-init", "learning-init", "plan-template", "period-template"):
        commands.add_parser(name)

    def command(name):
        child = commands.add_parser(name)
        child.add_argument("--synthetic", action="store_true")
        return child

    listing = command("register-listing")
    listing.add_argument("--operation-id", required=True)
    for name in ("ingest-event", "reconcile-order", "reconcile-page"):
        child = command(name)
        child.add_argument("--connection", type=Path, required=True)
        if name == "ingest-event":
            child.add_argument("--body-file", type=Path, required=True)
            child.add_argument("--headers-file", type=Path, required=True)
        elif name == "reconcile-order":
            child.add_argument("--order-id", required=True)
            child.add_argument("--resolve-conflict", action="store_true")
            child.add_argument("--expected-revision", type=int)
        else:
            child.add_argument("--updated-after", required=True)
            child.add_argument("--cursor")
    for name in ("orders", "order", "inbox", "handoff", "tracking", "notification-intent"):
        child = command(name)
        child.add_argument("--connection-id", required=True)
        if name in {"orders", "inbox"}:
            child.add_argument("--limit", type=int, default=20)
            child.add_argument("--after")
            if name == "inbox":
                child.add_argument("--include-processed", action="store_true")
        else:
            child.add_argument("--order-id", required=True)
            if name == "order":
                child.add_argument("--revision", type=int)
            else:
                child.add_argument("--request-id", required=True)
                child.add_argument("--expected-revision", type=int, required=True)
                child.add_argument("--file", type=Path, required=True)
    plan = command("plan-test")
    plan.add_argument("--dossier-id", required=True)
    plan.add_argument("--expected-revision", type=int, required=True)
    plan.add_argument("--request-id", required=True)
    plan.add_argument("--file", type=Path, required=True)
    review = command("review-period")
    review.add_argument("--test-id", required=True)
    review.add_argument("--expected-revision", type=int, required=True)
    review.add_argument("--request-id", required=True)
    review.add_argument("--file", type=Path, required=True)
    publish = command("publish-period")
    publish.add_argument("--test-id", required=True)
    publish.add_argument("--period-id", required=True)
    publish.add_argument("--revision", type=int, required=True)
    for name in ("test", "tests", "periods"):
        child = command(name)
        if name != "tests":
            child.add_argument("--test-id", required=True)
        if name != "test":
            child.add_argument("--limit", type=int, default=20)
            child.add_argument("--offset", type=int, default=0)
            if name == "periods":
                child.add_argument("--include-history", action="store_true")
    return root


def _template(action: str) -> dict:
    from .commerce_learning import ACTUAL_COSTS

    if action == "plan-template":
        return {
            "selection": "watch",
            "reason": "",
            "selection_policy": "",
            "cost_contract": {
                "required_costs": list(ACTUAL_COSTS),
                "revenue_basis": "settled_payments_less_executed_refunds_excluding_output_taxes",
                "coverage_policy": "",
            },
        }
    return {
        "period_start": "CHOOSE-UTC-TIME",
        "period_end": "CHOOSE-UTC-TIME",
        "country": "CHOOSE-COUNTRY",
        "currency": "CHOOSE-CURRENCY",
        "amount_kind": "actual",
        "realized_revenue": None,
        "realized_costs": dict.fromkeys(ACTUAL_COSTS),
        "evidence_refs": [],
        "coverage": {
            name: False
            for name in ("payments_complete", "refunds_complete", "fees_complete", "costs_complete")
        },
        "reviewer_note": "",
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.action in {"plan-template", "period-template"}:
            print(canonical(_template(args.action)))
            return 0
        from .catalog import Catalog
        from .collector import redact
        from .commerce_delivery import CommerceDelivery
        from .commerce_orders import CommerceOrders
        from .settings import Settings

        settings = Settings()  # type: ignore[call-arg]
        with ExitStack() as resources:
            catalog = Catalog(settings.database_url.get_secret_value())
            resources.callback(catalog.close)
            service = CommerceOrders(catalog, CommerceDelivery(catalog))
            if args.action in LEARNING_ACTIONS:
                from .commerce_learning import CommercialLearning
                from .history import History
                from .identity import Identity

                history = History(settings, catalog)
                resources.callback(history.flush_usage)
                result = _learning(CommercialLearning(catalog, history, Identity(catalog)), args)
            else:
                result = _orders(service, args)
        print(canonical(redact(result)))
        return 1 if isinstance(result, dict) and result.get("publication_state") == "pending" else 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from pydantic import ValidationError

        from .commerce_delivery import DeliveryError
        from .commerce_learning import LearningError
        from .commerce_orders import OrderError
        from .commerce_providers import ProviderError

        body: dict = {
            "error": "commerce_operation_failed",
            "hint": "Check explicit module initialization, workspace, revision "
            "and private connection configuration.",
        }
        if isinstance(error, (DeliveryError, LearningError, OrderError, ProviderError)):
            body["error"] = error.code
        elif isinstance(error, ValidationError):
            from .commerce_learning import (
                ACTUAL_COSTS,
                ActualCostContract,
                FinancialCoverage,
                ReviewedPeriod,
                TestPlan,
            )

            known_fields = set(ACTUAL_COSTS).union(
                ActualCostContract.model_fields,
                FinancialCoverage.model_fields,
                ReviewedPeriod.model_fields,
                TestPlan.model_fields,
            )
            body.update(
                error="invalid_commercial_record",
                fields=[
                    [
                        part if isinstance(part, int) or part in known_fields else "[field]"
                        for part in item["loc"]
                    ]
                    for item in error.errors(include_input=False)[:20]
                ],
            )
        print(canonical(body), file=sys.stderr)
        return 2


def _orders(service, args):
    synthetic = getattr(args, "synthetic", False)
    if args.action == "order-init":
        return service.initialize()
    if args.action == "register-listing":
        return service.register_listing(args.operation_id, synthetic)
    if args.action in {"ingest-event", "reconcile-order", "reconcile-page"}:
        from .commerce_connection import load_connection

        provider = load_connection(args.connection)
        if args.action == "ingest-event":
            with args.body_file.open("rb") as file:
                raw = file.read(provider.config.max_webhook_bytes + 1)
            headers = _read(args.headers_file, 16_384)
            if (
                not isinstance(headers, dict)
                or len(headers) > 100
                or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items())
            ):
                raise ValueError("Invalid webhook headers")
            return service.ingest(provider, raw, headers, synthetic)
        if args.action == "reconcile-order":
            return service.reconcile_order(
                provider,
                args.order_id,
                synthetic,
                resolve_conflict=args.resolve_conflict,
                expected_revision=args.expected_revision,
            )
        return service.reconcile_page(provider, args.updated_after, args.cursor, synthetic)
    if args.action in {"orders", "inbox"}:
        if args.action == "inbox":
            return service.inbox(
                args.connection_id,
                synthetic,
                args.limit,
                args.after,
                pending_only=not args.include_processed,
            )
        return service.orders(args.connection_id, synthetic, args.limit, args.after)
    if args.action == "order":
        return service.order(args.connection_id, args.order_id, synthetic, args.revision)
    payload = _read(args.file)
    # Explicit named signatures reject accidental extra customer or credential fields.
    method = {
        "handoff": service.handoff,
        "tracking": service.tracking,
        "notification-intent": service.request_notification,
    }[args.action]
    return method(
        args.connection_id,
        args.order_id,
        args.request_id,
        expected_revision=args.expected_revision,
        synthetic=synthetic,
        **payload,
    )


def _learning(service, args):
    from .commerce_learning import ReviewedPeriod, TestPlan

    synthetic = getattr(args, "synthetic", False)
    if args.action == "learning-init":
        return service.initialize()
    if args.action == "plan-test":
        return service.plan_test(
            args.dossier_id,
            args.expected_revision,
            args.request_id,
            synthetic,
            plan=TestPlan.model_validate(_read(args.file)),
        )
    if args.action == "review-period":
        return service.review_period(
            args.test_id,
            ReviewedPeriod.model_validate(_read(args.file)),
            args.expected_revision,
            args.request_id,
            synthetic,
        )
    if args.action == "publish-period":
        return service.publish(args.test_id, args.period_id, args.revision, synthetic)
    if args.action == "test":
        return service.test(args.test_id, synthetic)
    if args.action == "periods":
        return service.periods(
            args.test_id, synthetic, args.limit, args.offset, include_history=args.include_history
        )
    return service.tests(synthetic, args.limit, args.offset)
