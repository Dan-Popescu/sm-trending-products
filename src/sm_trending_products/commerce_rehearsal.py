"""Bounded, explicitly synthetic product-to-commerce rehearsal with local HTTP.

Completed reruns return historical reports. ``verify_rehearsal`` separately checks
stored public evidence without reopening a store or repeating external effects.
"""

from __future__ import annotations

import argparse
import base64
import hmac
import ipaddress
import json
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from psycopg.types.json import Jsonb

from .commerce import REQUIRED_COSTS, LaunchDraft, LaunchService
from .commerce_delivery import CommerceDelivery
from .commerce_learning import (
    ACTUAL_COSTS,
    ActualCostContract,
    CommercialLearning,
    FinancialCoverage,
    ReviewedPeriod,
    TestPlan,
)
from .commerce_orders import CommerceOrders
from .commerce_providers import ConnectionConfig, WooCommerceProvider
from .contracts import canonical, digest, utc
from .enrichment import EnrichmentBudget, TranscriptInput, TranscriptInsights
from .matching import MatchingProfile, ProductMatcher
from .module_schema import initialize_module, require_module
from .pipeline import Pipeline
from .trend_insights import TrendInsights

_VERSION = "commerce-rehearsal-v1"
_SECRET = "synthetic-local-rehearsal-only"
_MIGRATIONS = {
    1: """
    CREATE TABLE commerce_rehearsal_runs (
        run_id text PRIMARY KEY,
        version text NOT NULL,
        state text NOT NULL CHECK(state IN('running','completed','failed')),
        started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        report jsonb NOT NULL DEFAULT '{}'::jsonb
    );
    """
}


class RehearsalError(ValueError):
    """A bounded diagnostic code without secret configuration or customer data."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _guard(synthetic: bool, run_id: str) -> None:
    if synthetic is not True:
        raise RehearsalError("explicit_synthetic_mode_required")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", run_id):
        raise RehearsalError("invalid_rehearsal_run_id")


def _local_store(endpoint_url: str | None) -> None:
    # Never use default AWS endpoints for an engineering rehearsal. The named
    # Docker endpoint is the existing isolated CI object-store service.
    endpoint = urlsplit(endpoint_url or "")
    try:
        local = (
            endpoint.hostname in {"localhost", "objectstore"}
            or re.fullmatch(r"sm-trends-restore-objects-[0-9a-f]{12}", endpoint.hostname or "")
            is not None
            or ipaddress.ip_address(endpoint.hostname or "").is_loopback
        )
    except ValueError:
        local = False
    if endpoint.scheme != "http" or not local or endpoint.username or endpoint.password:
        raise RehearsalError("controlled_local_object_store_required")


class _ControlledShop:
    """One in-memory shop; only whitelisted draft/order paths exist."""

    def __init__(self, anchor: datetime):
        self.anchor = anchor
        self.product: dict | None = None
        self.order_phase = 0
        self.requests: dict[str, int] = {}
        self.url = ""

    def order(self) -> dict:
        assert self.product is not None
        return {
            "id": 71,
            "currency": "EUR",
            "total": "168.00",
            "total_tax": "28.00",
            "status": "processing" if self.order_phase == 0 else "completed",
            "date_modified_gmt": (self.anchor + timedelta(hours=self.order_phase + 1)).isoformat(),
            "date_paid_gmt": (self.anchor + timedelta(hours=1)).isoformat(),
            "line_items": [
                {
                    "id": 81,
                    "product_id": 51,
                    "variation_id": 0,
                    "sku": self.product["sku"],
                    "quantity": 5,
                }
            ],
            "refunds": [{"id": 91}] if self.order_phase >= 2 else [],
            "billing": {"email": "synthetic-customer@example.test", "address_1": "Synthetic only"},
        }

    def response(self, method: str, path: str, body: dict) -> tuple[int, Any]:
        self.requests[method] = self.requests.get(method, 0) + 1
        if sum(self.requests.values()) > 40:
            return 429, {"code": "fixture_request_budget"}
        parsed = urlsplit(path)
        route = parsed.path.removeprefix("/wp-json/wc/v3/")
        if not parsed.path.startswith("/wp-json/wc/v3/"):
            return 404, {}
        if method == "GET" and route == "settings/general/woocommerce_currency":
            return 200, {"id": "woocommerce_currency", "value": "EUR"}
        if method == "GET" and route == "settings/tax/woocommerce_prices_include_tax":
            return 200, {"id": "woocommerce_prices_include_tax", "value": "yes"}
        if method == "GET" and route == "products":
            found = self.product and parse_qs(parsed.query).get("sku") == [self.product["sku"]]
            return 200, [self.product] if found else []
        if method == "POST" and route == "products" and self.product is None:
            if body.get("status") != "draft" or body.get("type") != "simple":
                return 400, {}
            self.product = {**body, "id": 51, "permalink": self.url + "/products/synthetic-lamp"}
            return 201, self.product
        if route == "products/51" and self.product is not None:
            if method == "PUT" and body.get("status") == "draft":
                self.product.update(body)
            if method in {"GET", "PUT"}:
                return 200, self.product
        if method == "GET" and route == "orders/71":
            return 200, self.order()
        if method == "GET" and route == "orders/71/refunds/91" and self.order_phase >= 2:
            return 200, {"id": 91, "amount": "24.00", "refunded_payment": True}
        return 404, {}


@contextmanager
def _shop_server(shop: _ControlledShop):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(2)

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def do_PUT(self):
            self.respond()

        def respond(self):
            response: Any
            expected = "Basic " + base64.b64encode(b"synthetic-key:synthetic-secret").decode()
            if self.headers.get("Authorization") != expected:
                status, response = 401, {}
            else:
                size = self.headers.get("Content-Length", "0")
                if not size.isdecimal() or not 0 <= int(size) <= 32_768:
                    status, response = 413, {}
                else:
                    try:
                        body = json.loads(self.rfile.read(int(size))) if int(size) else {}
                        status, response = shop.response(self.command, self.path, body)
                    except (ValueError, TypeError, TimeoutError):
                        status, response = 400, {}
            data = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    shop.url = f"http://127.0.0.1:{server.server_port}"
    worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    worker.start()
    try:
        yield WooCommerceProvider(
            ConnectionConfig(
                "woocommerce",
                "rehearsal-" + digest(shop.anchor.isoformat())[:24],
                shop.url,
                enabled=True,
                test_mode=True,
                consumer_key="synthetic-key",
                consumer_secret="synthetic-secret",
                webhook_secret=_SECRET,
            )
        )
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)
        if worker.is_alive():
            raise RehearsalError("controlled_server_did_not_stop")


def _records(prefix: str, anchor: datetime) -> list[dict]:
    rows = []
    for video in range(3):
        for hours in range(168, -1, -6) if video < 2 else [0]:
            seen = anchor - timedelta(hours=hours)
            count = ((168 - hours) // 6 + 1) ** 2 * 100 * (video + 1)
            rows.append(
                {
                    "observation_id": digest([prefix, video, hours]),
                    "source": "commerce_rehearsal",
                    "entity_type": "video",
                    "entity_id": f"{prefix}-video-{video}",
                    "synthetic": True,
                    "market": "FR",
                    "observed_at": seen.isoformat(),
                    "available_at": seen.isoformat(),
                    "published_at": (anchor - timedelta(days=8)).isoformat(),
                    "metrics": {"view_count": {"lower": count, "upper": count}},
                    "attributes": {
                        "title": "Portable lamp" if video < 2 else "Lampe portable noire",
                        "creator_id": f"{prefix}-creator-{video}",
                        "brand": "SyntheticDemo",
                        "color": "white" if video < 2 else "black",
                        "creative_fingerprint": digest([prefix, "creative", video]),
                    },
                }
            )
    return rows


def _outcome(result: dict, product_id: str) -> dict:
    return next(
        row
        for row in result["result"]["scenarios"][0]["methods"]["policy"]["outcomes"]
        if row["product_id"] == product_id
    )


def _execute(pipeline: Pipeline, run_id: str, started: datetime, progress: dict) -> dict:
    prefix = "rehearsal-" + digest(run_id)[:20]
    anchor = started + timedelta(minutes=5)  # Explicit synthetic clock, never a live cutoff.
    if pipeline.products(False, limit=1) or len(pipeline.products(True, limit=8)) >= 8:
        raise RehearsalError("use_an_isolated_small_synthetic_workspace")
    progress["stage"] = "source_history"
    rows = _records(prefix, anchor)
    dataset_ids = pipeline.import_records(
        rows, synthetic=True, origin="synthetic_commerce_rehearsal"
    )
    pipeline.create_product(prefix, "Portable lamp", synthetic=True, market="FR")
    for index in range(2):
        pipeline.review_link(prefix, f"commerce_rehearsal:video:{prefix}-video-{index}")
    trends = TrendInsights(pipeline.catalog, pipeline.history).describe(prefix, anchor, True)
    matcher = ProductMatcher(pipeline.catalog)
    suggestions = matcher.suggest(
        prefix,
        anchor,
        True,
        profile=MatchingProfile(
            aliases=("Portable lamp", "Lampe portable"),
            brand="SyntheticDemo",
            variants={"color": "white"},
        ),
        limit=3,
        candidate_limit=10,
    )
    progress["stage"] = "transcript_reuse"
    enrichment = TranscriptInsights(pipeline.catalog, pipeline.provenance)
    enrichment.initialize()
    enrichment.configure_budget("local-rules", EnrichmentBudget(), synthetic=True)
    transcript = enrichment.import_transcript(
        TranscriptInput(
            product_id=prefix,
            entity_key=f"commerce_rehearsal:video:{prefix}-video-0",
            dataset_id=dataset_ids[0],
            observation_id=rows[0]["observation_id"],
            synthetic=True,
            text=(
                "Tired of a dark desk? This lamp helps make reading easier. "
                "Shop our synthetic demo."
            ),
            source_kind="human_transcript",
            language="en",
            permission_reference="synthetic-authored-text",
        ),
        as_of=anchor,
    )
    first = enrichment.analyze(
        prefix + "-text-1", prefix, transcript["transcript_id"], synthetic=True, as_of=anchor
    )
    reused = enrichment.analyze(
        prefix + "-text-2", prefix, transcript["transcript_id"], synthetic=True, as_of=anchor
    )
    if first["analysis_id"] != reused["analysis_id"]:
        raise RehearsalError("transcript_analysis_was_not_reused")
    progress["stage"] = "launch_dossier"
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    dossier_id = prefix + "-fr"
    dossier = launches.save(
        LaunchDraft(
            dossier_id=dossier_id,
            product_id=prefix,
            country="FR",
            currency="EUR",
            synthetic=True,
            title="Synthetic portable lamp",
            description="Authored demo: light < dark & useful.",
            supplier_name="Synthetic supplier",
            supplier_reference=prefix + "-quote",
            quote_expires_at=started + timedelta(days=30),
            stock_status="available",
            delivery_min_days=3,
            delivery_max_days=8,
            return_terms="Synthetic reviewed return terms",
            decision="test",
            retail_price=Decimal("36.00"),
            prices_include_tax=True,
            tax_per_order=Decimal("6.00"),
            costs={
                **dict.fromkeys(REQUIRED_COSTS, Decimal("0")),
                "goods": Decimal("8.25"),
                "shipping": Decimal("3.50"),
                "payment_fees": Decimal("1.25"),
                "returns_allowance": Decimal("2.00"),
            },
            desired_profit=Decimal("5.00"),
        ),
        0,
        "create-dossier",
    )
    if dossier["readiness"]["status"] != "ready":
        raise RehearsalError("synthetic_dossier_not_ready")
    progress["stage"] = "predeclared_ledger"
    learning = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    learning.initialize()
    plan = learning.plan_test(
        dossier_id,
        1,
        "commercial-test",
        True,
        as_of=anchor,
        plan=TestPlan(
            selection="test",
            reason="Synthetic multi-day and supplier rehearsal",
            selection_policy="Synthetic manual-selection policy v1; no automatic training",
            cost_contract=ActualCostContract(
                required_costs=ACTUAL_COSTS,
                revenue_basis="settled_payments_less_executed_refunds_excluding_output_taxes",
                coverage_policy="Review synthetic processor, refund, tax and invoice fixtures",
            ),
        ),
    )
    start = anchor + timedelta(minutes=1)
    end = start + timedelta(days=3)
    first_cutoff, late_cutoff = end + timedelta(minutes=1), end + timedelta(hours=1)
    frozen = pipeline.freeze(
        start,
        True,
        {
            "target": "profit",
            "currency": "EUR",
            "market": "FR",
            "top_k": 8,
            "threshold": 30,
            "horizon_days": [3],
            "launch_delay_hours": [0, 12, 24, 48],
            "profit_contract": plan["profit_contract"],
        },
    )
    if prefix not in frozen["result"]["selected"]:
        raise RehearsalError("use_an_isolated_synthetic_workspace")
    delivery = CommerceDelivery(pipeline.catalog)
    delivery.initialize()
    orders = CommerceOrders(pipeline.catalog, delivery)
    orders.initialize()
    progress["stage"] = "controlled_draft_orders"
    shop = _ControlledShop(anchor)
    with _shop_server(shop) as provider:
        operation = delivery.prepare(dossier_id, provider, 1, "prepare-draft", synthetic=True)
        if operation["state"] != "succeeded":
            raise RehearsalError("synthetic_draft_not_prepared")
        orders.register_listing(operation["operation_id"], True)

        def event(payload: dict) -> dict:
            raw = canonical(payload).encode()
            signature = base64.b64encode(hmac.digest(_SECRET.encode(), raw, "sha256")).decode()
            return orders.ingest(
                provider,
                raw,
                {
                    "X-WC-Webhook-Signature": signature,
                    "X-WC-Webhook-Source": shop.url,
                    "X-WC-Webhook-Topic": "unsigned-hint-only",
                },
                synthetic=True,
            )

        initial = event({"id": 71, "phase": 0, "billing": {"email": "synthetic-hint@example.test"}})
        duplicate = event(
            {"id": 71, "phase": 0, "billing": {"email": "synthetic-hint@example.test"}}
        )
        handoff = orders.handoff(
            provider.config.connection_id,
            "71",
            "handoff",
            prefix + "-quote",
            ["81"],
            synthetic=True,
        )
        shop.order_phase = 1
        completed = event({"id": 71, "phase": 1})
        delayed = event({"id": 71, "old_status": "pending"})
        tracking = orders.tracking(
            provider.config.connection_id,
            "71",
            "tracking",
            1,
            "synthetic-carrier",
            "SYNTHETIC-TRACK",
            synthetic=True,
        )
        notification = orders.request_notification(
            provider.config.connection_id, "71", "notify", "shipping_update", synthetic=True
        )
        shop.order_phase = 2
        refunded = event({"id": 71, "phase": 2})
        event({"id": 71, "phase": 2})
        if duplicate["state"] != "duplicate" or delayed["revision"] != completed["revision"]:
            raise RehearsalError("event_deduplication_failed")
        payments = refunded["payload"]["payments"]
        if (
            refunded["revision"] != 3
            or len(payments) != 1
            or payments[0]["kind"] != "refund"
            or Decimal(payments[0]["amount"]) != Decimal("24")
            or payments[0]["currency"] != "EUR"
        ):
            raise RehearsalError("order_reconciliation_failed")
        connection_id = provider.config.connection_id
    progress["stage"] = "reviewed_financials"
    period = ReviewedPeriod(
        period_start=start,
        period_end=end,
        country="FR",
        currency="EUR",
        realized_revenue=Decimal("120.00"),
        realized_costs={
            **dict.fromkeys(ACTUAL_COSTS, Decimal("0.00")),
            "goods": Decimal("50.00"),
            "advertising": Decimal("20.00"),
            "payment_fees": Decimal("10.00"),
        },
        evidence_refs=(
            refunded["reference"],
            "processor-report:" + prefix,
            "supplier-invoice:" + prefix,
        ),
        coverage=FinancialCoverage(
            payments_complete=True, refunds_complete=True, fees_complete=True, costs_complete=True
        ),
        reviewer_note="Synthetic statement: gross168-refund24-output tax24=net120; actual costs80",
    )
    reviewed = learning.review_period(
        plan["test_id"], period, 0, "actual-1", True, as_of=first_cutoff
    )
    learning.publish(plan["test_id"], reviewed["period_id"], 1, True)
    original = pipeline.mature(frozen["evaluation_id"], first_cutoff)
    if (
        _outcome(original, prefix)["status"] != "success"
        or _outcome(original, prefix)["lower"] != "40.00"
    ):
        raise RehearsalError("reviewed_profit_not_40_eur")
    progress["stage"] = "late_correction"
    changed = period.model_copy(
        update={
            "coverage": FinancialCoverage(
                payments_complete=True,
                refunds_complete=False,
                fees_complete=True,
                costs_complete=True,
            )
        }
    )
    correction = learning.review_period(
        plan["test_id"], changed, 1, "correction-2", True, as_of=late_cutoff
    )
    current = pipeline.mature(frozen["evaluation_id"], late_cutoff)
    historical = pipeline.mature(frozen["evaluation_id"], first_cutoff)
    if _outcome(current, prefix)["status"] != "censored" or historical != original:
        raise RehearsalError("late_correction_rewrote_history")
    _, mapping = pipeline.history.observations(late_cutoff, True, entity_keys={plan["ledger_key"]})
    return {
        "version": _VERSION,
        "run_id": run_id,
        "status": "completed",
        "synthetic": True,
        "execution_mode": "fresh_run",
        "synthetic_clock": anchor.isoformat(),
        "product_id": prefix,
        "dossier_id": dossier_id,
        "dossier_revision": 1,
        "operation_id": operation["operation_id"],
        "connection_id": connection_id,
        "order_id": "71",
        "order_revision": refunded["revision"],
        "commercial_test_id": plan["test_id"],
        "declaration_publication": plan["declaration_publication"],
        "financial_publications": [
            {
                key: review[key]
                for key in ("revision", "dataset_id", "publication_kind", "publication_state")
            }
            for review in learning.periods(plan["test_id"], True, include_history=True)
        ],
        "evaluation_id": frozen["evaluation_id"],
        "original_outcome_id": original["evaluation_id"],
        "late_outcome_id": current["evaluation_id"],
        "period_id": reviewed["period_id"],
        "first_cutoff": first_cutoff.isoformat(),
        "late_cutoff": late_cutoff.isoformat(),
        "transcript_request_id": prefix + "-text-1",
        "transcript_analysis_id": first["analysis_id"],
        "trend_report_id": trends["report_id"],
        "dataset_ids": sorted(set(dataset_ids) | {item["dataset_id"] for item in mapping}),
        "stages": {
            "history": {"video_count": 3, "observation_count": len(rows)},
            "trends": [
                {
                    key: row[key]
                    for key in (
                        "window_hours",
                        "observed_growth_lower",
                        "comparable_interval_count",
                    )
                }
                for row in trends["windows"]
            ],
            "matching": {
                "candidates": suggestions["candidate_count"],
                "requires_review": True,
                "conflict_count": sum(bool(row["conflicts"]) for row in suggestions["suggestions"]),
            },
            "transcript": {
                "analysis_reused": True,
                "provider": "local-rules",
                "cost_reserved": "0",
                "paid_calls": 0,
            },
            "draft": {"state": operation["state"], "public_listing_created": False},
            "orders": {
                "duplicate_suppressed": True,
                "reordered_hint_did_not_rewind": True,
                "platform_paid_at": initial["payload"]["paid_at"],
                "capture_amount_known": False,
                "gateway_confirmed_refund": payments[0]["amount"],
                "cash_complete": False,
            },
            "fulfillment": {
                "handoff": handoff["payload"]["state"],
                "carrier_verified": tracking["payload"]["carrier_verified"],
                "notification_delivered": notification["payload"]["confirmed_delivered"],
            },
            "financials": {
                "reviewed_profit": reviewed["realized_profit"],
                "currency": "EUR",
                "late_complete": correction["complete"],
                "late_status": _outcome(current, prefix)["status"],
                "old_cutoff_unchanged": True,
                "launch_delay_hours": [0, 12, 24, 48],
            },
        },
        "counts": {
            "controlled_http_requests": sum(shop.requests.values()),
            "controlled_http_methods": shop.requests,
            "live_source_requests": 0,
            "paid_provider_calls": 0,
        },
        "limits": [
            "Synthetic rehearsal is not measured commercial value or live acceptance",
            "Woo paid state is not captured cash; actuals are explicit synthetic statements",
            "Manual handoff/tracking and native notification requests perform no external action",
            "No raw video/audio, paid analysis, real supplier, customer payment or email is used",
        ],
    }


def rehearse(
    pipeline: Pipeline, *, synthetic: bool = False, run_id: str = "commerce-rehearsal-v1"
) -> dict:
    """Execute once per ID; incomplete executions never recreate remote drafts."""
    _guard(synthetic, run_id)
    _local_store(pipeline.settings.s3_endpoint_url)
    initialize_module(pipeline.catalog, "commerce_rehearsal", _MIGRATIONS)
    with pipeline.catalog.transaction() as connection:
        created = connection.execute(
            "INSERT INTO commerce_rehearsal_runs(run_id,version,state) VALUES(%s,%s,'running') "
            "ON CONFLICT DO NOTHING RETURNING *",
            (run_id, _VERSION),
        ).fetchone()
        row = (
            created
            or connection.execute(
                "SELECT * FROM commerce_rehearsal_runs WHERE run_id=%s",
                (run_id,),
            ).fetchone()
        )
        if created is not None:
            prefix = "rehearsal-" + digest(run_id)[:20]
            anchor = created["started_at"] + timedelta(minutes=5)
            created["report"] = {
                "version": _VERSION,
                "run_id": run_id,
                "synthetic": True,
                "product_id": prefix,
                "dossier_id": prefix + "-fr",
                "connection_id": "rehearsal-" + digest(anchor.isoformat())[:24],
                "diagnostic": "Find this dossier's stored operations before recovery",
            }
            connection.execute(
                "UPDATE commerce_rehearsal_runs SET report=%s WHERE run_id=%s",
                (Jsonb(created["report"]), run_id),
            )
    assert row is not None
    if row["version"] != _VERSION:
        raise RehearsalError("rehearsal_version_conflict")
    if created is None:
        return {
            **row["report"],
            "run_id": run_id,
            "status": row["state"],
            "synthetic": True,
            "execution_mode": "replayed_completed_run"
            if row["state"] == "completed"
            else "incomplete_run_no_retry",
            "fresh_http_requests": 0,
        }
    progress = {"stage": "initializing"}
    try:
        report = _execute(pipeline, run_id, row["started_at"], progress)
    except Exception as error:
        code = getattr(error, "code", "rehearsal_stage_failed")
        if not isinstance(code, str) or not re.fullmatch(r"[a-z_]{1,80}", code):
            code = "rehearsal_stage_failed"
        report = {
            **row["report"],
            "version": _VERSION,
            "run_id": run_id,
            "status": "failed",
            "synthetic": True,
            "failed_stage": progress["stage"],
            "error_code": code,
            "hint": "Inspect durable operations; use a new run ID for a fresh local rehearsal",
        }
    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "UPDATE commerce_rehearsal_runs SET state=%s,report=%s "
            "WHERE run_id=%s AND state='running'",
            (report["status"], Jsonb(report), run_id),
        )
    return report


def _learning_checks(pipeline: Pipeline, report: dict, test: dict, reviews: list[dict]) -> dict:
    """Follow the public ledger delivery and ownership proofs, not cached outcomes."""
    publication_fields = ("revision", "dataset_id", "publication_kind", "publication_state")
    publications = [{key: review[key] for key in publication_fields} for review in reviews]
    ledger_rows, _ = pipeline.history.observations(
        utc(report["late_cutoff"]), True, entity_keys={test["ledger_key"]}
    )
    declarations = [
        row
        for row in ledger_rows
        if row.attributes.get("publication_kind") == "declaration"
        and row.attributes.get("commercial_test_id") == test["test_id"]
        and row.attributes.get("dossier_id") == report["dossier_id"]
        and row.attributes.get("dossier_revision") == report["dossier_revision"]
        and row.attributes.get("currency") == "EUR"
    ]
    links = pipeline.identity.product_links(
        report["product_id"], utc(report["synthetic_clock"]), True
    )
    return {
        "review_history": len(reviews) == 2
        and reviews[0]["period_id"] == reviews[1]["period_id"] == report["period_id"]
        and reviews[0]["revision"] == 2
        and reviews[0]["complete"] is False
        and reviews[0]["superseded"] is False
        and reviews[1]["revision"] == 1
        and reviews[1]["complete"] is True
        and reviews[1]["superseded"] is True
        and reviews[1]["realized_profit"] == "40.00",
        "financial_publications": publications == report.get("financial_publications")
        and len(publications) == 2
        and [row["publication_kind"] for row in publications] == ["withdrawal", "actual"]
        and all(row["publication_state"] == "published" for row in publications)
        and len({row["dataset_id"] for row in publications}) == 2
        and all(row["dataset_id"] in report["dataset_ids"] for row in publications),
        "ledger_declaration": test["state"] == "ready"
        and test["ledger_declared_at"] is not None
        and utc(test["ledger_declared_at"]) <= utc(report["synthetic_clock"])
        and test["declaration_publication"] is not None
        and test["declaration_publication"] == report.get("declaration_publication")
        and test["declaration_publication"]["state"] == "published"
        and test["declaration_publication"]["dataset_id"] in report["dataset_ids"]
        and test["declaration_publication"]["dataset_id"]
        not in {row["dataset_id"] for row in publications}
        and test["dossier_id"] == report["dossier_id"]
        and test["dossier_revision"] == report["dossier_revision"]
        and len(declarations) == 1
        and declarations[0].available_at <= utc(report["synthetic_clock"])
        and any(
            link["decision_id"] == test["ledger_decision_id"]
            and link["entity_key"] == test["ledger_key"]
            and link["status"] == "confirmed"
            for link in links
        ),
    }


def verify_rehearsal(
    pipeline: Pipeline, *, synthetic: bool = False, run_id: str = "commerce-rehearsal-v1"
) -> dict:
    """Fresh bounded checks of persisted evidence; never contact the historical shop."""
    _guard(synthetic, run_id)
    _local_store(pipeline.settings.s3_endpoint_url)
    require_module(pipeline.catalog, "commerce_rehearsal")
    row = pipeline.catalog.one(
        "SELECT report,state FROM commerce_rehearsal_runs WHERE run_id=%s", (run_id,)
    )
    if row is None or row["state"] != "completed":
        raise RehearsalError("completed_rehearsal_required")
    report = row["report"]
    dossier = LaunchService(pipeline.catalog).dossier(report["dossier_id"], True)
    delivery = CommerceDelivery(pipeline.catalog)
    operation = delivery.operation(report["operation_id"], True)
    order = CommerceOrders(pipeline.catalog, delivery).order(report["connection_id"], "71", True)
    events = {event["kind"]: event for event in order["events"]}
    expected_events = {
        "handoff": {
            "supplier_reference": report["product_id"] + "-quote",
            "line_ids": ["81"],
            "state": "prepared",
            "external_action": "none",
            "source": "manual_operator",
        },
        "tracking": {
            "handoff_revision": 1,
            "carrier": "synthetic-carrier",
            "tracking_reference": "SYNTHETIC-TRACK",
            "status": "recorded",
            "source": "manual_operator",
            "carrier_verified": False,
        },
        "notification": {
            "notification_kind": "shipping_update",
            "state": "requested",
            "external_action": "none",
            "confirmed_delivered": False,
        },
    }
    learning = CommercialLearning(pipeline.catalog, pipeline.history, pipeline.identity)
    test = learning.test(report["commercial_test_id"], True)
    reviews = learning.periods(test["test_id"], True, include_history=True)
    transcript = TranscriptInsights(pipeline.catalog, pipeline.provenance).result(
        report["transcript_request_id"],
        report["product_id"],
        synthetic=True,
        as_of=utc(report["synthetic_clock"]),
    )
    # Public inspection, not mature(): maturation may publish new artifact nodes.
    prior = pipeline.evaluation(report["original_outcome_id"], True)
    current = pipeline.evaluation(report["late_outcome_id"], True)
    frozen = pipeline.evaluation(report["evaluation_id"], True)
    if prior is None or current is None or frozen is None:
        raise RehearsalError("rehearsal_evaluation_missing")
    checks = {
        "dossier_revision": dossier["revision"] == report["dossier_revision"]
        and dossier["product_id"] == report["product_id"]
        and dossier["currency"] == "EUR"
        and dossier["country"] == "FR"
        and dossier["synthetic"] is True,
        "draft_operation": operation["state"] == "succeeded"
        and operation["dossier_revision"] == report["dossier_revision"]
        and operation["dossier_id"] == report["dossier_id"]
        and operation["canonical_product_id"] == report["product_id"]
        and operation["connection_id"] == report["connection_id"]
        and operation["currency"] == "EUR",
        "sanitized_order": order["revision"] == 3
        and len(order["payload"]["payments"]) == 1
        and order["payload"]["payments"][0]["kind"] == "refund"
        and Decimal(order["payload"]["payments"][0]["amount"]) == Decimal("24")
        and order["payload"]["payments"][0]["currency"] == "EUR"
        and order["cash_complete"] is False,
        **{
            "manual_" + kind: kind in events
            and events[kind]["revision"] == 1
            and events[kind]["order_revision"] == (1 if kind == "handoff" else 2)
            and events[kind]["payload"] == payload
            for kind, payload in expected_events.items()
        },
        **_learning_checks(pipeline, report, test, reviews),
        "transcript_history": transcript["analysis_id"] == report["transcript_analysis_id"],
        "old_cutoff_profit": _outcome({"result": prior["payload"]}, report["product_id"])["lower"]
        == "40.00",
        "late_correction": _outcome({"result": current["payload"]}, report["product_id"])["status"]
        == "censored",
        "stored_evaluations_valid": not any(row["invalidated"] for row in (prior, current, frozen)),
        "history_objects": all(pipeline.verify(dataset)["ok"] for dataset in report["dataset_ids"]),
    }
    return {
        "run_id": run_id,
        "synthetic": True,
        "execution_mode": "fresh_evidence_validation",
        "status": "verified" if all(checks.values()) else "failed",
        "checks": checks,
        "controlled_http_requests": 0,
        "live_source_requests": 0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sm-trends commerce rehearse",
        description=__doc__,
        epilog="Examples: sm-trends commerce rehearse --synthetic --run-id pilot-1 "
        "--output rehearsal.json; sm-trends commerce rehearse --synthetic --run-id pilot-1 "
        "--verify --output restored-evidence.json",
    )
    parser.add_argument(
        "--synthetic", action="store_true", required=True, help="Require isolated test data"
    )
    parser.add_argument(
        "--run-id", default="commerce-rehearsal-v1", help="Stable execution/replay identifier"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New private JSON file; existing files are refused",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify saved evidence without provider/source requests",
    )
    args = parser.parse_args(argv)
    try:
        _guard(args.synthetic, args.run_id)
        from .settings import Settings

        settings = Settings()  # type: ignore[call-arg]
        _local_store(settings.s3_endpoint_url)
        if args.output.exists():
            raise RehearsalError("output_already_exists")
        with Pipeline(settings) as pipeline:
            action = verify_rehearsal if args.verify else rehearse
            report = action(pipeline, synthetic=True, run_id=args.run_id)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(canonical(report) + "\n")
        print(canonical({"status": report["status"], "synthetic": True, "output_written": True}))
        return 0 if report["status"] in {"completed", "verified"} else 1
    except Exception:
        print(
            canonical(
                {
                    "error": "synthetic_rehearsal_failed",
                    "hint": "Use isolated local PostgreSQL/S3 and a new output path",
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
