"""Strict safe projections, canonical business revisions and identifier bounds."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..commerce_providers import OrderSnapshot
from ..contracts import digest, utc


class OrderError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def identifier(value, maximum=200):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise OrderError("invalid_identifier")
    return value


def timestamp(value):
    try:
        if not isinstance(value, str) or not 1 <= len(value) <= 40:
            raise ValueError()
        return utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (ValueError, TypeError, AttributeError):
        raise OrderError("invalid_order_timestamp") from None


def money(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,16}(?:\.[0-9]{1,6})?", value):
        raise OrderError("invalid_order_money")
    normalized = format(Decimal(value), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def canonical_snapshot(snapshot: OrderSnapshot, connection_id, provider, order_id, synthetic):
    if (
        not isinstance(snapshot, OrderSnapshot)
        or snapshot.connection_id != connection_id
        or snapshot.provider != provider
        or snapshot.order_id != order_id
        or type(snapshot.synthetic) is not bool
        or snapshot.synthetic != synthetic
    ):
        raise OrderError("order_identity_mismatch")
    identifier(order_id)
    if not re.fullmatch(r"[A-Z]{3}", snapshot.currency):
        raise OrderError("invalid_order_currency")
    if type(snapshot.cancelled) is not bool or not re.fullmatch(
        r"[A-Za-z_-]{1,50}", snapshot.fulfillment_status
    ):
        raise OrderError("invalid_order_status")
    lines = []
    if len(snapshot.lines) > 100 or len(snapshot.payments) > 200 or len(snapshot.refunds) > 100:
        raise OrderError("unsupported_order_volume")
    for item in snapshot.lines:
        identifier(item.line_id)
        for value in (item.product_id, item.variant_id, item.sku):
            if value is not None:
                identifier(value, 100 if value == item.sku else 200)
        if type(item.quantity) is not int or not 0 <= item.quantity <= 1_000_000:
            raise OrderError("invalid_order_quantity")
        lines.append(
            {
                "line_id": item.line_id,
                "product_id": item.product_id,
                "variant_id": item.variant_id,
                "sku": item.sku,
                "quantity": item.quantity,
            }
        )
    payments, refunds = [], []
    for payment in snapshot.payments:
        identifier(payment.transaction_id)
        if payment.kind not in {"capture", "refund"} or payment.currency != snapshot.currency:
            raise OrderError("invalid_payment_currency_or_kind")
        payments.append(
            {
                "transaction_id": payment.transaction_id,
                "kind": payment.kind,
                "currency": payment.currency,
                "amount": money(payment.amount),
            }
        )
    for refund in snapshot.refunds:
        identifier(refund.refund_id)
        if refund.currency != snapshot.currency or type(refund.payment_confirmed) is not bool:
            raise OrderError("invalid_refund_currency_or_state")
        refunds.append(
            {
                "refund_id": refund.refund_id,
                "currency": refund.currency,
                "payment_confirmed": refund.payment_confirmed,
                "amount": money(refund.amount),
            }
        )

    def unique(items, key):
        records: dict[str, Any] = {}
        for item in items:
            if item[key] in records and records[item[key]] != item:
                raise OrderError("business_identity_conflict")
            records[item[key]] = item
        return [records[key] for key in sorted(records)]

    updated = timestamp(snapshot.updated_at)
    payload = {
        "currency": snapshot.currency,
        "total": money(snapshot.total),
        "cancelled": snapshot.cancelled,
        "fulfillment_status": snapshot.fulfillment_status,
        "lines": unique(lines, "line_id"),
        "payments": unique(payments, "transaction_id"),
        "refunds": unique(refunds, "refund_id"),
        "paid_at": timestamp(snapshot.paid_at).isoformat() if snapshot.paid_at else None,
    }
    return updated, payload, digest(payload)


def snapshot_reference(connection_id, order_id, revision):
    return "order-snapshot:" + digest(
        {"connection_id": connection_id, "order_id": order_id, "revision": revision}
    )


def bounded_page(limit):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise OrderError("invalid_page_limit")
