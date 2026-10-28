"""Immutable operator records, never supplier purchases or delivered messages."""

import re

from psycopg.types.json import Jsonb

from ..contracts import digest
from ._records import OrderError, identifier


def validate_event(kind, payload):
    if kind == "handoff":
        supplier_reference = identifier(payload["supplier_reference"], 128)
        if not re.fullmatch(r"[A-Za-z0-9._:/-]+", supplier_reference):
            raise OrderError("invalid_supplier_reference")
        lines = payload["line_ids"]
        if not 1 <= len(lines) <= 100 or len(set(lines)) != len(lines):
            raise OrderError("invalid_handoff_lines")
        for line in lines:
            identifier(line)
        payload["line_ids"] = sorted(lines)
        if payload["state"] not in {"prepared", "cancelled"}:
            raise OrderError("invalid_handoff_state")
    elif kind == "tracking":
        if type(payload["handoff_revision"]) is not int or payload["handoff_revision"] < 1:
            raise OrderError("invalid_handoff_revision")
        for key in ("carrier", "tracking_reference"):
            value = identifier(payload[key], 128)
            if not re.fullmatch(r"[A-Za-z0-9._:/-]+", value):
                raise OrderError("invalid_tracking_reference")
        if payload["status"] not in {"recorded", "in_transit", "delivered", "returned"}:
            raise OrderError("invalid_tracking_state")
    elif payload["notification_kind"] not in {
        "order_confirmation",
        "shipping_update",
        "refund_update",
    }:
        raise OrderError("invalid_notification_kind")


def append_event(
    catalog, kind, connection_id, order_id, request_id, payload, expected_revision, synthetic
):
    for value in (connection_id, order_id, request_id):
        identifier(value, 128 if value == request_id else 200)
    if type(expected_revision) is not int or not 0 <= expected_revision < 2_147_483_647:
        raise OrderError("invalid_revision")
    fingerprint = digest({"payload": payload, "expected_revision": expected_revision})
    with catalog.transaction() as connection:
        # Serialize each order's three small manual streams independently of HTTP.
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||"
            "':commerce-order-event:'||%s,492371254))",
            (connection_id + ":" + order_id,),
        )
        order = connection.execute(
            "SELECT h.*,s.payload,s.line_links FROM commerce_order_heads h "
            "JOIN commerce_order_connections c USING(connection_id) "
            "JOIN commerce_order_snapshots s ON s.connection_id=h.connection_id "
            "AND s.order_id=h.order_id AND s.revision=h.revision "
            "WHERE h.connection_id=%s AND h.order_id=%s AND c.synthetic=%s FOR SHARE OF h",
            (connection_id, order_id, synthetic),
        ).fetchone()
        if order is None:
            raise OrderError("order_not_found")
        previous = connection.execute(
            "SELECT * FROM commerce_order_events WHERE connection_id=%s AND order_id=%s "
            "AND kind=%s AND request_id=%s",
            (connection_id, order_id, kind, request_id),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != fingerprint:
                raise OrderError("event_request_conflict")
            return previous
        current = connection.execute(
            "SELECT revision,payload FROM commerce_order_events WHERE connection_id=%s "
            "AND order_id=%s AND kind=%s ORDER BY revision DESC LIMIT 1",
            (connection_id, order_id, kind),
        ).fetchone()
        revision = current["revision"] if current else 0
        if expected_revision != revision:
            raise OrderError("event_revision_conflict")
        cancelling = kind == "handoff" and payload["state"] == "cancelled"
        if order["state"] != "current" and not cancelling:
            raise OrderError("order_conflict_unresolved")
        if kind == "handoff":
            if cancelling:
                if (
                    current is None
                    or current["payload"]["state"] != "prepared"
                    or any(
                        current["payload"][key] != payload[key]
                        for key in ("supplier_reference", "line_ids")
                    )
                ):
                    raise OrderError("handoff_cancel_requires_current_preparation")
            else:
                matched = {
                    row["line_id"] for row in order["line_links"] if row["state"] == "matched"
                }
                if not set(payload["line_ids"]) <= matched:
                    raise OrderError("handoff_requires_owned_lines")
                if order["payload"]["cancelled"]:
                    raise OrderError("cancelled_order_handoff")
        if kind == "tracking":
            handoff = connection.execute(
                "SELECT revision,payload FROM commerce_order_events WHERE connection_id=%s "
                "AND order_id=%s AND kind='handoff' ORDER BY revision DESC LIMIT 1",
                (connection_id, order_id),
            ).fetchone()
            if (
                handoff is None
                or handoff["revision"] != payload["handoff_revision"]
                or handoff["payload"]["state"] != "prepared"
            ):
                raise OrderError("tracking_requires_current_handoff")
        row = connection.execute(
            "INSERT INTO commerce_order_events(connection_id,order_id,kind,revision,request_id,"
            "request_sha256,order_revision,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            (
                connection_id,
                order_id,
                kind,
                revision + 1,
                request_id,
                fingerprint,
                order["revision"],
                Jsonb(payload),
            ),
        ).fetchone()
        assert row is not None
        return row
