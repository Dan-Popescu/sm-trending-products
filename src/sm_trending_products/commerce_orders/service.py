"""Authenticated hints and read-only authoritative order reconciliation."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..commerce_delivery import CommerceDelivery, connection_fingerprint
from ..commerce_providers import CommerceProvider, ProviderError
from ..module_schema import initialize_module, require_module
from ._manual import append_event, validate_event
from ._records import (
    OrderError,
    bounded_page,
    canonical_snapshot,
    identifier,
    snapshot_reference,
    timestamp,
)
from ._schema import MIGRATIONS


class CommerceOrders:
    def __init__(self, catalog: Catalog, delivery=None):
        self.catalog = catalog
        self.delivery = delivery if delivery is not None else CommerceDelivery(catalog)

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "commerce_orders", MIGRATIONS)

    def _require(self):
        require_module(self.catalog, "commerce_orders")

    @staticmethod
    def _workspace(synthetic):
        if type(synthetic) is not bool:
            raise OrderError("invalid_workspace")

    @staticmethod
    def _bind(connection, connection_id, fingerprint, provider, synthetic):
        connection.execute(
            "INSERT INTO commerce_order_connections(connection_id,fingerprint,provider,synthetic) "
            "VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (connection_id, fingerprint, provider, synthetic),
        )
        row = connection.execute(
            "SELECT * FROM commerce_order_connections WHERE connection_id=%s FOR SHARE",
            (connection_id,),
        ).fetchone()
        if row is None or any(
            row[key] != value
            for key, value in (
                ("fingerprint", fingerprint),
                ("provider", provider),
                ("synthetic", synthetic),
            )
        ):
            raise OrderError("connection_identity_mismatch")

    def _provider(self, provider: CommerceProvider, synthetic):
        self._require()
        self._workspace(synthetic)
        config = provider.config
        if not config.enabled:
            raise OrderError("connection_disabled")
        if config.test_mode != synthetic:
            raise OrderError("workspace_mismatch")
        fingerprint = connection_fingerprint(config)
        with self.catalog.transaction() as connection:
            self._bind(connection, config.connection_id, fingerprint, config.provider, synthetic)
        return config.connection_id

    @staticmethod
    def _provider_call(method, *args):
        try:
            return method(*args)
        except ProviderError:
            raise
        except Exception:
            raise OrderError("order_provider_failed") from None

    def register_listing(self, operation_id: str, synthetic: bool = False) -> dict:
        self._require()
        self._workspace(synthetic)
        identifier(operation_id, 128)
        operation = self.delivery.operation(operation_id, synthetic=synthetic)
        if operation["state"] != "succeeded" or operation["synthetic"] != synthetic:
            raise OrderError("listing_operation_not_succeeded")
        if not operation["product_id"]:
            raise OrderError("listing_operation_missing_product")
        with self.catalog.transaction() as connection:
            self._bind(
                connection,
                operation["connection_id"],
                operation["connection_fingerprint"],
                operation["provider"],
                synthetic,
            )
            dossier = connection.execute(
                "SELECT d.product_id,d.synthetic,d.currency FROM commerce_dossiers d "
                "JOIN commerce_dossier_revisions r USING(dossier_id) "
                "JOIN products p ON p.product_id=d.product_id AND p.synthetic=d.synthetic "
                "WHERE d.dossier_id=%s AND r.revision=%s",
                (operation["dossier_id"], operation["dossier_revision"]),
            ).fetchone()
            if dossier is None or any(
                dossier[key] != operation[field]
                for key, field in (
                    ("product_id", "canonical_product_id"),
                    ("synthetic", "synthetic"),
                    ("currency", "currency"),
                )
            ):
                raise OrderError("listing_dossier_identity_mismatch")
            keys = (
                "operation_id",
                "connection_id",
                "canonical_product_id",
                "dossier_id",
                "dossier_revision",
                "product_id",
                "variant_id",
                "sku",
                "currency",
                "synthetic",
            )
            connection.execute(
                "INSERT INTO commerce_order_listings"
                "(operation_id,connection_id,canonical_product_id,"
                "dossier_id,dossier_revision,product_id,variant_id,sku,currency,synthetic) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                tuple(operation[key] for key in keys),
            )
            row = connection.execute(
                "SELECT * FROM commerce_order_listings WHERE operation_id=%s", (operation_id,)
            ).fetchone()
            if row is None or any(row[key] != operation[key] for key in keys):
                raise OrderError("listing_identity_conflict")
            return row

    @contextmanager
    def _fetch_guard(self, connection_id, order_id):
        # A session lock spans read-only HTTP without retaining a SQL transaction.
        # The final commit must use this exact session: a token alone cannot
        # detect a lost session before another worker has claimed the order.
        conninfo = self.catalog.pool.conninfo
        connection = psycopg.connect(
            conninfo() if callable(conninfo) else conninfo,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
        )
        try:
            connection.execute("SET statement_timeout='5s'")
            for slot in range(2):
                row = connection.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(current_schema()||"
                    "':commerce-order-slot:'||%s,492371252)) AS locked",
                    (str(slot),),
                ).fetchone()
                if row and row["locked"]:
                    break
            else:
                raise OrderError("order_capacity_busy")
            row = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(current_schema()||"
                "':commerce-order:'||%s,492371253)) AS locked",
                (connection_id + ":" + order_id,),
            ).fetchone()
            if not row or not row["locked"]:
                raise OrderError("order_busy")
            yield connection
        except (psycopg.OperationalError, psycopg.InterfaceError):
            raise OrderError("order_database_guard_unavailable") from None
        finally:
            connection.close()

    def ingest(
        self,
        provider: CommerceProvider,
        raw: bytes,
        headers: dict[str, str],
        synthetic: bool = False,
    ) -> dict:
        connection_id = self._provider(provider, synthetic)
        if not isinstance(raw, bytes) or len(raw) > provider.config.max_webhook_bytes:
            raise OrderError("webhook_too_large")
        hint = self._provider_call(provider.verify_webhook, raw, headers)
        identifier(hint.order_id)
        body_hash = hashlib.sha256(raw).hexdigest()
        with self.catalog.transaction() as connection:
            connection.execute(
                "INSERT INTO commerce_order_inbox(connection_id,body_sha256,order_id,synthetic) "
                "VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (connection_id, body_hash, hint.order_id, synthetic),
            )
            row = connection.execute(
                "SELECT * FROM commerce_order_inbox WHERE connection_id=%s AND body_sha256=%s",
                (connection_id, body_hash),
            ).fetchone()
            assert row is not None
            if row["order_id"] != hint.order_id:
                raise OrderError("hint_identity_conflict")
            if row["state"] == "completed":
                return {
                    "state": "duplicate",
                    "order_id": hint.order_id,
                    "connection_id": connection_id,
                    "body_sha256": body_hash,
                }
        # Inbox persistence precedes HTTP. The signed body is a hint, never money.
        return self.reconcile_order(provider, hint.order_id, synthetic=synthetic)

    def reconcile_order(
        self,
        provider: CommerceProvider,
        order_id: str,
        synthetic: bool = False,
        *,
        resolve_conflict: bool = False,
        expected_revision: int | None = None,
    ):
        connection_id = self._provider(provider, synthetic)
        identifier(order_id)
        if type(resolve_conflict) is not bool or (
            resolve_conflict and (type(expected_revision) is not int or expected_revision < 1)
        ):
            raise OrderError("conflict_resolution_requires_revision")
        with self._fetch_guard(connection_id, order_id) as guarded:
            token = uuid4().hex
            with guarded.transaction():
                row = guarded.execute(
                    "INSERT INTO commerce_order_heads"
                    "(connection_id,order_id,synthetic,fetch_token) "
                    "VALUES(%s,%s,%s,%s) ON CONFLICT(connection_id,order_id) DO UPDATE "
                    "SET fetch_token=excluded.fetch_token "
                    "RETURNING clock_timestamp() AS started_at",
                    (connection_id, order_id, synthetic, token),
                ).fetchone()
                assert row is not None
                started = row["started_at"]
            snapshot = self._provider_call(provider.read_order, order_id)
            updated, payload, payload_hash = canonical_snapshot(
                snapshot, connection_id, provider.config.provider, order_id, synthetic
            )
            decision = self._store(
                connection_id,
                order_id,
                token,
                updated,
                payload,
                payload_hash,
                resolve_conflict,
                expected_revision,
                started,
                guarded,
            )
        result = self.order(connection_id, order_id, synthetic=synthetic)
        result["reconciliation_state"] = decision
        return result

    def _store(
        self,
        connection_id,
        order_id,
        token,
        updated,
        payload,
        payload_hash,
        resolve,
        expected_revision,
        started,
        connection,
    ):
        # The same live session owns both advisory locks and this short commit.
        # A ping followed by a pooled transaction would leave a loss/commit race.
        with connection.transaction():
            connection.execute("SET LOCAL lock_timeout='5s'")
            head = connection.execute(
                "SELECT * FROM commerce_order_heads WHERE connection_id=%s AND order_id=%s "
                "FOR UPDATE",
                (connection_id, order_id),
            ).fetchone()
            if head is None or head["fetch_token"] != token:
                raise OrderError("order_fetch_claim_lost")
            if resolve and expected_revision != head["revision"]:
                raise OrderError("order_revision_conflict")
            links = self._links(connection, connection_id, payload)
            revision = head["revision"]
            reason = "unchanged"
            if revision and updated < head["source_updated_at"]:
                reason = "stale_read_ignored"
            elif (
                revision
                and updated == head["source_updated_at"]
                and (payload_hash != head["payload_sha256"] or head["state"] == "conflict")
                and not resolve
            ):
                connection.execute(
                    "INSERT INTO commerce_order_conflicts(connection_id,order_id,source_updated_at,"
                    "payload_sha256,payload) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (connection_id, order_id, updated, payload_hash, Jsonb(payload)),
                )
                connection.execute(
                    "UPDATE commerce_order_heads SET state='conflict' "
                    "WHERE connection_id=%s AND order_id=%s",
                    (connection_id, order_id),
                )
                reason = "same_timestamp_conflict"
            else:
                current = connection.execute(
                    "SELECT line_links FROM commerce_order_snapshots "
                    "WHERE connection_id=%s AND order_id=%s AND revision=%s",
                    (connection_id, order_id, revision),
                ).fetchone()
                unchanged = (
                    revision
                    and updated == head["source_updated_at"]
                    and payload_hash == head["payload_sha256"]
                    and current
                    and current["line_links"] == links
                    and not resolve
                )
                if not unchanged:
                    revision += 1
                    reason = "operator_reconciled" if resolve else "source_or_listing_refreshed"
                    connection.execute(
                        "INSERT INTO commerce_order_snapshots(connection_id,order_id,revision,"
                        "source_updated_at,payload_sha256,payload,line_links,reason) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            connection_id,
                            order_id,
                            revision,
                            updated,
                            payload_hash,
                            Jsonb(payload),
                            Jsonb(links),
                            reason,
                        ),
                    )
                    connection.execute(
                        "UPDATE commerce_order_heads SET revision=%s,state='current',"
                        "source_updated_at=%s,payload_sha256=%s "
                        "WHERE connection_id=%s AND order_id=%s",
                        (revision, updated, payload_hash, connection_id, order_id),
                    )
            # A stale/conflicting read cannot acknowledge a hint as successfully reconciled.
            if reason not in {"stale_read_ignored", "same_timestamp_conflict"}:
                connection.execute(
                    "UPDATE commerce_order_inbox SET state='completed',"
                    "completed_at=clock_timestamp(),completed_revision=%s WHERE connection_id=%s "
                    "AND order_id=%s AND state='pending' AND received_at<=%s",
                    (revision, connection_id, order_id, started),
                )
            return reason

    @staticmethod
    def _links(connection, connection_id, payload):
        candidates = connection.execute(
            "SELECT * FROM commerce_order_listings WHERE connection_id=%s AND product_id=ANY(%s) "
            "ORDER BY operation_id LIMIT 1001",
            (
                connection_id,
                list(
                    {
                        line["product_id"]
                        for line in payload["lines"]
                        if line["product_id"] is not None
                    }
                ),
            ),
        ).fetchall()
        if len(candidates) > 1000:
            return [
                {"line_id": line["line_id"], "state": "lookup_limit"} for line in payload["lines"]
            ]
        links = []
        for line in payload["lines"]:
            matched = [
                row
                for row in candidates
                if row["product_id"] == line["product_id"]
                and row["variant_id"] == line["variant_id"]
                and row["sku"] == line["sku"]
            ]
            anchors = {
                (
                    row["canonical_product_id"],
                    row["dossier_id"],
                    row["dossier_revision"],
                    row["currency"],
                )
                for row in matched
            }
            value = {"line_id": line["line_id"], "state": "unmatched"}
            if len(anchors) > 1:
                value["state"] = "ambiguous"
            elif anchors:
                product, dossier, revision, currency = next(iter(anchors))
                value["state"] = (
                    "matched" if currency == payload["currency"] else "currency_mismatch"
                )
                if value["state"] == "matched":
                    value.update(
                        canonical_product_id=product,
                        dossier_id=dossier,
                        dossier_revision=revision,
                        operation_id=matched[0]["operation_id"],
                    )
            links.append(value)
        return links

    def reconcile_page(
        self,
        provider: CommerceProvider,
        updated_after: str,
        cursor: str | None = None,
        synthetic: bool = False,
    ):
        connection_id = self._provider(provider, synthetic)
        timestamp(updated_after)
        if cursor is not None:
            identifier(cursor, 512)
        page = self._provider_call(provider.list_order_ids, updated_after, cursor)
        if len(page.order_ids) > 50 or (
            page.next_cursor is not None
            and (
                not isinstance(page.next_cursor, str)
                or not 1 <= len(page.next_cursor) <= 512
                or page.next_cursor == cursor
            )
        ):
            raise OrderError("invalid_order_page")
        results = []
        for order_id in dict.fromkeys(page.order_ids):
            result = self.reconcile_order(provider, order_id, synthetic=synthetic)
            results.append(
                {
                    key: result[key]
                    for key in (
                        "order_id",
                        "revision",
                        "state",
                        "reference",
                        "reconciliation_state",
                    )
                }
            )
        return {
            "connection_id": connection_id,
            "orders": results,
            "next_cursor": page.next_cursor,
            "coverage_complete": False,
        }

    def orders(
        self,
        connection_id: str,
        synthetic: bool = False,
        limit: int = 100,
        after: str | None = None,
    ):
        self._require()
        self._workspace(synthetic)
        bounded_page(limit)
        rows = self.catalog.all(
            "SELECT h.order_id,h.revision,h.state,h.source_updated_at FROM commerce_order_heads h "
            "JOIN commerce_order_connections c USING(connection_id) WHERE h.connection_id=%s "
            "AND c.synthetic=%s AND h.revision>0 AND h.order_id>%s ORDER BY h.order_id LIMIT %s",
            (connection_id, synthetic, after or "", limit + 1),
        )
        return {
            "items": rows[:limit],
            "next_cursor": rows[limit - 1]["order_id"] if len(rows) > limit else None,
        }

    def inbox(
        self,
        connection_id: str,
        synthetic: bool = False,
        limit: int = 100,
        after: str | None = None,
        pending_only: bool = True,
    ):
        self._require()
        self._workspace(synthetic)
        bounded_page(limit)
        if type(pending_only) is not bool:
            raise OrderError("invalid_inbox_filter")
        pending = "AND i.state='pending' " if pending_only else ""
        rows = self.catalog.all(
            "SELECT i.body_sha256,i.order_id,i.state,i.received_at,i.completed_at,"
            "i.completed_revision "
            "FROM commerce_order_inbox i JOIN commerce_order_connections c USING(connection_id) "
            "WHERE i.connection_id=%s AND c.synthetic=%s AND i.body_sha256>%s "
            + pending
            + "ORDER BY i.body_sha256 LIMIT %s",
            (connection_id, synthetic, after or "", limit + 1),
        )
        return {
            "items": rows[:limit],
            "next_cursor": rows[limit - 1]["body_sha256"] if len(rows) > limit else None,
        }

    def order(
        self,
        connection_id: str,
        order_id: str,
        synthetic: bool = False,
        revision: int | None = None,
    ):
        self._require()
        self._workspace(synthetic)
        if revision is not None and (type(revision) is not int or revision < 1):
            raise OrderError("invalid_revision")
        row = self.catalog.one(
            "SELECT s.*,h.state AS head_state,h.revision AS current_revision,c.synthetic "
            "FROM commerce_order_heads h "
            "JOIN commerce_order_connections c USING(connection_id) "
            "JOIN commerce_order_snapshots s ON s.connection_id=h.connection_id "
            "AND s.order_id=h.order_id AND s.revision=COALESCE(%s,h.revision) "
            "WHERE h.connection_id=%s AND h.order_id=%s AND c.synthetic=%s",
            (revision, connection_id, order_id, synthetic),
        )
        if row is None:
            raise OrderError("order_not_found")
        row["state"] = (
            row["head_state"] if row["revision"] == row["current_revision"] else "historical"
        )
        row["reference"] = snapshot_reference(connection_id, order_id, row["revision"])
        row["financial_allocation"] = "unallocated_order_totals"
        row["cash_complete"] = False
        row["events"] = self.catalog.all(
            "SELECT DISTINCT ON(kind) kind,revision,payload,order_revision,available_at "
            "FROM commerce_order_events WHERE connection_id=%s AND order_id=%s "
            "ORDER BY kind,revision DESC",
            (connection_id, order_id),
        )
        return row

    def _event(
        self, kind, connection_id, order_id, request_id, payload, expected_revision, synthetic
    ):
        self._require()
        self._workspace(synthetic)
        validate_event(kind, payload)
        return append_event(
            self.catalog,
            kind,
            connection_id,
            order_id,
            request_id,
            payload,
            expected_revision,
            synthetic,
        )

    def handoff(
        self,
        connection_id,
        order_id,
        request_id,
        supplier_reference,
        line_ids,
        expected_revision=0,
        synthetic=False,
        state="prepared",
    ):
        if not isinstance(line_ids, list | tuple) or not 1 <= len(line_ids) <= 100:
            raise OrderError("invalid_handoff_lines")
        return self._event(
            "handoff",
            connection_id,
            order_id,
            request_id,
            {
                "supplier_reference": supplier_reference,
                "line_ids": list(line_ids),
                "state": state,
                "external_action": "none",
                "source": "manual_operator",
            },
            expected_revision,
            synthetic,
        )

    def tracking(
        self,
        connection_id,
        order_id,
        request_id,
        handoff_revision,
        carrier,
        tracking_reference,
        expected_revision=0,
        synthetic=False,
        status="recorded",
    ):
        return self._event(
            "tracking",
            connection_id,
            order_id,
            request_id,
            {
                "handoff_revision": handoff_revision,
                "carrier": carrier,
                "tracking_reference": tracking_reference,
                "status": status,
                "source": "manual_operator",
                "carrier_verified": False,
            },
            expected_revision,
            synthetic,
        )

    def request_notification(
        self,
        connection_id,
        order_id,
        request_id,
        notification_kind,
        expected_revision=0,
        synthetic=False,
    ):
        return self._event(
            "notification",
            connection_id,
            order_id,
            request_id,
            {
                "notification_kind": notification_kind,
                "state": "requested",
                "external_action": "none",
                "confirmed_delivered": False,
            },
            expected_revision,
            synthetic,
        )
