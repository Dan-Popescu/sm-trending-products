"""Transactional launch revisions behind one private operator interface."""

from __future__ import annotations

from html import escape

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..contracts import digest
from ..module_schema import initialize_module, require_module
from ._assessment import assess
from ._schema import MIGRATIONS
from .models import LaunchDraft, LaunchError


class LaunchService:
    """Prepare scenarios without sending any storefront, payment or supplier request."""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "commerce", MIGRATIONS)

    def _require(self) -> None:
        try:
            require_module(self.catalog, "commerce")
        except ValueError as error:
            raise LaunchError("not_initialized", str(error)) from error

    def save(self, draft: LaunchDraft, expected_revision: int, request_id: str) -> dict:
        """Append with compare-and-swap; request IDs are scoped to a stable dossier.

        Replays return their original revision even after later edits. Revalidation
        here also rejects model_copy bypasses or mutable nested model values.
        """
        draft = LaunchDraft.model_validate(dict(draft))
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or not 0 <= expected_revision < 2_147_483_647
        ):
            raise LaunchError("invalid_revision")
        if (
            not isinstance(request_id, str)
            or not 1 <= len(request_id) <= 128
            or request_id != request_id.strip()
            or any(ord(char) < 32 for char in request_id)
        ):
            raise LaunchError("invalid_request_id")
        self._require()
        payload = draft.model_dump(mode="json")
        fingerprint = digest({"draft": payload, "expected_revision": expected_revision})
        with self.catalog.transaction() as connection:
            # The absent first revision also needs serialization; a row lock alone
            # would not protect two concurrent creates. This does not lock capture.
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended("
                "current_schema()||':commerce-dossier:'||%s,492371241))",
                (draft.dossier_id,),
            )
            product = connection.execute(
                "SELECT synthetic FROM products WHERE product_id=%s FOR SHARE",
                (draft.product_id,),
            ).fetchone()
            if product is None:
                raise LaunchError("unknown_product")
            if product["synthetic"] != draft.synthetic:
                raise LaunchError("workspace_mismatch")
            current = connection.execute(
                "SELECT * FROM commerce_dossiers WHERE dossier_id=%s FOR UPDATE",
                (draft.dossier_id,),
            ).fetchone()
            if current is not None and any(
                current[field] != getattr(draft, field)
                for field in ("product_id", "country", "currency", "synthetic")
            ):
                raise LaunchError("immutable_identity")
            replay = connection.execute(
                "SELECT r.*,d.created_at,clock_timestamp() AS evaluated_at "
                "FROM commerce_dossier_revisions r JOIN commerce_dossiers d USING(dossier_id) "
                "WHERE r.dossier_id=%s AND r.request_id=%s",
                (draft.dossier_id, request_id),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != fingerprint:
                    raise LaunchError("request_conflict")
                return self._record(replay)
            revision = current["revision"] if current else 0
            if expected_revision != revision:
                raise LaunchError("revision_conflict")
            clock = connection.execute("SELECT clock_timestamp() AS now").fetchone()
            assert clock is not None
            timestamp = clock["now"]
            if current is None:
                connection.execute(
                    "INSERT INTO commerce_dossiers(dossier_id,product_id,country,currency,"
                    "synthetic,revision,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        draft.dossier_id,
                        draft.product_id,
                        draft.country,
                        draft.currency,
                        draft.synthetic,
                        revision + 1,
                        timestamp,
                        timestamp,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE commerce_dossiers SET revision=%s,updated_at=%s WHERE dossier_id=%s",
                    (revision + 1, timestamp, draft.dossier_id),
                )
            connection.execute(
                "INSERT INTO commerce_dossier_revisions(dossier_id,revision,request_id,"
                "request_hash,draft,available_at) VALUES(%s,%s,%s,%s,%s,%s)",
                (
                    draft.dossier_id,
                    revision + 1,
                    request_id,
                    fingerprint,
                    Jsonb(payload),
                    timestamp,
                ),
            )
            return self._record(
                {
                    "draft": payload,
                    "revision": revision + 1,
                    "created_at": current["created_at"] if current else timestamp,
                    "available_at": timestamp,
                    "evaluated_at": timestamp,
                }
            )

    def dossier(self, dossier_id: str, synthetic: bool = False) -> dict:
        self._require()
        self._workspace(synthetic)
        row = self.catalog.one(
            "SELECT r.*,d.created_at,clock_timestamp() AS evaluated_at "
            "FROM commerce_dossiers d JOIN commerce_dossier_revisions r "
            "ON r.dossier_id=d.dossier_id AND r.revision=d.revision "
            "JOIN products p ON p.product_id=d.product_id AND p.synthetic=d.synthetic "
            "WHERE d.dossier_id=%s AND d.synthetic=%s",
            (dossier_id, synthetic),
        )
        if row is None:
            raise LaunchError("not_found")
        return self._record(row)

    def dossiers(self, synthetic: bool = False, limit: int = 100, offset: int = 0) -> list[dict]:
        self._require()
        self._workspace(synthetic)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1000
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or not 0 <= offset <= 1_000_000
        ):
            raise LaunchError("invalid_page")
        rows = self.catalog.all(
            "SELECT r.*,d.created_at,clock_timestamp() AS evaluated_at "
            "FROM commerce_dossiers d JOIN commerce_dossier_revisions r "
            "ON r.dossier_id=d.dossier_id AND r.revision=d.revision "
            "JOIN products p ON p.product_id=d.product_id AND p.synthetic=d.synthetic "
            "WHERE d.synthetic=%s ORDER BY d.updated_at DESC,d.dossier_id LIMIT %s OFFSET %s",
            (synthetic, limit, offset),
        )
        return [self._record(row) for row in rows]

    @staticmethod
    def _workspace(synthetic: bool) -> None:
        if not isinstance(synthetic, bool):
            raise LaunchError("invalid_workspace")

    @staticmethod
    def _record(row: dict) -> dict:
        draft = LaunchDraft.model_validate(row["draft"])
        readiness, economics = assess(draft, row["evaluated_at"])
        return {
            **{
                field: getattr(draft, field)
                for field in ("dossier_id", "product_id", "country", "currency", "synthetic")
            },
            "revision": row["revision"],
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["available_at"].isoformat(),
            "draft": draft.model_dump(mode="json"),
            "readiness": readiness,
            "economics": economics,
        }

    def draft_page(self, dossier_id: str, synthetic: bool = False) -> str:
        """Render authored copy as text, never raw source evidence or active links."""
        record = self.dossier(dossier_id, synthetic)
        draft = record["draft"]
        title = escape(draft["title"])
        description = escape(draft["description"]).replace("\n", "<br>")
        price = (
            "Price not declared"
            if draft["retail_price"] is None
            else (f"{escape(draft['retail_price'])} {escape(draft['currency'])}")
        )
        return (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="robots" content="noindex,nofollow">'
            f"<title>Private draft: {title}</title></head><body>"
            "<p>Private product-page draft. No checkout or publication is enabled.</p>"
            f"<h1>{title}</h1><p>{description}</p><p>{price}</p>"
            f"<p>Country: {escape(draft['country'])}</p>"
            f"<p>Returns: {escape(draft['return_terms'])}</p>"
            "<p>Commercial figures are scenarios, not measured profit.</p></body></html>"
        )
