"""Reviewed product identity with append-only, point-in-time decisions."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from .catalog import Catalog
from .contracts import FamilyMembership, Product, ProductLink, utc

Row = dict[str, Any]
PRODUCT_FIELDS = "product_id,name,created_at,synthetic,market"
LINK_FIELDS = (
    "product_id,entity_key,available_at,status,confidence,method,evidence_refs,decision_id"
)
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
ENTITY_KEY = re.compile(r"[A-Za-z0-9_.-]+:(video|ad|commercial_content|offer|cost):\S+\Z")


def _identifier(value: str, label: str) -> str:
    value = value.strip()
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must be a nonempty ASCII identifier of at most 128 characters")
    return value


def _entity_key(value: str) -> str:
    value = value.strip()
    if len(value) > 512 or not ENTITY_KEY.fullmatch(value):
        raise ValueError("Entity key must be a source-qualified supported entity identifier")
    return value


def _limit(value: int, maximum: int) -> int:
    if value < 1 or value > maximum:
        raise ValueError(f"Limit must be between 1 and {maximum}")
    return value


class Identity:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def products(self, synthetic: bool = False, *, limit: int = 1000, offset: int = 0) -> list[Row]:
        if offset < 0:
            raise ValueError("Offset must be nonnegative")
        return self.catalog.all(
            f"SELECT {PRODUCT_FIELDS} FROM products WHERE synthetic=%s "
            "ORDER BY created_at,product_id LIMIT %s OFFSET %s",
            (synthetic, _limit(limit, 5000), offset),
        )

    def create_product(
        self, product_id: str, name: str, synthetic: bool = False, market: str | None = None
    ) -> Row:
        product_id = _identifier(product_id, "Product ID")
        name = name.strip()
        if not name or len(name) > 1024:
            raise ValueError("Product name must contain between 1 and 1024 characters")
        if market is not None:
            market = market.strip().upper() or None
            if market is not None and not re.fullmatch(r"[A-Z0-9_-]{2,16}", market):
                raise ValueError("Market must be a short market code or unknown")
        with self.catalog.transaction() as connection:
            connection.execute(
                "INSERT INTO products(product_id,name,synthetic,market) VALUES(%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (product_id, name, synthetic, market),
            )
            row = self._product(connection, product_id)
            if any(
                row[key] != value
                for key, value in (("name", name), ("synthetic", synthetic), ("market", market))
            ):
                raise ValueError("Product ID already identifies a different product or workspace")
            return Product.model_validate(row).model_dump()

    @staticmethod
    def _database_time(connection: psycopg.Connection[Row]) -> datetime:
        row = connection.execute("SELECT clock_timestamp() AS ts").fetchone()
        assert row is not None
        return row["ts"]

    @staticmethod
    def _product(connection: psycopg.Connection[Row], product_id: str) -> Row:
        row = connection.execute(
            f"SELECT {PRODUCT_FIELDS} FROM products WHERE product_id=%s FOR KEY SHARE",
            (product_id,),
        ).fetchone()
        if row is None:
            raise ValueError("Unknown product")
        return dict(row)

    @staticmethod
    def _lock_entity(connection: psycopg.Connection[Row], entity_key: str, synthetic: bool) -> None:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"product-identity:{synthetic}:{entity_key}",),
        )

    @staticmethod
    def _validate_entity(
        connection: psycopg.Connection[Row], entity_key: str, synthetic: bool
    ) -> None:
        if (
            connection.execute(
                "SELECT entity_key FROM entities WHERE entity_key=%s AND synthetic=%s "
                "FOR KEY SHARE",
                (entity_key, synthetic),
            ).fetchone()
            is None
        ):
            raise ValueError("Entity is not recorded in the product's workspace")

    @staticmethod
    def _review_arguments(
        status: str, confidence: float, method: str, evidence_refs: Sequence[str]
    ) -> list[str]:
        if status not in {"confirmed", "proposed", "rejected"}:
            raise ValueError("Unknown link decision status")
        if not math.isfinite(confidence) or not 0 <= confidence <= 1:
            raise ValueError("Confidence must be finite and between zero and one")
        if not method or len(method) > 128:
            raise ValueError("A bounded matching method/version is required")
        refs = sorted(set(evidence_refs))
        if len(refs) > 1000 or any(not ref or len(ref) > 512 for ref in refs):
            raise ValueError("Evidence references must be nonempty bounded identifiers")
        return refs

    def review_link(
        self,
        product_id: str,
        entity_key: str,
        status: str = "confirmed",
        confidence: float = 1.0,
        method: str = "manual",
        evidence_refs: Sequence[str] = (),
    ) -> Row:
        product_id = _identifier(product_id, "Product ID")
        entity_key = _entity_key(entity_key)
        refs = self._review_arguments(status, confidence, method, evidence_refs)
        with self.catalog.transaction() as connection:
            product = self._product(connection, product_id)
            self._lock_entity(connection, entity_key, product["synthetic"])
            self._validate_entity(connection, entity_key, product["synthetic"])
            available_at = self._database_time(connection)
            return self._review(
                connection, product, entity_key, status, confidence, method, refs, available_at
            )

    def reassign_entities(
        self,
        entity_keys: Sequence[str],
        product_id: str,
        *,
        confidence: float = 1.0,
        method: str = "manual-transfer-v1",
        evidence_refs: Sequence[str] = (),
    ) -> list[Row]:
        """Transfer a reviewed group atomically; use another transfer to split or undo it."""
        product_id = _identifier(product_id, "Product ID")
        keys = list(dict.fromkeys(_entity_key(value) for value in entity_keys))
        if not keys or len(keys) > 1000:
            raise ValueError("A transfer must contain between 1 and 1000 distinct entities")
        refs = self._review_arguments("confirmed", confidence, method, evidence_refs)
        with self.catalog.transaction() as connection:
            product = self._product(connection, product_id)
            # Stable lock order prevents concurrent multi-entity transfers deadlocking.
            for key in sorted(keys):
                self._lock_entity(connection, key, product["synthetic"])
                self._validate_entity(connection, key, product["synthetic"])
            available_at = self._database_time(connection)
            return [
                self._review(
                    connection, product, key, "confirmed", confidence, method, refs, available_at
                )
                for key in keys
            ]

    @staticmethod
    def _insert_decision(
        connection: psycopg.Connection[Row],
        product_id: str,
        entity_key: str,
        status: str,
        confidence: float,
        method: str,
        refs: list[str],
        available_at: datetime,
    ) -> Row:
        row = connection.execute(
            "INSERT INTO product_links(decision_id,product_id,entity_key,available_at,status,"
            "confidence,method,evidence_refs) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
            f"RETURNING {LINK_FIELDS}",
            (
                uuid4().hex,
                product_id,
                entity_key,
                available_at,
                status,
                confidence,
                method,
                Jsonb(refs),
            ),
        ).fetchone()
        return ProductLink.model_validate(row).model_dump()

    def _review(
        self,
        connection: psycopg.Connection[Row],
        product: Row,
        entity_key: str,
        status: str,
        confidence: float,
        method: str,
        refs: list[str],
        available_at: datetime,
    ) -> Row:
        if status == "confirmed":
            # A rejection for a different product is not a global unlink operation.
            owners = connection.execute(
                "WITH latest AS (SELECT DISTINCT ON(l.product_id) l.* "
                "FROM product_links l JOIN products p USING(product_id) "
                "WHERE l.entity_key=%s AND p.synthetic=%s AND l.available_at<=%s "
                "ORDER BY l.product_id,l.available_at DESC,l.id DESC) "
                "SELECT * FROM latest WHERE status='confirmed' AND product_id<>%s",
                (entity_key, product["synthetic"], available_at, product["product_id"]),
            ).fetchall()
            for owner in owners:
                self._insert_decision(
                    connection,
                    owner["product_id"],
                    entity_key,
                    "rejected",
                    owner["confidence"],
                    "superseded-by-transfer-v1",
                    refs,
                    available_at,
                )
        current = connection.execute(
            f"SELECT {LINK_FIELDS} FROM product_links "
            "WHERE product_id=%s AND entity_key=%s AND available_at<=%s "
            "ORDER BY available_at DESC,id DESC LIMIT 1",
            (product["product_id"], entity_key, available_at),
        ).fetchone()
        if current and all(
            current[key] == value
            for key, value in (
                ("status", status),
                ("confidence", confidence),
                ("method", method),
                ("evidence_refs", refs),
            )
        ):
            return ProductLink.model_validate(current).model_dump()
        return self._insert_decision(
            connection,
            product["product_id"],
            entity_key,
            status,
            confidence,
            method,
            refs,
            available_at,
        )

    def links(self, as_of: datetime, synthetic: bool = False) -> list[Row]:
        """Current reviewed hypotheses at a cutoff; use link_history for replay."""
        cutoff = utc(as_of)
        return self.catalog.all(
            "WITH latest AS (SELECT DISTINCT ON(l.product_id,l.entity_key) l.* "
            "FROM product_links l JOIN products p USING(product_id) "
            "WHERE p.synthetic=%s AND p.created_at<=%s AND l.available_at<=%s "
            "ORDER BY l.product_id,l.entity_key,l.available_at DESC,l.id DESC), "
            "owners AS (SELECT DISTINCT ON(entity_key) decision_id FROM latest "
            "WHERE status='confirmed' ORDER BY entity_key,available_at DESC,id DESC) "
            f"SELECT {LINK_FIELDS} FROM latest WHERE status<>'confirmed' "
            "OR decision_id IN(SELECT decision_id FROM owners) "
            "ORDER BY available_at,id",
            (synthetic, cutoff, cutoff),
        )

    def link_history(self, as_of: datetime, synthetic: bool = False) -> list[Row]:
        cutoff = utc(as_of)
        fields = ",".join(f"l.{field}" for field in LINK_FIELDS.split(","))
        return self.catalog.all(
            f"SELECT {fields} FROM product_links l JOIN products p USING(product_id) "
            "WHERE p.synthetic=%s AND p.created_at<=%s AND l.available_at<=%s "
            "ORDER BY l.available_at,l.id",
            (synthetic, cutoff, cutoff),
        )

    def set_family(self, product_id: str, family_id: str) -> Row:
        product_id = _identifier(product_id, "Product ID")
        family_id = _identifier(family_id, "Family ID")
        with self.catalog.transaction() as connection:
            self._product(connection, product_id)
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"product-family:{product_id}",),
            )
            current = connection.execute(
                "SELECT product_id,family_id,available_at FROM family_memberships "
                "WHERE product_id=%s ORDER BY available_at DESC,id DESC LIMIT 1",
                (product_id,),
            ).fetchone()
            if current and current["family_id"] == family_id:
                return FamilyMembership.model_validate(current).model_dump()
            row = connection.execute(
                "INSERT INTO family_memberships(product_id,family_id) VALUES(%s,%s) "
                "RETURNING product_id,family_id,available_at",
                (product_id, family_id),
            ).fetchone()
            return FamilyMembership.model_validate(row).model_dump()

    def families(self, as_of: datetime, synthetic: bool = False) -> list[Row]:
        cutoff = utc(as_of)
        return self.catalog.all(
            "SELECT DISTINCT ON(f.product_id) f.product_id,f.family_id,f.available_at "
            "FROM family_memberships f JOIN products p USING(product_id) "
            "WHERE p.synthetic=%s AND p.created_at<=%s AND f.available_at<=%s "
            "ORDER BY f.product_id,f.available_at DESC,f.id DESC",
            (synthetic, cutoff, cutoff),
        )

    def family_history(self, as_of: datetime, synthetic: bool = False) -> list[Row]:
        cutoff = utc(as_of)
        return self.catalog.all(
            "SELECT f.product_id,f.family_id,f.available_at FROM family_memberships f "
            "JOIN products p USING(product_id) WHERE p.synthetic=%s AND p.created_at<=%s "
            "AND f.available_at<=%s ORDER BY f.available_at,f.id",
            (synthetic, cutoff, cutoff),
        )

    def suggest_links(self, query: str, synthetic: bool = False, limit: int = 20) -> list[Row]:
        query = " ".join(query.casefold().split())
        if not query or len(query) > 256:
            raise ValueError("Candidate query must contain between 1 and 256 characters")
        limit = _limit(limit, 100)
        candidates = self.catalog.all(
            "SELECT entity_key,title,source,entity_type,synthetic FROM entities "
            "WHERE synthetic=%s AND to_tsvector('simple',title) @@ plainto_tsquery('simple',%s) "
            "ORDER BY ts_rank_cd(to_tsvector('simple',title),plainto_tsquery('simple',%s)) DESC,"
            "entity_key LIMIT 1000",
            (synthetic, query, query),
        )
        query_terms = set(query.split())
        ranked = []
        for row in candidates:
            title = " ".join(str(row["title"]).casefold().split())
            terms = set(title.split())
            overlap = len(query_terms & terms) / len(query_terms | terms)
            similarity = (SequenceMatcher(None, query, title).ratio() + overlap) / 2
            ranked.append(
                {
                    **row,
                    "score": round(similarity, 6),
                    "method": "text-candidate-v1",
                    "requires_review": True,
                }
            )
        return sorted(ranked, key=lambda row: (-row["score"], row["entity_key"]))[:limit]
