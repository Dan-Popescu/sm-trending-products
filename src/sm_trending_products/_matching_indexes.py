"""Optional identifier indexes: explicit online setup and fail-closed indexed reads."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from unicodedata import normalize

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .catalog import Catalog

BARCODES = frozenset({"ean", "gtin", "upc"})
BARCODE_PATTERN = "^(?:[0-9]{8}|[0-9]{12}|[0-9]{13}|[0-9]{14})$"


class MatchingIndexesNotReady(ValueError):
    code = "matching_indexes_not_ready"

    def __init__(self, indexes: list[str]):
        self.indexes = indexes
        super().__init__(
            "Identifier search requires ready matching indexes. "
            "Run ProductMatcher.initialize_indexes() explicitly; retry after it completes."
        )


@dataclass(frozen=True)
class _Index:
    field: str
    name: str
    expression: str
    predicate: str


def _index(field: str) -> _Index:
    # Use PostgreSQL's canonical deparser spelling, checked against pg_index below.
    # NFKC handles compatibility characters; trim only the explicit ASCII whitespace set.
    value = f"btrim(NORMALIZE(attributes ->> '{field}'::text, NFKC), ' \t\n\r\v\f'::text)"
    expression = f"lpad({value}, 14, '0'::text)" if field in BARCODES else value
    predicate = (
        "(entity_type = ANY (ARRAY['video'::text, 'ad'::text, "
        "'commercial_content'::text, 'offer'::text])) "
        f"AND jsonb_typeof(attributes -> '{field}'::text) = 'string'::text "
        f"AND length(attributes ->> '{field}'::text) <= 256 AND "
        + (
            f"{value} ~ '{BARCODE_PATTERN}'::text"
            if field in BARCODES
            else f"length({value}) <= 256 AND {value} <> ''::text"
        )
    )
    signature = sha256((expression + predicate).encode()).hexdigest()[:10]
    return _Index(field, f"entity_match_{field}_{signature}", expression, predicate)


INDEXES = tuple(_index(field) for field in sorted(BARCODES | {"sku", "mpn", "model"}))


def _states(connection) -> dict[str, dict]:
    rows = connection.execute(
        "SELECT c.relname AS name,i.indisvalid AND i.indisready AND i.indislive AS valid, "
        "a.amname,i.indnkeyatts,i.indnatts,i.indisunique, "
        "pg_get_indexdef(i.indexrelid,1,true) AS first_key, "
        "pg_get_indexdef(i.indexrelid,2,true) AS expression, "
        "pg_get_expr(i.indpred,i.indrelid,true) AS predicate "
        "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "JOIN pg_am a ON a.oid=c.relam "
        "WHERE i.indrelid='entities'::regclass AND c.relname=ANY(%s)",
        ([item.name for item in INDEXES],),
    ).fetchall()
    return {row["name"]: row for row in rows}


def _definition_matches(state: dict, item: _Index) -> bool:
    return (
        state["amname"] == "btree"
        and state["indnkeyatts"] == state["indnatts"] == 2
        and not state["indisunique"]
        and state["first_key"] == "synthetic"
        and state["expression"] == item.expression
        and state["predicate"] == item.predicate
    )


def _require_ready(connection) -> None:
    states = _states(connection)
    missing = [
        item.name
        for item in INDEXES
        if item.name not in states
        or not states[item.name]["valid"]
        or not _definition_matches(states[item.name], item)
    ]
    if missing:
        raise MatchingIndexesNotReady(missing)


def lookup(connection, identifiers: dict[str, str]) -> tuple[str, list[str]]:
    _require_ready(connection)
    values = {
        (field, value.zfill(14) if key in BARCODES else normalize("NFKC", value))
        for key, value in identifiers.items()
        for field in (BARCODES if key in BARCODES else (key,))
    }
    clauses, params = [], []
    for item in INDEXES:
        for field, value in sorted(values):
            if field == item.field:
                clauses.append(f"({item.predicate} AND {item.expression}=%s)")
                params.append(value)
    return "(" + " OR ".join(clauses) + ")", params


def initialize(catalog: Catalog) -> dict:
    # Deliberately use a dedicated autocommit session: CIC cannot run in a transaction.
    # No acquisition advisory lock, capture protocol, trigger or SQL migration is changed.
    conninfo = catalog.pool.conninfo
    with psycopg.connect(
        conninfo() if callable(conninfo) else conninfo,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=10,
    ) as connection:
        connection.execute("SET lock_timeout='2s'")
        connection.execute("SET statement_timeout='30min'")
        connection.execute("SET maintenance_work_mem='32MB'")
        connection.execute("SET max_parallel_maintenance_workers=0")
        table = connection.execute(
            "SELECT n.nspname AS schema FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.oid='entities'::regclass"
        ).fetchone()
        if table is None:
            raise ValueError("Initialize the catalog before preparing matching indexes")
        locked = connection.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS acquired",
            (table["schema"] + ":sm_product_matching_indexes_v2",),
        ).fetchone()
        if not locked or not locked["acquired"]:
            raise ValueError("Matching index initialization is already running; retry later")
        created = []
        for item in INDEXES:
            state = _states(connection).get(item.name)
            if state and not _definition_matches(state, item):
                raise ValueError("Matching index definition differs; inspect the named index")
            if state and state["valid"]:
                continue
            qualified = sql.Identifier(table["schema"], item.name)
            if state:
                # PostgreSQL can leave an invalid concurrent build after cancellation.
                connection.execute(sql.SQL("DROP INDEX CONCURRENTLY {}").format(qualified))
            connection.execute(
                sql.SQL("CREATE INDEX CONCURRENTLY {} ON {} (synthetic,({})) WHERE {}").format(
                    sql.Identifier(item.name),
                    sql.Identifier(table["schema"], "entities"),
                    sql.SQL(item.expression),
                    sql.SQL(item.predicate),
                )
            )
            created.append(item.name)
        sizes = connection.execute(
            "SELECT c.relname AS name,pg_relation_size(c.oid) AS bytes FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=ANY(%s) ORDER BY c.relname",
            (table["schema"], [item.name for item in INDEXES]),
        ).fetchall()
        # Also refuse an unexpected/deparser-incompatible definition immediately after setup.
        _require_ready(connection)
        return {"status": "ready", "created": created, "indexes": sizes}
