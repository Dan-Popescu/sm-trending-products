"""Bounded read-only DuckDB exploration over verified immutable Parquet inputs.

Hardening follows https://duckdb.org/docs/current/operations_manual/securing_duckdb/overview
and https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions.
SQL parsing and tokenization use DuckDB's own API, not a text-only SQL allowlist.
"""

from __future__ import annotations

import math
import tempfile
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from .cache import pin_cached
from .contracts import canonical, utc
from .settings import Settings

if TYPE_CHECKING:
    from .history import History

QUERY_TIMEOUT_MS = 30000
MAX_INPUT_FILES = 10000
COLUMNS = {
    "schema_version": "VARCHAR",
    "observation_id": "VARCHAR",
    "source": "VARCHAR",
    "entity_type": "VARCHAR",
    "entity_id": "VARCHAR",
    "entity_key": "VARCHAR",
    "observed_at": "TIMESTAMPTZ",
    "available_at": "TIMESTAMPTZ",
    "published_at": "TIMESTAMPTZ",
    "synthetic": "BOOLEAN",
    "market": "VARCHAR",
    "account_id": "VARCHAR",
    "metrics_json": "JSON",
    "attributes_json": "JSON",
    "missing_fields_json": "JSON",
    "raw_dataset_id": "VARCHAR",
}


@dataclass(frozen=True)
class ParquetInput:
    path: Path
    dataset_id: str
    dataset_known_at: datetime


def _select_statement(connection: Any, sql_query: str) -> str:
    if (
        not isinstance(sql_query, str)
        or not sql_query.strip()
        or len(sql_query.encode()) > 1024 * 1024
    ):
        raise ValueError("SQL must be nonempty and no larger than one MiB")
    try:
        statements = connection.extract_statements(sql_query)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError("Exactly one SELECT or WITH query is permitted")
        tokens = [
            (position, kind)
            for position, kind in duckdb.tokenize(sql_query)
            if kind != duckdb.token_type.comment
        ]
        if not tokens:
            raise ValueError("Exactly one SELECT or WITH query is permitted")
        all_tokens = duckdb.tokenize(sql_query)
        ends = {
            position: all_tokens[index + 1][0] if index + 1 < len(all_tokens) else len(sql_query)
            for index, (position, _) in enumerate(all_tokens)
        }
        first = sql_query[tokens[0][0] : ends[tokens[0][0]]].strip().upper()
        if first not in {"SELECT", "WITH"}:
            raise ValueError("Exactly one SELECT or WITH query is permitted")
        while tokens and sql_query[tokens[-1][0] : ends[tokens[-1][0]]].strip() == ";":
            tokens.pop()
        return sql_query[: ends[tokens[-1][0]]].rstrip()
    except duckdb.Error as error:
        raise ValueError("Invalid exploration SQL") from error


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _register_observations(connection: Any, inputs: Sequence[ParquetInput]) -> None:
    cast_columns = ",".join(
        f'CAST(p."{name}" AS {kind}) AS "{name}"' for name, kind in COLUMNS.items()
    )
    if inputs:
        connection.read_parquet(
            sorted({str(item.path.resolve()) for item in inputs}), union_by_name=True, filename=True
        ).create_view("__parquet_inputs")
        connection.execute(
            "CREATE TEMP TABLE __dataset_inputs("
            "filename VARCHAR,dataset_id VARCHAR,dataset_known_at TIMESTAMPTZ)"
        )
        connection.executemany(
            "INSERT INTO __dataset_inputs VALUES(?,?,?)",
            [
                (str(item.path.resolve()), item.dataset_id, utc(item.dataset_known_at))
                for item in inputs
            ],
        )
        connection.execute(
            "CREATE TEMP VIEW observation_versions AS SELECT "
            + cast_columns
            + ",CAST(p.attributes_json AS JSON) AS attrs_json,"
            "d.dataset_id AS source_dataset_id,"
            "d.dataset_known_at "
            "FROM __parquet_inputs p JOIN __dataset_inputs d ON p.filename=d.filename"
        )
    else:
        empty = ",".join(f'CAST(NULL AS {kind}) AS "{name}"' for name, kind in COLUMNS.items())
        connection.execute(
            "CREATE TEMP VIEW observation_versions AS SELECT "
            + empty
            + ",CAST(NULL AS JSON) AS attrs_json,CAST(NULL AS VARCHAR) AS source_dataset_id,"
            "CAST(NULL AS TIMESTAMPTZ) AS dataset_known_at WHERE FALSE"
        )
    semantics = ",".join(f'"{name}" := "{name}"' for name in COLUMNS if name != "raw_dataset_id")
    conflict = connection.execute(
        "SELECT observation_id FROM observation_versions GROUP BY observation_id "
        "HAVING count(DISTINCT sha256(to_json(struct_pack(" + semantics + "))))>1 LIMIT 1"
    ).fetchone()
    if conflict:
        raise ValueError("Conflicting payloads under one observation identifier")
    connection.execute(
        "CREATE TEMP VIEW observations AS SELECT * FROM observation_versions "
        "QUALIFY row_number() OVER (PARTITION BY observation_id "
        "ORDER BY dataset_known_at,source_dataset_id,raw_dataset_id NULLS LAST)=1"
    )


def query_parquet(
    inputs: Sequence[ParquetInput], sql_query: str, settings: Settings, *, max_rows: int = 1000
) -> dict[str, Any]:
    """Query explicitly selected local inputs with an isolated locked connection.

    ``explore`` obtains and pins these inputs from committed manifests. This lower
    boundary also supports direct verification with actual normalized Parquet.
    Both logical SQL LIMIT and fetchmany bound result materialization.
    """
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= 10000:
        raise ValueError("Result limit must be an integer between 1 and 10000")
    if len(inputs) > MAX_INPUT_FILES:
        raise ValueError("Too many input files; narrow the selected dataset range")
    root = settings.cache_dir.resolve()
    for item in inputs:
        path = item.path.resolve(strict=True)
        if item.path.is_symlink() or not path.is_relative_to(root) or not path.is_file():
            raise ValueError("Exploration inputs must be verified files inside the cache root")
        utc(item.dataset_known_at)
    settings.spool_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=settings.spool_dir, prefix="explore-") as temporary:
        workspace = Path(temporary)
        (workspace / "secrets").mkdir()
        with duckdb.connect(
            ":memory:",
            config={
                "autoload_known_extensions": False,
                "autoinstall_known_extensions": False,
                "allow_community_extensions": False,
                "allow_unsigned_extensions": False,
                "allow_persistent_secrets": False,
                "secret_directory": str(workspace / "secrets"),
            },
        ) as connection:
            statement = _select_statement(connection, sql_query)
            for key, value in {
                "memory_limit": settings.duckdb_memory_limit,
                "threads": settings.duckdb_threads,
                "TimeZone": "UTC",
                "temp_directory": str(workspace / "spill"),
                "max_temp_directory_size": f"{settings.cache_max_bytes}B",
                "max_execution_time": QUERY_TIMEOUT_MS,
                "allowed_paths": sorted({str(item.path.resolve()) for item in inputs}),
                "enable_external_access": False,
            }.items():
                connection.execute(f"SET {key} = ?", [value])
            _register_observations(connection, inputs)
            connection.execute("SET lock_configuration=true")
            # DuckDB 2 relations are stateless; apply its native logical LIMIT
            # before executing and using the connection's bounded fetch API.
            bounded = connection.sql(statement).limit(max_rows + 1).sql_query()
            cursor = connection.execute(bounded)
            description = cursor.description or []
            fetched = cursor.fetchmany(max_rows + 1)
            return {
                "columns": [str(item[0]) for item in description],
                "types": [str(item[1]) for item in description],
                "rows": [[_json_value(value) for value in row] for row in fetched[:max_rows]],
                "row_count": min(len(fetched), max_rows),
                "truncated": len(fetched) > max_rows,
                "max_rows": max_rows,
                "input_files": len({item.path for item in inputs}),
                "dataset_ids": sorted({item.dataset_id for item in inputs}),
                "limits": {
                    "memory": settings.duckdb_memory_limit,
                    "threads": settings.duckdb_threads,
                    "query_timeout_ms": QUERY_TIMEOUT_MS,
                    "spill_bytes": settings.cache_max_bytes,
                },
                "scope": "selected_committed_datasets_only",
            }


def explore(
    history: History, manifests: list[dict], sql_query: str, *, max_rows: int = 1000
) -> dict[str, Any]:
    """Explore completed history while retention fencing protects selected evidence."""
    if any(item.get("state") != "committed" for item in manifests):
        raise ValueError("Exploration accepts completed datasets only")
    ids = sorted({item["dataset_id"] for item in manifests})
    before = dict(history.metrics)
    with history.catalog.transaction() as transaction:
        transaction.execute("SELECT pg_advisory_xact_lock_shared(492371205)")
        actual = transaction.execute(
            "SELECT * FROM datasets WHERE dataset_id=ANY(%s) ORDER BY created_at,dataset_id",
            (ids,),
        ).fetchall()
        if len(actual) != len(ids) or any(row["state"] != "committed" for row in actual):
            raise ValueError("Selected evidence was invalidated or is no longer completed")
        by_id = {item["dataset_id"]: item for item in manifests}
        if any(
            canonical(item["manifest"]) != canonical(by_id[item["dataset_id"]]["manifest"])
            for item in actual
        ):
            raise ValueError("Selected manifest differs from the authoritative completed dataset")
        if len({item["synthetic"] for item in actual}) > 1:
            raise ValueError("Real and synthetic workspaces cannot be queried together")
        objects = {obj["key"]: obj for row in actual for obj in row["manifest"]["normalized"]}
        if len(objects) > MAX_INPUT_FILES:
            raise ValueError("Too many input files; narrow the selected dataset range")
        if sum(obj["bytes"] for obj in objects.values()) > history.settings.cache_max_bytes:
            raise ValueError("Selected inputs exceed cache budget; narrow the dataset range")
        with ExitStack() as pins:
            paths = {
                key: pins.enter_context(pin_cached(history, obj)) for key, obj in objects.items()
            }
            if (
                sum(path.stat().st_size for path in paths.values())
                > history.settings.cache_max_bytes
            ):
                raise ValueError("Verified inputs exceed the configured cache budget")
            inputs = [
                ParquetInput(paths[obj["key"]], row["dataset_id"], row["created_at"])
                for row in actual
                for obj in row["manifest"]["normalized"]
            ]
            result = query_parquet(inputs, sql_query, history.settings, max_rows=max_rows)
    result["object_store_metrics"] = {
        key: history.metrics[key] - value for key, value in before.items()
    }
    return result
