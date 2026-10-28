"""Explicit additive migrations for optional modules, independent from capture SQL."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping

from .catalog import Catalog


def initialize_module(catalog: Catalog, module: str, migrations: Mapping[int, str]) -> dict:
    """Apply immutable, contiguous module migrations in one short transaction.

    Callers supply packaged application SQL, never user-provided statements.
    The module lock is unrelated to collection, retention and publication locks.
    """
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", module) or not migrations:
        raise ValueError("Invalid optional module migration set")
    if any(
        isinstance(version, bool)
        or not isinstance(version, int)
        or not isinstance(body, str)
        or not body.strip()
        for version, body in migrations.items()
    ) or list(sorted(migrations)) != list(range(1, len(migrations) + 1)):
        raise ValueError("Module migrations must be nonempty and contiguous from one")
    with catalog.transaction() as connection:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended("
            "current_schema()||':optional-module-initialization',492371240))"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS application_module_migrations ("
            "module text NOT NULL,version integer NOT NULL CHECK(version>0),"
            "sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),"
            "PRIMARY KEY(module,version))"
        )
        existing = {
            row["version"]: row["sha256"]
            for row in connection.execute(
                "SELECT version,sha256 FROM application_module_migrations WHERE module=%s",
                (module,),
            )
        }
        if set(existing) - set(migrations):
            raise ValueError("Optional module is newer than this initializer")
        if set(existing) != set(range(1, max(existing, default=0) + 1)):
            raise ValueError("Optional module migration history is incomplete")
        for version, body in sorted(migrations.items()):
            checksum = hashlib.sha256(body.encode()).hexdigest()
            if version in existing:
                if existing[version] != checksum:
                    raise ValueError("An applied optional migration was changed")
                continue
            connection.execute(body, prepare=False)
            connection.execute(
                "INSERT INTO application_module_migrations(module,version,sha256) VALUES(%s,%s,%s)",
                (module, version, checksum),
            )
    return {"module": module, "version": max(migrations), "status": "initialized"}


def require_module(catalog: Catalog, module: str, minimum_version: int = 1) -> None:
    """Read-only readiness check; optional review views never initialize tables."""
    relation = catalog.one("SELECT to_regclass('application_module_migrations') AS relation")
    if not relation or not relation["relation"]:
        raise ValueError("Optional module is not initialized; run its init command")
    row = catalog.one(
        "SELECT max(version) AS version FROM application_module_migrations WHERE module=%s",
        (module,),
    )
    if not row or row["version"] is None or row["version"] < minimum_version:
        raise ValueError("Optional module is not initialized; run its init command")
