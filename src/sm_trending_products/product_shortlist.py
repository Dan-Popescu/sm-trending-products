"""Bounded canonical product IDs for an operator's namespace-specific shortlist."""

from collections.abc import Sequence

from .catalog import Catalog
from .contracts import digest
from .identity import IDENTIFIER

MAX_SHORTLIST_PRODUCTS = 10


def shortlist_ids(value: Sequence[str]) -> tuple[str, ...]:
    """Validate before SQL or session admission; names and source facts never belong here."""
    if (
        isinstance(value, str | bytes)
        or not isinstance(value, Sequence)
        or len(value) > MAX_SHORTLIST_PRODUCTS
        or any(not isinstance(item, str) or not IDENTIFIER.fullmatch(item) for item in value)
        or len(set(value)) != len(value)
    ):
        raise ValueError("Provide at most ten distinct canonical product identifiers")
    return tuple(value)


def shortlist_workspace(catalog: Catalog, synthetic: bool) -> str:
    """An opaque context key from the actual connection, never its credential DSN."""
    if type(synthetic) is not bool:
        raise ValueError("Invalid product workspace")
    with catalog.evidence_transaction(synthetic) as connection:
        row = connection.execute("SELECT current_schema() AS schema").fetchone()
        if row is None or row["schema"] is None:
            raise ValueError("A current product workspace is required")
        return digest(
            {
                "host": connection.info.host,
                "port": connection.info.port,
                "database": connection.info.dbname,
                "role": connection.info.user,
                "schema": row["schema"],
            }
        )
