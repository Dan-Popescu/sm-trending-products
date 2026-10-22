"""Canonical source operations shared by collection entry points and refresh."""

SOURCES = {"tiktok_display", "tiktok_ads", "tiktok_commercial"}


def canonical_operation(source: str, operation: str | None = None) -> str:
    """Normalize the historical commercial ``list`` alias at every input boundary."""
    if source not in SOURCES:
        raise ValueError("Unsupported source")
    operation = operation or ("list" if source == "tiktok_display" else "query")
    if operation not in {"list", "query"}:
        raise ValueError("Unsupported source operation")
    return operation if source == "tiktok_display" else "query"
