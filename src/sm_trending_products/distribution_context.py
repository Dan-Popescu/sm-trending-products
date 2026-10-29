"""Shared missingness gate for comparisons of source distribution contexts."""

from __future__ import annotations

import re


def known_distribution_context(value: object) -> bool:
    """Reject missing context labels without redefining source-specific labels.

    Kernels may preserve an explicit declaration as ``declared:<value>``. This
    gate interprets missingness only; it never rewrites that label or a report.
    """
    if not isinstance(value, str):
        return False
    declared = value.strip().casefold().removeprefix("declared:").strip()
    normalized = re.sub(r"[\s_-]+", "_", declared)
    return normalized not in {
        "",
        "unknown",
        "unknown_distribution",
        "mixed",
        "unspecified",
        "not_available",
        "not_applicable",
        "unavailable",
        "n/a",
        "none",
        "null",
    }
