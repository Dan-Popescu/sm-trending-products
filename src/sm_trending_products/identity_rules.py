"""Shared point-in-time ownership rules for Python and PostgreSQL consumers.

Callers supply one real/synthetic namespace and retain decisions outside their
current product partition: a newer owner must supersede an older partition's
claim. Pair-level timestamp ties follow the supplied database event sequence.
Competing owners at the latest timestamp have no defensible winner.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime

from .contracts import ProductLink, utc


def resolve_links(
    links: Sequence[ProductLink], as_of: datetime, min_confidence: float = 0.0
) -> dict[str, ProductLink]:
    """Resolve ownership first, then apply confidence without resurrecting old owners.

    Sort is stable so same-pair, same-time events retain their database order.
    UUID lexical order and confidence are never substitutes for event ordering.
    """
    if not math.isfinite(min_confidence) or not 0 <= min_confidence <= 1:
        raise ValueError("Minimum confidence must be finite and between zero and one")
    cutoff = utc(as_of)
    pairs: dict[tuple[str, str], ProductLink] = {}
    for link in sorted(links, key=lambda item: utc(item.available_at)):
        if utc(link.available_at) <= cutoff:
            pairs[(link.product_id, link.entity_key)] = link
    confirmed: dict[str, list[ProductLink]] = defaultdict(list)
    for link in pairs.values():
        if link.status == "confirmed":
            confirmed[link.entity_key].append(link)
    resolved = {}
    for entity_key in sorted(confirmed):
        candidates = confirmed[entity_key]
        latest = max(item.available_at for item in candidates)
        owners = [item for item in candidates if item.available_at == latest]
        if len(owners) == 1 and owners[0].confidence >= min_confidence:
            resolved[entity_key] = owners[0]
    return resolved


# Parameter order: synthetic namespace, product-known cutoff, link-known cutoff.
# This SQL mirrors resolve_links while keeping database consumers set-based.
# id is the database's explicit same-pair event sequence; it does not break ties
# between competing product owners, whose claims remain ambiguous.
LINK_STATE_CTES = """
current_link_pairs AS (
    SELECT DISTINCT ON(l.product_id,l.entity_key) l.*
    FROM product_links l JOIN products p USING(product_id)
    WHERE p.synthetic=%s AND p.created_at<=%s AND l.available_at<=%s
    ORDER BY l.product_id,l.entity_key,l.available_at DESC,l.id DESC
),
confirmed_link_candidates AS (
    SELECT *,
        MAX(available_at) OVER(PARTITION BY entity_key) AS latest_owner_at,
        COUNT(*) OVER(PARTITION BY entity_key,available_at) AS simultaneous_owners
    FROM current_link_pairs WHERE status='confirmed'
),
resolved_link_owners AS (
    SELECT * FROM confirmed_link_candidates
    WHERE available_at=latest_owner_at AND simultaneous_owners=1
)
"""
