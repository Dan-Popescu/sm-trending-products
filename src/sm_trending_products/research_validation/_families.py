"""Conservative leakage registry; unknown families remain separate products."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from ..catalog import Catalog
from .models import ResearchError, ValidationProtocol


def family_registry(
    catalog: Catalog, protocol: ValidationProtocol, cutoff: datetime, synthetic: bool
) -> dict[str, list[str]]:
    owners = {
        product_id: name
        for name, partition in protocol.partitions.items()
        for product_id in partition.product_ids
    }
    products = catalog.all(
        "SELECT product_id FROM products WHERE product_id=ANY(%s) AND synthetic=%s "
        "AND created_at<=%s",
        (list(owners), synthetic, cutoff),
    )
    if {row["product_id"] for row in products} != set(owners):
        raise ResearchError("unknown_or_wrong_namespace_product")
    families = catalog.all(
        "SELECT product_id,family_id,available_at FROM family_memberships "
        "WHERE product_id=ANY(%s) AND available_at<=%s ORDER BY available_at,id LIMIT 10001",
        (list(owners), cutoff),
    )
    entities = catalog.all(
        "SELECT product_id,entity_key FROM product_links WHERE product_id=ANY(%s) "
        "AND status='confirmed' AND available_at<=%s ORDER BY id LIMIT 50001",
        (list(owners), cutoff),
    )
    if len(families) > 10000 or len(entities) > 50000:
        raise ResearchError("family_registry_capacity_exceeded")
    labels: dict[str, set[str]] = {key: {"product:" + key} for key in owners}
    ties: dict[tuple[str, datetime], set[str]] = defaultdict(set)
    for row in families:
        labels[row["product_id"]].add("family:" + row["family_id"])
        ties[row["product_id"], row["available_at"]].add(row["family_id"])
    if any(len(values) > 1 for values in ties.values()):
        raise ResearchError("ambiguous_family_timestamp")
    for row in entities:
        labels[row["product_id"]].add("entity:" + row["entity_key"])
    assert_separated(protocol, {key: sorted(values) for key, values in labels.items()})
    return {key: sorted(values) for key, values in labels.items()}


def assert_separated(protocol: ValidationProtocol, registry: dict[str, list[str]]) -> None:
    groups: dict[str, str] = {}
    for name, partition in protocol.partitions.items():
        for product_id in partition.product_ids:
            for label in registry[product_id]:
                if label in groups and groups[label] != name:
                    raise ResearchError("family_or_entity_crosses_partitions")
                groups[label] = name
