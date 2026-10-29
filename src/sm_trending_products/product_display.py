"""Bounded safe product labels while retaining canonical selection values."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence

from .display_safety import sanitize_display

MAX_PRODUCTS = 210
MAX_LABEL_BYTES = 4_000_000


def product_labels(
    product_ids: Sequence[str], *, secret_values: Sequence[str] = ()
) -> dict[str, str]:
    """Keep distinct ordinary labels; disambiguate redacted collisions or refuse.

    The map keys remain the original IDs used by actions. This pure function
    reads no runtime credentials and never changes the supplied sequence.
    """
    if (
        not isinstance(product_ids, Sequence)
        or isinstance(product_ids, (str, bytes, bytearray))
        or len(product_ids) > MAX_PRODUCTS
    ):
        raise ValueError("Invalid bounded product selection")
    identifiers = [product_ids[index] for index in range(len(product_ids))]
    if any(not isinstance(identifier, str) for identifier in identifiers):
        raise ValueError("Product identifiers require strings")
    labels = sanitize_display(identifiers, secret_values=secret_values, max_nodes=MAX_PRODUCTS + 1)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("Product identifiers must be unique")
    counts = Counter(labels)
    if any(count > 1 for count in counts.values()):
        labels = sanitize_display(
            [
                f"{index + 1} · {label}" if counts[label] > 1 else label
                for index, label in enumerate(labels)
            ],
            secret_values=secret_values,
            max_nodes=MAX_PRODUCTS + 1,
        )
    if (
        len(set(labels)) != len(identifiers)
        or sum(len(label.encode()) for label in labels) > MAX_LABEL_BYTES
    ):
        raise ValueError("Product display labels are unavailable")
    return dict(zip(identifiers, labels, strict=True))
