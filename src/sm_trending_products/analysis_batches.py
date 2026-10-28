"""Adaptive product partitions shared by published and frozen assessments."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field

from .contracts import Product, ProductLink


class RowBudgetExceeded(ValueError):
    """A reader cannot materialize its selected scope within the configured limit."""

    def __init__(self, limit: int):
        self.limit = limit
        super().__init__(f"Analysis partition exceeds row budget ({limit}); narrow the input range")


class MetadataBudgetExceeded(ValueError):
    """Required Evidence metadata cannot fit an admitted product partition."""

    def __init__(self, resource: str, limit: int):
        self.resource, self.limit = resource, limit
        super().__init__(f"Analysis partition exceeds metadata {resource} budget ({limit})")


@dataclass
class WindowMetadataBudget:
    """Admit encoded Evidence metadata plus conservative working-set overhead."""

    max_objects: int
    max_bytes: int
    objects: int = 0
    bytes: int = 0
    declines: Counter[str] = field(default_factory=Counter)

    def admit(self, objects: int, encoded_bytes: int, *, optional: bool = False) -> bool:
        if self.objects + objects > self.max_objects:
            if optional:
                self.declines["optional_metadata_object_budget"] += 1
                return False
            raise MetadataBudgetExceeded("object", self.max_objects)
        if self.bytes + encoded_bytes > self.max_bytes:
            if optional:
                self.declines["optional_metadata_byte_budget"] += 1
                return False
            raise MetadataBudgetExceeded("byte", self.max_bytes)
        self.objects += objects
        self.bytes += encoded_bytes
        return True


def bounded_product_batches[T](
    products: Sequence[Product],
    links: Sequence[ProductLink],
    batch_size: int,
    load: Callable[[set[str]], T],
) -> Iterator[tuple[list[Product], set[str], T]]:
    """Read bounded scopes, splitting only on the reader's typed budget signal.

    A product that alone exceeds the limit remains an explicit error. Consumers
    combine compact summaries and normalize once across the complete universe.
    """
    if batch_size < 1:
        raise ValueError("Product batch size must be positive")
    keys_by_product: dict[str, set[str]] = defaultdict(set)
    for link in links:
        keys_by_product[link.product_id].add(link.entity_key)

    def read_partition(batch: list[Product]) -> Iterator[tuple[list[Product], set[str], T]]:
        keys = {key for product in batch for key in keys_by_product[product.product_id]}
        try:
            loaded = load(keys)
        except RowBudgetExceeded:
            if len(batch) < 2:
                raise
            midpoint = len(batch) // 2
            yield from read_partition(batch[:midpoint])
            yield from read_partition(batch[midpoint:])
        else:
            yield batch, keys, loaded

    for start in range(0, len(products), batch_size):
        yield from read_partition(list(products[start : start + batch_size]))
