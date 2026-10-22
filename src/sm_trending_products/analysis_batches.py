"""Adaptive product partitions shared by published and frozen assessments."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence

from .contracts import Product, ProductLink


class RowBudgetExceeded(ValueError):
    """A reader cannot materialize its selected scope within the configured limit."""

    def __init__(self, limit: int):
        self.limit = limit
        super().__init__(f"Analysis partition exceeds row budget ({limit}); narrow the input range")


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
