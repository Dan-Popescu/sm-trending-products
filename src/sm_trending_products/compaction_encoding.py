"""Bound compaction's temporary row dictionaries without changing Parquet bytes."""

from __future__ import annotations

import io
from collections.abc import Callable, Sequence

import polars as pl

from .contracts import Observation, canonical

_ROW_BATCH = 8192


def encode_compaction(
    observations: Sequence[Observation],
    legacy_encoder: Callable[[Sequence[Observation]], bytes],
) -> bytes:
    """Keep small files on the old writer; batch dictionaries for large/skewed files.

    The final native columns and Zstandard writer match the legacy representation.
    Rechunking preserves identical physical bytes, including immutable staged
    checksums. Native columns still scale with one bounded compaction file; only
    temporary Python row dictionaries are limited to one 8,192-row batch.
    """
    if len(observations) <= _ROW_BATCH:
        return legacy_encoder(observations)
    frames = []
    for start in range(0, len(observations), _ROW_BATCH):
        rows = []
        for observation in observations[start : start + _ROW_BATCH]:
            row = observation.model_dump(mode="json")
            row["entity_key"] = observation.entity_key
            for name in ("metrics", "attributes", "missing_fields"):
                row[name + "_json"] = canonical(row.pop(name))
            rows.append(row)
        frames.append(pl.DataFrame(rows, infer_schema_length=None))
        del rows
    # Optional fields can be all-null in early batches and strings in later ones.
    # Relaxed concatenation reproduces whole-file inference without truncation.
    frame = pl.concat(frames, how="vertical_relaxed", rechunk=True)
    frames.clear()
    stream = io.BytesIO()
    frame.write_parquet(stream, compression="zstd")
    return stream.getvalue()
