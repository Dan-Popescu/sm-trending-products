"""Measure bounded Parquet layouts without contacting SQL or object storage."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import resource
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import polars as pl

from sm_trending_products.contracts import Observation
from sm_trending_products.history import History


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Example: POLARS_MAX_THREADS=2 PYTHONPATH=src "
            "python scripts/benchmark-parquet-layout.py --rows 20000 --target 5000"
        ),
    )
    parser.add_argument("--rows", type=int, required=True, help="10–100,000 observations")
    parser.add_argument("--target", type=int, required=True, help="1–100,000 target rows/file")
    args = parser.parse_args()
    if not 10 <= args.rows <= 100_000 or not 1 <= args.target <= 100_000:
        parser.error("Use --rows 10..100000 and --target 1..100000; see --help for an example")
    entity_count = min(2000, args.rows // 10)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        Observation.model_validate(
            {
                "observation_id": hashlib.sha256(f"{i}".encode()).hexdigest(),
                "source": "parquet_fixture",
                "entity_type": "video",
                "entity_id": f"video-{i % entity_count:05}",
                "observed_at": start + timedelta(minutes=i // entity_count * 15),
                "available_at": start + timedelta(minutes=i // entity_count * 15),
                "published_at": start - timedelta(days=7),
                "synthetic": True,
                "metrics": {
                    "view_count": {"lower": i * 101, "upper": i * 101},
                    "like_count": {"lower": i * 5, "upper": i * 5},
                },
                "attributes": {
                    "creator_id": f"creator-{i % entity_count % 91:03}",
                    "title": f"Useful physical product {i % entity_count % 50}",
                    "description": "Product details and a practical demonstration",
                },
                "raw_dataset_id": hashlib.sha256(f"parent-{i // 20}".encode()).hexdigest(),
            }
        )
        for i in range(args.rows)
    ]
    # The measured methods use only settings and the unchanged production encoder.
    # Skip History initialization so this probe has no disk, database or S3 effects.
    history = History.__new__(History)
    history.settings = SimpleNamespace(  # type: ignore[assignment]
        history_partition_buckets=64, history_target_rows_per_file=1000
    )
    cpu, wall = time.process_time(), time.perf_counter()
    objects, buckets = history.normalized_objects(
        rows, "measurement", target_rows_per_file=args.target
    )
    encode_cpu, encode_wall = time.process_time() - cpu, time.perf_counter() - wall
    entity_key = rows[0].entity_key
    selected = [(obj, body) for obj, body in objects if entity_key in obj["entity_keys"]]
    cpu, wall = time.process_time(), time.perf_counter()
    selected_rows = sum(
        pl.read_parquet(io.BytesIO(body)).filter(pl.col("entity_key") == entity_key).height
        for _, body in selected
    )
    selected_cpu, selected_wall = time.process_time() - cpu, time.perf_counter() - wall
    cpu, wall = time.process_time(), time.perf_counter()
    decoded = [pl.read_parquet(io.BytesIO(body)) for _, body in objects]
    scan_cpu, scan_wall = time.process_time() - cpu, time.perf_counter() - wall
    actual = pl.concat(decoded).sort("observation_id")
    expected = sorted((row.observation_id, row.raw_dataset_id) for row in rows)
    assert list(actual.select("observation_id", "raw_dataset_id").iter_rows()) == expected
    assert selected_rows == sum(row.entity_key == entity_key for row in rows)
    print(
        json.dumps(
            {
                "rows": args.rows,
                "target": args.target,
                "entities": entity_count,
                "objects": len(objects),
                "buckets": buckets,
                "bytes": sum(len(body) for _, body in objects),
                "max_object_bytes": max(len(body) for _, body in objects),
                "one_entity_get_bytes": sum(obj["bytes"] for obj, _ in selected),
                "one_entity_rows": selected_rows,
                "encode_cpu_s": round(encode_cpu, 4),
                "encode_wall_s": round(encode_wall, 4),
                "scan_cpu_s": round(scan_cpu, 4),
                "scan_wall_s": round(scan_wall, 4),
                "one_entity_cpu_s": round(selected_cpu, 4),
                "one_entity_wall_s": round(selected_wall, 4),
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "verified_observation_and_source_ids": True,
            }
        )
    )


if __name__ == "__main__":
    main()
