"""Compare exact bounded compaction encoding and rejected native-column candidates.

Each invocation is one fresh process. No database, network or persistent history
is used; candidate native formats exist only inside this measurement script.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import io
import json
import resource
import tempfile
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from sm_trending_products.compaction_encoding import encode_compaction
from sm_trending_products.contracts import Observation, canonical
from sm_trending_products.history import History
from sm_trending_products.observation_codec import decode_columnar_observation

_TIMES = ("observed_at", "available_at", "published_at")
_BATCH_ROWS = 8192


def fixture(count: int) -> list[Observation]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        Observation.model_validate(
            {
                "observation_id": hashlib.sha256(str(index).encode()).hexdigest(),
                "source": "parquet_fixture",
                "entity_type": "video",
                "entity_id": f"video-{index % 2000:05}",
                "observed_at": start + timedelta(minutes=index // 2000 * 15),
                "available_at": start + timedelta(minutes=index // 2000 * 15),
                "published_at": start - timedelta(days=7),
                "synthetic": True,
                "metrics": {
                    "view_count": {
                        "lower": index * 101 if index % 7919 else 2**120,
                        "upper": index * 101 if index % 7919 else 2**120,
                    },
                    "like_count": {"lower": index * 5, "upper": index * 5},
                },
                "attributes": {
                    "creator_id": f"creator-{index % 2000 % 91:03}",
                    "title": f"Useful physical product {index % 2000 % 50}",
                    "description": "Product details and a practical demonstration",
                },
                "raw_dataset_id": hashlib.sha256(f"parent-{index // 20}".encode()).hexdigest(),
            }
        )
        for index in range(count)
    ]


def legacy(rows: Sequence[Observation]) -> bytes:
    # The encoder uses no initialized history/client state.
    return History.encode(History.__new__(History), rows)


def candidate_native(rows: list[Observation], *, typed_metrics: bool) -> bytes:
    """Rejected prototype: typed UTC clocks/offsets and optional lossless metric bounds."""
    names = (
        "schema_version",
        "observation_id",
        "source",
        "entity_type",
        "entity_id",
        "observed_at",
        "available_at",
        "synthetic",
        "published_at",
        "market",
        "account_id",
        "raw_dataset_id",
        "entity_key",
        "metrics_json",
        "attributes_json",
        "missing_fields_json",
    )
    metric = pa.list_(
        pa.struct(
            [
                ("name", pa.string()),
                ("lower", pa.uint64()),
                ("upper", pa.uint64()),
                ("lower_large", pa.string()),
                ("upper_large", pa.string()),
                ("precision", pa.string()),
                ("semantics", pa.string()),
                ("unit", pa.string()),
                ("raw", pa.string()),
                ("model_version", pa.string()),
            ]
        )
    )
    fields = [
        (
            name,
            pa.bool_()
            if name == "synthetic"
            else pa.timestamp("us", tz="UTC")
            if name in _TIMES
            else metric
            if typed_metrics and name == "metrics_json"
            else pa.null()
            if name in ("market", "account_id")
            else pa.string(),
        )
        for name in names
    ]
    schema = pa.schema([*fields, *((name + "_offset_us", pa.int64()) for name in _TIMES)])
    stream = io.BytesIO()
    with pq.ParquetWriter(stream, schema, compression="zstd") as writer:
        for start in range(0, len(rows), _BATCH_ROWS):
            encoded = []
            for observation in rows[start : start + _BATCH_ROWS]:
                row = observation.model_dump(mode="json", exclude=set(_TIMES))
                row["entity_key"] = observation.entity_key
                for name in ("metrics", "attributes", "missing_fields"):
                    row[name + "_json"] = canonical(row.pop(name))
                for name in _TIMES:
                    instant = getattr(observation, name)
                    row[name] = instant
                    row[name + "_offset_us"] = (
                        instant.utcoffset() // timedelta(microseconds=1) if instant else None
                    )
                if typed_metrics:
                    row["metrics_json"] = [
                        {
                            "name": name,
                            **{
                                key: value
                                if key not in ("lower", "upper") or value < 2**64
                                else None
                                for key, value in value.model_dump().items()
                            },
                            "lower_large": str(value.lower) if value.lower >= 2**64 else None,
                            "upper_large": str(value.upper) if value.upper >= 2**64 else None,
                        }
                        for name, value in observation.metrics.items()
                    ]
                encoded.append(row)
            writer.write_table(pa.Table.from_pylist(encoded, schema=schema))
    return stream.getvalue()


def decode(row: dict[str, Any], encoder: str) -> Observation:
    if encoder.startswith("native"):
        for name in _TIMES:
            offset = row.pop(name + "_offset_us")
            if row[name] is not None:
                row[name] = row[name].astimezone(timezone(timedelta(microseconds=offset)))
    if encoder != "native-metrics":
        return decode_columnar_observation(row)
    metrics = {}
    for metric in row.pop("metrics_json"):
        name = metric.pop("name")
        for bound in ("lower", "upper"):
            large = metric.pop(bound + "_large")
            if large is not None:
                metric[bound] = int(large)
        metrics[name] = metric
    row.pop("entity_key")
    row["metrics"] = metrics
    for name in ("attributes", "missing_fields"):
        row[name] = json.loads(row.pop(name + "_json"))
    return Observation.model_validate(row)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--encoder", choices=("legacy", "bounded", "native-time", "native-metrics"), required=True
    )
    parser.add_argument("--rows", choices=(20000, 100000), type=int, required=True)
    args = parser.parse_args()
    rows = fixture(args.rows)
    cpu, wall = time.process_time(), time.perf_counter()
    if args.encoder == "legacy":
        body = legacy(rows)
    elif args.encoder == "bounded":
        body = encode_compaction(rows, legacy)
    else:
        body = candidate_native(rows, typed_metrics=args.encoder == "native-metrics")
    encode_cpu, encode_wall = time.process_time() - cpu, time.perf_counter() - wall
    encode_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    cutoff = rows[-1].available_at
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "compaction.parquet"
        path.write_bytes(body)

        def read(*, selective: bool) -> tuple[list[Observation], float, float]:
            cpu = time.process_time()
            query = pl.scan_parquet(path)
            available = pl.col("available_at")
            observed = pl.col("observed_at")
            if not args.encoder.startswith("native"):
                available = available.str.to_datetime(time_zone="UTC")
                observed = observed.str.to_datetime(time_zone="UTC")
            query = query.filter(available <= cutoff)
            if selective:
                query = query.filter(
                    (observed >= cutoff - timedelta(hours=1))
                    & (pl.col("entity_key") == rows[0].entity_key)
                )
            frame = query.collect(engine="streaming")
            scan_cpu = time.process_time() - cpu
            cpu = time.process_time()
            observations = [decode(row, args.encoder) for row in frame.iter_rows(named=True)]
            return observations, scan_cpu, time.process_time() - cpu

        full, full_scan_cpu, full_decode_cpu = read(selective=False)
        assert full == rows
        del full
        gc.collect()
        selected, selected_scan_cpu, selected_decode_cpu = read(selective=True)
        assert selected == [
            row
            for row in rows
            if row.entity_key == rows[0].entity_key
            and row.observed_at >= cutoff - timedelta(hours=1)
        ]
    print(
        json.dumps(
            {
                "encoder": args.encoder,
                "rows": args.rows,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "encode_cpu_s": encode_cpu,
                "encode_wall_s": encode_wall,
                "encode_peak_rss_kib": encode_peak,
                "full_filter_cpu_s": full_scan_cpu,
                "full_decode_cpu_s": full_decode_cpu,
                "selective_rows": len(selected),
                "selective_filter_cpu_s": selected_scan_cpu,
                "selective_decode_cpu_s": selected_decode_cpu,
                "lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "exact_observations": True,
                "polars": pl.__version__,
            }
        )
    )


if __name__ == "__main__":
    main()
