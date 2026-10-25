"""Measure source observation reconstruction in a fresh process without SQL or S3."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import time
from datetime import UTC, datetime, timedelta

from sm_trending_products.contracts import Observation, canonical
from sm_trending_products.observation_codec import decode_columnar_observation


def legacy_decode(row: dict) -> Observation:
    """Exact pre-change History._decode, kept only as a measurement reference."""
    row.pop("entity_key")
    for name in ("metrics", "attributes", "missing_fields"):
        row[name] = json.loads(row.pop(name + "_json"))
    return Observation.model_validate(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Examples: PYTHONPATH=src python scripts/benchmark-observation-decode.py "
            "--decoder legacy --rows 50000; run again with --decoder shared "
            "in a separate process. No credentials or services are needed."
        ),
    )
    parser.add_argument("--decoder", choices=("legacy", "shared"), required=True)
    parser.add_argument("--rows", type=int, default=50_000, help="1–100,000 retained observations")
    args = parser.parse_args()
    if not 1 <= args.rows <= 100_000:
        parser.error("Use --rows 1..100000; see --help for examples")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    encoded = []
    for index in range(args.rows):
        at = start + timedelta(minutes=index // 2000 * 15)
        value = 2**120 + index if index % 1000 == 0 else index * 101
        encoded.append(
            {
                "schema_version": "1",
                "observation_id": hashlib.sha256(str(index).encode()).hexdigest(),
                "source": "decode_fixture",
                "entity_type": "video",
                "entity_id": f"video-{index % 2000:05}",
                "entity_key": f"decode_fixture:video:video-{index % 2000:05}",
                "observed_at": at.isoformat(),
                "available_at": at.isoformat(),
                "published_at": start.isoformat(),
                "synthetic": True,
                "market": "US",
                "account_id": None,
                "raw_dataset_id": hashlib.sha256(f"parent-{index // 20}".encode()).hexdigest(),
                "metrics_json": canonical(
                    {
                        name: {
                            "lower": count,
                            "upper": count,
                            "precision": "exact",
                            "semantics": "cumulative",
                            "unit": "count",
                            "raw": None,
                            "model_version": None,
                        }
                        for name, count in (("view_count", value), ("like_count", index * 5))
                    }
                ),
                "attributes_json": canonical(
                    {
                        "creator_id": f"creator-{index % 91:03}",
                        "title": f"Useful product {index % 50}",
                        "description": "Evidence retained for future research. " * 6,
                        "unknown": {"items": [None, False, "été ♜"]},
                    }
                ),
                "missing_fields_json": '{"sales":"not provided"}',
            }
        )
    decoder = legacy_decode if args.decoder == "legacy" else decode_columnar_observation
    cpu, wall = time.process_time(), time.perf_counter()
    decoded = [decoder(dict(row)) for row in encoded]
    elapsed_cpu, elapsed_wall = time.process_time() - cpu, time.perf_counter() - wall
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Verify the entire result outside the timed decode and peak-RSS observation.
    hasher = hashlib.sha256()
    for row in decoded:
        hasher.update(canonical(row.model_dump(mode="json")).encode())
    print(
        json.dumps(
            {
                "decoder": args.decoder,
                "rows": args.rows,
                "decode_cpu_seconds": round(elapsed_cpu, 6),
                "decode_elapsed_seconds": round(elapsed_wall, 6),
                "peak_process_rss_kib": peak_rss,
                "full_result_sha256": hasher.hexdigest(),
                "limits": "RSS includes encoded fixtures and retained output, not VPS services",
            }
        )
    )


if __name__ == "__main__":
    main()
