#!/usr/bin/env python3
"""Compare exact analytical computation with an immutable pre-change module.

No database, provider access, or source collection is used. Each logical product
has an actual retained observation window. A single product is resident at a time.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import os
import platform
import resource
import statistics
import sys
import time
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pydantic

from sm_trending_products import analytics, feature_projection
from sm_trending_products.contracts import (
    AssessmentPolicy,
    Measurement,
    Observation,
    Product,
    ProductLink,
    canonical,
)
from sm_trending_products.history import History

BASELINE = "761b7796f77d83a7c10cf818042c01e57cb5e999"


EXPECTED_BASELINE_HASHES = {
    "analytics": "f24b49df01df17cfdcb70d680ab5352e4bdc8b6680dbc80c0b433ade5ad0d53d",
    "windows": "b56d1408669c3ec5ec5e080a435805038af9c6a355ab94539e79e73f8d930c20",
}


def module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location("sm_trending_products." + name, path)
    if spec is None or spec.loader is None:
        raise ValueError("Baseline module cannot be loaded")
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    return loaded


def window(product_id: str, videos: int, samples: int, cutoff: datetime):
    product = Product(
        product_id=product_id,
        name=product_id,
        synthetic=True,
        created_at=cutoff - timedelta(days=100),
    )
    observations = []
    links = []
    for entity in range(videos):
        identity = f"{product_id}-v{entity}"
        links.append(
            ProductLink(
                product_id=product_id,
                entity_key=f"compute_fixture:video:{identity}",
                available_at=product.created_at,
            )
        )
        for point in range(samples):
            observed = cutoff - timedelta(hours=(samples - point - 1) * 6)
            observations.append(
                Observation(
                    observation_id=hashlib.sha256(f"{identity}-{point}".encode()).hexdigest(),
                    source="compute_fixture",
                    entity_type="video",
                    entity_id=identity,
                    observed_at=observed,
                    available_at=observed,
                    published_at=product.created_at,
                    synthetic=True,
                    metrics={"view_count": Measurement(lower=point * 100, upper=point * 100)},
                    attributes={"creator_id": identity},
                    raw_dataset_id=hashlib.sha256(
                        f"{product_id}-dataset-{point}".encode()
                    ).hexdigest(),
                )
            )
    return product, observations, links


def rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def decode(body: bytes, decoder):
    frame = pl.read_parquet(io.BytesIO(body))
    return [decoder(row) for row in frame.iter_rows(named=True)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-analytics", required=True, type=Path)
    parser.add_argument("--baseline-windows", required=True, type=Path)
    parser.add_argument("--products", type=int, default=100)
    parser.add_argument("--videos", type=int, default=20)
    parser.add_argument("--samples", type=int, default=120)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--max-seconds", type=float, default=300)
    parser.add_argument("--max-rss-mib", type=int, default=1024)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not (
        1 <= args.products <= 1000
        and 1 <= args.videos <= 100
        and 2 <= args.samples <= 360
        and args.videos * args.samples <= 100_000
        and 1 <= args.repeats <= 10
        and 0 < args.max_seconds <= 3600
        and 128 <= args.max_rss_mib <= 2048
    ):
        parser.error(
            "Require bounded products<=1000, videos<=100, samples<=360, repeats<=10 and budgets"
        )
    for name, path in (("analytics", args.baseline_analytics), ("windows", args.baseline_windows)):
        if hashlib.sha256(path.read_bytes()).hexdigest() != EXPECTED_BASELINE_HASHES[name]:
            parser.error("Baseline file does not match the immutable declared commit")
    old = module("_baseline_compute_761b779", args.baseline_analytics)
    old_windows = module("_baseline_windows_761b779", args.baseline_windows)
    cutoff = datetime(2026, 10, 24, tzinfo=UTC)
    policy = AssessmentPolicy(lookback_days=90)
    # Initialize native expression/Parquet paths before timing either adapter.
    warm_product, warm_observations, warm_links = window("warmup", 1, 2, cutoff)
    for engine, codec in ((old, old_windows), (analytics, feature_projection)):
        engine.assess(
            warm_observations, [warm_product], warm_links, cutoff, True, policy, normalize=False
        )
        warm_body = codec._encode_window(warm_observations, 64 * 1024**2)
        if warm_body is None:
            raise ValueError("Native warm-up unexpectedly exceeded the window bound")
        decode(
            warm_body,
            History._decode if codec is old_windows else feature_projection._decode_window,
        )
    timings: dict[str, list[float]] = {}
    cpu: dict[str, list[float]] = {}
    sizes = {"baseline_state_bytes": 0, "treatment_state_bytes": 0}
    started = time.monotonic()
    report: dict[str, Any] = {
        "status": "running",
        "complete": False,
        "baseline_commit": BASELINE,
        "baseline_files": {
            "analytics_sha256": hashlib.sha256(args.baseline_analytics.read_bytes()).hexdigest(),
            "windows_sha256": hashlib.sha256(args.baseline_windows.read_bytes()).hexdigest(),
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "polars": pl.__version__,
            "pydantic": pydantic.__version__,
            "pyarrow": pa.__version__,
            "affinity_cpus": len(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else None,
            "cpu_quota": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
            if Path("/sys/fs/cgroup/cpu.max").exists()
            else None,
        },
        "treatment_files": {
            "analytics_sha256": hashlib.sha256(Path(analytics.__file__).read_bytes()).hexdigest(),
            "windows_sha256": hashlib.sha256(
                Path(feature_projection.__file__).read_bytes()
            ).hexdigest(),
        },
        "workload": {
            "products": args.products,
            "videos": args.videos,
            "samples": args.samples,
            "refresh_hours": 6,
            "repeats": args.repeats,
            "observations_per_complete_pass": args.products * args.videos * args.samples,
        },
        "bounds": {
            "max_seconds": args.max_seconds,
            "max_rss_bytes": args.max_rss_mib * 1024**2,
            "resident_product_scopes": 1,
        },
        "completed_products": 0,
        "complete_assessment_parity": False,
        "complete_observation_parity": False,
        "encoded_byte_identity": False,
        "native_warmup": "One two-observation window per adapter, excluded from timed components",
        "boundary": (
            "In-memory product-window computation; excludes SQL/S3/provenance and source access"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def publish():
        report["elapsed_seconds"] = time.monotonic() - started
        report["peak_process_rss_bytes"] = rss_bytes()
        report["sizes"] = sizes
        report["timings"] = {
            name: {
                "count": len(values),
                "total_seconds": sum(values),
                "median_seconds": statistics.median(values),
            }
            for name, values in timings.items()
        }
        report["cpu"] = {
            name: {"count": len(values), "total_seconds": sum(values)}
            for name, values in cpu.items()
        }
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n")
        temporary.replace(args.output)

    def measured(name, fn):
        if time.monotonic() - started > args.max_seconds:
            raise TimeoutError("declared_deadline")
        if rss_bytes() > args.max_rss_mib * 1024**2:
            raise MemoryError("declared_rss_limit")
        wall, process = time.perf_counter(), time.process_time()
        result = fn()
        timings.setdefault(name, []).append(time.perf_counter() - wall)
        cpu.setdefault(name, []).append(time.process_time() - process)
        return result

    publish()
    try:
        for index in range(args.products):
            product, observations, links = window(f"p{index}", args.videos, args.samples, cutoff)
            for repeat in range(args.repeats):
                results = {}
                adapters = [
                    ("baseline", old, old_windows),
                    ("treatment", analytics, feature_projection),
                ]
                if (index + repeat) % 2:
                    adapters.reverse()
                for name, engine, codec in adapters:
                    assessed = measured(
                        name + "_assess",
                        partial(
                            engine.assess,
                            observations,
                            [product],
                            links,
                            cutoff,
                            True,
                            policy,
                            normalize=False,
                        ),
                    )
                    body = measured(
                        name + "_encode", partial(codec._encode_window, observations, 64 * 1024**2)
                    )
                    if body is None:
                        raise ValueError("Window exceeds the declared byte ceiling")

                    decoder = (
                        History._decode if name == "baseline" else feature_projection._decode_window
                    )
                    decoded = measured(name + "_decode", partial(decode, body, decoder))
                    results[name] = assessed, decoded, body
                assert results["baseline"][2] == results["treatment"][2]
                assert canonical(results["baseline"][0]) == canonical(results["treatment"][0])
                assert (
                    [row.model_dump(mode="json") for row in results["baseline"][1]]
                    == [row.model_dump(mode="json") for row in results["treatment"][1]]
                    == [row.model_dump(mode="json") for row in observations]
                )
                if repeat == 0:
                    sizes["baseline_state_bytes"] += len(results["baseline"][2])
                    sizes["treatment_state_bytes"] += len(results["treatment"][2])
            report["completed_products"] = index + 1
            del results, decoded, observations
            publish()
        report.update(
            status="complete",
            complete=True,
            complete_assessment_parity=True,
            complete_observation_parity=True,
            encoded_byte_identity=True,
        )
    except (TimeoutError, MemoryError) as error:
        report.update(status="stopped", stop_reason=str(error))
    except BaseException as error:
        report.update(status="failed", error_type=type(error).__name__)
        raise
    finally:
        publish()
    print(json.dumps({key: report[key] for key in ("status", "complete", "completed_products")}))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
