"""Measure bounded local cache admission and warm reads without external services.

Run the same script with baseline/candidate ``PYTHONPATH`` values. Ambient files
contain inert bytes: this measures the real cache, not Parquet or pipeline work.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import resource
import statistics
import tempfile
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sm_trending_products import cache
from sm_trending_products.history import History


def measure(operation, repetitions):
    wall, cpu = [], []
    for _ in range(repetitions):
        cpu_start, wall_start = time.process_time_ns(), time.perf_counter_ns()
        operation()
        wall.append((time.perf_counter_ns() - wall_start) / 1_000_000)
        cpu.append((time.process_time_ns() - cpu_start) / 1_000_000)
    return {
        "repetitions": repetitions,
        "median_ms": statistics.median(wall),
        "p95_ms": sorted(wall)[math.ceil(repetitions * 0.95) - 1],
        "mean_ms": statistics.mean(wall),
        "maximum_ms": max(wall),
        "total_ms": sum(wall),
        "cpu_median_ms": statistics.median(cpu),
        "cpu_total_ms": sum(cpu),
    }


class MemoryObjects:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def run(count, repetitions, warm_repetitions, pressure):
    with tempfile.TemporaryDirectory(prefix="cache-admission-") as directory:
        root = Path(directory)
        history = History(
            SimpleNamespace(
                cache_dir=root,
                cache_max_bytes=(8 if pressure else 128) * 1024 * 1024,
                s3_bucket="unused",
            ),
            None,
        )
        shared = cache._Cache(history)
        window = shared.paths("local-feature-state/initial-pressure-window")[0]
        if pressure:
            window.write_bytes(b"w" * 65536)
        ambient_bytes = 8 * 1024 * 1024 - (65536 if pressure else 0)
        quotient, remainder = divmod(ambient_bytes, count)
        ambient = [shared.paths(f"ambient/{index}")[0] for index in range(count)]
        for index, path in enumerate(ambient):
            path.write_bytes(b"x" * (quotient + int(index < remainder)))
        started = time.perf_counter()
        if pressure:
            shared._reconcile_budget(root / "startup")
        else:
            shared.admit_local(b"startup")
        startup_ms = (time.perf_counter() - started) * 1000
        snapshots = []
        original = cache._Cache._snapshot

        def snapshot(instance):
            before = time.perf_counter_ns()
            result = original(instance)
            snapshots.append((time.perf_counter_ns() - before) / 1_000_000)
            return result

        index = 0

        def admission():
            nonlocal index
            body = hashlib.sha256(str(index).encode()).digest() * 32
            index += 1
            current = cache._Cache(history)
            key = f"local-feature-state/{hashlib.sha256(body).hexdigest()}.parquet"
            with current.object_lock(key):
                assert current.admit_local(body)

        with patch.object(cache._Cache, "_snapshot", snapshot):
            admitted = measure(admission, repetitions)
        admitted["snapshot_count"] = len(snapshots)
        admitted["snapshot_total_ms"] = sum(snapshots)
        evicted = [path for path in ambient if not path.exists()]
        evicted_bytes = sum(
            quotient + int(index < remainder)
            for index, path in enumerate(ambient)
            if not path.exists()
        )

        warm = []
        objects = {f"warm-{size}": b"w" * size for size in (1024, 128 * 1024)}
        history._client = MemoryObjects(objects)
        for key, body in () if pressure else objects.items():
            obj = {"key": key, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
            assert cache.cached_path(history, obj).read_bytes() == body
            ordinary = measure(partial(cache.cached_path, history, obj), warm_repetitions)

            def session_read(reference=obj):
                with cache.pin_cached(history, reference):
                    pass

            with cache.read_session(history):
                session_read()
                session = measure(session_read, warm_repetitions)
            warm.append({"payload_bytes": len(body), "ordinary": ordinary, "read_session": session})
        return {
            "ambient_files": count,
            "ambient_logical_bytes": ambient_bytes,
            "budget_bytes": history.settings.cache_max_bytes,
            "initial_payload_bytes": 8 * 1024 * 1024,
            "pressure": pressure,
            "admitted_payload_bytes": 1024,
            "startup_admission_ms_excluded_from_ordinary": startup_ms,
            "ordinary_admission": admitted,
            "source_files_evicted_in_measured_admissions": len(evicted),
            "source_bytes_evicted_in_measured_admissions": evicted_bytes,
            "initial_window_retained": window.exists() if pressure else None,
            "warm_reads": warm,
            "all_ambient_files_retained": all(path.exists() for path in ambient),
            "process_cumulative_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }


def near_fit():
    """Expose optional-eviction GET cost when the hot set would otherwise fit."""
    with tempfile.TemporaryDirectory(prefix="cache-near-fit-") as directory:
        root = Path(directory)
        objects = {f"source/{index}": b"x" * 1024 for index in range(1024)}
        history = History(
            SimpleNamespace(cache_dir=root, cache_max_bytes=1024 * 1024, s3_bucket="unused"),
            None,
        )
        history._client = MemoryObjects(objects)
        shared = cache._Cache(history)
        for key, body in objects.items():
            shared.paths(key)[0].write_bytes(body)
        shared._reconcile_budget(root / "startup")
        window = shared.admit_local(b"w" * 32768)
        evicted = sum(not shared.paths(key)[0].exists() for key in objects)
        for index in range(32, 1024):
            key = f"source/{index}"
            obj = {"key": key, "sha256": hashlib.sha256(objects[key]).hexdigest(), "bytes": 1024}
            assert cache.cached_path(history, obj).read_bytes() == objects[key]
        return {
            "budget_bytes": 1024 * 1024,
            "initial_source_files": 1024,
            "hot_source_files": 992,
            "cold_source_files": 32,
            "new_window_bytes": 32768,
            "initial_evicted_source_files": evicted,
            "source_GETs_for_hot_reuse": history.metrics["get_requests"],
            "download_bytes_for_hot_reuse": history.metrics["download_bytes"],
            "window_retained": shared.paths(window["key"])[0].exists(),
        }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  PYTHONPATH=src python scripts/measure-cache-admission.py "
            "--output output/cache-admission.json\n"
            "  PYTHONPATH=src python scripts/measure-cache-admission.py "
            "--file-counts 1000 30000 --repetitions 31 --output output/cache-admission.json\n"
            "  PYTHONPATH=src python scripts/measure-cache-admission.py --pressure "
            "--file-counts 30000 --repetitions 256 --output output/cache-pressure.json"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--file-counts", type=int, nargs="+", default=[1000, 5000, 30000])
    parser.add_argument("--repetitions", type=int, default=31)
    parser.add_argument(
        "--warm-repetitions",
        type=int,
        default=300,
        help="Warm-read repetitions (headroom mode only)",
    )
    parser.add_argument("--pressure", action="store_true", help="Start with an 8 MiB full cache")
    args = parser.parse_args()
    if (
        args.repetitions < 1
        or args.warm_repetitions < 1
        or any(not 1 <= count <= 8 * 1024 * 1024 for count in args.file_counts)
    ):
        parser.error("positive repetitions and file counts between 1 and 8388608 are required")
    implementations = {}
    for name in ("cache.py", "_cache_accounting.py"):
        path = Path(cache.__file__).with_name(name)
        if path.exists():
            implementations[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    result = {
        "fixture": "cache-admission-pressure-v1"
        if args.pressure
        else "cache-admission-headroom-v1",
        "measured_at": datetime.now(UTC).isoformat(),
        "implementation_sha256": implementations,
        "scope": (
            "Actual local cache admission, including stripe lock, short-lived _Cache, and fsync. "
            "Equal logical bytes, inert payloads. Headroom mode does not evict; pressure mode "
            "starts full and measures eviction. Warm source objects use memory "
            "instead of network. CPU/RSS cover only this process; cumulative peak RSS is not "
            "per-file memory. No PostgreSQL, parsing, full pipeline or VPS capacity claim."
        ),
        "cases": [
            run(count, args.repetitions, args.warm_repetitions, args.pressure)
            for count in args.file_counts
        ],
    }
    if args.pressure:
        result["near_fit"] = near_fit()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "cases": len(result["cases"])}))


if __name__ == "__main__":
    main()
