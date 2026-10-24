"""Isolated synthetic source workload exercising the supported production path."""

from __future__ import annotations

import json
import math
import os
import platform
import resource
import shutil
import stat
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .contracts import canonical, now


def _disk_usage(root: Path) -> dict:
    """Count nested files and allocated directory/control-file blocks as well."""
    logical, physical, entries, reserved = 0, 0, 0, 0
    inodes = set()
    paths = [root, *root.rglob("*")] if root.exists() else []
    for path in paths:
        try:
            metadata = path.lstat()
        except FileNotFoundError:  # An active collector/cache may remove a temporary file.
            continue
        entries += 1
        inode = (metadata.st_dev, metadata.st_ino)
        if inode in inodes:
            continue
        inodes.add(inode)
        logical += metadata.st_size if stat.S_ISREG(metadata.st_mode) else 0
        physical += metadata.st_blocks * 512
        if path.parent.name == ".downloads" and path.suffix == ".reserve":
            try:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(descriptor, "rb") as handle:
                    reserved += max(0, int(handle.read(64)))
            except (OSError, ValueError):  # Concurrent reservation creation/removal.
                pass
    return {
        "file_bytes": logical,
        "allocated_bytes": physical,
        "entries": entries,
        "reservation_declared_bytes": reserved,
    }


def _group_videos(video_count: int, product_for: Callable[[int], int]) -> dict[int, list[int]]:
    groups: dict[int, list[int]] = defaultdict(list)
    for video in range(video_count):
        groups[product_for(video)].append(video)
    return groups


def benchmark(
    pipeline,
    records: int,
    output: Path,
    product_count: int = 100,
    *,
    workers: int = 1,
    source_latency_ms: float = 0,
    hot_fraction: float = 0,
    compaction: bool = False,
    request_capacity: float = 10000,
    requests_per_second: float = 10000,
    arrival_rate: float = 0,
    drain_seconds: float = 30,
) -> dict:
    if not 4 <= records <= 1_000_000 or not 1 <= product_count <= records // 2:
        raise ValueError("Use 4..1000000 records and at most records/2 products")
    if not 1 <= workers <= 16 or isinstance(workers, bool):
        raise ValueError("Worker count must be between 1 and 16")
    if not math.isfinite(source_latency_ms) or not 0 <= source_latency_ms <= 10000:
        raise ValueError("Source latency must be between 0 and 10000 milliseconds")
    if not math.isfinite(hot_fraction) or not 0 <= hot_fraction <= 1:
        raise ValueError("Hot-product fraction must be between 0 and 1")
    if not math.isfinite(request_capacity) or request_capacity < 1:
        raise ValueError("Request capacity must be finite and positive")
    if not math.isfinite(requests_per_second) or requests_per_second <= 0:
        raise ValueError("Request refill must be finite and positive")
    if not math.isfinite(arrival_rate) or arrival_rate < 0:
        raise ValueError("Arrival rate must be finite and nonnegative")
    if not math.isfinite(drain_seconds) or drain_seconds <= 0:
        raise ValueError("Drain duration must be finite and positive")
    occupied = pipeline.catalog.one(
        "SELECT EXISTS(SELECT 1 FROM datasets) "
        "OR EXISTS(SELECT 1 FROM products WHERE synthetic) "
        "OR EXISTS(SELECT 1 FROM entities WHERE synthetic) "
        "OR EXISTS(SELECT 1 FROM collection_tasks WHERE synthetic) "
        "OR EXISTS(SELECT 1 FROM evaluation_runs WHERE synthetic) "
        "OR EXISTS(SELECT 1 FROM assessment_runs) AS occupied"
    )
    if occupied["occupied"]:
        raise ValueError(
            "Benchmark requires an empty isolated synthetic workspace and verification inputs"
        )
    run_id = "benchmark-" + uuid.uuid4().hex[:12]
    base = now() + timedelta(minutes=1)
    page_size = min(20, records // 2)
    video_count = max(product_count, page_size)
    pages = (records + page_size - 1) // page_size

    def product_for(video: int) -> int:
        return 0 if video / video_count < hot_fraction else video % product_count

    product_videos = _group_videos(video_count, product_for)
    source_stats_lock = threading.Lock()
    source_stats: dict[str, dict] = {}

    def count_bytes(name, sizes):
        with source_stats_lock:
            counters = source_stats.setdefault(
                name, {"count": 0, "total": 0, "min": None, "max": None}
            )
            for size in sizes:
                counters["count"] += 1
                counters["total"] += size
                counters["min"] = min(counters["min"], size) if counters["min"] else size
                counters["max"] = max(counters["max"], size) if counters["max"] else size
            counters["mean"] = counters["total"] / counters["count"] if counters["count"] else None

    class Source(BaseHTTPRequestHandler):
        def do_POST(self):
            time.sleep(source_latency_ms / 1000)
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length))
            start = int(payload.get("cursor", 0))
            videos = []
            for index in range(start, min(start + page_size, records)):
                video = index % video_count
                product = product_for(video)
                round_number = index // video_count
                videos.append(
                    {
                        "id": f"{run_id}-{video}",
                        "create_time": int(base.timestamp()),
                        "title": f"Synthetic product {product}",
                        "duration": 15,
                        "view_count": 100 + round_number * (product + 1) * 100,
                        "like_count": 10 + round_number * (product + 1),
                        "comment_count": round_number,
                        "share_count": round_number,
                    }
                )
            body = json.dumps(
                {
                    "data": {"videos": videos, "has_more": False, "cursor": 0},
                    "error": {"code": "ok", "message": ""},
                }
            ).encode()
            count_bytes(
                "source_record_bytes", [len(json.dumps(video).encode()) for video in videos]
            )
            count_bytes("source_response_bytes", [len(body)])
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    thread = threading.Thread(target=source.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{source.server_port}"
    try:
        from .pipeline import Pipeline

        start, cpu_start = time.perf_counter(), time.process_time()
        before = dict(pipeline.history.metrics)
        extra_metrics: dict[str, int] = defaultdict(int)
        analysis_before = dict(pipeline.history.analysis_metrics)
        extra_analysis: dict[str, int] = defaultdict(int)
        metrics_lock = threading.Lock()
        samples: deque[dict] = deque(maxlen=1000)
        sample_count = 0
        collected = 0
        failures: list[dict] = []
        drain_timed_out = False
        phases: dict[str, dict] = {}
        report: dict = {
            "benchmark_id": run_id,
            "synthetic": True,
            "namespace_isolation": "empty synthetic workspace and global verification inputs "
            "required; no concurrent workload",
            "ok": False,
            "complete": False,
            "status": "running",
            "hardware": {"architecture": platform.machine(), "python": platform.python_version()},
            "workload": {
                "requested_records": records,
                "product_count": product_count,
                "page_size": page_size,
                "pages": pages,
                "workers": workers,
                "source_latency": "local controlled HTTP with configured delay",
                "source_latency_ms": source_latency_ms,
                "hot_fraction": hot_fraction,
                "request_capacity": request_capacity,
                "requests_per_second": requests_per_second,
                "arrival_rate_tasks_per_second": arrival_rate,
                "drain_seconds": drain_seconds,
                "compaction": compaction,
                "partition_buckets": pipeline.settings.history_partition_buckets,
                "cache_max_bytes": pipeline.settings.cache_max_bytes,
                "analytical_lookback_days": 30,
            },
            "measurements": {
                "queue_initial_tasks": pages,
                "requested_tasks": pages,
                "admitted_tasks": 0,
                "not_enqueued_tasks": pages,
            },
            "phases": phases,
            "checks": {},
            "errors": failures,
            "compaction": None,
            "measurement_limits": [
                "Parent CPU and RSS exclude PostgreSQL and external services",
                "Child CPU/RSS include collector children, not concurrent full-stack memory",
                "Controlled loopback latency does not establish live source throughput",
                "Drain deadlines are checked between bounded collector calls",
                "Queue samples retain the latest 1000 samples; maxima cover every sample",
                "Spool usage covers the configured directory, not child anonymous temporary files",
                "Compaction active files exclude older objects retained for lineage",
                "Cold analytical phases clear both owned product projections and raw-window "
                "pointers, then use an empty owned file cache; warm phases retain all three",
            ],
            "operating_envelope": "measured_sample_only",
            "cost_model": {
                "required_prices": [
                    "compute_per_hour",
                    "storage_per_gb_month",
                    "s3_get_per_1000",
                    "s3_put_per_1000",
                    "egress_per_gb",
                ],
                "note": "Measured usage is supplied; provider prices and retention "
                "are separate inputs",
            },
        }
        output.parent.mkdir(parents=True, exist_ok=True)

        def checkpoint():
            temporary = output.with_name(output.name + ".tmp")
            temporary.write_text(canonical(report) + "\n")
            temporary.replace(output)

        @contextmanager
        def phase(name, application=pipeline):
            phase_start, parent_cpu = time.perf_counter(), time.process_time()
            child = resource.getrusage(resource.RUSAGE_CHILDREN)
            child_cpu = child.ru_utime + child.ru_stime
            metrics = dict(application.history.metrics)
            analysis_metrics = dict(application.history.analysis_metrics)
            cache_before = _disk_usage(application.settings.cache_dir)
            report["active_phase"] = name
            checkpoint()
            try:
                yield
            finally:
                child = resource.getrusage(resource.RUSAGE_CHILDREN)
                phases[name] = {
                    "elapsed_seconds": time.perf_counter() - phase_start,
                    "cpu_seconds_parent": time.process_time() - parent_cpu,
                    "cpu_seconds_children": child.ru_utime + child.ru_stime - child_cpu,
                    "storage_traffic": {
                        key: value - metrics[key]
                        for key, value in application.history.metrics.items()
                    },
                    "analysis_work": {
                        key: value - analysis_metrics[key]
                        for key, value in application.history.analysis_metrics.items()
                    },
                    "cache_before": cache_before,
                    "cache_usage": _disk_usage(application.settings.cache_dir),
                    "spool_usage": _disk_usage(application.settings.spool_dir),
                }
                if application is not pipeline:
                    for key, value in phases[name]["storage_traffic"].items():
                        extra_metrics[key] += value
                    for key, value in phases[name]["analysis_work"].items():
                        extra_analysis[key] += value
                elif name == "collection" and workers > 1:
                    for key, value in extra_metrics.items():
                        phases[name]["storage_traffic"][key] += value
                report["active_phase"] = None
                checkpoint()

        def queue_sample(*, force=False):
            nonlocal sample_count
            sampled_at = time.perf_counter() - start
            if not force and samples and sampled_at - samples[-1]["elapsed_seconds"] < 1:
                return samples[-1]["backlog_tasks"]
            rows = pipeline.catalog.all(
                "SELECT state,count(*) AS count,"
                "GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)))::float8 "
                "AS oldest_age_seconds FROM collection_tasks WHERE account_id=%s GROUP BY state",
                (run_id,),
            )
            counts = {row["state"]: row["count"] for row in rows}
            backlog = sum(counts.get(state, 0) for state in ("pending", "leased"))
            sample = {
                "elapsed_seconds": sampled_at,
                "states": counts,
                "backlog_tasks": backlog,
                "oldest_unfinished_age_seconds": max(
                    (
                        row["oldest_age_seconds"]
                        for row in rows
                        if row["state"] in {"pending", "leased"}
                    ),
                    default=0,
                ),
            }
            samples.append(sample)
            sample_count += 1
            measurements = report["measurements"]
            measurements["queue_samples"] = list(samples)
            measurements["queue_sample_count"] = sample_count
            measurements["admitted_tasks"] = sum(counts.values())
            measurements["not_enqueued_tasks"] = pages - sum(counts.values())
            measurements["sampled_cache_usage"] = _disk_usage(pipeline.settings.cache_dir)
            measurements["sampled_spool_usage"] = _disk_usage(pipeline.settings.spool_dir)
            measurements["elapsed_seconds"] = time.perf_counter() - start
            measurements["cpu_seconds_parent"] = time.process_time() - cpu_start
            measurements["peak_backlog_tasks"] = max(
                measurements.get("peak_backlog_tasks", 0), backlog
            )
            measurements["queue_final"] = [
                {"state": row["state"], "count": row["count"]} for row in rows
            ]
            checkpoint()
            return backlog

        def enqueue(page):
            # Fixture evidence clocks are separate from real SQL queue latency clocks.
            observation_time = base + timedelta(minutes=page + 1)
            pipeline.enqueue(
                "tiktok_display",
                run_id,
                "list",
                {
                    "cursor": page * page_size,
                    "max_count": page_size,
                    "endpoint_override": endpoint,
                    "fixture_observed_at": observation_time.isoformat(),
                },
                synthetic=True,
                idempotency_key=f"{run_id}-{page}",
            )

        producer_done = threading.Event()
        stop = threading.Event()
        deadline = [float("inf")]

        def collect(application):
            processed, errors = 0, []
            metrics = dict(application.history.metrics)
            while not stop.is_set():
                if arrival_rate and time.perf_counter() >= deadline[0]:
                    break
                result = application.collect_batch(
                    1 if arrival_rate else min(100, pages), synthetic=True
                )
                processed += result["processed"]
                errors.extend(result["errors"])
                fatal = any(error["category"] != "quota_exhausted" for error in result["errors"])
                if fatal:
                    stop.set()
                    break
                if not arrival_rate:
                    if result["processed"] == 0 or result["errors"]:
                        break
                elif result["processed"] == 0:
                    unfinished = application.catalog.one(
                        "SELECT count(*) AS count FROM collection_tasks WHERE account_id=%s "
                        "AND state IN ('pending','leased')",
                        (run_id,),
                    )["count"]
                    if producer_done.is_set() and not unfinished:
                        break
                    stop.wait(0.05)
            return (
                processed,
                errors,
                {key: value - metrics[key] for key, value in application.history.metrics.items()},
            )

        def independent_worker(index):
            if workers == 1:
                return collect(pipeline)
            settings = pipeline.settings.model_copy(update={"worker_id": f"{run_id}-{index}"})
            with Pipeline(settings) as application:
                metrics = dict(application.history.metrics)
                try:
                    return collect(application)
                finally:
                    with metrics_lock:
                        for key, value in application.history.metrics.items():
                            extra_metrics[key] += value - metrics[key]

        def analyze_pair(label, cutoff, cache_root):
            settings = pipeline.settings.model_copy(update={"cache_dir": cache_root})
            with Pipeline(settings) as application:
                product_ids = [f"{run_id}-product-{index}" for index in range(product_count)]
                # The synthetic workspace is isolated, and these exact products
                # belong to this run. An empty directory alone no longer means a
                # cold computation: SQL feature hits can bypass it completely.
                with application.catalog.transaction() as connection:
                    connection.execute(
                        "DELETE FROM feature_product_projections WHERE product_id=ANY(%s)",
                        (product_ids,),
                    )
                    connection.execute(
                        "DELETE FROM feature_product_windows WHERE product_id=ANY(%s)",
                        (product_ids,),
                    )
                compute_cache_before = application.catalog.one(
                    "SELECT (SELECT count(*) FROM feature_product_projections "
                    "WHERE product_id=ANY(%s)) AS feature_projections,"
                    "(SELECT count(*) FROM feature_product_windows "
                    "WHERE product_id=ANY(%s)) AS raw_windows",
                    (product_ids, product_ids),
                )
                with phase(f"analysis_{label}_cold", application):
                    cold = application.assess(cutoff, synthetic=True)
                phases[f"analysis_{label}_cold"].update(
                    cache_regime="cold_files_and_computation",
                    compute_cache_before=compute_cache_before,
                )
                with phase(f"analysis_{label}_warm", application):
                    warm = application.assess(cutoff, synthetic=True)
                phases[f"analysis_{label}_warm"]["cache_regime"] = "warm_files_and_computation"
            report["checks"][f"{label}_cold_warm_same_run"] = cold["run_id"] == warm["run_id"]
            report.setdefault("analysis_runs", {})[label] = {
                "cold": cold["run_id"],
                "warm": warm["run_id"],
                "owned_cache": True,
                "cold_computation_reset": True,
                "cold_initial_file_bytes": phases[f"analysis_{label}_cold"]["cache_before"][
                    "file_bytes"
                ],
            }
            return cold

        try:
            pipeline.catalog.configure_budget(
                "tiktok_display", run_id, request_capacity, requests_per_second
            )
            with phase("collection"):
                if not arrival_rate:
                    for page in range(pages):
                        enqueue(page)
                    producer_done.set()
                queue_sample(force=True)
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    try:
                        futures = [
                            pool.submit(independent_worker, index) for index in range(workers)
                        ]
                        if arrival_rate:
                            producer_start = time.perf_counter()
                            for page in range(pages):
                                # Admit fixed requested work even under collection overload.
                                due = producer_start + page / arrival_rate
                                while time.perf_counter() < due:
                                    time.sleep(max(0, min(0.1, due - time.perf_counter())))
                                    queue_sample()
                                enqueue(page)
                                queue_sample()
                            deadline[0] = time.perf_counter() + drain_seconds
                            producer_done.set()
                            queue_sample(force=True)
                        while any(not future.done() for future in futures):
                            wait(futures, timeout=0.1)
                            backlog = queue_sample()
                            if arrival_rate and time.perf_counter() >= deadline[0] and backlog:
                                drain_timed_out = True
                                stop.set()
                        results = [future.result() for future in futures]
                    finally:
                        # Signal inside the executor context, before it joins worker futures.
                        producer_done.set()
                        stop.set()
                collected = sum(result[0] for result in results)
                failures.extend(error for result in results for error in result[1])
                queue_sample(force=True)
                if arrival_rate and time.perf_counter() >= deadline[0]:
                    drain_timed_out = bool(samples[-1]["backlog_tasks"])
            completed = collected == pages and not any(
                error["category"] != "quota_exhausted" or not arrival_rate for error in failures
            )
            report["checks"]["collection_completed"] = completed
            if completed:
                with phase("identity"):
                    for product in range(product_count):
                        product_id = f"{run_id}-product-{product}"
                        pipeline.create_product(
                            product_id, f"Synthetic product {product}", synthetic=True
                        )
                        for video in product_videos.get(product, ()):
                            pipeline.review_link(
                                product_id, f"tiktok_display:video:{run_id}-{video}"
                            )
                cutoff = base + timedelta(minutes=pages + 2)
                with tempfile.TemporaryDirectory(
                    prefix="sm-benchmark-analysis-", dir=pipeline.settings.cache_dir.parent
                ) as temporary:
                    cold = analyze_pair("pre", cutoff, Path(temporary) / "pre")
                    report["checks"]["repeated_analysis_same_run"] = report["checks"][
                        "pre_cold_warm_same_run"
                    ]
                    if compaction and pages >= 2:
                        with phase("compaction"):
                            report["compaction"] = pipeline.compact(
                                synthetic=True, max_datasets=min(pages, 1000)
                            )
                        post = analyze_pair("post", cutoff, Path(temporary) / "post")
                        report["checks"]["post_compaction_semantics_unchanged"] = (
                            cold["rows"] == post["rows"]
                        )
                    with phase("verification"):
                        verification = pipeline.verify()
                    report["checks"]["objects_verified"] = verification["ok"]
                    with phase("decision"):
                        decision = pipeline.freeze(cutoff, synthetic=True)
                    report["checks"]["forward_decision_created"] = bool(decision["evaluation_id"])
        except Exception as error:
            failures.append(
                {"category": "benchmark_failed", "exception_type": type(error).__name__}
            )
            report["checks"]["workflow_completed"] = False
        finally:
            producer_done.set()
            stop.set()

        elapsed = time.perf_counter() - start
        actual = pipeline.catalog.one(
            "SELECT COALESCE(sum(row_count),0) AS rows,"
            "COALESCE(sum(quarantine_count),0) AS quarantine FROM datasets "
            "WHERE task_id IN(SELECT task_id FROM collection_tasks WHERE account_id=%s)",
            (run_id,),
        )
        latency = pipeline.catalog.one(
            "SELECT count(*) AS count,"
            "min(EXTRACT(EPOCH FROM completed_at-created_at))::float8 AS min,"
            "percentile_cont(0.5) WITHIN GROUP(ORDER BY "
            "EXTRACT(EPOCH FROM completed_at-created_at)) AS p50,"
            "percentile_cont(0.95) WITHIN GROUP(ORDER BY "
            "EXTRACT(EPOCH FROM completed_at-created_at)) AS p95,"
            "max(EXTRACT(EPOCH FROM completed_at-created_at))::float8 AS max "
            "FROM collection_tasks WHERE account_id=%s AND completed_at IS NOT NULL",
            (run_id,),
        )
        deferred = pipeline.catalog.one(
            "SELECT count(*) AS count FROM collection_tasks "
            "WHERE account_id=%s AND state='pending' AND "
            "(last_error='quota_exhausted' OR NOT request_budget_ready(source,account_id,NULL))",
            (run_id,),
        )["count"]
        queue_sample(force=True)
        done = samples[-1]["states"].get("done", 0)
        traffic = {
            key: pipeline.history.metrics[key] - before[key] + extra_metrics[key] for key in before
        }
        disk = shutil.disk_usage(pipeline.settings.cache_dir)
        collection_seconds = phases.get("collection", {}).get("elapsed_seconds", 0)
        report["measurements"].update(
            {
                "elapsed_seconds": elapsed,
                "cpu_seconds_parent": time.process_time() - cpu_start,
                "peak_parent_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "peak_child_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "requested_tasks": pages,
                "admitted_tasks": sum(samples[-1]["states"].values()),
                "not_enqueued_tasks": pages - sum(samples[-1]["states"].values()),
                "processed_pages": done,
                "actual_records": int(actual["rows"]),
                "quarantine_count": int(actual["quarantine"]),
                "actual_records_per_second": int(actual["rows"]) / elapsed,
                "collection_seconds": collection_seconds,
                "collection_records_per_second": int(actual["rows"]) / collection_seconds
                if collection_seconds
                else 0,
                "cold_analysis_seconds": phases.get("analysis_pre_cold", {}).get(
                    "elapsed_seconds", 0
                ),
                "warm_analysis_seconds": phases.get("analysis_pre_warm", {}).get(
                    "elapsed_seconds", 0
                ),
                "compaction_seconds": phases.get("compaction", {}).get("elapsed_seconds", 0),
                "cache_usage": _disk_usage(pipeline.settings.cache_dir),
                "spool_usage": _disk_usage(pipeline.settings.spool_dir),
                "cache_bytes": _disk_usage(pipeline.settings.cache_dir)["file_bytes"],
                "disk_free_bytes": disk.free,
                "disk_total_bytes": disk.total,
                "storage_traffic": traffic,
                "analysis_work": {
                    key: pipeline.history.analysis_metrics[key]
                    - analysis_before[key]
                    + extra_analysis[key]
                    for key in analysis_before
                },
                "task_latency_seconds": latency,
                "quota_waiting_tasks": pipeline.catalog.quota_waiting(synthetic=True),
                "quota_deferred_tasks": deferred,
                "drain_timed_out": drain_timed_out,
                "error_count": len(failures),
                **source_stats,
            }
        )
        report["checks"]["requested_records_published"] = (
            int(actual["rows"]) == records and not actual["quarantine"]
        )
        report["checks"].setdefault("collection_completed", done == pages)
        report["failed_checks"] = [name for name, passed in report["checks"].items() if not passed]
        report["ok"] = not report["failed_checks"]
        report["complete"] = True
        report["status"] = (
            "completed" if report["ok"] else "drain_timeout" if drain_timed_out else "failed"
        )
        checkpoint()
        return report
    finally:
        source.shutdown()
        source.server_close()
        thread.join(timeout=2)
