"""Isolated synthetic source workload exercising the supported production path."""

from __future__ import annotations

import json
import math
import platform
import resource
import shutil
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .contracts import canonical, now


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
    run_id = "benchmark-" + uuid.uuid4().hex[:12]
    base = now() + timedelta(minutes=1)
    page_size = min(20, records // 2)
    video_count = max(product_count, page_size)
    pages = (records + page_size - 1) // page_size

    def product_for(video: int) -> int:
        return 0 if video / video_count < hot_fraction else video % product_count

    product_videos = _group_videos(video_count, product_for)

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
    start = time.perf_counter()
    cpu_start = time.process_time()
    before = dict(pipeline.history.metrics)
    worker_metrics: list[dict] = []
    try:
        pipeline.catalog.configure_budget(
            "tiktok_display", run_id, request_capacity, requests_per_second
        )
        for page in range(pages):
            # Page timestamps are monotone; all repeated entities receive a later observation.
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

        def collect(application):
            collected, failures = 0, []
            while True:
                result = application.collect_batch(min(100, pages), synthetic=True)
                failures.extend(result["errors"])
                collected += result["processed"]
                if result["processed"] == 0 or result["errors"]:
                    return collected, failures, dict(application.history.metrics)

        if workers == 1:
            collected, failures, _ = collect(pipeline)
        else:
            from .pipeline import Pipeline

            def independent_worker(index):
                settings = pipeline.settings.model_copy(update={"worker_id": f"{run_id}-{index}"})
                with Pipeline(settings) as application:
                    return collect(application)

            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(independent_worker, range(workers)))
            collected = sum(result[0] for result in results)
            failures = [error for result in results for error in result[1]]
            worker_metrics = [result[2] for result in results]
        if collected != pages or failures:
            report = {
                "benchmark_id": run_id,
                "synthetic": True,
                "ok": False,
                "errors": failures,
                "workload": {
                    "workers": workers,
                    "requested_records": records,
                    "source_latency_ms": source_latency_ms,
                    "hot_fraction": hot_fraction,
                    "request_capacity": request_capacity,
                    "requests_per_second": requests_per_second,
                },
                "measurements": {
                    "processed_pages": collected,
                    "elapsed_seconds": time.perf_counter() - start,
                    "queue_initial_tasks": pages,
                    "queue_final": pipeline.catalog.all(
                        "SELECT state,count(*) AS count FROM collection_tasks "
                        "WHERE account_id=%s GROUP BY state",
                        (run_id,),
                    ),
                },
                "checks": {"collection_completed": False},
                "failed_checks": ["collection_completed"],
                "task_status": pipeline.status()["queue_errors"],
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(canonical(report) + "\n")
            return report
        for product in range(product_count):
            product_id = f"{run_id}-product-{product}"
            pipeline.create_product(product_id, f"Synthetic product {product}", synthetic=True)
            for video in product_videos.get(product, ()):
                pipeline.review_link(product_id, f"tiktok_display:video:{run_id}-{video}")
        cutoff = base + timedelta(minutes=pages + 2)
        cold_start = time.perf_counter()
        cold = pipeline.assess(cutoff, synthetic=True)
        cold_seconds = time.perf_counter() - cold_start
        warm_start = time.perf_counter()
        warm = pipeline.assess(cutoff, synthetic=True)
        warm_seconds = time.perf_counter() - warm_start
        compact_result = None
        compact_seconds = 0.0
        if compaction and pages >= 2:
            compact_start = time.perf_counter()
            compact_result = pipeline.compact(synthetic=True, max_datasets=min(pages, 1000))
            compact_seconds = time.perf_counter() - compact_start
        verification = pipeline.verify()
        decision = pipeline.freeze(cutoff, synthetic=True)
        elapsed = time.perf_counter() - start
        metrics = {key: pipeline.history.metrics[key] - before[key] for key in before}
        for worker in worker_metrics:
            for key, value in worker.items():
                metrics[key] = metrics.get(key, 0) + value
        actual = pipeline.catalog.one(
            "SELECT COALESCE(sum(row_count),0) AS rows,COALESCE(sum(quarantine_count),0) AS "
            "quarantine "
            "FROM datasets WHERE task_id IN(SELECT task_id FROM collection_tasks "
            "WHERE account_id=%s)",
            (run_id,),
        )
        actual = {"rows": int(actual["rows"]), "quarantine": int(actual["quarantine"])}
        queue = pipeline.catalog.all(
            "SELECT state,count(*) AS count FROM collection_tasks "
            "WHERE account_id=%s GROUP BY state",
            (run_id,),
        )
        disk = shutil.disk_usage(pipeline.settings.cache_dir)
        report = {
            "ok": True,
            "benchmark_id": run_id,
            "synthetic": True,
            "hardware": {"architecture": platform.machine(), "python": platform.python_version()},
            "workload": {
                "requested_records": records,
                "product_count": product_count,
                "page_size": page_size,
                "pages": pages,
                "source_latency": "local controlled HTTP with configured delay",
                "source_latency_ms": source_latency_ms,
                "hot_fraction": hot_fraction,
                "workers": workers,
                "request_capacity": request_capacity,
                "requests_per_second": requests_per_second,
                "compaction": compaction,
                "partition_buckets": pipeline.settings.history_partition_buckets,
                "cache_max_bytes": pipeline.settings.cache_max_bytes,
                "analytical_lookback_days": 30,
            },
            "measurements": {
                "elapsed_seconds": elapsed,
                "cpu_seconds_parent": time.process_time() - cpu_start,
                "peak_parent_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "peak_child_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "processed_pages": collected,
                "actual_records": actual["rows"],
                "quarantine_count": actual["quarantine"],
                "actual_records_per_second": actual["rows"] / elapsed,
                "cold_analysis_seconds": cold_seconds,
                "warm_analysis_seconds": warm_seconds,
                "compaction_seconds": compact_seconds,
                "queue_initial_tasks": pages,
                "queue_final": queue,
                "cache_bytes": sum(
                    path.stat().st_size
                    for path in pipeline.settings.cache_dir.iterdir()
                    if path.is_file()
                ),
                "disk_free_bytes": disk.free,
                "disk_total_bytes": disk.total,
                "storage_traffic": metrics,
                "error_count": len(failures),
            },
            "checks": {
                "objects_verified": verification["ok"],
                "requested_records_published": actual["rows"] == records
                and actual["quarantine"] == 0,
                "repeated_analysis_same_run": cold["run_id"] == warm["run_id"],
                "forward_decision_created": bool(decision["evaluation_id"]),
            },
            "errors": failures,
            "compaction": compact_result,
            "measurement_limits": [
                "Parent CPU and RSS exclude PostgreSQL and external services",
                "Child RSS is a high-water mark, not concurrent full-stack memory",
                "Controlled loopback latency does not establish live source throughput",
                "A finite workload does not establish sustained queue or production fault limits",
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
        report["failed_checks"] = [name for name, passed in report["checks"].items() if not passed]
        report["ok"] = not report["failed_checks"]
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(canonical(report) + "\n")
        return report
    finally:
        source.shutdown()
        source.server_close()
        thread.join(timeout=2)
