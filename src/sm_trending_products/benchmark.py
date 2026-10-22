"""Isolated synthetic source workload exercising the supported production path."""

from __future__ import annotations

import json
import platform
import resource
import threading
import time
import uuid
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .contracts import canonical, now


def benchmark(pipeline, records: int, output: Path, product_count: int = 100) -> dict:
    if not 4 <= records <= 1_000_000 or not 1 <= product_count <= records // 2:
        raise ValueError("Use 4..1000000 records and at most records/2 products")
    run_id = "benchmark-" + uuid.uuid4().hex[:12]
    base = now() + timedelta(minutes=1)
    page_size = min(20, records // 2)
    video_count = max(product_count, page_size)
    pages = (records + page_size - 1) // page_size

    class Source(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length))
            start = int(payload.get("cursor", 0))
            videos = []
            for index in range(start, min(start + page_size, records)):
                video = index % video_count
                product = video % product_count
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
    try:
        pipeline.catalog.configure_budget("tiktok_display", run_id, 10000, 10000)
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
        collected = 0
        failures = []
        while collected < pages:
            result = pipeline.collect_batch(min(100, pages - collected), synthetic=True)
            failures.extend(result["errors"])
            if result["processed"] == 0:
                break
            collected += result["processed"]
        if collected != pages or failures:
            report = {
                "benchmark_id": run_id,
                "synthetic": True,
                "ok": False,
                "errors": failures,
                "measurements": {"processed_pages": collected},
                "checks": {"collection_completed": False},
                "task_status": pipeline.status()["queue_errors"],
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(canonical(report) + "\n")
            return report
        for product in range(product_count):
            product_id = f"{run_id}-product-{product}"
            pipeline.create_product(product_id, f"Synthetic product {product}", synthetic=True)
            for video in range(product, video_count, product_count):
                pipeline.review_link(product_id, f"tiktok_display:video:{run_id}-{video}")
        cutoff = base + timedelta(minutes=pages + 2)
        cold_start = time.perf_counter()
        cold = pipeline.assess(cutoff, synthetic=True)
        cold_seconds = time.perf_counter() - cold_start
        warm_start = time.perf_counter()
        warm = pipeline.assess(cutoff, synthetic=True)
        warm_seconds = time.perf_counter() - warm_start
        verification = pipeline.verify()
        decision = pipeline.freeze(cutoff, synthetic=True)
        elapsed = time.perf_counter() - start
        metrics = {key: pipeline.history.metrics[key] - before[key] for key in before}
        actual = pipeline.catalog.one(
            "SELECT COALESCE(sum(row_count),0) AS rows,COALESCE(sum(quarantine_count),0) AS "
            "quarantine "
            "FROM datasets WHERE task_id IN(SELECT task_id FROM collection_tasks "
            "WHERE account_id=%s)",
            (run_id,),
        )
        actual = {"rows": int(actual["rows"]), "quarantine": int(actual["quarantine"])}
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
                "source_latency": "local controlled HTTP",
                "workers": 1,
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
        output.write_text(canonical(report) + "\n")
        return report
    finally:
        source.shutdown()
        source.server_close()
        thread.join(timeout=2)
