"""Concrete bounded synthetic capture/research qualification, never a production runner."""

from __future__ import annotations

import hashlib
import json
import os
import random
import resource
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import canonical, now
from .pipeline import Pipeline
from .research_workload import (
    Workload,
    reports,
    require_empty_scratch,
    require_measured_reports,
    require_scratch,
    require_scratch_settings,
    result_identity,
    seed,
)
from .settings import Settings

CUTOFF = datetime(2026, 10, 30, tzinfo=UTC)


class Campaign(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    collector_binary: str
    concurrent: bool = True
    arrivals: int = Field(default=12, ge=4, le=128)
    videos_per_arrival: int = Field(default=2, ge=1, le=16)
    arrival_interval_seconds: float = Field(default=0.02, ge=0, le=1)
    source_latency_seconds: float = Field(default=0.08, ge=0.01, le=1)
    retained_days: int = Field(default=7, ge=7, le=30)
    cache_bytes: int = Field(default=16 * 1024**2, ge=1_000_000, le=16 * 1024**2)
    source_padding_bytes: int = Field(default=0, ge=0, le=512000)
    max_report_iterations: int = Field(default=16, ge=2, le=32)
    larger_profile: bool = False

    @model_validator(mode="after")
    def bounded(self):
        if not Path(self.collector_binary).is_file() or not os.access(
            self.collector_binary, os.X_OK
        ):
            raise ValueError(
                "Require an existing executable collector; this runner never builds it"
            )
        if not self.larger_profile and (self.arrivals > 16 or self.retained_days > 7):
            raise ValueError("Larger campaigns require explicit opt-in")
        return self


def useful_overlap(collection: list[dict], reading: list[dict]) -> list[dict]:
    """A complete first publication call must fit inside the actual public report call."""
    return [
        {"task_id": task["task_id"], "report_iteration": read["iteration"]}
        for task in collection
        for read in reading
        if read["report_call_started"]
        <= task["publish_call_started"]
        < task["publish_call_completed"]
        <= read["report_call_completed"]
        and task["first_normalization"]
        and read["sql_execute_calls"] > 0
        and read["reports"] > 0
        and task["normalized_datasets"] > 0
    ]


def task_snapshot(application: Pipeline) -> list[dict]:
    """Correlate exact committed normalized children with their captured task."""
    return application.catalog.all(
        "SELECT t.task_id,t.state,EXTRACT(EPOCH FROM clock_timestamp()-t.due_at) AS age,"
        "child.dataset_id AS normalized_dataset_id,child.row_count AS normalized_rows "
        "FROM collection_tasks t LEFT JOIN datasets parent ON parent.task_id=t.task_id "
        "AND parent.state='committed' AND parent.manifest->>'kind'='raw_capture' "
        "LEFT JOIN datasets child ON child.supersedes=parent.dataset_id "
        "AND child.state='committed' AND child.transform_version='1' ORDER BY t.task_id"
    )


def worker(payload: dict, *, write_report, sql_counts) -> int:
    """Owned child entry point; both adapters are existing runner accounting helpers."""
    output = Path(payload["checkpoint"])
    result: dict = {"ok": False, "status": "setting_up", "phases": []}
    lock, upload_lock = threading.Lock(), threading.Lock()
    stop = threading.Event()
    collection: list[dict] = []
    readings: list[dict] = []
    arrivals: list[dict] = []
    errors: list[str] = []
    requests: list[dict] = []
    queue: list[dict] = []
    uploaded = 0
    threads: list[threading.Thread] = []
    source = None
    started = time.perf_counter()

    def checkpoint():
        # Callers hold lock when concurrent threads are active.
        write_report(output, result)

    def configure(settings: Settings, cache: str) -> Pipeline:
        settings = settings.model_copy(update={"cache_dir": Path(payload["owned"]) / cache})
        application = Pipeline(settings)
        original = application.history._put

        def put(key, body, **kwargs):
            nonlocal uploaded
            with upload_lock:
                if uploaded + len(body) > payload["max_upload_bytes"]:
                    raise ValueError("Synthetic cumulative upload budget exceeded")
                uploaded += len(body)
            return original(key, body, **kwargs)

        application.history._put = put  # type: ignore[method-assign]
        return application

    try:
        campaign = Campaign.model_validate(payload["campaign"])
        workload = Workload.model_validate(payload["workload"])
        if workload.case != "research" or (not campaign.larger_profile and workload.rows > 1000):
            raise ValueError("Require bounded public research profile; larger history needs opt-in")
        settings = Settings.model_validate(payload["settings"])
        settings.cache_max_bytes = campaign.cache_bytes
        require_scratch_settings(settings)
        if now() <= CUTOFF:
            raise ValueError("Historical fixture cutoff must precede physical acquisition time")
        checkpoint()
        phase_start, cpu = time.perf_counter(), time.thread_time()
        with configure(settings, "setup-cache") as application:
            setup: dict = {"name": "setup", "status": "running"}
            try:
                require_empty_scratch(application)
                application.initialize(create_bucket=True)
                fixture = seed(
                    application, workload, cutoff=CUTOFF, retained_days=campaign.retained_days
                )
                setup.update(status="completed", result=fixture)
            except Exception as error:
                setup.update(status="refused", error_type=type(error).__name__)
                raise
            finally:
                setup.update(
                    storage=dict(application.history.metrics),
                    wall_seconds=time.perf_counter() - phase_start,
                    thread_cpu_seconds=time.thread_time() - cpu,
                )
                result["setup"] = setup
                checkpoint()
        result["campaign"] = campaign.model_dump(mode="json")
        result["collection_latency_scope"] = (
            "Normalized child observed after publication return, SQL snapshot and checkpoint lock; "
            "arrival-to-durable latency is an upper bound, not the exact commit timestamp"
        )
        result["accounting"] = {
            "expected_rows": campaign.arrivals * campaign.videos_per_arrival,
            "current_rows": None,
            "missing_rows": None,
            "duplicate_rows": None,
        }
        result["retained_history"] = {
            **fixture,
            "products": workload.products,
            "videos": workload.products * workload.entities,
            "retained_days": campaign.retained_days,
            "requested_observations": workload.rows,
            "batch_rows": workload.batch_rows,
        }

        def read_report(iteration: int, cache: str) -> str:
            phase_start, cpu = time.perf_counter(), time.thread_time()
            with lock:
                result["active_phase"] = "historical_report"
                checkpoint()
            phase = {
                "name": "historical_report",
                "iteration": iteration,
                "status": "running",
                "started": phase_start - started,
                "cache": cache,
            }
            with (
                sql_counts(current_thread_only=True) as counts,
                configure(settings, cache) as reader,
            ):
                try:
                    phase["report_call_started"] = time.perf_counter() - started
                    value = reports(reader, workload, cutoff=CUTOFF)
                    phase["report_call_completed"] = time.perf_counter() - started
                    phase.update(result=result_identity(value), reports=len(value))
                    require_measured_reports(value)
                    encoded = canonical(value)
                    phase["status"] = "completed"
                except Exception as error:
                    phase.update(status="refused", error_type=type(error).__name__)
                    raise
                finally:
                    phase.update(
                        completed=time.perf_counter() - started,
                        wall_seconds=time.perf_counter() - phase_start,
                        thread_cpu_seconds=time.thread_time() - cpu,
                        sql_execute_calls=counts["execute_calls"],
                        sql_scope="Report application thread only; pool and other threads excluded",
                        storage=dict(reader.history.metrics),
                    )
                    with lock:
                        if phase["status"] == "completed":
                            readings.append(phase)
                        result["phases"].append(phase)
                        checkpoint()
            return encoded

        baseline = read_report(-1, "baseline-cache")
        # Only intervals after collection starts count as concurrent useful work.
        readings.clear()

        class Source(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                index = int(body.get("cursor", 0))
                request_started = time.perf_counter() - started
                with lock:
                    requests.append(
                        {
                            "index": index,
                            "started": request_started,
                            "request_bytes": int(self.headers["Content-Length"]),
                            "response_bytes": None,
                            "completed": None,
                        }
                    )
                stop.wait(campaign.source_latency_seconds)
                encoded = canonical(
                    {
                        "data": {
                            "videos": [
                                {"id": f"arrival-{index}-{video}", "view_count": 10 + index}
                                for video in range(campaign.videos_per_arrival)
                            ],
                            "has_more": False,
                            "cursor": 0,
                        },
                        "error": {"code": "ok"},
                        "fixture_padding": random.Random(index)
                        .randbytes(campaign.source_padding_bytes)
                        .hex(),
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                with lock:
                    request = next(r for r in requests if r["index"] == index)
                    request.update(
                        response_bytes=len(encoded), completed=time.perf_counter() - started
                    )

            def log_message(self, *args):
                pass

        source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
        source.daemon_threads = True
        server_thread = threading.Thread(target=source.serve_forever, daemon=True)
        server_thread.start()
        endpoint = f"http://127.0.0.1:{source.server_port}"
        collection_start = time.perf_counter()
        producer_done, drained = threading.Event(), threading.Event()

        def produce():
            try:
                with configure(settings, "arrival-cache") as producer:
                    require_scratch(producer)
                    producer.catalog.configure_budget(
                        "tiktok_display", "concurrent-fixture", 10000, 10000
                    )
                    for index in range(campaign.arrivals):
                        if stop.wait(
                            max(
                                0,
                                collection_start
                                + index * campaign.arrival_interval_seconds
                                - time.perf_counter(),
                            )
                        ):
                            break
                        with lock:
                            task_id = producer.enqueue(
                                "tiktok_display",
                                "concurrent-fixture",
                                "list",
                                {
                                    "cursor": index,
                                    "endpoint_override": endpoint,
                                },
                                synthetic=True,
                            )
                            arrivals.append(
                                {
                                    "index": index,
                                    "task_id": task_id,
                                    "admitted": time.perf_counter() - started,
                                }
                            )
                            result["admitted_arrivals"] = len(arrivals)
                            checkpoint()
            except Exception as error:
                errors.append(type(error).__name__)
                stop.set()
            finally:
                producer_done.set()

        def collect():
            collector = None
            previous: dict = {}
            try:
                with configure(settings, "concurrent-cache") as collector:
                    require_scratch(collector)
                    previous = dict(collector.history.metrics)
                    while not stop.is_set():
                        task_start, cpu = time.perf_counter(), time.thread_time()
                        captured = collector.capture_batch(1, True)
                        normalized_before = {
                            row["normalized_dataset_id"]
                            for row in task_snapshot(collector)
                            if row["normalized_dataset_id"]
                        }
                        publish_call_started = time.perf_counter() - started
                        published = collector.publish_captures(1, True)
                        publish_call_completed = time.perf_counter() - started
                        if captured["errors"] or published["errors"]:
                            raise RuntimeError("Controlled capture or normalization failed")
                        rows = task_snapshot(collector)
                        pending = [row for row in rows if not row["normalized_dataset_id"]]
                        with lock:
                            queue.append(
                                {
                                    "elapsed_seconds": time.perf_counter() - started,
                                    "size": len(pending),
                                    "oldest_age_seconds": max(
                                        (float(row["age"]) for row in pending), default=0
                                    ),
                                }
                            )
                            if published["processed"]:
                                done_ids = {item["task_id"] for item in collection}
                                newly_done = [
                                    row
                                    for row in rows
                                    if row["state"] == "done"
                                    and row["normalized_dataset_id"]
                                    and row["task_id"] not in done_ids
                                ]
                                for row in newly_done:
                                    arrival = next(
                                        a for a in arrivals if a["task_id"] == row["task_id"]
                                    )
                                    completed = time.perf_counter() - started
                                    item = {
                                        "task_id": row["task_id"],
                                        "started": task_start - started,
                                        "completed": completed,
                                        "arrival_to_durable_seconds": completed
                                        - arrival["admitted"],
                                        "publish_call_started": publish_call_started,
                                        "publish_call_completed": publish_call_completed,
                                        "first_normalization": row["normalized_dataset_id"]
                                        not in normalized_before
                                        and row["normalized_dataset_id"] in published["datasets"],
                                        "normalized_datasets": 1,
                                        "normalized_dataset_id": row["normalized_dataset_id"],
                                        "normalized_rows": row["normalized_rows"],
                                    }
                                    collection.append(item)
                                storage = {
                                    key: value - previous.get(key, 0)
                                    for key, value in collector.history.metrics.items()
                                }
                                previous = dict(collector.history.metrics)
                                result["phases"].append(
                                    {
                                        "name": "capture_and_normalize",
                                        "status": "completed",
                                        "wall_seconds": time.perf_counter() - task_start,
                                        "thread_cpu_seconds": time.thread_time() - cpu,
                                        "storage": storage,
                                    }
                                )
                            result["collection"] = list(collection)
                            result["queue_samples"] = list(queue)
                            result["source_http"] = list(requests)
                            checkpoint()
                        if producer_done.is_set() and len(collection) == campaign.arrivals:
                            break
                        stop.wait(0.01)
            except Exception as error:
                errors.append(type(error).__name__)
                stop.set()
            finally:
                if collector is not None:
                    storage = {
                        key: value - previous.get(key, 0)
                        for key, value in collector.history.metrics.items()
                    }
                    if any(storage.values()):
                        with lock:
                            result["phases"].append(
                                {"name": "capture_partial", "status": "stopped", "storage": storage}
                            )
                            checkpoint()
                drained.set()

        threads = [threading.Thread(target=produce), threading.Thread(target=collect)]
        for thread in threads:
            thread.start()
        identities = []
        if campaign.concurrent:
            for iteration in range(campaign.max_report_iterations):
                identities.append(read_report(iteration, "concurrent-cache") == baseline)
                if drained.is_set() and iteration >= 1:
                    break
        for thread in threads:
            thread.join()
        if errors:
            raise RuntimeError("Controlled worker failed")
        collection_seconds = max((item["completed"] for item in collection), default=0) - (
            collection_start - started
        )
        # Verify historical bytes after drain for both cases, then admit actual current arrivals.
        identities.append(
            read_report(campaign.max_report_iterations, "postdrain-cache") == baseline
        )
        with configure(settings, "verification-cache") as verifier:
            keys = {
                f"tiktok_display:video:arrival-{index}-{video}"
                for index in range(campaign.arrivals)
                for video in range(campaign.videos_per_arrival)
            }
            current = verifier.history.read_scoped(
                as_of=now(),
                synthetic=True,
                entity_keys=keys,
                max_rows=campaign.arrivals * campaign.videos_per_arrival,
            )
            historical = verifier.history.read_scoped(
                as_of=CUTOFF,
                synthetic=True,
                entity_keys=keys,
                max_rows=campaign.arrivals * campaign.videos_per_arrival,
            )
            raw_objects = verifier.catalog.all(
                "SELECT manifest->'raw' AS obj FROM datasets WHERE state='committed' "
                "AND manifest->>'kind'='raw_capture'"
            )
            result["shared_cache"] = {
                "budget_bytes": settings.cache_max_bytes,
                "scope": "Concurrent reports and capture replay share concurrent-cache",
                "captured_raw_bytes": sum(row["obj"]["bytes"] for row in raw_objects),
                "captured_raw_names_evicted_after_replay": sum(
                    not (
                        Path(payload["owned"])
                        / "concurrent-cache"
                        / hashlib.sha256(row["obj"]["key"].encode()).hexdigest()
                    ).exists()
                    for row in raw_objects
                ),
            }
            objects_verified = verifier.verify()["ok"]
            result["phases"].append(
                {
                    "name": "verification",
                    "status": "completed",
                    "storage": dict(verifier.history.metrics),
                }
            )
        overlaps = useful_overlap(collection, readings)
        expected = campaign.arrivals * campaign.videos_per_arrival
        unique = len({item.observation_id for item in current})
        checks = {
            "all_arrivals_admitted": len(arrivals) == campaign.arrivals,
            "all_arrivals_durable": len(collection) == campaign.arrivals,
            "current_rows_complete": len(current) == unique == expected,
            "historical_arrivals_excluded": not historical,
            "arrivals_available_after_cutoff": bool(current)
            and all(item.available_at > CUTOFF and item.observed_at > CUTOFF for item in current),
            "historical_complete_bytes_preserved": all(identities),
            "objects_verified": objects_verified,
            "useful_overlap": bool(overlaps) if campaign.concurrent else True,
            "one_request_per_arrival": len(requests)
            == len({r["index"] for r in requests})
            == campaign.arrivals,
        }
        result.update(
            ok=all(checks.values()),
            status="completed" if all(checks.values()) else "failed_checks",
            worker_usage_complete=True,
            identity_preserved=all(identities),
            complete_bytes_preserved=all(identities),
            checks=checks,
            collection_seconds=collection_seconds,
            useful_overlap=overlaps,
            accounting={
                "expected_rows": expected,
                "current_rows": len(current),
                "missing_rows": max(0, expected - unique),
                "duplicate_rows": len(current) - unique,
                "missing_arrivals": campaign.arrivals - len(collection),
                "errors": errors,
            },
            process_scope={
                "worker_pid": os.getpid(),
                "postgresql_cpu_seconds": None,
                "postgresql_rss_bytes": None,
                "whole_host_peak_rss_bytes": None,
            },
        )
        return 0 if result["ok"] else 1
    except Exception as error:
        result.update(
            status="refused" if isinstance(error, ValueError) else "worker_failed",
            error_type=type(error).__name__,
        )
        return 1
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=12)
        if source is not None:
            source.shutdown()
            source.server_close()
        result["worker_lifetime_cpu_seconds"] = time.process_time()
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        result["worker_lifetime_reaped_children_cpu_seconds"] = (
            children.ru_utime + children.ru_stime
        )
        result["admitted_arrivals"] = len(arrivals)
        result["collection"] = collection
        result["source_http"] = requests
        result["errors"] = errors
        write_report(output, result)
