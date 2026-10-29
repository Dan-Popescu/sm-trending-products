"""Measure public research reports in owned synthetic PostgreSQL/HTTP Moto scratch state."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import os
import resource
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
from moto.server import ThreadedMotoServer
from psycopg import sql

from sm_trending_products.contracts import canonical, now
from sm_trending_products.costs import Store
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.research_workload import (
    CASES,
    Workload,
    price_usage,
    reports,
    require_empty_scratch,
    require_local_test_database,
    require_measured_reports,
    require_scratch_settings,
    result_identity,
    seed,
)
from sm_trending_products.settings import PlanningSettings, Settings

ROOT = Path(__file__).resolve().parents[1]
# Import definitions only. Its guarded main() is never invoked here.
SPEC = importlib.util.spec_from_file_location(
    "offline_benchmark", ROOT / "scripts/offline-benchmark.py"
)
assert SPEC and SPEC.loader
existing = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(existing)
MIB = 1024**2


def memory() -> dict:
    rss = None
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return {
        "process_rss_bytes_at_end": rss,
        "process_lifetime_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        * (1 if sys.platform == "darwin" else 1024),
        "process_tree_peak_rss_bytes": None,
        "host_peak_rss_bytes": None,
    }


@contextmanager
def sql_counts():
    """Count this isolated worker's execute calls, including its own pool threads."""
    counts = {"execute_calls": 0}
    execute = psycopg.Connection.execute
    count_lock = threading.Lock()

    def counted(connection, query, *args, **kwargs):
        with count_lock:
            counts["execute_calls"] += 1
        return execute(connection, query, *args, **kwargs)

    psycopg.Connection.execute = counted
    try:
        yield counts
    finally:
        psycopg.Connection.execute = execute


def worker(payload: dict) -> int:
    output = Path(payload["checkpoint"])
    report = {"ok": False, "status": "setting_up", "phases": []}
    existing.write_report(output, report)
    uploaded_total = 0
    cold_result = None
    try:
        workload = Workload.model_validate(payload["workload"])
        require_scratch_settings(Settings.model_validate(payload["settings"]))
        for phase in ("setup", "cold", "warm"):
            report["active_phase"] = phase
            existing.write_report(output, report)
            started, cpu_started = time.perf_counter(), time.process_time()
            measured = {"name": phase, "status": "running"}
            settings = Settings.model_validate(payload["settings"])
            # Setup imports use a different cache; cold begins with an absent application cache.
            settings.cache_dir = Path(payload["owned"]) / (
                "setup-cache" if phase == "setup" else "read-cache"
            )
            with sql_counts() as sql_work, Pipeline(settings) as application:
                original_put = application.history._put

                def bounded_put(key, body, *, original_put=original_put, **kwargs):
                    nonlocal uploaded_total
                    if uploaded_total + len(body) > payload["max_upload_bytes"]:
                        raise ValueError("Synthetic upload budget exceeded")
                    uploaded_total += len(body)
                    return original_put(key, body, **kwargs)

                application.history._put = bounded_put
                try:
                    if phase == "setup":
                        require_empty_scratch(application)
                        application.initialize(create_bucket=True)
                        measured["result"] = seed(application, workload)
                    else:
                        result = reports(application, workload)
                        measured["result"] = result_identity(result)
                        encoded = canonical(result)
                        measured["result_bytes"] = len(encoded.encode())
                        if phase == "cold":
                            cold_result = encoded
                        else:
                            measured["complete_bytes_match_cold"] = encoded == cold_result
                        require_measured_reports(result)
                    measured["status"] = "completed"
                except Exception as error:
                    measured.update(
                        status="refused" if isinstance(error, ValueError) else "failed",
                        error_type=type(error).__name__,
                    )
                finally:
                    measured.update(
                        wall_seconds=time.perf_counter() - started,
                        cpu_seconds=time.process_time() - cpu_started,
                        storage=dict(application.history.metrics),
                        analysis_reuse=dict(application.history.analysis_metrics),
                        memory=memory(),
                        sql_execute_calls=sql_work["execute_calls"],
                    )
            if phase == "setup":
                report["setup"] = measured
            else:
                report["phases"].append(measured)
            existing.write_report(output, report)
            if measured["status"] != "completed":
                report["status"] = (
                    "report_refused" if measured["status"] == "refused" else "worker_failed"
                )
                return 1
        cold, warm = report["phases"]
        report["identity_preserved"] = cold["result"] == warm["result"]
        report["complete_bytes_preserved"] = warm["complete_bytes_match_cold"]
        report.update(
            ok=report["identity_preserved"] and report["complete_bytes_preserved"],
            status="completed"
            if report["identity_preserved"] and report["complete_bytes_preserved"]
            else "identity_mismatch",
        )
        return 0 if report["ok"] else 1
    except Exception as error:
        report.update(
            status="refused" if isinstance(error, ValueError) else "worker_failed",
            error_type=type(error).__name__,
        )
        return 1
    finally:
        # Fresh worker lifetime: includes imports and close/flush outside phase timers.
        report["worker_lifetime_cpu_seconds"] = time.process_time()
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        report["worker_lifetime_reaped_children_cpu_seconds"] = (
            children.ru_utime + children.ru_stime
        )
        existing.write_report(output, report)


def test_database_url() -> str:
    database = os.environ.get("TEST_DATABASE_URL", "")
    require_local_test_database(database)
    if PlanningSettings().acquisition_enabled or os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        raise ValueError(
            "Require acquisition disabled and a dedicated loopback native test database"
        )
    return database


def _run_case(
    workload: Workload,
    *,
    output: Path,
    timeout: float = 120,
    max_disk_bytes: int = 256 * MIB,
    prices: Store | None = None,
) -> dict:
    """One fresh worker and stores per case; no collector or external provider is run."""
    report = {
        "version": "research-workload-v1",
        "synthetic": True,
        "ok": False,
        "status": "refused",
        "workload": workload.model_dump(),
        "phases": [],
        "optional_prices": None,
        "cleanup": {"schema_dropped": None, "files_removed": None, "errors": []},
    }
    try:
        workload = Workload.model_validate(workload)
        if (
            isinstance(timeout, bool)
            or not 0 < timeout <= 180
            or type(max_disk_bytes) is not int
            or not MIB <= max_disk_bytes <= 512 * MIB
        ):
            raise ValueError("Limits: runtime at most 180s and owned disk 1..512 MiB")
        database_url = test_database_url()
    except Exception as error:
        report["error_type"] = type(error).__name__
        existing.write_report(output, report)
        return report
    schema = "research_workload_" + uuid4().hex
    scoped_url = existing.scoped_database_url(database_url, schema)
    owned = Path(tempfile.mkdtemp(prefix="sm-research-workload-"))
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    process, created, server_started = None, False, False
    started, cpu_started = time.monotonic(), time.process_time()
    samples, known, reason = [], set(), None
    report.update(
        schema=schema,
        status="running",
        bounds={
            "timeout_seconds": timeout,
            "max_owned_disk_bytes": max_disk_bytes,
            "max_uploaded_bytes": max_disk_bytes,
            "sample_interval_seconds": 0.25,
            "stop_memory_headroom_bytes": 256 * MIB,
            "stop_filesystem_free_bytes": 512 * MIB,
            "cleanup_sql_timeout_seconds": 5,
        },
    )
    report["environment"] = {
        "recorded_at": now().isoformat(),
        "versions": {
            name: importlib.metadata.version(name)
            for name in ("moto", "psycopg", "boto3", "polars", "pyarrow")
        },
        "revision": existing.command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "tree": existing.command(["git", "-C", str(ROOT), "rev-parse", "HEAD^{tree}"]),
        "dirty_checkout": bool(existing.command(["git", "-C", str(ROOT), "status", "--porcelain"])),
        "hardware": existing.hardware_facts(owned),
    }
    checkpoint = owned / "checkpoint.json"
    try:
        with psycopg.connect(database_url, autocommit=True, connect_timeout=5) as connection:
            connection.execute("SET statement_timeout='5000ms'")
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            created = True
            report["environment"]["database_version"] = connection.execute(
                "SELECT version()"
            ).fetchone()[0]
        server.start()
        server_started = True
        host, port = server.get_host_and_port()
        settings = Settings(
            database_url=scoped_url,
            s3_endpoint_url=f"http://{host}:{port}",
            s3_bucket=schema.replace("_", "-"),
            cache_dir=owned / "read-cache",
            spool_dir=owned / "spool",
            cache_max_bytes=16 * MIB,
            acquisition_enabled=False,
            collector_binary="unused-offline-only",
        )
        values = settings.model_dump(mode="json")
        values["database_url"] = scoped_url
        environment = {
            **os.environ,
            "AWS_ACCESS_KEY_ID": "research-fixture",
            "AWS_SECRET_ACCESS_KEY": "research-fixture",
            "AWS_SESSION_TOKEN": "",
            "AWS_EC2_METADATA_DISABLED": "true",
            "TMPDIR": str(owned),
        }
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            env=environment,
            start_new_session=True,
        )
        assert process.stdin is not None
        process.stdin.write(
            canonical(
                {
                    "settings": values,
                    "workload": workload.model_dump(),
                    "owned": str(owned),
                    "max_upload_bytes": max_disk_bytes,
                    "checkpoint": str(checkpoint),
                }
            )
        )
        process.stdin.close()
        next_disk_sample, snapshot, disk = 0.0, {}, 0
        while process.poll() is None:
            elapsed = time.monotonic() - started
            if elapsed >= next_disk_sample:
                snapshot = existing.database_snapshot(scoped_url)
                disk = existing.owned_disk_bytes(owned)
                next_disk_sample = elapsed + 1
            progress = existing.read_report(checkpoint)
            sample = {
                "elapsed_seconds": elapsed,
                "phase": progress.get("active_phase", "startup"),
                **existing.session_resources(process.pid, known),
                "owned_file_bytes": disk,
                "schema_bytes": snapshot.get("schema_bytes"),
                "memory_headroom_bytes": existing.memory_headroom(),
            }
            samples.append(sample)
            uploads = sum(
                item.get("storage", {}).get("upload_bytes", 0)
                for item in [progress.get("setup", {}), *progress.get("phases", [])]
            )
            if elapsed >= timeout:
                reason = "case_deadline"
            elif snapshot.get("schema_bytes") is None:
                reason = "disk_measurement_unavailable"
            elif disk + snapshot["schema_bytes"] > max_disk_bytes or uploads > max_disk_bytes:
                reason = "owned_disk_limit"
            elif shutil.disk_usage(owned).free < 512 * MIB:
                reason = "disk_headroom_limit"
            elif (
                sample["memory_headroom_bytes"] is not None
                and sample["memory_headroom_bytes"] < 256 * MIB
            ):
                reason = "memory_headroom_limit"
            if reason:
                existing.stop_session(process)
                break
            time.sleep(0.25)
        report.update(existing.read_report(checkpoint))
        report["exit_code"] = process.returncode
        if reason:
            report.update(ok=False, status=reason)
        elif process.returncode != 0:
            report.update(ok=False, status=report.get("status", "worker_failed"))
        elif not report.get("identity_preserved"):
            report.update(ok=False, status="missing_worker_result")
        report["final_database_snapshot"] = existing.database_snapshot(scoped_url)
        report["published_data"] = existing.data_summary(scoped_url)
        report["final_owned_file_bytes"] = existing.owned_disk_bytes(owned)
        schema_bytes = report["final_database_snapshot"].get("schema_bytes")
        if schema_bytes is None:
            report.update(ok=False, status="disk_measurement_unavailable")
        elif report["final_owned_file_bytes"] + schema_bytes > max_disk_bytes:
            report.update(ok=False, status="owned_disk_limit")
    except Exception as error:
        report.update(
            ok=False,
            status="runner_interrupted" if isinstance(error, InterruptedError) else "runner_failed",
            error_type=type(error).__name__,
        )
    finally:
        if process is not None:
            try:
                existing.stop_session(process)
            except Exception as error:
                report["cleanup"]["errors"].append(type(error).__name__)
        if server_started:
            try:
                server.stop()
            except Exception as error:
                report["cleanup"]["errors"].append(type(error).__name__)
        if created:
            try:
                with psycopg.connect(
                    database_url, autocommit=True, connect_timeout=5
                ) as connection:
                    connection.execute("SET statement_timeout='5000ms'")
                    connection.execute(
                        sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                    )
                report["cleanup"]["schema_dropped"] = True
            except Exception as error:
                report["cleanup"]["schema_dropped"] = False
                report["cleanup"]["errors"].append(type(error).__name__)
        try:
            shutil.rmtree(owned)
            report["cleanup"]["files_removed"] = not owned.exists()
        except Exception as error:
            report["cleanup"]["files_removed"] = False
            report["cleanup"]["errors"].append(type(error).__name__)
    phases = [report.get("setup", {}), *report.get("phases", [])]
    totals = {
        key: sum(phase.get("storage", {}).get(key, 0) for phase in phases)
        for key in (
            "get_requests",
            "put_requests",
            "head_requests",
            "upload_bytes",
            "download_bytes",
        )
    }
    completed_phases = {phase.get("name") for phase in phases if phase}
    in_flight = report.get("active_phase")
    accounting_complete = in_flight in completed_phases or process is None
    report["checkpointed_usage"] = totals
    report["usage_accounting_complete"] = accounting_complete
    report["usage"] = totals if accounting_complete else {key: None for key in totals}
    if prices is not None and accounting_complete:
        report["optional_prices"] = price_usage(totals, prices)
    complete_samples = bool(samples) and all(
        sample["descendant_discovery_complete"] for sample in samples
    )
    report["supervisor"] = {
        "wall_seconds": time.monotonic() - started,
        "cpu_seconds_including_moto": time.process_time() - cpu_started,
        "samples": samples,
        "descendant_discovery_complete": complete_samples,
        "sampled_peak_session_rss_bytes": max(sample["session_rss_bytes"] for sample in samples)
        if complete_samples
        else None,
        "sampled_peak_discovered_rss_bytes": max(
            (sample["discovered_rss_bytes"] for sample in samples), default=None
        ),
    }
    report["measurement_limits"] = [
        "Each case owns a fresh worker process, schema and HTTP Moto; phases own fresh "
        "Pipeline instances.",
        "Cold clears only the application file cache by using an absent directory; warm "
        "reuses it. OS, PostgreSQL and Moto caches remain warm after setup.",
        "Wall/CPU includes instance construction and report/identity serialization; "
        "Python CPU excludes PostgreSQL and supervisor/Moto. Worker lifetime CPU "
        "includes imports and close/flush outside phase timers.",
        "Process lifetime RSS includes setup and imports; endpoint RSS is not a phase "
        "peak. Session RSS is sampled, can miss peaks and is null if child discovery "
        "fails.",
        "PostgreSQL/Moto/other host processes are excluded from worker RSS; whole "
        "VPS/process-tree peak memory remains unknown.",
        "Owned disk is sampled files plus schema relations; excludes WAL, SQL temp "
        "files, server logs and Moto RAM. Upload admission caps cumulative encoded "
        "payload before each write.",
        "Storage counters are History client logical requests/bytes, not provider "
        "bills; http_attempts includes retries. Partial in-flight phase usage can be "
        "unavailable after forced termination.",
        "Prices are optional dated scenario arithmetic, separate from measured usage. "
        "No source, paid analysis or commerce activation is run.",
        "Local synthetic planning evidence only; no target-VPS capacity certificate or "
        "production cost is established.",
    ]
    if report["cleanup"]["errors"]:
        report.update(ok=False, status="cleanup_failed")
    existing.write_report(output, report)
    return report


def run_case(workload: Workload, **options) -> dict:
    """Install owned cleanup on interruption, including callers that import this script."""
    previous = {}

    def interrupted(signum, frame):
        for number in previous:
            signal.signal(number, signal.SIG_IGN)
        raise InterruptedError("Research workload interrupted")

    if threading.current_thread() is threading.main_thread():
        previous = {
            number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)
        }
    try:
        return _run_case(workload, **options)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def read_prices(path: Path) -> Store:
    """Admit a regular descriptor before reading; FIFOs/devices cannot block preflight."""
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 65536:
            raise ValueError("Price input requires a bounded regular file")
        encoded = stream.read(65537)
    if len(encoded) > 65536:
        raise ValueError("Price input exceeds its byte budget")
    return Store.model_validate_json(encoded)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=CASES, default="combined")
    for name, _default in (
        ("products", 2),
        ("entities", 2),
        ("samples", 8),
        ("texts", 2),
        ("text-characters", 128),
        ("panel-size", 2),
        ("max-rows", 10000),
        ("text-limit", 100),
        ("batch-rows", 1000),
    ):
        parser.add_argument("--" + name, type=int)
    parser.add_argument("--shape", choices=("regular", "skewed", "revisions"), default="regular")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--max-disk-mib", type=int, default=256)
    parser.add_argument(
        "--prices", type=Path, help="Optional existing costs.Store JSON, at most 64 KiB"
    )
    args = vars(parser.parse_args(argv))
    output, timeout, disk, price_path = (
        args.pop(key) for key in ("output", "timeout", "max_disk_mib", "prices")
    )
    try:
        prices = read_prices(price_path) if price_path else None
        result = run_case(
            Workload(**{key: value for key, value in args.items() if value is not None}),
            output=output,
            timeout=timeout,
            max_disk_bytes=disk * MIB,
            prices=prices,
        )
    except (ValueError, OSError):
        parser.error("Invalid bounded workload or price input")
    print(canonical({"ok": result["ok"], "status": result["status"], "output": str(output)}))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(worker(json.load(sys.stdin)) if sys.argv[1:] == ["--worker"] else main())
