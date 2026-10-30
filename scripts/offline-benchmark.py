"""Supervise one fresh native PostgreSQL/HTTP Moto measurement, without live secrets."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from moto.server import ThreadedMotoServer
from psycopg import sql

from sm_trending_products.cli import dispatch, parser
from sm_trending_products.contracts import canonical, now
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings

GIB = 1024**3
ROOT = Path(__file__).resolve().parents[1]


def write_report(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".runner-tmp")
    temporary.write_text(canonical(value) + "\n")
    temporary.replace(path)


def read_report(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def scoped_database_url(database_url: str, schema: str) -> str:
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query.update(options=f"-c search_path={schema}", connect_timeout="5")
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, quote_via=quote),
            parsed.fragment,
        )
    )


def read_cgroup(name: str) -> str | None:
    try:
        return (Path("/sys/fs/cgroup") / name).read_text().strip()
    except OSError:
        return None


def memory_headroom() -> int | None:
    limit, current = read_cgroup("memory.max"), read_cgroup("memory.current")
    if limit and limit.isdecimal() and current and current.isdecimal():
        return max(0, int(limit) - int(current))
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def command(arguments: list[str]) -> str | None:
    try:
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=5, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def hardware_facts(directory: Path) -> dict:
    cpu_model = None
    try:
        cpu_model = next(
            line.partition(":")[2].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        )
    except (OSError, StopIteration):
        pass
    disk = shutil.disk_usage(directory)
    return {
        "os": platform.system(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(),
        "affinity_cpus": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "cgroup": {
            name: read_cgroup(name)
            for name in ("cpu.max", "cpu.stat", "memory.max", "memory.current", "pids.max")
        },
        "memory_headroom_bytes": memory_headroom(),
        "disk_total_bytes": disk.total,
        "disk_free_bytes": disk.free,
        "filesystem": command(["stat", "-f", "-c", "%T", str(directory)]),
    }


def collector_identity(path: Path, profile: str) -> dict:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return {"path": str(path.resolve()), "sha256": checksum.hexdigest(), "profile": profile}


def session_resources(session_id: int, known_pids: set[int], *, details: bool = False) -> dict:
    """Visit only the owned worker and its thread-child links, never scan host PIDs."""
    pending, visited, members = list(known_pids | {session_id}), set(), []
    complete = True
    while pending:
        pid = pending.pop()
        if pid in visited:
            continue
        visited.add(pid)
        directory = Path("/proc") / str(pid)
        try:
            fields = (directory / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[3]) != session_id or fields[0] == "Z":
                continue
            members.append(
                (
                    pid,
                    max(0, int(fields[21])) * os.sysconf("SC_PAGE_SIZE"),
                    (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                )
            )
            for task in (directory / "task").iterdir():
                try:
                    pending.extend(int(value) for value in (task / "children").read_text().split())
                except FileNotFoundError:
                    # Some managed /proc mounts omit children entirely. Do not
                    # silently present the worker alone as the whole session.
                    if task.exists():
                        complete = False
                except OSError:
                    complete = False
        except (OSError, ValueError, IndexError):
            continue
    known_pids.update(pid for pid, _, _ in members)
    return {
        **(
            {
                "processes": [
                    {"pid": pid, "rss_bytes": rss, "cpu_seconds": cpu} for pid, rss, cpu in members
                ]
            }
            if details
            else {}
        ),
        "session_rss_bytes": sum(rss for _, rss, _ in members) if complete else None,
        "discovered_rss_bytes": sum(rss for _, rss, _ in members),
        "live_discovered_cpu_seconds": sum(cpu for _, _, cpu in members),
        "process_count": len(members),
        "descendant_discovery_complete": complete,
    }


def stop_session(process: subprocess.Popen) -> None:
    """The caller created this child with start_new_session=True; signal only that group."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    # The leader may have exited while a collector ignored SIGTERM.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=2)


def owned_disk_bytes(directory: Path) -> int:
    seen, allocated = set(), 0
    for path in directory.rglob("*"):
        try:
            info = path.stat()
            identity = (info.st_dev, info.st_ino)
            if identity not in seen and path.is_file():
                seen.add(identity)
                allocated += info.st_blocks * 512
        except FileNotFoundError:
            continue
    return allocated


def database_snapshot(database_url: str) -> dict:
    try:
        with psycopg.connect(database_url, connect_timeout=2) as connection:
            connection.execute("SET statement_timeout='1000ms'")
            if connection.execute("SELECT to_regclass('collection_tasks')").fetchone()[0] is None:
                return {"queue": [], "schema_bytes": 0}
            queue = connection.execute(
                "SELECT state,count(*) FROM collection_tasks GROUP BY state ORDER BY state"
            ).fetchall()
            size = connection.execute(
                "SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=current_schema() AND c.relkind IN ('r','m')"
            ).fetchone()[0]
            return {
                "queue": [{"state": state, "count": count} for state, count in queue],
                "schema_bytes": int(size),
            }
    except psycopg.Error:
        return {"queue": None, "schema_bytes": None, "unavailable": "snapshot_query_failed"}


def data_summary(database_url: str) -> dict:
    """Summarize published descriptor bytes without additional object-store requests."""
    try:
        with psycopg.connect(database_url, connect_timeout=2) as connection:
            connection.execute("SET statement_timeout='1000ms'")
            objects = connection.execute(
                "WITH refs AS (SELECT 'raw' AS kind,manifest->'raw' AS obj,"
                "compacted_into IS NULL AS active FROM datasets WHERE state='committed' UNION ALL "
                "SELECT 'normalized',obj,compacted_into IS NULL FROM datasets "
                "CROSS JOIN LATERAL jsonb_array_elements(manifest->'normalized') obj "
                "WHERE state='committed') SELECT kind,obj->>'key',max((obj->>'bytes')::bigint),"
                "bool_or(active) FROM refs WHERE obj->>'bytes' IS NOT NULL "
                "GROUP BY kind,obj->>'key'"
            ).fetchall()
            policies = connection.execute(
                "SELECT source,retention_days,verified FROM source_policies ORDER BY source"
            ).fetchall()
            original_rows = connection.execute(
                "SELECT COALESCE(sum(row_count),0) FROM datasets "
                "WHERE state='committed' AND task_id IS NOT NULL"
            ).fetchone()[0]
        result = {}
        for kind in ("raw", "normalized"):
            for scope in ("retained", "active"):
                sizes = sorted(
                    size
                    for item, _, size, active in objects
                    if item == kind and (scope == "retained" or active)
                )
                result[f"{scope}_{kind}_objects"] = {
                    "count": len(sizes),
                    "total_bytes": sum(sizes),
                    "min_bytes": min(sizes) if sizes else None,
                    "max_bytes": max(sizes) if sizes else None,
                    "mean_bytes": sum(sizes) / len(sizes) if sizes else None,
                    "p50_bytes": sizes[(len(sizes) - 1) // 2] if sizes else None,
                }
        result["original_published_rows"] = int(original_rows)
        result["retention_policies"] = [
            {"source": source, "retention_days": days, "verified": verified}
            for source, days, verified in policies
        ]
        result["scope"] = (
            "Unique objects referenced by committed manifests; excludes manifests/derived objects"
        )
        return result
    except psycopg.Error:
        return {"unavailable": "descriptor_summary_query_failed"}


def worker(payload: dict) -> int:
    args = parser().parse_args(["benchmark", *payload["arguments"]])
    try:
        with Pipeline(Settings.model_validate(payload["settings"])) as application:
            application.initialize(create_bucket=True)
            result = (
                interruption_probe(application, args.output)
                if payload["interruption_probe"]
                else dispatch(application, args)
            )
        write_report(args.output, result)
        return 0 if result["ok"] else 1
    except Exception as error:
        result = read_report(args.output)
        # Exception text can contain connection strings; its type is sufficient here.
        result.update(ok=False, complete=True, status="failed", runner_error=type(error).__name__)
        write_report(args.output, result)
        return 1


def probe_worker(payload: dict) -> int:
    with Pipeline(Settings.model_validate(payload["settings"])) as application:
        original = application.history._put

        def put(key, body, **kwargs):
            result = original(key, body, **kwargs)
            if key.startswith("raw/"):
                Path(payload["marker"]).write_text("raw_object_uploaded\n")
                while True:
                    time.sleep(1)
            return result

        application.history._put = put
        application.collect_batch(1, synthetic=True)
    return 1


def interruption_probe(application: Pipeline, output: Path) -> dict:
    """Kill one raw-uploaded worker; drain a small backlog through actual lease expiry."""
    values = application.settings.model_dump()
    values.update(lease_seconds=40, collector_timeout_seconds=5)
    settings = Settings.model_validate(values)
    account, requests = "interruption-" + uuid4().hex[:12], []

    class Source(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            page = int(body.get("cursor", 0))
            requests.append(page)
            response = canonical(
                {
                    "data": {
                        "videos": [
                            {"id": f"{account}-{page}-{index}", "view_count": 10}
                            for index in range(3)
                        ],
                        "has_more": False,
                        "cursor": 0,
                    },
                    "error": {"code": "ok"},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    thread = threading.Thread(target=source.serve_forever, daemon=True)
    thread.start()
    start, crashed, samples = time.monotonic(), None, []
    marker = application.settings.spool_dir / "interruption-marker"
    marker.parent.mkdir(parents=True, exist_ok=True)
    try:
        application.catalog.configure_budget("tiktok_display", account, 1000, 1000)
        tasks = [
            application.enqueue(
                "tiktok_display",
                account,
                "list",
                {"cursor": page, "endpoint_override": f"http://127.0.0.1:{source.server_port}"},
                synthetic=True,
            )
            for page in range(20)
        ]
        worker_settings = settings.model_dump(mode="json")
        worker_settings["database_url"] = settings.database_url.get_secret_value()
        crashed = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--probe-worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        assert crashed.stdin is not None
        crashed.stdin.write(canonical({"settings": worker_settings, "marker": str(marker)}))
        crashed.stdin.close()
        marker_deadline = time.monotonic() + 15
        while not marker.exists() and time.monotonic() < marker_deadline:
            if crashed.poll() is not None:
                raise RuntimeError("Interruption worker exited before raw upload")
            time.sleep(0.05)
        if not marker.exists():
            raise TimeoutError("Raw publication boundary was not reached")
        leased = application.catalog.one(
            "SELECT task_id,lease_until,lease_version FROM collection_tasks WHERE state='leased'"
        )
        crash_at = time.monotonic()
        crashed.kill()
        crashed.wait(timeout=5)
        completed_while_fenced = 0
        with Pipeline(settings) as replacement:
            while time.monotonic() - start < 90:
                result = replacement.collect_batch(20, synthetic=True)
                states = replacement.catalog.all(
                    "SELECT state,count(*) AS count FROM collection_tasks "
                    "GROUP BY state ORDER BY state"
                )
                samples.append({"elapsed_seconds": time.monotonic() - start, "queue": states})
                write_report(
                    output,
                    {
                        "ok": False,
                        "complete": False,
                        "status": "recovering",
                        "queue_samples": samples,
                    },
                )
                task = replacement.catalog.one(
                    "SELECT state,lease_until>clock_timestamp() AS fenced FROM collection_tasks "
                    "WHERE task_id=%s",
                    (leased["task_id"],),
                )
                if task["fenced"]:
                    completed_while_fenced += result["processed"]
                if states == [{"state": "done", "count": len(tasks)}]:
                    break
                if result["errors"]:
                    raise RuntimeError("Replacement collection failed")
                time.sleep(0.25)
            replacement_metrics = dict(replacement.history.metrics)
        task = application.catalog.one(
            "SELECT state,lease_version FROM collection_tasks WHERE task_id=%s",
            (leased["task_id"],),
        )
        observations, _ = application.history.observations(now(), synthetic=True)
        checks = {
            "killed_after_raw_upload": crashed.returncode == -signal.SIGKILL,
            "backlog_progressed_before_expiry": completed_while_fenced > 0,
            "reclaimed_with_new_fence": task["state"] == "done"
            and task["lease_version"] > leased["lease_version"],
            "no_lost_or_duplicate_observations": len(observations) == 60
            and len({item.observation_id for item in observations}) == 60,
            "no_staged_datasets": application.catalog.one(
                "SELECT count(*) AS count FROM datasets WHERE state='staged'"
            )["count"]
            == 0,
            "objects_verified": application.verify()["ok"],
        }
        traffic = {
            key: value + replacement_metrics.get(key, 0)
            for key, value in application.history.metrics.items()
        }
        return {
            "ok": all(checks.values()),
            "complete": True,
            "status": "completed" if all(checks.values()) else "failed",
            "synthetic": True,
            "probe": "raw_upload_worker_termination_actual_lease_expiry",
            "checks": checks,
            "failed_checks": [name for name, passed in checks.items() if not passed],
            "measurements": {
                "elapsed_seconds": time.monotonic() - start,
                "recovery_seconds": time.monotonic() - crash_at,
                "actual_records": len(observations),
                "tasks": len(tasks),
                "tasks_completed_while_lease_fenced": completed_while_fenced,
                "source_requests": len(requests),
                "storage_traffic": traffic,
            },
            "queue_samples": samples,
            "workload": {"lease_seconds": 40, "collector_timeout_seconds": 5},
            "measurement_limits": [
                "40-second experimental lease; crashed worker traffic is excluded"
            ],
            "operating_envelope": "controlled_interruption_sample_only",
        }
    finally:
        if crashed is not None and crashed.poll() is None:
            crashed.kill()
            crashed.wait(timeout=5)
        source.shutdown()
        source.server_close()
        thread.join(timeout=2)


def _run_case(
    arguments: list[str],
    *,
    collector_binary: Path,
    collector_profile: str = "unspecified",
    case_timeout: float = 180,
    max_disk_bytes: int = GIB,
    interruption: bool = False,
) -> dict:
    if not 0 < case_timeout <= 180 or not 0 < max_disk_bytes <= GIB:
        raise ValueError("Case limits must be positive and at most 180 seconds / 1 GiB")
    args = parser().parse_args(["benchmark", *arguments])
    if args.records > 10000 or args.workers > 4:
        raise ValueError("Offline cases are limited to 10000 records and four workers")
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        raise ValueError("Set TEST_DATABASE_URL to an isolated native test database")
    identity = collector_identity(collector_binary, collector_profile)
    schema = "benchmark_" + uuid4().hex
    scoped_url = scoped_database_url(database_url, schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.unlink(missing_ok=True)
    before_hardware = hardware_facts(args.output.parent)
    before_cpu, started = time.process_time(), time.monotonic()
    process, settings, schema_created = None, None, False
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    result, samples, cleanup_errors, reason = {}, [], [], None
    last_checkpoint = {}
    database_version = None
    with tempfile.TemporaryDirectory(prefix="sm-trends-benchmark-") as temporary:
        owned = Path(temporary)
        try:
            with psycopg.connect(database_url, autocommit=True, connect_timeout=5) as connection:
                connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                schema_created = True
                database_version = connection.execute("SELECT version()").fetchone()[0]
            server.start()
            host, port = server.get_host_and_port()
            settings = Settings(
                database_url=scoped_url,
                s3_bucket=schema.replace("_", "-"),
                s3_endpoint_url=f"http://{host}:{port}",
                aws_default_region="us-east-1",
                cache_dir=owned / "cache",
                spool_dir=owned / "spool",
                collector_binary=str(collector_binary.resolve()),
            )
            if interruption:
                settings = Settings.model_validate(
                    {**settings.model_dump(), "lease_seconds": 40, "collector_timeout_seconds": 5}
                )
            values = settings.model_dump(mode="json")
            values["database_url"] = scoped_url
            (owned / "tmp").mkdir()
            env = {
                **os.environ,
                "AWS_ACCESS_KEY_ID": "offline-fixture",
                "AWS_SECRET_ACCESS_KEY": "offline-fixture",
                "AWS_SESSION_TOKEN": "",
                "AWS_EC2_METADATA_DISABLED": "true",
                "TIKTOK_DISPLAY_ACCESS_TOKEN": "offline-fixture",
                "TIKTOK_CCL_ACCESS_TOKEN": "offline-fixture",
                "TMPDIR": str(owned / "tmp"),
            }
            with args.output.with_suffix(".worker.log").open("w") as log:
                process = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), "--case-worker"],
                    stdin=subprocess.PIPE,
                    stdout=log,
                    stderr=log,
                    text=True,
                    env=env,
                    start_new_session=True,
                )
                assert process.stdin is not None
                process.stdin.write(
                    canonical(
                        {
                            "settings": values,
                            "arguments": arguments,
                            "interruption_probe": interruption,
                        }
                    )
                )
                process.stdin.close()
                next_snapshot, latest_database, known_pids, latest_disk = 0.0, {}, set(), 0
                while process.poll() is None:
                    elapsed = time.monotonic() - started
                    if elapsed >= next_snapshot:
                        latest_database = database_snapshot(scoped_url)
                        latest_disk = owned_disk_bytes(owned)
                        next_snapshot = elapsed + 1
                    sample = {
                        "elapsed_seconds": elapsed,
                        **session_resources(process.pid, known_pids),
                        "owned_file_bytes": latest_disk,
                        "memory_headroom_bytes": memory_headroom(),
                        **latest_database,
                    }
                    samples.append(sample)
                    checkpoint = read_report(args.output)
                    if checkpoint:
                        last_checkpoint = checkpoint
                    if elapsed >= case_timeout:
                        reason = "case_deadline"
                    elif (
                        sample["owned_file_bytes"] + (sample.get("schema_bytes") or 0)
                        > max_disk_bytes
                    ):
                        reason = "owned_disk_limit"
                    elif (
                        sample["memory_headroom_bytes"] is not None
                        and sample["memory_headroom_bytes"] < 2 * GIB
                    ):
                        reason = "memory_headroom_limit"
                    elif shutil.disk_usage(owned).free < 2 * GIB:
                        reason = "disk_headroom_limit"
                    if reason:
                        break
                    time.sleep(0.25)
                stop_session(process)
            result = read_report(args.output) or last_checkpoint
            if reason:
                result.update(
                    ok=False,
                    complete=True,
                    status=reason,
                    interrupted_phase=result.get("active_phase", result.get("status")),
                )
            elif process.returncode != 0:
                result.update(ok=False, complete=True, status=result.get("status", "worker_failed"))
            elif not result:
                result.update(ok=False, complete=True, status="missing_worker_report")
        except Exception as error:
            result = read_report(args.output) or last_checkpoint
            result.update(
                ok=False,
                complete=True,
                status="runner_interrupted"
                if isinstance(error, InterruptedError)
                else "runner_failed",
                runner_error=type(error).__name__,
            )
        finally:
            if process is not None:
                try:
                    stop_session(process)
                except (OSError, subprocess.TimeoutExpired) as error:
                    cleanup_errors.append(type(error).__name__)
            if schema_created:
                result["final_database_snapshot"] = database_snapshot(scoped_url)
                result["published_data"] = data_summary(scoped_url)
            try:
                server.stop()
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
            if schema_created:
                try:
                    with psycopg.connect(
                        database_url, autocommit=True, connect_timeout=5
                    ) as connection:
                        connection.execute("SET statement_timeout='5000ms'")
                        connection.execute(
                            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                        )
                except psycopg.Error as error:
                    cleanup_errors.append(type(error).__name__)
        result["runner"] = {
            "schema": schema,
            "case_timeout_seconds": case_timeout,
            "max_owned_disk_bytes": max_disk_bytes,
            "elapsed_seconds": time.monotonic() - started,
            "exit_code": process.returncode if process is not None else None,
            "stop_reason": reason,
            "cleanup_errors": cleanup_errors,
            "samples": samples,
            "sample_interval_seconds": 0.25,
            "sampled_peak_session_rss_bytes": max(
                (row["session_rss_bytes"] for row in samples), default=0
            )
            if all(row["descendant_discovery_complete"] for row in samples)
            else None,
            "sampled_peak_discovered_process_rss_bytes": max(
                (row["discovered_rss_bytes"] for row in samples), default=0
            ),
            "descendant_discovery_complete": bool(samples)
            and all(row["descendant_discovery_complete"] for row in samples),
            "supervisor_cpu_seconds": time.process_time() - before_cpu,
            "supervisor_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        if cleanup_errors:
            result.update(ok=False, status="cleanup_failed")
        result.setdefault("complete", True)
        result["environment"] = {
            "revision": command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
            "tree": command(["git", "-C", str(ROOT), "rev-parse", "HEAD^{tree}"]),
            "recorded_at": now().isoformat(),
            "dirty_checkout": bool(command(["git", "-C", str(ROOT), "status", "--porcelain"])),
            "collector": identity,
            "hardware_before": before_hardware,
            "hardware_after": hardware_facts(args.output.parent),
            "versions": {
                name: importlib.metadata.version(name)
                for name in ("moto", "polars", "duckdb", "pyarrow", "psycopg", "boto3")
            },
            "effective_settings": {
                key: value
                for key, value in (settings.model_dump(mode="json") if settings else {}).items()
                if key not in {"database_url", "s3_endpoint_url"}
            },
            "thread_environment": {
                name: os.environ.get(name) for name in ("POLARS_MAX_THREADS", "RAYON_NUM_THREADS")
            },
        }
        result["verification_boundary"] = {
            "database": database_version,
            "object_store": "supervisor-owned loopback HTTP Moto",
            "worker_mode": "crashed child process and normal replacement Pipeline"
            if interruption
            else "one Python process with Pipeline threads and Rust children",
            "synthetic": True,
            "production_acceptance": False,
        }
        result.setdefault("measurement_limits", []).extend(
            [
                "Session RSS is sampled at 250 ms; brief peaks may be missed",
                "Missing /proc children: aggregate RSS unavailable; "
                "discovered RSS is a lower bound",
                "Supervisor CPU/RSS includes Moto; worker CPU includes the controlled source",
                "PostgreSQL/Dagster/dashboard and other host processes are excluded from RSS/CPU",
                "Logical observation timestamps do not measure live upstream freshness",
                "Schema/files exclude PostgreSQL WAL, temp files and container disk",
            ]
        )
        write_report(args.output, result)
        return result


def run_case(arguments: list[str], **options) -> dict:
    # The matrix imports this function, so signal cleanup belongs here, not only in main().
    previous = {}

    def interrupted(signum, frame):
        for number in previous:
            signal.signal(number, signal.SIG_IGN)
        raise InterruptedError(f"Runner interrupted by signal {signum}")

    if threading.current_thread() is threading.main_thread():
        previous = {
            number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)
        }
    try:
        return _run_case(arguments, **options)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def main() -> int:
    if sys.argv[1:] == ["--case-worker"]:
        return worker(json.load(sys.stdin))
    if sys.argv[1:] == ["--probe-worker"]:
        return probe_worker(json.load(sys.stdin))
    options = argparse.ArgumentParser(add_help=False)
    options.add_argument(
        "--collector-binary", type=Path, default=ROOT / "collector/target/debug/sm-collector"
    )
    options.add_argument(
        "--collector-profile", choices=("debug", "release", "unspecified"), default="debug"
    )
    options.add_argument("--case-timeout", type=float, default=180)
    options.add_argument("--max-disk-bytes", type=int, default=GIB)
    options.add_argument("--interruption-probe", action="store_true")
    runner, arguments = options.parse_known_args()

    result = run_case(
        arguments,
        collector_binary=runner.collector_binary,
        collector_profile=runner.collector_profile,
        case_timeout=runner.case_timeout,
        max_disk_bytes=runner.max_disk_bytes,
        interruption=runner.interruption_probe,
    )
    print(canonical(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
