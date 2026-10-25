"""Measure current always-on roles against isolated synthetic PostgreSQL/S3 data.

This bounded probe measures startup, idle and a short paced acquisition/replay
window. It does not establish retained-history throughput or deployed capacity.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import json
import math
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
TICKS = os.sysconf("SC_CLK_TCK")


class DockerConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(3)
        self.sock.connect("/var/run/docker.sock")


def postgres_sample(container: str) -> dict:
    with contextlib.closing(DockerConnection("localhost")) as connection:
        connection.request("GET", f"/containers/{container}/stats?stream=false&one-shot=true")
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError("Cannot read isolated PostgreSQL container statistics")
        value = json.loads(response.read())
    memory = value["memory_stats"]
    cache = memory.get("stats", {}).get("inactive_file", 0)
    return {
        "process_count": value.get("pids_stats", {}).get("current"),
        "cpu_seconds": value["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9,
        "pss_bytes": None,
        "rss_bytes": None,
        "cgroup_memory_bytes": memory["usage"],
        "cgroup_memory_limit_bytes": memory["limit"],
        "cgroup_inactive_file_bytes": cache,
        "cgroup_working_set_bytes": max(0, memory["usage"] - cache),
        "exit_code": None,
    }


def available_port() -> int:
    with socket.socket() as connection:
        connection.bind(("127.0.0.1", 0))
        return connection.getsockname()[1]


def process_table() -> dict[int, tuple[int, float, int, int]]:
    """Own/reaped-descendant CPU and sampled RSS/PSS; missing PSS is explicit."""
    rows = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            # stat fields 4,14..17,24; tail starts with field 3.
            cpu = sum(int(fields[index]) for index in (11, 12, 13, 14)) / TICKS
            rss = int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
            pss = 0 if rss == 0 else -1
            try:
                for line in (entry / "smaps_rollup").read_text().splitlines():
                    if line.startswith("Pss:"):
                        pss = int(line.split()[1]) * 1024
                        break
            except OSError:
                pass
            rows[int(entry.name)] = (int(fields[1]), cpu, rss, pss)
        except (OSError, ValueError, IndexError):
            continue
    return rows


def descendants(table: dict, pid: int) -> list[int]:
    selected = {pid} if pid in table else set()
    while True:
        more = {child for child, row in table.items() if row[0] in selected} - selected
        if not more:
            return sorted(selected)
        selected.update(more)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def sample_roles(processes: dict[str, subprocess.Popen], postgres_container: str) -> dict:
    table = process_table()
    result = {}
    roots = [(name, process.pid, process.poll()) for name, process in processes.items()]
    for name, pid, exit_code in roots:
        members = descendants(table, pid)
        result[name] = {
            "process_count": len(members),
            "cpu_seconds": sum(table[pid][1] for pid in members),
            "rss_bytes": sum(table[pid][2] for pid in members),
            "pss_bytes": sum(table[pid][3] for pid in members)
            if members and all(table[pid][3] >= 0 for pid in members)
            else None,
            "exit_code": exit_code,
        }
    result["postgres"] = postgres_sample(postgres_container)
    return result


def phase_statistics(samples: list[dict]) -> dict:
    result = {}
    for phase in dict.fromkeys(sample["phase"] for sample in samples):
        rows = [sample for sample in samples if sample["phase"] == phase]
        duration = rows[-1]["monotonic"] - rows[0]["monotonic"]
        roles = {}
        for name in rows[-1]["roles"]:
            if name not in rows[0]["roles"]:
                continue
            first, last = rows[0]["roles"][name], rows[-1]["roles"][name]
            cpu = last["cpu_seconds"] - first["cpu_seconds"]
            memory = [row["roles"][name]["pss_bytes"] for row in rows]
            roles[name] = {
                "cpu_seconds": cpu if cpu >= 0 else None,
                "mean_occupied_cpu_cores": cpu / duration if duration > 0 and cpu >= 0 else None,
                "pss_p50_bytes": percentile(memory, 0.5)
                if all(value is not None for value in memory)
                else None,
                "pss_p95_bytes": percentile(memory, 0.95)
                if all(value is not None for value in memory)
                else None,
                "peak_process_count": max(row["roles"][name]["process_count"] or 0 for row in rows),
                "pss_sample_count": sum(value is not None for value in memory),
            }
        application_names = [name for name in roles if name not in {"s3_fixture", "postgres"}]
        resident = []
        for row in rows:
            memory = [row["roles"][name]["pss_bytes"] for name in application_names]
            if all(value is not None for value in memory):
                resident.append(sum(memory))
        postgres = [row["roles"]["postgres"] for row in rows]
        result[phase] = {
            "seconds": duration,
            "sample_count": len(rows),
            "roles": roles,
            "application_pss_p50_bytes": percentile(resident, 0.5),
            "application_pss_p95_bytes": percentile(resident, 0.95),
            "application_pss_sample_count": len(resident),
            "postgres_cgroup_memory_p95_bytes": percentile(
                [row["cgroup_memory_bytes"] for row in postgres], 0.95
            ),
            "postgres_cgroup_working_set_p95_bytes": percentile(
                [row["cgroup_working_set_bytes"] for row in postgres], 0.95
            ),
            "postgres_inactive_file_p95_bytes": percentile(
                [row["cgroup_inactive_file_bytes"] for row in postgres], 0.95
            ),
        }
    return result


def measure(args: argparse.Namespace) -> dict:
    from sm_trending_products.pipeline import Pipeline
    from sm_trending_products.settings import Settings

    parsed = urlsplit(args.database_url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("Use an isolated loopback PostgreSQL test server")
    if not args.collector_binary.is_file():
        raise ValueError("Build the current collector before running the probe")
    token = uuid4().hex[:12]
    schema, dagster_database = f"runtime_{token}", f"runtime_dagster_{token}"
    ports = {name: available_port() for name in ("s3", "dagster", "dashboard", "source")}
    request_count = 0

    class Source(BaseHTTPRequestHandler):
        def do_POST(self):
            nonlocal request_count
            request_count += 1
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            body = json.dumps(
                {
                    "data": {
                        "videos": [
                            {"id": f"runtime-{index}", "view_count": 10 + request_count}
                            for index in range(args.records_per_capture)
                        ],
                        "has_more": False,
                        "cursor": 0,
                    },
                    "error": {"code": "ok"},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *arguments):
            pass

    processes: dict[str, subprocess.Popen] = {}
    logs = {}
    samples = []
    report = {
        "complete": False,
        "started_at": datetime.now(UTC).isoformat(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT)
        .decode()
        .strip(),
        "synthetic": True,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "collector_sha256": hashlib.sha256(args.collector_binary.read_bytes()).hexdigest(),
        "collector_profile": "caller_supplied; debug in the recorded local qualification",
        "measurement_limits": [
            "Short low-stock workload; no retained-history or live-source capacity claim",
            "Native roles share one CPU affinity; other agents share the four-CPU host quota",
            "PSS apportions shared resident pages; it excludes host/OS and kernel memory",
            "No dashboard browser session or heavy analytical job is included",
            "The local S3 emulator is measured separately and is not a VPS application role",
            "No VPS provider, source grant or external network latency is measured",
            "PostgreSQL uses isolated-container CPU and cgroup memory, not process PSS",
        ],
        "workload": {
            "idle_seconds_requested": args.idle_seconds,
            "active_seconds_requested": args.active_seconds,
            "capture_arrivals_per_second": args.arrivals_per_second,
            "records_per_capture": args.records_per_capture,
            "drain_seconds_limit": args.drain_seconds,
        },
        "host": {
            "cpu_quota": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
            "memory_limit_bytes": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
            "application_affinity_cpu": min(os.sched_getaffinity(0)),
        },
    }

    def checkpoint():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        destination = args.output.with_suffix(args.output.suffix + ".tmp")
        destination.write_text(json.dumps(report, indent=2) + "\n")
        destination.replace(args.output)

    checkpoint()
    with tempfile.TemporaryDirectory(prefix="sm-runtime-") as temporary:
        directory = Path(temporary)
        environment = os.environ.copy()
        query = dict(parse_qsl(parsed.query))
        query["options"] = f"-c search_path={schema}"
        scoped_url = urlunsplit(parsed._replace(query=urlencode(query, quote_via=quote)))
        dagster_url = urlunsplit(parsed._replace(path=f"/{dagster_database}", query=""))
        dagster_url = dagster_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        environment.update(
            DATABASE_URL=scoped_url,
            S3_ENDPOINT_URL=f"http://127.0.0.1:{ports['s3']}",
            S3_BUCKET=f"runtime-{token}",
            AWS_ACCESS_KEY_ID="runtime-fixture-access",
            AWS_SECRET_ACCESS_KEY="runtime-fixture-secret",
            AWS_EC2_METADATA_DISABLED="true",
            AWS_DEFAULT_REGION="us-east-1",
            TIKTOK_DISPLAY_ACCESS_TOKEN="runtime-fixture-token",
            ACQUISITION_ENABLED="true",
            CACHE_DIR=str(directory / "cache"),
            SPOOL_DIR=str(directory / "spool"),
            DAGSTER_HOME=str(directory / "dagster"),
            DAGSTER_DATABASE_URL=dagster_url,
            COLLECTOR_BINARY=str(args.collector_binary.resolve()),
            PYTHONPATH=str(ROOT / "src"),
            POLARS_MAX_THREADS="1",
            RAYON_NUM_THREADS="1",
            DUCKDB_THREADS="1",
            PIPELINE_NAMESPACE=f"runtime-{token}",
        )
        dagster_home = directory / "dagster"
        dagster_home.mkdir()
        dagster_configuration = (ROOT / "deploy/dagster/dagster.yaml").read_text()
        storage_end = dagster_configuration.index("run_coordinator:")
        dagster_configuration = (
            "storage:\n  postgres:\n    postgres_url:\n      env: DAGSTER_DATABASE_URL\n"
            + dagster_configuration[storage_end:]
        ).replace("/var/lib/sm-trends/dagster/compute-logs", str(dagster_home / "compute-logs"))
        (dagster_home / "dagster.yaml").write_text(dagster_configuration)
        source = ThreadingHTTPServer(("127.0.0.1", ports["source"]), Source)
        thread = threading.Thread(target=source.serve_forever, daemon=True)
        thread.start()
        with psycopg.connect(args.database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            connection.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dagster_database))
            )

        def start(name: str, arguments: list[str]) -> None:
            logs[name] = (directory / f"{name}.log").open("w+")
            role_environment = environment | {"WORKER_ID": f"runtime-{name}-{token}"}
            processes[name] = subprocess.Popen(
                arguments,
                cwd=ROOT,
                env=role_environment,
                stdout=logs[name],
                stderr=subprocess.STDOUT,
                start_new_session=True,
                preexec_fn=lambda: os.sched_setaffinity(0, {min(os.sched_getaffinity(0))}),
            )

        def sample(phase: str):
            roles = sample_roles(processes, args.postgres_container)
            samples.append({"monotonic": time.monotonic(), "phase": phase, "roles": roles})
            memory = sum(row["pss_bytes"] or 0 for name, row in roles.items() if name != "postgres")
            if memory > args.max_application_pss_mib * 1024**2:
                raise RuntimeError("Application PSS reached the declared safety bound")
            exited = [name for name, row in roles.items() if row["exit_code"] is not None]
            if exited:
                raise RuntimeError("A measured role exited: " + ",".join(exited))
            if len(samples) % 20 == 0:
                report["samples"] = samples
                checkpoint()

        def wait_window(seconds: float, phase: str):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                sample(phase)
                time.sleep(min(0.5, max(0, deadline - time.monotonic())))

        try:
            start(
                "s3_fixture",
                [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(ports["s3"])],
            )
            deadline = time.monotonic() + 30
            while True:
                try:
                    urllib.request.urlopen(environment["S3_ENDPOINT_URL"], timeout=1).close()
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("S3 fixture did not start") from None
                    time.sleep(0.1)
            os.environ.update(environment)
            settings = Settings(**{key.lower(): value for key, value in environment.items()})
            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from sm_trending_products.orchestration import defs; "
                    "defs.get_repository_def().load_all_definitions()",
                ],
                env=environment,
                check=True,
                timeout=20,
                capture_output=True,
            )
            report["readiness"] = {"complete_repository_load": True}
            with Pipeline(settings) as pipeline:
                pipeline.initialize(create_bucket=True)
                pipeline.catalog.configure_budget("tiktok_display", "runtime-account", 1000, 1000)
                cli = [
                    sys.executable,
                    "-c",
                    "from sm_trending_products.cli import main; raise SystemExit(main())",
                ]
                start("capture", [*cli, "capture", "--loop", "--synthetic"])
                start("normalizer", [*cli, "publish-captures", "--loop", "--synthetic"])
                start(
                    "planner",
                    [
                        sys.executable,
                        "-m",
                        "sm_trending_products.capture_coordinator",
                        "--loop",
                        "--synthetic",
                    ],
                )
                executable_directory = Path(sys.executable).parent
                start(
                    "dagster_webserver",
                    [
                        str(executable_directory / "dagster-webserver"),
                        "-m",
                        "sm_trending_products.orchestration",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(ports["dagster"]),
                    ],
                )
                start(
                    "dagster_daemon",
                    [
                        str(executable_directory / "dagster-daemon"),
                        "run",
                        "-m",
                        "sm_trending_products.orchestration",
                    ],
                )
                start(
                    "dashboard",
                    [
                        str(executable_directory / "streamlit"),
                        "run",
                        "src/sm_trending_products/dashboard.py",
                        "--server.address",
                        "127.0.0.1",
                        "--server.port",
                        str(ports["dashboard"]),
                        "--server.headless",
                        "true",
                        "--browser.gatherUsageStats",
                        "false",
                    ],
                )
                # Warmup is measured but kept separate from steady idle work.
                wait_window(args.warmup_seconds, "startup")
                for role, endpoint in (
                    ("dagster_webserver", f"http://127.0.0.1:{ports['dagster']}/server_info"),
                    ("dashboard", f"http://127.0.0.1:{ports['dashboard']}/_stcore/health"),
                ):
                    with urllib.request.urlopen(endpoint, timeout=3) as response:
                        if response.status != 200:
                            raise RuntimeError(f"{role} did not become healthy")
                query = (
                    "{ repositoriesOrError { __typename "
                    "... on RepositoryConnection { nodes { name } } } }"
                )
                graphql = urllib.request.Request(
                    f"http://127.0.0.1:{ports['dagster']}/graphql",
                    data=json.dumps({"query": query}).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(graphql, timeout=10) as response:
                    repositories = json.loads(response.read())["data"]["repositoriesOrError"]
                if (
                    repositories["__typename"] != "RepositoryConnection"
                    or not repositories["nodes"]
                ):
                    raise RuntimeError("The running Dagster workspace did not load its repository")
                report["readiness"]["running_workspace_repositories"] = repositories["nodes"]
                wait_window(args.idle_seconds, "idle")
                start_active = time.monotonic()
                admitted = 0
                while time.monotonic() - start_active < args.active_seconds:
                    if time.monotonic() >= start_active + admitted / args.arrivals_per_second:
                        pipeline.enqueue(
                            "tiktok_display",
                            "runtime-account",
                            "list",
                            {"endpoint_override": f"http://127.0.0.1:{ports['source']}"},
                            synthetic=True,
                            idempotency_key=f"runtime-{admitted}",
                        )
                        admitted += 1
                    sample("active")
                    time.sleep(0.25)
                drain_deadline = time.monotonic() + args.drain_seconds
                while time.monotonic() < drain_deadline:
                    counts = pipeline.catalog.one(
                        "SELECT count(*) FILTER(WHERE state='committed' "
                        "AND manifest->>'kind'='raw_capture') AS captures, "
                        "count(*) FILTER(WHERE state='committed' "
                        "AND supersedes IS NOT NULL) AS normalized, "
                        "COALESCE(sum(row_count) FILTER(WHERE state='committed' "
                        "AND supersedes IS NOT NULL),0)::bigint AS observations, "
                        "(SELECT count(*) FROM collection_tasks) AS total_tasks, "
                        "(SELECT count(*) FROM collection_tasks "
                        "WHERE state IN ('pending','leased')) AS unfinished_tasks, "
                        "(SELECT count(*) FROM collection_tasks "
                        "WHERE state='quarantined') AS quarantined_tasks "
                        "FROM datasets"
                    )
                    if (
                        counts["captures"] == counts["normalized"] == counts["total_tasks"]
                        and not counts["unfinished_tasks"]
                        and not counts["quarantined_tasks"]
                    ):
                        break
                    wait_window(0.5, "drain")
                report["workload"].update(
                    admitted_tasks=admitted, source_requests=request_count, **counts
                )
                report["complete"] = (
                    counts["captures"] == counts["normalized"] == counts["total_tasks"]
                    and counts["captures"] == request_count
                    and not counts["unfinished_tasks"]
                    and not counts["quarantined_tasks"]
                )
        except BaseException as error:
            report["error"] = {"type": type(error).__name__, "message": str(error)}
        finally:
            report["samples"] = samples
            report["phases"] = phase_statistics(samples)
            table = process_table()
            owned_children = {
                pid
                for process in processes.values()
                for pid in descendants(table, process.pid)
                if pid != process.pid
            }
            for pid in owned_children:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGTERM)
            for process in processes.values():
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
            for process in processes.values():
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            report["role_log_tails"] = {}
            workspace_errors = []
            for name, log in logs.items():
                log.seek(0)
                lines = log.read().splitlines()
                report["role_log_tails"][name] = lines[-12:]
                if name in {"dagster_daemon", "dagster_webserver"} and any(
                    "Error loading repository location" in line for line in lines
                ):
                    workspace_errors.append(name)
                log.close()
            if workspace_errors:
                report["complete"] = False
                report["workspace_errors"] = workspace_errors
            source.shutdown()
            source.server_close()
            thread.join(timeout=2)
            with psycopg.connect(args.database_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(dagster_database)
                    )
                )
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            report["cleanup_complete"] = True
            report["finished_at"] = datetime.now(UTC).isoformat()
            checkpoint()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--collector-binary", type=Path, required=True)
    parser.add_argument("--postgres-container", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmup-seconds", type=float, default=30)
    parser.add_argument("--idle-seconds", type=float, default=30)
    parser.add_argument("--active-seconds", type=float, default=60)
    parser.add_argument("--drain-seconds", type=float, default=30)
    parser.add_argument("--arrivals-per-second", type=float, default=0.5)
    parser.add_argument("--records-per-capture", type=int, default=20)
    parser.add_argument("--max-application-pss-mib", type=int, default=1400)
    args = parser.parse_args()
    if not all(
        0 < value <= 120
        for value in (
            args.warmup_seconds,
            args.idle_seconds,
            args.active_seconds,
            args.drain_seconds,
        )
    ):
        parser.error("Each window must be positive and at most 120 seconds")
    if not 0 < args.arrivals_per_second <= 1 or not 1 <= args.records_per_capture <= 100:
        parser.error("Use at most one capture per second and at most100 records per capture")
    if not 128 <= args.max_application_pss_mib <= 2048:
        parser.error("Application PSS bound must be128..2048MiB")
    result = measure(args)
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"samples", "role_log_tails"}
            },
            indent=2,
        )
    )
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
