"""Opt-in two-image Compose releases against one-shot HTTP, PostgreSQL and S3.

Set CONTINUITY_DOCKER_MANIFEST to a fixture-image manifest built by
scripts/build-continuity-fixtures.py. No production credentials or host are used.
"""

from __future__ import annotations

import inspect
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from sm_trending_products.capture_contracts import CapturePolicy
from sm_trending_products.catalog import Catalog
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.release_control import (
    DeploymentController,
    ReleaseManifest,
    VerifiedRelease,
)
from sm_trending_products.release_evidence import CatalogCaptureVerifier
from sm_trending_products.release_host import CatalogWorkerGate, ComposeReleaseHost
from sm_trending_products.resource_cpu import CpuModeController
from sm_trending_products.settings import Settings

pytestmark = pytest.mark.integration


def container_resource_probe():
    root = Path("/sys/fs/cgroup")
    body = {
        "sample_unix_seconds": time.time(),
        "cgroup_v2": (root / "cgroup.controllers").is_file(),
        "gaps": [],
    }

    def read(name):
        try:
            return (root / name).read_text().strip()
        except OSError as error:
            body["gaps"].append(name + ":" + type(error).__name__)
            return None

    cpu = read("cpu.stat")
    body["cpu_usage_usec"] = (
        dict(line.split() for line in cpu.splitlines()).get("usage_usec") if cpu else None
    )
    if body["cpu_usage_usec"] is not None:
        body["cpu_usage_usec"] = int(body["cpu_usage_usec"])
    for name, key in [
        ("memory.current", "memory_current_bytes"),
        ("memory.peak", "memory_peak_bytes"),
        ("memory.max", "memory_max_bytes"),
    ]:
        value = read(name)
        body[key] = int(value) if value and value.isdigit() else value
    body["cpu_max"] = read("cpu.max")
    spool = Path("/var/lib/sm-trends/spool")
    body["spool"] = {
        "scope": "shared_capture_spool_volume",
        "file_count": None,
        "file_bytes": None,
        "truncated": False,
        "entry_limit": 1000,
    }
    try:
        usage = os.statvfs(spool)
        body["spool"]["filesystem_available_bytes"] = usage.f_bavail * usage.f_frsize
        body["spool"]["filesystem_total_bytes"] = usage.f_blocks * usage.f_frsize
        count, size, visited = 0, 0, 0
        pending = [str(spool)]
        while pending:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    visited += 1
                    if visited > 1000:
                        body["spool"]["truncated"] = True
                        pending.clear()
                        break
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        count += 1
                        size += entry.stat(follow_symlinks=False).st_size
        body["spool"].update(file_count=count, file_bytes=size)
    except OSError as error:
        body["gaps"].append("spool:" + type(error).__name__)
    print(json.dumps(body))


RESOURCE_PROBE = (
    "import json,os,time\nfrom pathlib import Path\n"
    + inspect.getsource(container_resource_probe)
    + "\ncontainer_resource_probe()"
)


def docker(*arguments, timeout=60, environment=None):
    selected = dict(os.environ)
    for key in (
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
    ):
        selected.pop(key, None)
    selected.update(environment or {})
    result = subprocess.run(
        ["docker", "--host=unix:///var/run/docker.sock", *arguments],
        env=selected,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, f"Fixture Docker command failed: {arguments[:2]}"
    return result.stdout


def eventually(check, *, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("Controlled Docker campaign did not reach its bounded checkpoint")


class OneShotSource:
    def __init__(self):
        self.requests = {}
        self.entered, self.release = threading.Event(), threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                key = payload["fixture_page"]
                owner.requests[key] = owner.requests.get(key, 0) + 1
                if owner.requests[key] != 1:
                    self.send_response(410)
                    self.end_headers()
                    return
                if key == "held":
                    owner.entered.set()
                    if not owner.release.wait(40):
                        self.send_response(504)
                        self.end_headers()
                        return
                body = json.dumps(
                    {
                        "data": {"videos": [{"id": key, "view_count": 10}], "has_more": False},
                        "error": {"code": "ok"},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def endpoint(self):
        return f"http://host.docker.internal:{self.server.server_port}"

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class Campaign:
    def __init__(self, directory, manifest, *, cpu_roles=False):
        self.directory, self.manifest = directory, manifest
        self.project = "continuity-" + uuid4().hex[:12]
        self.source = OneShotSource()
        self.pipeline = None
        self.images = manifest["images"]
        self.resource_samples = []
        self.blue = self.release("blue")
        self.green = self.release("green")
        self.host = ComposeReleaseHost(
            project=self.project,
            compose_file=directory / "compose.json",
            env_file=directory / "fixture.env",
            lock_directory=directory / "locks",
            slot_images=self.images,
            slot_workers={"blue": "fixture-blue", "green": "fixture-green-pending"},
            docker=shutil.which("docker") or "/usr/bin/docker",
        )
        self.host.env_file.write_text("")
        relay = directory / "fixture-relay.py"
        relay.write_bytes((Path(__file__).parent / "fixtures/continuity_relay.py").read_bytes())
        relay.chmod(0o644)  # The image's UID 10001 must read this fixture bind mount.
        environment = {
            "DATABASE_URL": "postgresql://fixture:fixture-only@postgres:5432/fixture",
            "S3_BUCKET": self.project,
            "S3_ENDPOINT_URL": "http://objectstore:5000",
            "AWS_ACCESS_KEY_ID": "fixture-access",
            "AWS_SECRET_ACCESS_KEY": "fixture-secret",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "TIKTOK_DISPLAY_ACCESS_TOKEN": "fixture-token",
            "FIXTURE_SOURCE_URL": self.source.endpoint(),
            "ACQUISITION_ENABLED": "true",
            "PIPELINE_NAMESPACE": self.project,
            "POLARS_MAX_THREADS": "2",
            "NO_PROXY": "localhost,127.0.0.1,postgres,objectstore,host.docker.internal",
            "no_proxy": "localhost,127.0.0.1,postgres,objectstore,host.docker.internal",
        }
        services = {
            "postgres": {
                "image": "postgres:17.11-bookworm",
                "environment": {
                    "POSTGRES_USER": "fixture",
                    "POSTGRES_PASSWORD": "fixture-only",
                    "POSTGRES_DB": "fixture",
                },
                "ports": ["127.0.0.1::5432"],
            },
            "objectstore": {
                "image": "motoserver/moto:5.2.4",
                "ports": ["127.0.0.1::5000"],
                "environment": {"MOTO_HOST": "0.0.0.0", "MOTO_PORT": "5000"},
            },
        }
        for slot, service in (("blue", "worker"), ("green", "worker-green")):
            services[service] = {
                "image": "${CAPTURE_" + slot.upper() + "_IMAGE}",
                "command": ["python", "/app/fixture-relay.py"],
                "environment": {
                    **environment,
                    "WORKER_ID": "${CAPTURE_" + slot.upper() + "_WORKER_ID}",
                },
                "labels": {"io.sm-trends.worker-id": "${CAPTURE_" + slot.upper() + "_WORKER_ID}"},
                "volumes": [
                    "spool:/var/lib/sm-trends/spool",
                    "cache:/var/lib/sm-trends/cache",
                    str(relay) + ":/app/fixture-relay.py:ro",
                ],
                "extra_hosts": ["host.docker.internal:host-gateway"],
                "read_only": True,
                "tmpfs": ["/tmp:rw,noexec,nosuid,size=128m,uid=10001,gid=10001"],
                "init": True,
                "restart": "unless-stopped",
                "stop_grace_period": "2m",
                "mem_limit": "512m",
                "cpus": 0.5 if cpu_roles else 1,
            }
        if cpu_roles:
            services["postgres"].update(cpus=0.25, mem_limit="512m")
            services["objectstore"].update(cpus=0.25, mem_limit="256m")
            for service, variable, default in (
                ("dagster-daemon", "DAGSTER_DAEMON_CPUS", "0.5"),
                ("dagster-webserver", "DAGSTER_WEBSERVER_CPUS", "0.25"),
                ("dashboard", "DASHBOARD_CPUS", "0.25"),
            ):
                services[service] = {
                    # The existing CI registry image has a small Alpine runtime.
                    # Avoid three dependency-heavy app copies on VFS hosts.
                    "image": "registry:2.8.3",
                    # Real containers qualify live quota changes and process
                    # continuity; this fixture does not measure Dagster throughput.
                    "command": ["/bin/sleep", "86400"],
                    "cpus": "${" + variable + ":-" + default + "}",
                    "mem_limit": "256m",
                    "init": True,
                }
        self.host.compose_file.write_text(
            json.dumps({"services": services, "volumes": {"spool": {}, "cache": {}}})
        )

    def release(self, slot):
        return VerifiedRelease.for_controlled_test(
            ReleaseManifest(
                image=self.images[slot],
                revision=self.manifest["revision"],
                schema_min=28,
                schema_max=40,
                capture_protocol=1,
            )
        )

    def compose(self, *arguments):
        return self.host._command([*self.host._compose(), *arguments])

    def port(self, service, port):
        container = self.compose("ps", "--all", "--quiet", service).strip()
        body = json.loads(docker("inspect", container))[0]
        return body["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"]

    def start(self):
        self.compose("up", "--detach", "--no-build", "postgres", "objectstore")
        database = (
            f"postgresql://fixture:fixture-only@127.0.0.1:{self.port('postgres', 5432)}/fixture"
        )
        endpoint = f"http://127.0.0.1:{self.port('objectstore', 5000)}"
        import urllib.request

        import psycopg

        def ready():
            try:
                with psycopg.connect(database, connect_timeout=1) as connection:
                    connection.execute("SELECT 1")
                urllib.request.urlopen(endpoint, timeout=1).close()
                return True
            except (psycopg.Error, OSError):
                return False

        eventually(ready)
        self.pipeline = Pipeline(
            Settings(
                database_url=database,
                s3_bucket=self.project,
                s3_endpoint_url=endpoint,
                cache_dir=self.directory / "cache",
                spool_dir=self.directory / "spool",
                collector_binary=self.manifest["collector_binary"],
                pipeline_namespace=self.project,
            )
        )
        self.pipeline.initialize(create_bucket=True)
        self.pipeline.catalog.configure_budget("tiktok_display", "fixture-account", 1000, 1000)
        self.pipeline.capture_contracts.register(
            CapturePolicy(
                source="tiktok_display",
                account_id="fixture-account",
                operation="list",
                version="docker-fixture-1",
                cadence_seconds=1,
                lateness_seconds=120,
                recoverability="ephemeral",
            ),
            synthetic=True,
        )
        self.host.prepare(self.project, self.blue.manifest, "blue")
        self.host.start(self.project, self.blue.manifest, "blue", "fixture-blue")
        self.worker("fixture-blue", "running")
        self.sample_resources("predecessor_started")

    def worker(self, worker_id, state=None, *, after_generation=0):
        return eventually(
            lambda: (
                row
                if (row := self.pipeline.worker_control.status(worker_id))
                and row["generation"] > after_generation
                and (state is None or row["state"] == state)
                else None
            )
        )

    def enqueue(self, key):
        return self.pipeline.enqueue(
            "tiktok_display",
            "fixture-account",
            "list",
            {"endpoint_override": "http://127.0.0.1:19443", "fixture_page": key},
            synthetic=True,
            idempotency_key=key,
        )

    def done(self, task_id):
        try:
            return eventually(
                lambda: self.pipeline.catalog.one(
                    "SELECT * FROM collection_tasks WHERE task_id=%s AND state='done'", (task_id,)
                )
            )
        except AssertionError:
            state = self.pipeline.catalog.one(
                "SELECT state,last_error,attempts,lease_owner,due_at "
                "FROM collection_tasks WHERE task_id=%s",
                (task_id,),
            )
            raise AssertionError(
                json.dumps({"task": state, "requests": self.source.requests}, default=str)
            ) from None

    def controller(self):
        return DeploymentController(
            self.pipeline.catalog,
            self.host,
            CatalogWorkerGate(self.pipeline.worker_control),
            CatalogCaptureVerifier(self.pipeline.catalog),
            allow_synthetic=True,
        )

    def plan(self):
        version = self.pipeline.catalog.one(
            "SELECT max(version) AS version FROM schema_migrations"
        )["version"]
        planned = self.controller().plan(
            self.green,
            target=self.project,
            current=self.blue,
            current_slot="blue",
            current_worker_id="fixture-blue",
            schema_version=version,
            synthetic=True,
        )
        self.host = replace(
            self.host,
            slot_workers={"blue": "fixture-blue", "green": planned["candidate_worker_id"]},
        )
        return planned

    def sample_resources(self, phase):
        """Bounded read-only snapshots of owned capture containers, never host RSS."""
        for slot in ("blue", "green"):
            sample = {"phase": phase, "slot": slot, "observed_unix_seconds": time.time()}
            container = self.host._container(slot)
            if not container:
                self.resource_samples.append({**sample, "status": "absent"})
                continue
            identity = json.loads(docker("inspect", container, timeout=5))[0]
            assert identity["Config"]["Image"] == self.images[slot]
            assert (
                identity["Config"]["Labels"]["io.sm-trends.worker-id"]
                == self.host.slot_workers[slot]
            )
            sample.update(
                container_id=container,
                container_started_at=identity["State"]["StartedAt"],
                configured_limits={
                    "nano_cpus": identity["HostConfig"]["NanoCpus"],
                    "memory_bytes": identity["HostConfig"]["Memory"],
                },
            )
            if not identity["State"]["Running"]:
                self.resource_samples.append({**sample, "status": "stopped"})
                continue
            try:
                measurement = json.loads(
                    docker("exec", container, "python", "-c", RESOURCE_PROBE, timeout=5)
                )
                sample.update(status="sampled", measurement=measurement)
                if (
                    not measurement["cgroup_v2"]
                    or measurement["cpu_usage_usec"] is None
                    or measurement["memory_current_bytes"] is None
                ):
                    stats = json.loads(
                        docker(
                            "stats", "--no-stream", "--format", "{{json .}}", container, timeout=5
                        )
                    )
                    sample["docker_stats_snapshot"] = {
                        key: stats.get(key)
                        for key in ("CPUPerc", "MemUsage", "MemPerc", "BlockIO", "PIDs")
                    }
                    measurement["gaps"].append(
                        "cgroup_v2_counters_unavailable_docker_stats_snapshot_only"
                    )
            except (AssertionError, subprocess.SubprocessError, json.JSONDecodeError) as error:
                sample.update(status="unavailable", measurement_gap=type(error).__name__)
            self.resource_samples.append(sample)

    def record(self, scenario, extra=None):
        self.sample_resources("scenario_completed")
        obligations = self.pipeline.catalog.one(
            "SELECT count(*) AS total,count(*) FILTER(WHERE durable_at IS NOT NULL) AS durable,"
            "count(*) FILTER(WHERE captured_at>deadline_at OR "
            "(durable_at IS NULL AND deadline_at<clock_timestamp())) AS missed "
            "FROM capture_obligations"
        )
        assert obligations["total"] == obligations["durable"] and obligations["missed"] == 0
        assert all(count == 1 for count in self.source.requests.values())
        assert self.pipeline.verify()["ok"]
        output = os.environ.get("CONTINUITY_DOCKER_EVIDENCE_DIR")
        if output:
            destination = Path(output)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / f"{scenario}.json").write_text(
                json.dumps(
                    {
                        "scenario": scenario,
                        "runtime_revision": self.manifest["revision"],
                        "images": self.images,
                        "fixture_images": True,
                        "production_acceptance": False,
                        "requests_per_one_shot_page": self.source.requests,
                        "obligations": obligations,
                        "resource_samples": self.resource_samples,
                        "resource_semantics": {
                            "cpu": "Cumulative cgroup usage_usec for the sampled runtime epoch; "
                            "not percent or a universal peak",
                            "memory": "cgroup current and lifetime peak at the sample; "
                            "includes the bounded probe process",
                            "disk": "Shared capture spool snapshot, entry cap and filesystem "
                            "available bytes; not a lifetime peak or per-worker attribution",
                            "epochs": "Compare counters only for the same container_id "
                            "and container_started_at",
                            "missing": "Unavailable counters remain null or explicit gaps; "
                            "absent/stopped containers have no fabricated measurements",
                        },
                        **(extra or {}),
                    },
                    indent=2,
                )
                + "\n"
            )

    def close(self):
        self.source.close()
        try:
            self.compose("stop", "--timeout", "-1", "worker", "worker-green")
        finally:
            if self.pipeline:
                self.pipeline.close()
            self.compose("down", "--volumes", "--remove-orphans", "--timeout", "5")


@pytest.fixture
def campaign(tmp_path, monkeypatch, request):
    path = os.environ.get("CONTINUITY_DOCKER_MANIFEST")
    if not path:
        pytest.skip("Set CONTINUITY_DOCKER_MANIFEST for the isolated two-image Docker campaign")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fixture-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    value = Campaign(
        tmp_path, json.loads(Path(path).read_text()), cpu_roles=getattr(request, "param", False)
    )
    try:
        value.start()
        yield value
    finally:
        value.close()


def crash_controller_after_start(configuration):
    """Actual process death after real Compose start, before SQL phase recording."""

    class InterruptedHost(ComposeReleaseHost):
        def start(self, *arguments):
            super().start(*arguments)
            os.kill(os.getpid(), signal.SIGKILL)

    body = json.loads(Path(configuration).read_text())
    pipeline = Pipeline(Settings.model_validate(body["settings"]))
    host = InterruptedHost(
        **{
            **body["host"],
            **{
                key: Path(body["host"][key])
                for key in ("compose_file", "env_file", "lock_directory")
            },
        }
    )
    DeploymentController(
        pipeline.catalog,
        host,
        CatalogWorkerGate(pipeline.worker_control),
        CatalogCaptureVerifier(pipeline.catalog),
        allow_synthetic=True,
    ).apply(body["release_id"])
    raise AssertionError("Crash boundary was not reached")


def test_two_images_preserve_one_shot_inflight_capture_through_controller_crash_and_handover(
    campaign,
):
    held = campaign.enqueue("held")
    assert campaign.source.entered.wait(15)
    planned = campaign.plan()
    configuration = campaign.directory / "controller-fixture.json"
    settings = campaign.pipeline.settings.model_dump(mode="json")
    settings["database_url"] = campaign.pipeline.settings.database_url.get_secret_value()
    configuration.write_text(
        json.dumps(
            {
                "settings": settings,
                "release_id": planned["release_id"],
                "host": {
                    key: str(value) if isinstance(value, Path) else value
                    for key, value in vars(campaign.host).items()
                },
            }
        )
    )
    configuration.chmod(0o600)
    crashed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys;from test_docker_release_campaign import crash_controller_after_start;"
            "crash_controller_after_start(sys.argv[1])",
            str(configuration),
        ],
        capture_output=True,
        timeout=30,
    )
    assert crashed.returncode == -signal.SIGKILL
    assert campaign.controller().status(planned["release_id"])["phase"] == "prepared"
    candidate = campaign.worker(planned["candidate_worker_id"], "running")
    result = campaign.controller().apply(planned["release_id"])
    assert result["reason"] == "capture_proof_missing"
    assert campaign.host.healthy(campaign.project, campaign.blue.manifest, "blue", "fixture-blue")
    campaign.sample_resources("replacement_started_without_capture_proof")
    qualification = campaign.enqueue("replacement-proof")
    campaign.done(qualification)
    result = campaign.controller().apply(planned["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "predecessor_busy"
    assert campaign.pipeline.worker_control.status("fixture-blue")["state"] == "draining"
    assert campaign.host.healthy(campaign.project, campaign.blue.manifest, "blue", "fixture-blue")
    campaign.sample_resources("predecessor_draining_with_capture_inflight")
    campaign.source.release.set()
    campaign.done(held)
    campaign.worker("fixture-blue", "drained")
    result = campaign.controller().apply(planned["release_id"])
    assert result["status"] == "completed" and result["phase"] == "retired"
    assert result["evidence"]["worker_generation"] == candidate["generation"]
    assert not campaign.host.healthy(
        campaign.project, campaign.blue.manifest, "blue", "fixture-blue"
    )
    campaign.done(campaign.enqueue("after-handover"))
    campaign.record("controller-crash-inflight-handover", {"final_phase": result["phase"]})


def test_unqualified_actual_candidate_rolls_back_without_stopping_the_predecessor(campaign):
    planned = campaign.plan()
    result = campaign.controller().apply(planned["release_id"])
    candidate = campaign.worker(planned["candidate_worker_id"], "running")
    result = campaign.controller().apply(planned["release_id"])
    assert result["reason"] == "capture_proof_missing"
    assert campaign.pipeline.worker_control.status("fixture-blue")["state"] == "running"
    # Route a real accepted operation to the candidate, then let the controller
    # restore predecessor admission while that unqualified request is in flight.
    control = campaign.pipeline.worker_control
    old = control.status("fixture-blue")
    control.request_drain("fixture-blue", old["generation"])
    campaign.worker("fixture-blue", "drained")
    held = campaign.enqueue("held")
    assert campaign.source.entered.wait(15)
    result = campaign.controller().apply(planned["release_id"])
    assert result["reason"] == "capture_proof_missing"
    assert control.status("fixture-blue")["state"] == "running"
    result = campaign.controller().rollback(planned["release_id"])
    assert result["status"] == "blocked" and result["reason"] == "candidate_busy"
    assert campaign.host.healthy(
        campaign.project, campaign.green.manifest, "green", planned["candidate_worker_id"]
    )
    campaign.done(campaign.enqueue("during-candidate-drain"))
    campaign.sample_resources("candidate_draining_with_capture_inflight")
    campaign.source.release.set()
    campaign.done(held)
    campaign.worker(planned["candidate_worker_id"], "drained")
    result = eventually(
        lambda: (
            row
            if (row := campaign.controller().rollback(planned["release_id"]))["status"] == "aborted"
            else None
        )
    )
    assert campaign.host.healthy(campaign.project, campaign.blue.manifest, "blue", "fixture-blue")
    assert not campaign.host.healthy(
        campaign.project, campaign.green.manifest, "green", planned["candidate_worker_id"]
    )
    campaign.done(campaign.enqueue("after-unqualified-rollback"))
    campaign.record(
        "blocked-readiness-rollback",
        {"candidate_generation": candidate["generation"], "final_status": result["status"]},
    )


def test_compose_restart_retains_pause_and_fences_previous_generation(campaign):
    control = campaign.pipeline.worker_control
    old = control.status("fixture-blue")
    control.request_drain("fixture-blue", old["generation"])
    campaign.worker("fixture-blue", "drained")
    campaign.compose("restart", "--timeout", "-1", "worker")
    current = campaign.worker("fixture-blue", "drained", after_generation=old["generation"])
    campaign.sample_resources("paused_after_restart")
    task = campaign.enqueue("after-paused-restart")
    time.sleep(0.2)
    assert campaign.source.requests == {}
    with pytest.raises(ValueError, match="generation"):
        control.request_drain("fixture-blue", old["generation"])
    with pytest.raises(ValueError, match="generation"):
        control.resume("fixture-blue", old["generation"])
    control.resume("fixture-blue", current["generation"])
    campaign.done(task)
    campaign.record(
        "compose-restart-generation-fence",
        {"old_generation": old["generation"], "new_generation": current["generation"]},
    )


def cpu_runtime_snapshot(campaign):
    """Read only process identities and resource settings of this owned project."""
    containers = campaign.compose("ps", "--quiet", "--all").split()
    snapshot = {}
    for row in json.loads(docker("inspect", *containers)):
        service = row["Config"]["Labels"]["com.docker.compose.service"]
        snapshot[service] = {
            "container_id": row["Id"],
            "pid": row["State"]["Pid"],
            "started_at": row["State"]["StartedAt"],
            "restart_count": row["RestartCount"],
            "running": row["State"]["Running"],
            "resources": row["HostConfig"],
        }
    return snapshot


def prepare_cpu_mode(campaign):
    campaign.compose(
        "up", "--detach", "--no-build", "dagster-daemon", "dagster-webserver", "dashboard"
    )
    with campaign.pipeline.catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO deployment_targets(target,active_manifest,active_proof,active_slot,"
            "active_worker_id,synthetic) VALUES(%s,%s,%s,'blue','fixture-blue',true)",
            (
                campaign.project,
                Jsonb(campaign.blue.manifest.model_dump(mode="json")),
                Jsonb(campaign.blue.proof),
            ),
        )
    return CpuModeController(campaign.host, campaign.pipeline.catalog)


def assert_cpu_mode_continuity(before, after, analytical_quotas):
    assert set(before) == set(after)
    for service, initial in before.items():
        expected = {**initial, "resources": dict(initial["resources"])}
        if service in analytical_quotas:
            expected["resources"]["NanoCpus"] = analytical_quotas[service]
        assert after[service] == expected


@pytest.mark.parametrize("campaign", [True], indirect=True)
def test_live_cpu_modes_preserve_inflight_capture_and_processing_pins(campaign):
    cpu = prepare_cpu_mode(campaign)
    before = cpu_runtime_snapshot(campaign)
    worker = campaign.worker("fixture-blue", "running")
    standard = {
        "dagster-daemon": 500_000_000,
        "dagster-webserver": 250_000_000,
        "dashboard": 250_000_000,
    }
    economy = {key: value // 2 for key, value in standard.items()}
    preview = cpu.switch("economy", dry_run=True)
    assert preview["status"] == "preview"
    assert cpu_runtime_snapshot(campaign) == before
    assert not (campaign.host.lock_directory / "cpu-mode.json").exists()

    held = campaign.enqueue("held")
    assert campaign.source.entered.wait(15)
    assert cpu.switch("economy")["mode"] == "economy"
    assert_cpu_mode_continuity(before, cpu_runtime_snapshot(campaign), economy)
    assert not campaign.source.release.is_set()
    assert cpu.switch("standard")["mode"] == "standard"
    assert_cpu_mode_continuity(before, cpu_runtime_snapshot(campaign), standard)
    assert not campaign.source.release.is_set()
    assert campaign.worker("fixture-blue", "running")["generation"] == worker["generation"]

    campaign.source.release.set()
    campaign.done(held)
    campaign.done(campaign.enqueue("after-live-cpu-switch"))
    assert campaign.source.requests == {"held": 1, "after-live-cpu-switch": 1}

    # A later deliberate replayable-role recreation must inherit the committed
    # economy pins. It is separate from, and never performed by, the hot switch.
    assert cpu.switch("economy")["mode"] == "economy"
    dashboard = cpu_runtime_snapshot(campaign)["dashboard"]["container_id"]
    campaign.compose("up", "--detach", "--no-deps", "--no-build", "--force-recreate", "dashboard")
    recreated = cpu_runtime_snapshot(campaign)
    assert recreated["dashboard"]["container_id"] != dashboard
    assert recreated["dashboard"]["resources"]["NanoCpus"] == economy["dashboard"]
    assert recreated["worker"] == before["worker"]
    assert recreated["postgres"] == before["postgres"]
    assert CpuModeController(campaign.host, campaign.pipeline.catalog).status()["mode"] == "economy"
    assert cpu.switch("standard")["mode"] == "standard"
    campaign.done(campaign.enqueue("after-persisted-cpu-recreation"))
    campaign.record(
        "hot-cpu-inflight-capture-and-persistence",
        {
            "capture_container_unchanged": True,
            "capture_generation": worker["generation"],
            "standard_nanocpus": standard,
            "economy_nanocpus": economy,
            "analytical_workload": "idle Alpine containers; quota/continuity qualification only",
        },
    )


def crash_cpu_controller_after_update(configuration):
    """Kill the real host process after Docker applies its first live quota."""

    class InterruptedHost(ComposeReleaseHost):
        def _command(self, arguments, **kwargs):
            result = super()._command(arguments, **kwargs)
            if arguments[0] == "update":
                os.kill(os.getpid(), signal.SIGKILL)
            return result

    body = json.loads(Path(configuration).read_text())
    host = InterruptedHost(
        **{
            **body["host"],
            **{
                key: Path(body["host"][key])
                for key in ("compose_file", "env_file", "lock_directory")
            },
        }
    )
    catalog = Catalog(body["database_url"])
    try:
        CpuModeController(host, catalog).switch("economy")
    finally:
        catalog.close()
    raise AssertionError("CPU crash boundary was not reached")


@pytest.mark.parametrize("campaign", [True], indirect=True)
def test_live_cpu_partial_update_recovers_after_host_process_death(campaign):
    cpu = prepare_cpu_mode(campaign)
    before = cpu_runtime_snapshot(campaign)
    worker = campaign.worker("fixture-blue", "running")
    configuration = campaign.directory / "cpu-crash-fixture.json"
    configuration.write_text(
        json.dumps(
            {
                "database_url": campaign.pipeline.settings.database_url.get_secret_value(),
                "host": {
                    key: str(value) if isinstance(value, Path) else value
                    for key, value in vars(campaign.host).items()
                },
            }
        )
    )
    configuration.chmod(0o600)
    held = campaign.enqueue("held")
    assert campaign.source.entered.wait(15)
    try:
        crashed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys;from test_docker_release_campaign import "
                "crash_cpu_controller_after_update;"
                "crash_cpu_controller_after_update(sys.argv[1])",
                str(configuration),
            ],
            capture_output=True,
            timeout=30,
        )
        assert crashed.returncode == -signal.SIGKILL
        assert cpu.status()["status"] == "recovery_required"
        partial = cpu_runtime_snapshot(campaign)
        changed = {
            service: row["resources"]["NanoCpus"]
            for service, row in partial.items()
            if row["resources"]["NanoCpus"] != before[service]["resources"]["NanoCpus"]
        }
        assert len(changed) == 1
        assert set(changed) <= {"dagster-daemon", "dagster-webserver", "dashboard"}
        assert_cpu_mode_continuity(before, partial, changed)
        recovered = CpuModeController(campaign.host, campaign.pipeline.catalog).reconcile()
        assert recovered["recovered"] is True and recovered["mode"] == "standard"
        assert cpu_runtime_snapshot(campaign) == before
        assert campaign.worker("fixture-blue", "running")["generation"] == worker["generation"]
    finally:
        cpu.reconcile()
        campaign.source.release.set()
    campaign.done(held)
    campaign.done(campaign.enqueue("after-cpu-controller-crash"))
    campaign.record(
        "hot-cpu-controller-crash-recovery",
        {
            "capture_container_unchanged": True,
            "capture_generation": worker["generation"],
            "partial_analytical_updates": len(changed),
            "recovered_mode": recovered["mode"],
        },
    )
