"""Actual CLI crash recovery with native SQL/HTTP and explicit Docker/cosign fixtures."""

import json
import os
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from psycopg.types.json import Jsonb
from test_raw_capture import one_shot_pages, page
from test_release_control import release

from sm_trending_products.raw_capture import RawCaptureService
from sm_trending_products.release_control import release_manifest_bytes
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


def test_processing_cli_recovers_after_controller_kill_without_restarting_capture(
    pipeline, tmp_path
):
    active, candidate = release("a").manifest, release("b").manifest
    proof = {"method": "cosign_bundle", "bundle": {"accepted": True}}
    worker = WorkerControl(pipeline.catalog).register("old", synthetic=True)
    pipeline.catalog.execute(
        "INSERT INTO deployment_targets(target,active_manifest,active_proof,active_slot,"
        "active_worker_id,synthetic) VALUES('fixture',%s,%s,'blue','old',true)",
        (Jsonb(active.model_dump(mode="json")), Jsonb(proof)),
    )
    for name, contents in (
        ("database", pipeline.settings.database_url.get_secret_value()),
        ("runtime.env", "PRIVATE_VALUE=never-print-this\n"),
        ("compose.yaml", "services: {}\n"),
        ("manifest.json", release_manifest_bytes(candidate).decode()),
        ("bundle.json", '{"accepted":true}'),
    ):
        path = tmp_path / name
        path.write_text(contents)
        path.chmod(0o600)
    trace, state = tmp_path / "trace.jsonl", tmp_path / "docker-state.json"
    roles = (
        "worker",
        "capture-planner",
        "normalizer",
        "dagster-daemon",
        "dagster-webserver",
        "dashboard",
    )
    state.write_text(
        json.dumps(
            {
                "images": [active.image],
                "services": {
                    name: {
                        "image": active.image,
                        "running": True,
                        "worker": "old" if name == "worker" else None,
                    }
                    for name in roles
                },
            }
        )
    )
    docker = tmp_path / "docker"
    docker.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """
import hashlib,json,os,signal,socket,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path
args=sys.argv[1:]
assert args.pop(0)=='--host=unix:///var/run/docker.sock'
with Path(os.environ['PROCESSING_TRACE']).open('a') as stream: stream.write(json.dumps(args)+'\\n')
path=Path(os.environ['PROCESSING_STATE']);state=json.loads(path.read_text());services=state['services']
identifier=lambda name: hashlib.sha256(name.encode()).hexdigest()
if args[0]=='pull':
    state['images'].append(args[1]);path.write_text(json.dumps(state))
elif args[:2]==['image','inspect']:
    if args[-1] not in state['images']: raise SystemExit(1)
    print(json.dumps([args[-1]]))
elif args[0]=='compose' and 'ps' in args:
    if args[-1] in services: print(identifier(args[-1]))
elif args[0]=='inspect':
    service=next(name for name in services if identifier(name)==args[-1]);value=services[service]
    if 'Config.Hostname' in args[2]:
        hostname=socket.gethostname() if service=='capture-planner-green' else 'previous-planner'
        print(json.dumps(hostname));print(json.dumps(value.get('started_at','2020-01-01T00:00:00Z')))
    else:
        for key in ('image','running','worker'): print(json.dumps(value[key]))
elif args[0]=='compose' and 'up' in args:
    name=args[-1]
    variables={'capture-planner':'ACQUISITION_PLANNER_BLUE_IMAGE','capture-planner-green':'ACQUISITION_PLANNER_GREEN_IMAGE','normalizer':'NORMALIZER_IMAGE','dagster-daemon':'ANALYTICS_IMAGE','dagster-webserver':'ANALYTICS_IMAGE','dashboard':'DASHBOARD_IMAGE'}
    assert name in variables and '--no-deps' in args and '--no-build' in args
    services[name]={'image':os.environ[variables[name]],'running':True,'worker':None,'started_at':datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(state))
    marker=Path(os.environ['PROCESSING_CRASH_MARKER'])
    if name=='normalizer' and not marker.exists():
        marker.write_text('controller killed after durable desired pins and host action')
        os.kill(os.getppid(),signal.SIGKILL)
elif args[0]=='exec':
    assert args[2:]==['python','-m','sm_trending_products.capture_coordinator']
    raise SystemExit(subprocess.run([sys.executable,*args[3:]]).returncode)
elif args[0]=='stop':
    name=next(name for name in services if identifier(name)==args[-1])
    assert name in {'capture-planner','capture-planner-green'} and '--time=-1' in args
    services[name]['running']=False;path.write_text(json.dumps(state))
else: raise SystemExit(9)
"""
    )
    docker.chmod(0o700)
    cosign = tmp_path / "cosign"
    cosign.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """
import json,sys
from pathlib import Path
args=sys.argv[1:];assert args[:2]==['verify-blob','--offline']
value=json.loads(Path(args[args.index('--bundle')+1]).read_text())
raise SystemExit(0 if value.get('accepted') else 1)
"""
    )
    cosign.chmod(0o700)
    config = tmp_path / "controller.json"
    config.write_text(
        json.dumps(
            {
                "target": "fixture",
                "repository": "example/project",
                "image_repository": "ghcr.io/example/pipeline",
                "certificate_identity": "controlled-workflow",
                "compose_file": str(tmp_path / "compose.yaml"),
                "env_file": str(tmp_path / "runtime.env"),
                "database_url_file": str(tmp_path / "database"),
                "state_directory": str(tmp_path / "private-state"),
                "spool_dir": str(tmp_path),
                "docker": str(docker),
                "cosign": str(cosign),
            }
        )
    )
    config.chmod(0o600)
    environment = {
        **os.environ,
        "PROCESSING_STATE": str(state),
        "PROCESSING_TRACE": str(trace),
        "PROCESSING_CRASH_MARKER": str(tmp_path / "crashed"),
        "S3_BUCKET": pipeline.history.bucket,
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
    }
    command = [
        sys.executable,
        "-m",
        "sm_trending_products.release_cli",
        "refresh-processing",
        "--config",
        str(config),
        "--controlled-test",
    ]
    entered, finish = threading.Event(), threading.Event()
    acquisition = RawCaptureService(
        pipeline.settings, pipeline.catalog, pipeline.history, pipeline.collector, "old"
    )
    acquisition.worker_generation = worker["generation"]
    with one_shot_pages([page("protected")], entered=entered, release=finish) as (
        endpoint,
        requests,
    ):
        pipeline.enqueue(
            "tiktok_display",
            "fixture-account",
            "list",
            {"endpoint_override": endpoint},
            synthetic=True,
        )
        with ThreadPoolExecutor(1) as pool:
            capture = pool.submit(acquisition.capture_batch, 1, True)
            try:
                assert entered.wait(10), capture.result(timeout=1)
                crashed = subprocess.run(
                    command
                    + [
                        "--manifest",
                        str(tmp_path / "manifest.json"),
                        "--bundle",
                        str(tmp_path / "bundle.json"),
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
                assert crashed.returncode == -signal.SIGKILL, crashed.stdout + crashed.stderr
            finally:
                finish.set()
            result = capture.result(timeout=15)
            assert result["processed"] == 1 and not result["errors"]
        assert len(requests) == 1
    # The source is now offline. No request/manifest downloads are needed to resume.
    (tmp_path / "manifest.json").unlink()
    (tmp_path / "bundle.json").unlink()
    resumed = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=20)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert json.loads(resumed.stdout)["status"] == "completed"
    assert "never-print-this" not in resumed.stdout + resumed.stderr
    assert acquisition.publish_pending(1, True)["processed"] == 1
    operations = [json.loads(line) for line in trace.read_text().splitlines()]
    starts = [args[-1] for args in operations if args[0] == "compose" and "up" in args]
    assert (
        starts.count("normalizer") == 1 and "worker" not in starts and "worker-green" not in starts
    )
    assert len([args for args in operations if args[0] == "pull"]) == 1
    capture_state = json.loads(state.read_text())["services"]["worker"]
    assert capture_state == {"image": active.image, "running": True, "worker": "old"}
    assert WorkerControl(pipeline.catalog).status("old")["generation"] == worker["generation"]
    assert (
        "NORMALIZER_IMAGE=" + candidate.image
        in (tmp_path / "private-state/processing.env").read_text()
    )
