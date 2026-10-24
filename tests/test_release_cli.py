"""Run the actual host CLI against HTTP assets, native PostgreSQL and command fixtures.

The verifier executable is an explicit invocation fixture, not Sigstore evidence.
Actual container and cryptographic qualification are separate campaigns.
"""

import functools
import hashlib
import json
import os
import subprocess
import sys
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import pytest
from test_release_control import release

from sm_trending_products.release_control import (
    DeploymentRequest,
    deployment_request_bytes,
    release_manifest_bytes,
)
from sm_trending_products.worker_control import WorkerControl

pytestmark = pytest.mark.integration


def test_host_cli_bootstraps_pulls_and_resumes_without_network_or_stopping_old(pipeline, tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    for character in ("a", "b"):
        path = root / "example/project/releases/download" / f"candidate-{character * 40}"
        path.mkdir(parents=True)
        (path / "manifest.json").write_bytes(release_manifest_bytes(release(character).manifest))
        (path / "bundle.json").write_text('{"controlled_valid":true}')
    request = DeploymentRequest(
        target="fixture",
        sequence=2,
        revision="b" * 40,
        manifest_sha256=hashlib.sha256(release_manifest_bytes(release("b").manifest)).hexdigest(),
    )
    request_path = root / "example/project/releases/download/pilot-deploy-request"
    request_path.mkdir(parents=True)
    (request_path / "request.json").write_bytes(deployment_request_bytes(request))
    (request_path / "request-bundle.json").write_text('{"controlled_valid":true}')
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(SimpleHTTPRequestHandler, directory=str(root))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    database = tmp_path / "database-url"
    database.write_text(pipeline.settings.database_url.get_secret_value())
    database.chmod(0o600)
    env_file = tmp_path / "runtime.env"
    env_file.write_text("PRIVATE_FIXTURE_VALUE=never-print-this\n")
    env_file.chmod(0o600)
    compose = tmp_path / "compose.yaml"
    compose.write_text("services: {}\n")
    docker = tmp_path / "docker"
    docker_state = tmp_path / "docker-state.json"
    trace = tmp_path / "commands.jsonl"
    docker_state.write_text(
        json.dumps(
            {"worker": {"image": release("a").manifest.image, "running": True, "worker": "old"}}
        )
    )
    docker.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """
import json,os,sys
from pathlib import Path
from sm_trending_products.catalog import Catalog
from sm_trending_products.worker_control import WorkerControl
args=sys.argv[1:]
assert args.pop(0)=='--host=unix:///var/run/docker.sock'
with Path(os.environ['CLI_TRACE']).open('a') as handle: handle.write(json.dumps(args)+'\\n')
state=Path(os.environ['CLI_STATE'])
slots=json.loads(state.read_text())
if args[0]=='pull': pass
elif args[:2]==['image','inspect']: print(json.dumps([args[-1]]))
elif args[0]=='compose' and 'ps' in args:
    if args[-1] in slots: print(('a' if args[-1]=='worker' else 'b')*64)
elif args[0]=='inspect':
    value=slots['worker' if args[-1].startswith('a') else 'worker-green']
    print(json.dumps(value['image']));print(json.dumps(value['running']));print(json.dumps(value['worker']))
elif args[0]=='compose' and 'up' in args:
    service=args[-1];slot='BLUE' if service=='worker' else 'GREEN'
    worker=os.environ['CAPTURE_'+slot+'_WORKER_ID']
    slots[service]={'image':os.environ['CAPTURE_'+slot+'_IMAGE'],'worker':worker,'running':True}
    state.write_text(json.dumps(slots))
    catalog=Catalog(Path(os.environ['CLI_DATABASE_FILE']).read_text())
    try: WorkerControl(catalog).register(worker,synthetic=True)
    finally: catalog.close()
elif args[0]=='stop':
    assert '--time=-1' in args
    service='worker' if args[-1].startswith('a') else 'worker-green'
    slots[service]['running']=False;state.write_text(json.dumps(slots))
    catalog=Catalog(Path(os.environ['CLI_DATABASE_FILE']).read_text())
    try:
        control=WorkerControl(catalog);worker=slots[service]['worker']
        control.finish(worker,control.status(worker)['generation'])
    finally: catalog.close()
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
args=sys.argv[1:]
assert args[:2]==['verify-blob','--offline']
bundle=json.loads(Path(args[args.index('--bundle')+1]).read_text())
raise SystemExit(0 if bundle.get('controlled_valid') else 1)
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
                "compose_file": str(compose),
                "env_file": str(env_file),
                "database_url_file": str(database),
                "state_directory": str(tmp_path / "private-state"),
                "spool_dir": str(tmp_path),
                "docker": str(docker),
                "cosign": str(cosign),
                "artifact_base_url": f"http://127.0.0.1:{server.server_port}",
            }
        )
    )
    config.chmod(0o600)
    WorkerControl(pipeline.catalog).register("old", synthetic=True)
    environment = {
        **os.environ,
        "CLI_STATE": str(docker_state),
        "CLI_TRACE": str(trace),
        "CLI_DATABASE_FILE": str(database),
    }

    def run(*arguments):
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "sm_trending_products.release_cli",
                *arguments,
                "--config",
                str(config),
                "--controlled-test",
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert "never-print-this" not in process.stdout + process.stderr
        return process, json.loads(process.stdout)

    try:
        process, result = run("bootstrap", "--revision", "a" * 40, "--worker-id", "old")
        assert process.returncode == 0 and result["status"] == "bootstrapped"
        (request_path / "request-bundle.json").write_text('{"controlled_valid":false}')
        process, result = run("pull")
        assert process.returncode == 2 and result["reason"] == "release_signature_invalid"
        (request_path / "request-bundle.json").write_text('{"controlled_valid":true}')
        bad_hash = request.model_copy(update={"manifest_sha256": "0" * 64})
        (request_path / "request.json").write_bytes(deployment_request_bytes(bad_hash))
        process, result = run("pull")
        assert (
            process.returncode == 2 and result["reason"] == "deployment_request_manifest_mismatch"
        )
        assert not pipeline.catalog.one("SELECT * FROM deployment_requests WHERE target='fixture'")
        (request_path / "request.json").write_bytes(deployment_request_bytes(request))
        process, result = run("pull")
        assert process.returncode == 2 and result["reason"] == "capture_proof_missing"
        release_id = result["release_id"]
        accepted = pipeline.catalog.one("SELECT * FROM deployment_requests WHERE target='fixture'")
        assert accepted and accepted["sequence"] == 2 and accepted["release_id"] == release_id
        pins = (tmp_path / "private-state/slots.env").read_text()
        assert release("b").manifest.image in pins and result["candidate_worker_id"] in pins
        assert "never-print-this" not in pins
        server.shutdown()
        server.server_close()
        process, result = run("pull")
        assert process.returncode == 2 and result["release_id"] == release_id
        assert result["reason"] == "capture_proof_missing"
        assert WorkerControl(pipeline.catalog).status("old")["state"] == "running"
        commands = [json.loads(line) for line in trace.read_text().splitlines()]
        assert not any(row[0] == "stop" for row in commands)
        assert sum(row[0] == "pull" for row in commands) == 1
        # Cancellation is durable across the timer's next pull while work drains.
        process, result = run("rollback", "--release-id", release_id)
        assert process.returncode == 2 and result["reason"] == "candidate_busy"
        process, result = run("pull")
        assert process.returncode == 2 and result["abort_requested"]
        control = WorkerControl(pipeline.catalog)
        candidate = control.status(result["candidate_worker_id"])
        control.acknowledge_drain(candidate["worker_id"], candidate["generation"])
        process, result = run("pull")
        assert process.returncode == 0 and result["status"] == "aborted"
        server = ThreadingHTTPServer(
            ("127.0.0.1", server.server_port),
            functools.partial(SimpleHTTPRequestHandler, directory=str(root)),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        old = request.model_copy(update={"sequence": 1})
        (request_path / "request.json").write_bytes(deployment_request_bytes(old))
        process, result = run("pull")
        assert process.returncode == 2 and result["reason"] == "deployment_request_replayed"
        changed = request.model_copy(update={"manifest_sha256": "0" * 64})
        (request_path / "request.json").write_bytes(deployment_request_bytes(changed))
        process, result = run("pull")
        assert (
            process.returncode == 2 and result["reason"] == "deployment_request_sequence_conflict"
        )
        config.chmod(0o644)
        process, result = run("status")
        assert process.returncode == 2 and result["reason"] == "host_file_permissions_invalid"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
