"""Actual subprocess arguments and private host locking for release operations."""

import json
import sys
from dataclasses import replace

import pytest

from sm_trending_products.release_control import (
    ReleaseBlocked,
    ReleaseManifest,
    ReleaseTrust,
    release_manifest_bytes,
)
from sm_trending_products.release_host import ComposeReleaseHost


def manifest():
    return ReleaseManifest(
        image="ghcr.io/example/pipeline@sha256:" + "b" * 64,
        revision="b" * 40,
        schema_min=28,
        schema_max=40,
        capture_protocol=1,
    )


def test_host_uses_only_digest_pinned_capture_slot_commands_and_no_force_kill(
    tmp_path, monkeypatch
):
    image = manifest().image
    trace, state = tmp_path / "trace.jsonl", tmp_path / "state.json"
    state.write_text(json.dumps({"image": image, "running": False, "worker": "candidate"}))
    binary = tmp_path / "docker"
    binary.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """
import json,os,sys
from pathlib import Path
args=sys.argv[1:]
assert args.pop(0)=='--host=unix:///var/run/docker.sock'
assert os.environ['NORMALIZER_IMAGE']==os.environ['CAPTURE_GREEN_IMAGE']
assert 'ANALYTICS_IMAGE' not in os.environ
with Path(os.environ['TRACE']).open('a') as handle:
    handle.write(json.dumps(args)+'\\n')
state=Path(os.environ['STATE'])
value=json.loads(state.read_text())
if args[0]=='pull': pass
elif args[:2]==['image','inspect']: print(json.dumps([value['image']]))
elif args[0]=='compose' and 'ps' in args:
    if value.get('exists',True): print('c'*64)
elif args[0]=='compose' and 'up' in args:
    value['running']=True;state.write_text(json.dumps(value))
elif args[0]=='inspect':
    print(json.dumps(value['image']));print(json.dumps(value['running']));print(json.dumps(value['worker']))
elif args[0]=='stop':
    assert '--time=-1' in args
    value['running']=False;state.write_text(json.dumps(value))
else: raise SystemExit(9)
"""
    )
    binary.chmod(0o700)
    monkeypatch.setenv("TRACE", str(trace))
    monkeypatch.setenv("STATE", str(state))
    monkeypatch.setenv("DOCKER_HOST", "tcp://unrelated-host:2375")
    monkeypatch.setenv("NORMALIZER_IMAGE", "untrusted:mutable")
    monkeypatch.setenv("ANALYTICS_IMAGE", "untrusted:mutable")

    class Capacity:
        calls = 0

        def admit(self):
            self.calls += 1

    capacity = Capacity()
    host = ComposeReleaseHost(
        project="fixture",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / ".env",
        lock_directory=tmp_path / "locks",
        docker=str(binary),
        slot_images={"blue": "ghcr.io/example/pipeline@sha256:" + "a" * 64, "green": image},
        slot_workers={"blue": "old", "green": "candidate"},
        capacity=capacity,
        role_images={"NORMALIZER_IMAGE": image},
    )
    with host.exclusive("fixture"):
        with pytest.raises(ReleaseBlocked, match="deployment_host_busy"):
            with host.exclusive("fixture"):
                pytest.fail("A second controller acquired the same host")
        host.prepare("fixture", manifest(), "green")
        host.start("fixture", manifest(), "green", "candidate")
        host.start("fixture", manifest(), "green", "candidate")
        assert capacity.calls == 2  # Pull plus first start, no double-count for existing slot.
        assert host.healthy("fixture", manifest(), "green", "candidate")
        host.retire("fixture", manifest(), "green", "candidate")
    commands = [json.loads(line) for line in trace.read_text().splitlines()]
    mutations = [row for row in commands if row[0] in {"compose", "stop"} and "ps" not in row]
    assert len(mutations) == 2
    assert mutations[0][-1] == "worker-green"
    assert "--no-deps" in mutations[0] and "--no-build" in mutations[0]
    assert all(
        not set(row) & {"down", "rm", "postgres", "--volumes", "--force"} for row in commands
    )

    class Denied:
        def admit(self):
            raise ReleaseBlocked("insufficient_release_capacity")

    denied = replace(host, capacity=Denied())
    before = trace.read_bytes()
    with pytest.raises(ReleaseBlocked, match="insufficient_release_capacity"):
        denied.prepare("fixture", manifest(), "green")
    assert trace.read_bytes() == before
    with pytest.raises(ReleaseBlocked, match="insufficient_release_capacity"):
        denied.start("fixture", manifest(), "green", "candidate")
    after = [json.loads(line) for line in trace.read_text().splitlines()]
    assert sum(row[0] == "compose" and "up" in row for row in after) == 1
    assert not host.absent("fixture", "green")  # A stopped container is still present.
    value = json.loads(state.read_text())
    value["exists"] = False
    state.write_text(json.dumps(value))
    assert host.absent("fixture", "green")
    with pytest.raises(ValueError, match="Unexpected processing image override"):
        replace(host, role_images={"POSTGRES_IMAGE": image})
    with pytest.raises(ValueError, match="immutable image digests"):
        replace(host, role_images={"NORMALIZER_IMAGE": "untrusted:latest"})


def test_release_verifier_fails_closed_and_pins_host_identity_not_bundle_claims(tmp_path):
    payload = manifest()
    with pytest.raises(ReleaseBlocked, match="release_verifier_unavailable"):
        ReleaseTrust(
            "expected-workflow", ("ghcr.io/example/pipeline",), str(tmp_path / "missing")
        ).verify(payload, {"method": "cosign_bundle", "bundle": {}})
    verifier = tmp_path / "verifier"
    marker = tmp_path / "invocation.json"
    verifier.write_text(
        "#!"
        + sys.executable
        + "\n"
        + f"""
import json,sys
from pathlib import Path
args=sys.argv[1:]
Path({str(marker)!r}).write_text(json.dumps({{'args':args,'payload':Path(args[-1]).read_text()}}))
raise SystemExit(1)
"""
    )
    verifier.chmod(0o700)
    trust = ReleaseTrust("expected-workflow", ("ghcr.io/example/pipeline",), str(verifier))
    with pytest.raises(ReleaseBlocked, match="release_signature_invalid"):
        trust.verify(payload, {"method": "cosign_bundle", "bundle": {"identity": "untrusted"}})
    invocation = json.loads(marker.read_text())
    assert invocation["args"][:2] == ["verify-blob", "--offline"]
    assert (
        invocation["args"][invocation["args"].index("--certificate-identity") + 1]
        == "expected-workflow"
    )
    assert (
        invocation["args"][invocation["args"].index("--certificate-oidc-issuer") + 1]
        == "https://token.actions.githubusercontent.com"
    )
    assert invocation["payload"].encode() == release_manifest_bytes(payload)
    with pytest.raises(ReleaseBlocked, match="image_repository_not_allowed"):
        ReleaseTrust("expected-workflow", ("ghcr.io/another/repo",), str(verifier)).verify(
            payload, {"method": "cosign_bundle", "bundle": {}}
        )


def test_processing_role_start_is_digest_pinned_idempotent_and_rejects_protected_roles(
    tmp_path, monkeypatch
):
    image = manifest().image
    trace, state = tmp_path / "role-trace.jsonl", tmp_path / "role-state.json"
    state.write_text(json.dumps({"exists": False}))
    binary = tmp_path / "docker-role"
    binary.write_text(
        "#!"
        + sys.executable
        + "\n"
        + """
import json,os,sys
from pathlib import Path
args=sys.argv[1:]
assert args.pop(0)=='--host=unix:///var/run/docker.sock'
with Path(os.environ['TRACE']).open('a') as handle:
    handle.write(json.dumps(args)+'\\n')
state=Path(os.environ['STATE'])
value=json.loads(state.read_text())
if args[0]=='compose' and 'ps' in args:
    assert args[-1]=='dashboard'
    if value['exists']: print('c'*64)
elif args[0]=='compose' and 'up' in args:
    assert args[-1]=='dashboard'
    assert '--no-deps' in args and '--no-build' in args and '--pull' in args
    value.update(exists=True,image=os.environ['DASHBOARD_IMAGE'])
    state.write_text(json.dumps(value))
elif args[0]=='inspect':
    print(json.dumps(value['image']));print('true');print('null')
else: raise SystemExit(9)
"""
    )
    binary.chmod(0o700)
    monkeypatch.setenv("TRACE", str(trace))
    monkeypatch.setenv("STATE", str(state))
    host = ComposeReleaseHost(
        project="fixture",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "runtime.env",
        lock_directory=tmp_path,
        docker=str(binary),
        slot_images={slot: image for slot in ("blue", "green")},
        slot_workers={"blue": "old", "green": "candidate"},
        role_images={"DASHBOARD_IMAGE": image},
    )
    assert host.start_role("dashboard", image).identifier == "c" * 64
    assert host.start_role("dashboard", image).identifier == "c" * 64
    running = host.running_role("dashboard", image)
    assert running is not None and running.identifier == "c" * 64
    before = trace.read_bytes()
    with pytest.raises(ReleaseBlocked, match="processing_service_not_allowed"):
        host.start_role("postgres", image)
    with pytest.raises(ReleaseBlocked, match="processing_role_configuration_mismatch"):
        host.start_role("dashboard", image.replace("b" * 64, "a" * 64))
    assert trace.read_bytes() == before
    commands = [json.loads(line) for line in before.decode().splitlines()]
    assert sum("up" in command for command in commands) == 1
    assert len(commands) == 10  # Same two reads per running-role check, no added inspection.


def test_planner_probe_rejects_raw_container_ids_before_any_command(tmp_path, monkeypatch):
    commands = []

    def command(self, arguments, **kwargs):
        commands.append(arguments)
        return '{"status":"not_due"}'

    monkeypatch.setattr(ComposeReleaseHost, "_command", command)
    image = manifest().image
    host = ComposeReleaseHost(
        project="fixture",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "runtime.env",
        lock_directory=tmp_path,
        slot_images={slot: image for slot in ("blue", "green")},
        slot_workers={"blue": "old", "green": "candidate"},
    )
    with pytest.raises(ReleaseBlocked, match="processing_service_not_allowed"):
        host.planner_probe("c" * 64)
    assert commands == []


def test_planner_role_handles_preserve_journal_identity_and_reject_wrong_scopes(
    tmp_path, monkeypatch
):
    commands = []

    def command(self, arguments, **kwargs):
        commands.append(arguments)
        if arguments[0] == "compose":
            return "c" * 64 if arguments[-1] == "capture-planner" else "d" * 64
        if arguments[0] == "inspect":
            return '"planner-host"\n"2026-10-28T00:00:00Z"'
        if arguments[0] == "exec":
            return '{"status":"not_due"}'
        if arguments[0] == "stop":
            return ""
        raise AssertionError(arguments)

    monkeypatch.setattr(ComposeReleaseHost, "_command", command)
    image = manifest().image
    host = ComposeReleaseHost(
        project="fixture",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "runtime.env",
        lock_directory=tmp_path,
        slot_images={slot: image for slot in ("blue", "green")},
        slot_workers={"blue": "old", "green": "candidate"},
    )
    planner = host.role_container("capture-planner")
    replayable = host.role_container("dashboard")
    assert planner is not None and replayable is not None
    assert host.planner_identity(planner) == {
        "container": "c" * 64,
        "hostname": "planner-host",
        "started_at": "2026-10-28T00:00:00Z",
    }
    assert host.planner_probe(planner) == {"status": "not_due"}
    host.stop_planner(planner)
    assert len(commands) == 5  # Two selections, one identity read, probe and stop.
    assert commands[-1] == ["stop", "--time=-1", "c" * 64]
    before = commands.copy()
    invalid = [
        "c" * 64,
        replayable,
        replace(planner, project="other"),
        replace(planner, service="postgres"),
    ]
    for operation in (host.planner_identity, host.planner_probe, host.stop_planner):
        for container in invalid:
            with pytest.raises(ReleaseBlocked, match="processing_service_not_allowed"):
                operation(container)
    assert commands == before
