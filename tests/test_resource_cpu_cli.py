"""The host CLI exposes noninteractive, previewable CPU operations only."""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from sm_trending_products import release_cli
from sm_trending_products.release_control import ReleaseBlocked
from sm_trending_products.release_host import ComposeReleaseHost
from sm_trending_products.resource_cpu_state import CpuPolicy, write_cpu_policy


@pytest.mark.parametrize(
    "arguments",
    [
        ["cpu-mode"],
        ["cpu-mode", "--mode", "unknown"],
        ["pull", "--mode", "economy"],
        ["pull", "--dry-run"],
    ],
)
def test_invalid_cpu_arguments_fail_before_opening_host_configuration(arguments, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Invalid arguments must not open the host configuration")

    monkeypatch.setattr(release_cli, "load_config", unexpected)
    with pytest.raises(SystemExit) as error:
        release_cli.main([*arguments, "--config", "/private/controller.json"])
    assert error.value.code == 2


@pytest.mark.parametrize("command", ["cpu-mode", "cpu-status", "cpu-recover"])
def test_cpu_help_exposes_preview_and_recovery_without_loading_secrets(command, capsys):
    with pytest.raises(SystemExit) as error:
        release_cli.main([command, "--help"])
    assert error.value.code == 0
    output = capsys.readouterr().out
    assert "--dry-run" in output and "cpu-recover" in output and "Examples:" in output


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (["cpu-mode", "--mode", "economy", "--dry-run"], ("switch", "economy", True)),
        (["cpu-mode", "--mode", "standard"], ("switch", "standard", False)),
        (["cpu-status"], ("status",)),
        (["cpu-recover"], ("reconcile",)),
    ],
)
def test_cpu_cli_dispatches_and_closes_database(arguments, expected, monkeypatch, capsys):
    calls = []
    config = SimpleNamespace(database_url_file=Path("/private/database-url"))
    monkeypatch.setattr(release_cli, "load_config", lambda *args, **kwargs: config)
    monkeypatch.setattr(release_cli, "private_file", lambda *args, **kwargs: b"fixture-only")

    class Database:
        def __init__(self, value):
            assert value == "fixture-only"

        def close(self):
            calls.append(("close",))

    class Modes:
        def switch(self, mode, dry_run=False):
            calls.append(("switch", mode, dry_run))
            return {"status": "preview" if dry_run else "stable", "mode": mode}

        def status(self):
            calls.append(("status",))
            return {"status": "stable"}

    class Runtime:
        def __init__(self, *args, **kwargs):
            pass

        def cpu_modes(self):
            return Modes()

        def reconcile_cpu(self):
            calls.append(("reconcile",))
            return {"status": "stable", "recovered": True}

    monkeypatch.setattr(release_cli, "Catalog", Database)
    monkeypatch.setattr(release_cli, "HostRuntime", Runtime)
    assert release_cli.main([*arguments, "--config", "/private/controller.json"]) == 0
    assert calls == [expected, ("close",)]
    assert json.loads(capsys.readouterr().out)["status"] in {"stable", "preview"}


def test_cpu_recovery_precedes_deployment_and_outbound_access(monkeypatch):
    runtime = object.__new__(release_cli.HostRuntime)

    def blocked():
        raise ReleaseBlocked("cpu_recovery_required")

    monkeypatch.setattr(runtime, "reconcile_cpu", blocked)
    for operation in (
        runtime.pull,
        lambda: runtime.apply("not-read"),
        lambda: runtime.rollback("not-read"),
        runtime.refresh_processing_release,
    ):
        with pytest.raises(ReleaseBlocked, match="cpu_recovery_required"):
            operation()


@pytest.fixture
def cpu_host(tmp_path):
    image = "ghcr.io/example/pipeline@sha256:" + "a" * 64
    return ComposeReleaseHost(
        "fixture",
        tmp_path / "compose.yaml",
        tmp_path / "runtime.env",
        tmp_path,
        {"blue": image, "green": image},
        {"blue": "blue", "green": "green"},
    )


def test_persisted_cpu_pins_override_inherited_variables_and_preview_is_read_only(
    tmp_path, monkeypatch, cpu_host
):
    host = cpu_host
    baseline = {
        "dagster-daemon": 500_000_000,
        "dagster-webserver": 250_000_000,
        "dashboard": 250_000_000,
    }
    quotas = {key: value // 2 for key, value in baseline.items()}
    write_cpu_policy(
        tmp_path,
        CpuPolicy(
            target="fixture",
            mode="economy",
            baseline=baseline,
            quotas=quotas,
            activated_at=datetime.now(UTC),
        ),
    )
    before = (tmp_path / "cpu-mode.json").read_bytes()
    captured = []

    def command(arguments, **kwargs):
        captured.append((arguments, kwargs["env"]))
        return SimpleNamespace(stdout="{}", returncode=0)

    monkeypatch.setattr("sm_trending_products.release_host.subprocess.run", command)
    monkeypatch.setenv("DAGSTER_DAEMON_CPUS", "99")
    config = [*host._compose(), "config", "--format", "json"]
    host._command(config)
    assert captured[-1][1]["DAGSTER_DAEMON_CPUS"] == "0.250000000"
    host._command(config, cpu_overrides=baseline)
    assert captured[-1][1]["DAGSTER_DAEMON_CPUS"] == "0.500000000"
    assert (tmp_path / "cpu-mode.json").read_bytes() == before
    with pytest.raises(ValueError, match="restricted"):
        host._command([*host._compose(), "up", "--detach", "dashboard"], cpu_overrides=baseline)
    assert len(captured) == 2

    monkeypatch.setattr(
        "sm_trending_products.resource_cpu_state.cpu_transition_pending", lambda *a: True
    )
    for arguments in (
        ["pull", host.slot_images["blue"]],
        ["stop", "container"],
        [*host._compose(), "up", "--detach", "dashboard"],
    ):
        with pytest.raises(ReleaseBlocked, match="cpu_transition_recovery_required"):
            host._command(arguments)
    assert len(captured) == 2
    host._command(["inspect", "container"])
    assert len(captured) == 3  # Recovery can still inspect runtime state.


@pytest.mark.parametrize("service", ["dagster-daemon", "dagster-webserver"])
@pytest.mark.parametrize(
    "environment,safe",
    [
        ({}, False),
        ({"ACQUISITION_ENABLED": "false"}, False),
        ({"ACQUISITION_ENABLED": "true", "acquisition_enabled": "false"}, False),
        ({"ACQUISITION_ENABLED": "true"}, True),
    ],
)
def test_pending_processing_refuses_future_legacy_collection_with_economy_pins(
    cpu_host, monkeypatch, service, environment, safe
):
    baseline = {
        "dagster-daemon": 500_000_000,
        "dagster-webserver": 250_000_000,
        "dashboard": 250_000_000,
    }
    write_cpu_policy(
        cpu_host.lock_directory,
        CpuPolicy(
            target=cpu_host.project,
            mode="economy",
            baseline=baseline,
            quotas={key: value // 2 for key, value in baseline.items()},
            activated_at=datetime.now(UTC),
        ),
    )
    commands = []

    def config(host, arguments, **kwargs):
        commands.append(arguments)
        assert arguments == [*host._compose(), "config", "--format", "json", service]
        return json.dumps({"services": {service: {"environment": environment}}})

    monkeypatch.setattr(ComposeReleaseHost, "_command", config)
    if safe:
        cpu_host.admit_role(service)
    else:
        with pytest.raises(ReleaseBlocked, match="cpu_legacy_collection_active"):
            cpu_host.admit_role(service)
    assert len(commands) == 1  # Configuration only; no container creation or source changes.
