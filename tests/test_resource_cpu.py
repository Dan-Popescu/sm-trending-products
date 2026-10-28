"""The CPU controller changes analytical quotas only, including uncertain failures."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from sm_trending_products import resource_cpu
from sm_trending_products.release_capacity import CapacityBudget
from sm_trending_products.release_control import ReleaseBlocked
from sm_trending_products.release_host import ComposeReleaseHost
from sm_trending_products.resource_cpu import CpuModeController
from sm_trending_products.resource_cpu_state import (
    CPU_VARIABLES,
    cpu_environment,
    cpu_transition_pending,
    load_cpu_policy,
)


class Inventory:
    def __init__(self, host):
        self.host = host
        self.rows = {}
        self.commands = []
        self.fail_update = None
        self.crash_update = None
        self.calls = 0
        services = ["worker", "postgres", "normalizer", *CPU_VARIABLES]
        values = [750_000_000, 500_000_000, 500_000_000, 500_000_000, 250_000_000, 250_000_000]
        for number, (service, cpus) in enumerate(zip(services, values, strict=True), start=1):
            identifier = format(number, "064x")
            self.rows[identifier] = [
                identifier,
                host.project,
                service,
                "False",
                "ghcr.io/example/pipeline@sha256:" + "a" * 64,
                True,
                100 + number,
                "2026-10-28T00:00:00.000000000Z",
                0,
                {
                    "Memory": 1024**3,
                    "NanoCpus": cpus,
                    "CpuQuota": 0,
                    "CpuPeriod": 0,
                    "CpusetCpus": "",
                    "MemorySwap": 2 * 1024**3,
                    "PidsLimit": 128,
                },
            ]
        self.original = copy.deepcopy(self.rows)

    def command(self, host, arguments, *, timeout=45, cpu_overrides=None):
        assert host.project == self.host.project and host.compose_file == self.host.compose_file
        self.commands.append(arguments)
        if arguments[0] == "ps":
            return "\n".join(self.rows)
        if arguments[0] == "inspect":
            return "\n".join(
                "\t".join(json.dumps(value) for value in self.rows[key]) for key in arguments[3:]
            )
        if arguments[0] == "compose" and "config" in arguments:
            pins = (
                {CPU_VARIABLES[key]: str(value / 10**9) for key, value in cpu_overrides.items()}
                if cpu_overrides
                else cpu_environment(host.lock_directory, host.project)
            )
            return json.dumps(
                {
                    "services": {
                        row[2]: {
                            "cpus": float(pins[CPU_VARIABLES[row[2]]])
                            if row[2] in CPU_VARIABLES and pins
                            else row[9]["NanoCpus"] / 10**9
                        }
                        for row in self.original.values()
                    }
                }
            )
        if arguments[0] == "update":
            self.calls += 1
            self.rows[arguments[-1]][9]["NanoCpus"] = round(float(arguments[2]) * 10**9)
            if self.crash_update == self.calls:
                raise KeyboardInterrupt("controller terminated after Docker applied quota")
            if self.fail_update == self.calls:
                raise ReleaseBlocked("deployment_host_command_failed")
            return arguments[-1]
        raise AssertionError(arguments)


class CatalogFixture:
    pending = False
    healthy = True
    gap = False

    def one(self, query, parameters=()):
        if "FROM deployment_runs" in query:
            return {"pending": 1} if self.pending else None
        if "FROM capture_obligations" in query:
            return {"healthy": not self.gap}
        if "FROM deployment_targets" in query:
            now = datetime.now(UTC)
            return {
                "active_manifest": {
                    "image": "ghcr.io/example/pipeline@sha256:" + "a" * 64,
                    "revision": "a" * 40,
                    "schema_min": 35,
                    "schema_max": 40,
                    "capture_protocol": 1,
                },
                "active_slot": "blue",
                "active_worker_id": "fixture-blue",
                "synthetic": True,
                "state": "running" if self.healthy else "drained",
                "heartbeat_at": now - timedelta(seconds=1),
                "checked_at": now,
            }
        raise AssertionError(query)


@pytest.fixture
def controller(tmp_path, monkeypatch):
    directory = tmp_path / "state"
    directory.mkdir(mode=0o700)
    host = ComposeReleaseHost(
        project="fixture",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "runtime.env",
        lock_directory=directory,
        slot_images={
            slot: "ghcr.io/example/pipeline@sha256:" + "a" * 64 for slot in ("blue", "green")
        },
        slot_workers={"blue": "fixture-blue", "green": "fixture-green"},
    )
    inventory = Inventory(host)
    monkeypatch.setattr(
        ComposeReleaseHost,
        "_command",
        lambda host, arguments, **kw: inventory.command(host, arguments, **kw),
    )
    monkeypatch.setattr(ComposeReleaseHost, "healthy", lambda *args: True)
    result = CpuModeController(host, CatalogFixture())
    result.fixture = inventory
    return result


def test_preview_is_read_only_and_switch_keeps_protected_processes(controller):
    inventory = controller.fixture
    assert controller.status()["mode"] == "unconfigured"
    result = controller.switch("economy", dry_run=True)
    assert result["status"] == "preview"
    assert not (controller.host.lock_directory / "cpu-mode.json").exists()
    assert not any(command[0] == "update" for command in inventory.commands)
    result = controller.switch("economy")
    assert result["analytical_cpus"] == {
        "dagster-daemon": 0.25,
        "dagster-webserver": 0.125,
        "dashboard": 0.125,
    }
    for key, row in inventory.rows.items():
        before = inventory.original[key]
        if row[2] not in CPU_VARIABLES:
            assert row == before
        assert row[:9] == before[:9]
    assert cpu_environment(controller.host.lock_directory, "fixture") == {
        "DAGSTER_DAEMON_CPUS": "0.250000000",
        "DAGSTER_WEBSERVER_CPUS": "0.125000000",
        "DASHBOARD_CPUS": "0.125000000",
    }
    controller.switch("standard")
    assert inventory.rows == inventory.original
    assert controller.reconcile() is None


def test_invalid_mode_and_lock_contention_never_mutate(controller):
    with pytest.raises(ReleaseBlocked, match="cpu_mode_invalid"):
        controller.switch("arbitrary")
    with (
        controller.host.exclusive("fixture"),
        pytest.raises(ReleaseBlocked, match="deployment_host_busy"),
    ):
        controller.switch("economy")
    assert controller.fixture.calls == 0


@pytest.mark.parametrize("reason", ["release", "processing", "worker", "gap"])
def test_unsafe_switch_is_refused(controller, reason):
    if reason == "release":
        controller.catalog.pending = True
    elif reason == "processing":
        path = controller.host.lock_directory / "processing-refresh.json"
        path.write_text(json.dumps({"target": "fixture", "schema_version": 1, "status": "waiting"}))
        path.chmod(0o600)
    elif reason == "worker":
        controller.catalog.healthy = False
    else:
        controller.catalog.gap = True
    with pytest.raises(ReleaseBlocked):
        controller.switch("economy")
    assert controller.fixture.calls == 0


@pytest.mark.parametrize("failure", [1, 2, 3])
def test_uncertain_partial_docker_failure_rolls_back_every_original_quota(controller, failure):
    controller.fixture.fail_update = failure
    with pytest.raises(ReleaseBlocked, match="cpu_update_rolled_back"):
        controller.switch("economy")
    assert controller.fixture.rows == controller.fixture.original
    assert not cpu_transition_pending(controller.host.lock_directory, "fixture")
    assert controller.status()["mode"] == "standard"


@pytest.mark.parametrize("failure", [1, 2, 3])
def test_process_death_recovers_durable_intent_before_later_operations(controller, failure):
    controller.fixture.crash_update = failure
    with pytest.raises(KeyboardInterrupt):
        controller.switch("economy")
    assert cpu_transition_pending(controller.host.lock_directory, "fixture")
    with pytest.raises(ReleaseBlocked, match="cpu_recovery_required"):
        controller.switch("standard")
    result = controller.reconcile()
    assert result["recovered"]
    assert controller.fixture.rows == controller.fixture.original
    assert not cpu_transition_pending(controller.host.lock_directory, "fixture")


def test_guard_restores_standard_but_does_not_interfere_with_release_draining(controller):
    controller.switch("economy")
    controller.catalog.healthy = False
    controller.catalog.pending = True
    assert controller.reconcile() is None
    assert controller.status()["mode"] == "economy"
    controller.catalog.pending = False
    result = controller.reconcile()
    assert result["reason"] == "cpu_capture_guard_restored_standard"
    assert controller.fixture.rows == controller.fixture.original


@pytest.mark.parametrize("kind", ["unknown", "duplicate", "quota", "cpuset", "unlimited", "pid"])
def test_ambiguous_or_incompatible_inventories_are_rejected(controller, kind):
    row = next(iter(controller.fixture.rows.values()))
    if kind == "unknown":
        row[2] = "unexamined"
    elif kind == "duplicate":
        row[2] = "postgres"
    elif kind == "quota":
        row[9]["CpuQuota"] = 100_000
    elif kind == "cpuset":
        row[9]["CpusetCpus"] = "0"
    elif kind == "unlimited":
        row[9]["NanoCpus"] = 0
    else:
        row[6] = 0
    with pytest.raises(ReleaseBlocked, match="cpu_inventory_invalid"):
        controller.switch("economy")
    assert controller.fixture.calls == 0


def test_increased_cpu_requires_fresh_aggregate_headroom(controller, monkeypatch):
    from dataclasses import replace

    controller.switch("economy")
    capacity = SimpleNamespace(
        _allocations=lambda: {"allocated_cpus": 3.75},
        budget=CapacityBudget(cpu_reserve=0.25),
    )
    controller.host = replace(controller.host, capacity=capacity)
    controller.fixture.host = controller.host
    monkeypatch.setattr(resource_cpu, "_host_limits", lambda: {"cpus": 4})
    calls = controller.fixture.calls
    with pytest.raises(ReleaseBlocked, match="cpu_headroom_insufficient"):
        controller.switch("standard")
    assert controller.fixture.calls == calls
    assert controller.status()["mode"] == "economy"


def test_cpu_state_is_private_bounded_validated_and_does_not_accept_symlinks(controller):
    directory = controller.host.lock_directory
    assert cpu_environment(directory, "fixture") == {}
    assert not cpu_transition_pending(directory, "fixture")
    controller.switch("economy")
    path = directory / "cpu-mode.json"
    assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(ReleaseBlocked, match="cpu_policy_invalid"):
        load_cpu_policy(directory, "other-target")
    path.chmod(0o644)
    with pytest.raises(ReleaseBlocked, match="cpu_policy_permissions_invalid"):
        controller.status()
    path.chmod(0o600)
    original = path.read_bytes()
    path.unlink()
    source = directory / "elsewhere"
    source.write_bytes(original)
    path.symlink_to(source)
    with pytest.raises(ReleaseBlocked, match="cpu_policy_unavailable"):
        controller.status()
    path.unlink()
    path.write_text("x" * (128 * 1024 + 1))
    path.chmod(0o600)
    with pytest.raises(ReleaseBlocked, match="cpu_policy_too_large"):
        controller.status()


def test_external_restart_blocks_recovery_without_touching_capture(controller):
    controller.fixture.crash_update = 1
    with pytest.raises(KeyboardInterrupt):
        controller.switch("economy")
    row = next(iter(controller.fixture.rows.values()))
    row[6] += 1
    calls = controller.fixture.calls
    with pytest.raises(ReleaseBlocked, match="cpu_runtime_changed"):
        controller.reconcile()
    assert controller.fixture.calls == calls
    assert cpu_transition_pending(controller.host.lock_directory, "fixture")


def test_unusable_or_malformed_policy_fails_closed(controller):
    path = controller.host.lock_directory / "cpu-mode.json"
    path.write_text('{"schema_version":1,"target":"fixture"}')
    path.chmod(0o600)
    with pytest.raises(ReleaseBlocked, match="cpu_policy_invalid"):
        controller.switch("economy")
    assert controller.fixture.calls == 0


def test_baseline_cannot_adopt_an_unrecorded_manual_cpu_change(controller):
    row = next(row for row in controller.fixture.rows.values() if row[2] == "dagster-daemon")
    row[9]["NanoCpus"] = 400_000_000
    with pytest.raises(ReleaseBlocked, match="cpu_baseline_mismatch"):
        controller.switch("economy")
    assert controller.fixture.calls == 0


def test_recovery_failure_stays_journaled_and_is_retryable(controller):
    controller.fixture.crash_update = 2
    with pytest.raises(KeyboardInterrupt):
        controller.switch("economy")
    controller.fixture.fail_update = 3
    with pytest.raises(ReleaseBlocked, match="deployment_host_command_failed"):
        controller.reconcile()
    assert cpu_transition_pending(controller.host.lock_directory, "fixture")
    controller.fixture.fail_update = None
    assert controller.reconcile()["recovered"]
    assert controller.fixture.rows == controller.fixture.original


def test_post_switch_capture_guard_rolls_back_without_mutating_protected_resources(
    controller, monkeypatch
):
    original = controller._healthy
    probes = 0

    def degrade():
        nonlocal probes
        probes += 1
        return original() and probes == 1

    monkeypatch.setattr(controller, "_healthy", degrade)
    with pytest.raises(ReleaseBlocked, match="cpu_update_rolled_back"):
        controller.switch("economy")
    assert controller.fixture.rows == controller.fixture.original
    assert controller.status()["mode"] == "standard"


def test_state_rejects_coerced_or_unapproved_quota_values(controller):
    controller.switch("economy")
    path = controller.host.lock_directory / "cpu-mode.json"
    source = json.loads(path.read_text())
    for value in (True, "125000000", 1, 0):
        row = copy.deepcopy(source)
        row["quotas"]["dashboard"] = value
        path.write_text(json.dumps(row))
        with pytest.raises(ReleaseBlocked, match="cpu_policy_invalid"):
            controller.status()


def test_target_restart_during_update_rolls_back_only_when_epoch_is_still_verified(
    controller, monkeypatch
):
    original = controller.fixture.command

    def restarted(host, arguments, **kwargs):
        result = original(host, arguments, **kwargs)
        if arguments[0] == "update":
            controller.fixture.rows[arguments[-1]][6] += 1
        return result

    monkeypatch.setattr(
        ComposeReleaseHost,
        "_command",
        lambda host, arguments, **kw: restarted(host, arguments, **kw),
    )
    with pytest.raises(ReleaseBlocked, match="cpu_recovery_required"):
        controller.switch("economy")
    assert cpu_transition_pending(controller.host.lock_directory, "fixture")
    assert controller.fixture.calls == 1
    for row in controller.fixture.rows.values():
        if row[2] not in CPU_VARIABLES:
            assert row == controller.fixture.original[row[0]]


def test_cpu_floors_do_not_increase_existing_small_standard_quotas(controller):
    for row in controller.fixture.rows.values():
        if row[2] in CPU_VARIABLES:
            row[9]["NanoCpus"] = 150_000_000
    controller.fixture.original = copy.deepcopy(controller.fixture.rows)
    result = controller.switch("economy")
    assert result["analytical_cpus"] == {
        "dagster-daemon": 0.15,
        "dagster-webserver": 0.1,
        "dashboard": 0.1,
    }


def test_compose_template_that_ignores_cpu_pins_is_refused_before_any_change(
    controller, monkeypatch
):
    original = controller.fixture.command
    monkeypatch.setattr(
        ComposeReleaseHost,
        "_command",
        lambda host, arguments, **kwargs: original(
            host, arguments, timeout=kwargs.get("timeout", 45)
        ),
    )
    with pytest.raises(ReleaseBlocked, match="cpu_compose_pins_unsupported"):
        controller.switch("economy")
    assert controller.fixture.calls == 0
    assert not (controller.host.lock_directory / "cpu-mode.json").exists()


@pytest.mark.integration
def test_native_capture_guard_uses_current_pending_windows_and_a_bounded_budget(
    controller, pipeline
):
    import time

    from psycopg.types.json import Jsonb

    from sm_trending_products.capture_contracts import CapturePolicy

    controller.catalog = pipeline.catalog
    manifest = CatalogFixture().one("FROM deployment_targets")["active_manifest"]
    pipeline.worker_control.register("fixture-blue", synthetic=True)
    pipeline.catalog.execute(
        "INSERT INTO deployment_targets(target,active_manifest,active_proof,active_slot,"
        "active_worker_id,synthetic) "
        "VALUES(%s,%s,%s,'blue','fixture-blue',true)",
        (controller.host.project, Jsonb(manifest), Jsonb({"method": "controlled_fixture"})),
    )
    assert controller._healthy()
    task = pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {},
        False,
        due_at=datetime.now(UTC),
        idempotency_key="other",
    )
    assert controller._healthy()
    unknown = pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {},
        True,
        due_at=datetime.now(UTC),
        idempotency_key="unknown",
    )
    assert not controller._healthy()
    pipeline.catalog.execute(
        "DELETE FROM collection_tasks WHERE task_id IN (%s,%s)", (task, unknown)
    )
    pipeline.capture_contracts.register(
        CapturePolicy(
            source="tiktok_display",
            account_id="fixture-account",
            operation="query",
            version="cpu-live-guard",
            cadence_seconds=60,
            lateness_seconds=3600,
        ),
        synthetic=True,
    )
    # Future collection windows do not consume the current due-inspection budget.
    pipeline.catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,"
        "synthetic,due_at) "
        "SELECT 'cpu-future-'||n::text,'tiktok_display','fixture-account','query','{}'::jsonb,true,"
        "statement_timestamp()+interval '1 day' FROM generate_series(1,1001) n"
    )
    assert controller._healthy()
    pipeline.catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,"
        "synthetic,due_at) "
        "SELECT 'cpu-due-'||n::text,'tiktok_display','fixture-account','query','{}'::jsonb,true,"
        "statement_timestamp() FROM generate_series(1,1000) n"
    )
    assert controller._healthy()
    extra = pipeline.catalog.enqueue(
        "tiktok_display",
        "fixture-account",
        "query",
        {},
        True,
        due_at=datetime.now(UTC),
        idempotency_key="over-budget",
    )
    assert not controller._healthy()
    pipeline.catalog.execute("DELETE FROM collection_tasks WHERE task_id=%s", (extra,))
    assert controller._healthy()
    pipeline.catalog.execute("DELETE FROM collection_tasks WHERE task_id LIKE 'cpu-due-%%'")
    pipeline.capture_contracts.register(
        CapturePolicy(
            source="tiktok_display",
            account_id="expiry-account",
            operation="query",
            version="cpu-expiry",
            cadence_seconds=60,
            lateness_seconds=0,
        ),
        synthetic=True,
    )
    expired = pipeline.catalog.enqueue(
        "tiktok_display",
        "expiry-account",
        "query",
        {},
        True,
        due_at=datetime.now(UTC) + timedelta(milliseconds=100),
        idempotency_key="expiry",
    )
    pipeline.catalog.execute(
        "UPDATE collection_tasks SET due_at=clock_timestamp()+interval '1 day' WHERE task_id=%s",
        (expired,),
    )
    time.sleep(0.15)
    assert not controller._healthy()
    assert controller.fixture.calls == 0


def test_lowering_cpu_is_allowed_when_host_has_no_spare_capacity(controller, monkeypatch):
    from dataclasses import replace

    capacity = SimpleNamespace(
        _allocations=lambda: {"allocated_cpus": 4.0},
        budget=CapacityBudget(cpu_reserve=0.25),
    )
    controller.host = replace(controller.host, capacity=capacity)
    controller.fixture.host = controller.host
    monkeypatch.setattr(resource_cpu, "_host_limits", lambda: {"cpus": 4})
    assert controller.switch("economy")["mode"] == "economy"
    calls = controller.fixture.calls
    with pytest.raises(ReleaseBlocked, match="cpu_headroom_insufficient"):
        controller.switch("standard")
    assert controller.fixture.calls == calls
    assert controller.status()["mode"] == "economy"


def test_recovering_an_interrupted_cpu_increase_needs_no_fresh_headroom(controller, monkeypatch):
    from dataclasses import replace

    controller.switch("economy")
    expected = copy.deepcopy(controller.fixture.rows)
    capacity = SimpleNamespace(
        _allocations=lambda: {"allocated_cpus": 3.0},
        budget=CapacityBudget(cpu_reserve=0.25),
    )
    controller.host = replace(controller.host, capacity=capacity)
    controller.fixture.host = controller.host
    monkeypatch.setattr(resource_cpu, "_host_limits", lambda: {"cpus": 4})
    controller.fixture.crash_update = controller.fixture.calls + 1
    with pytest.raises(KeyboardInterrupt):
        controller.switch("standard")
    assert cpu_transition_pending(controller.host.lock_directory, "fixture")

    def unavailable():
        raise OSError("capacity read unavailable after the interrupted increase")

    capacity._allocations = unavailable
    monkeypatch.setattr(resource_cpu, "_host_limits", unavailable)
    assert controller.reconcile()["recovered"]
    assert controller.fixture.rows == expected
    assert controller.status()["mode"] == "economy"


def test_mixed_changes_admit_each_positive_delta_without_offsetting_reductions(
    controller, monkeypatch
):
    from dataclasses import replace

    capacity = SimpleNamespace(
        _allocations=lambda: {"allocated_cpus": 3.75},
        budget=CapacityBudget(cpu_reserve=0.25),
    )
    controller.host = replace(controller.host, capacity=capacity)
    controller.fixture.host = controller.host
    monkeypatch.setattr(resource_cpu, "_host_limits", lambda: {"cpus": 4})
    current = controller._inventory()
    quotas = controller._quotas(current)
    quotas["dagster-daemon"] += 100_000_000
    quotas["dagster-webserver"] -= 100_000_000
    quotas["dashboard"] -= 100_000_000
    with pytest.raises(ReleaseBlocked, match="cpu_headroom_insufficient"):
        controller._admit(current, quotas)
    assert controller.fixture.calls == 0
