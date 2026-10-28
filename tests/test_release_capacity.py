"""Admission uses available resources, actual allocations and shared disk budgets."""

import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from sm_trending_products import release_capacity as capacity
from sm_trending_products.release_capacity import GIB, CapacityBudget, HostCapacity
from sm_trending_products.release_control import ReleaseBlocked


@pytest.fixture
def host(tmp_path, monkeypatch):
    docker, spool = tmp_path / "docker", tmp_path / "spool"
    docker.mkdir()
    spool.mkdir()
    result = HostCapacity(docker, spool)
    monkeypatch.setattr(
        capacity,
        "_host_limits",
        lambda: {"memory_total_bytes": 16 * GIB, "memory_available_bytes": 8 * GIB, "cpus": 8.0},
    )
    monkeypatch.setattr(
        HostCapacity,
        "_allocations",
        lambda self: {
            "running_containers": 2,
            "allocated_memory_bytes": 4 * GIB,
            "allocated_cpus": 2.0,
        },
    )
    monkeypatch.setattr(capacity.shutil, "disk_usage", lambda path: SimpleNamespace(free=20 * GIB))
    return result


def test_available_capacity_reports_only_structured_measurements(host):
    result = host.admit()
    assert result["allowed"] and result["reasons"] == []
    assert result["measurements"]["shared_filesystem"]
    assert result["requirements"]["image_unpack_bytes"] == 4 * GIB
    assert str(host.docker_data_dir) not in str(result)


def test_cpu_increase_admission_includes_existing_allocations_and_reserve(host):
    host.admit_cpu_increase(5_000_000_000)
    with pytest.raises(ReleaseBlocked, match="cpu_headroom_insufficient"):
        host.admit_cpu_increase(5_000_000_001)


@pytest.mark.parametrize(
    "measurement,reason",
    [
        ({"memory_available_bytes": 2 * GIB}, "available_memory_insufficient"),
        ({"memory_total_bytes": 6 * GIB}, "allocated_memory_headroom_insufficient"),
        ({"cpus": 3.5}, "cpu_headroom_insufficient"),
    ],
)
def test_overlap_requires_available_memory_and_configured_allocation_headroom(
    host, monkeypatch, measurement, reason
):
    values = {"memory_total_bytes": 16 * GIB, "memory_available_bytes": 8 * GIB, "cpus": 8.0}
    values.update(measurement)
    monkeypatch.setattr(capacity, "_host_limits", lambda: values)
    result = host.assess()
    assert not result["allowed"] and reason in result["reasons"]
    with pytest.raises(ReleaseBlocked, match="insufficient_release_capacity"):
        host.admit()


def test_same_filesystem_reserves_unpack_and_spool_growth_together(host, monkeypatch):
    monkeypatch.setattr(capacity.shutil, "disk_usage", lambda path: SimpleNamespace(free=8 * GIB))
    assert host.assess()["reasons"] == ["shared_disk_headroom_insufficient"]


def test_distinct_filesystems_each_require_their_own_headroom(host, monkeypatch):
    original_stat = Path.stat

    def separate_spool(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == host.spool_dir:
            return SimpleNamespace(st_dev=result.st_dev + 1, st_mode=result.st_mode)
        return result

    monkeypatch.setattr(Path, "stat", separate_spool)
    monkeypatch.setattr(
        capacity.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=7 * GIB if path == host.docker_data_dir else GIB // 2),
    )
    result = host.assess()
    assert not result["measurements"]["shared_filesystem"]
    assert set(result["reasons"]) == {
        "docker_disk_headroom_insufficient",
        "spool_disk_headroom_insufficient",
    }


def test_missing_measurements_fail_closed_without_exposing_diagnostics(host, monkeypatch):
    def unavailable():
        raise OSError("private-diagnostic-token")

    monkeypatch.setattr(capacity, "_host_limits", unavailable)
    result = host.assess()
    assert result["reasons"] == ["host_capacity_unavailable"]
    assert "private-diagnostic" not in str(result)
    with pytest.raises(ReleaseBlocked, match="release_capacity_unavailable"):
        host.admit()


def test_missing_spool_directory_does_not_create_or_delete_anything(host):
    missing = host.spool_dir / "not-provisioned"
    result = replace(host, spool_dir=missing).assess()
    assert "filesystem_capacity_unavailable" in result["reasons"]
    assert not missing.exists()
    receipt = host.spool_dir / "unshipped.json"
    receipt.write_text("preserve original input")
    assert host.assess()["allowed"]
    assert receipt.read_text() == "preserve original input"


@pytest.mark.parametrize(
    "field,value",
    [
        ("candidate_cpus", 0),
        ("cpu_reserve", float("nan")),
        ("candidate_cpus", float("inf")),
        ("image_unpack_bytes", 0),
        ("candidate_memory_bytes", True),
    ],
)
def test_invalid_trusted_budget_is_rejected(field, value):
    with pytest.raises(ValueError):
        CapacityBudget(**{field: value})


@pytest.mark.parametrize("unbounded", ["memory", "cpu", "missing", "root", "timeout"])
def test_docker_inventory_requires_explicit_limits_and_matching_data_root(
    tmp_path, monkeypatch, unbounded
):
    directory = tmp_path / "docker"
    directory.mkdir()
    host = HostCapacity(directory, tmp_path)
    identifier = "a" * 64

    def command(self, arguments, deadline):
        if arguments[0] == "info":
            return str(tmp_path if unbounded == "root" else directory)
        if arguments[0] == "ps":
            return identifier
        if unbounded == "timeout":
            raise subprocess.TimeoutExpired("never expose command details", 10)
        if unbounded == "missing":
            return ""
        memory = 0 if unbounded == "memory" else GIB
        cpu = 0 if unbounded == "cpu" else 1_000_000_000
        return f'{identifier} {memory} {cpu} 0 0 ""'

    monkeypatch.setattr(HostCapacity, "_command", command)
    with pytest.raises((ValueError, subprocess.TimeoutExpired)):
        host._allocations()
    assert "docker_capacity_unavailable" in host.assess()["reasons"]


def test_docker_commands_are_bounded_local_read_only_and_hide_environment_selectors(
    tmp_path, monkeypatch
):
    host = HostCapacity(tmp_path, tmp_path)
    identifiers = ["a" * 64, "b" * 64]
    commands = []
    monkeypatch.setenv("DOCKER_HOST", "tcp://untrusted.example:2375")
    monkeypatch.setenv("DOCKER_CONTEXT", "other-host")

    def run(arguments, **kwargs):
        assert arguments[:2] == ["/usr/bin/docker", "--host=unix:///var/run/docker.sock"]
        assert 0 < kwargs["timeout"] <= 10
        assert not {"DOCKER_HOST", "DOCKER_CONTEXT"} & kwargs["env"].keys()
        operation = arguments[2]
        commands.append(operation)
        if operation == "info":
            output = str(tmp_path)
        elif operation == "ps":
            output = "\n".join(identifiers)
        else:
            assert operation == "inspect"
            output = f'{identifiers[0]} {GIB} 1000000000 0 0 ""\n'
            output += f'{identifiers[1]} {2 * GIB} 0 50000 100000 "0-3"'
        return SimpleNamespace(returncode=0, stdout=output)

    monkeypatch.setattr(capacity.subprocess, "run", run)
    assert host._allocations() == {
        "running_containers": 2,
        "allocated_memory_bytes": 3 * GIB,
        "allocated_cpus": 1.5,
    }
    assert commands == ["info", "ps", "inspect", "ps"]


def test_nested_cgroup_bounds_use_available_bytes_and_parent_cpu_quota(tmp_path, monkeypatch):
    meminfo, membership, root = tmp_path / "meminfo", tmp_path / "cgroup", tmp_path / "unified"
    leaf = root / "controller.slice"
    leaf.mkdir(parents=True)
    meminfo.write_text("MemTotal: 2048 kB\nMemAvailable: 1024 kB\n")
    membership.write_text("0::/controller.slice\n")
    (root / "cgroup.controllers").write_text("cpu memory cpuset")
    (root / "cpu.max").write_text("25000 100000")
    (root / "memory.max").write_text("max")
    (leaf / "cpu.max").write_text("50000 100000")
    (leaf / "memory.max").write_text("1000")
    (leaf / "memory.current").write_text("200")
    (leaf / "cpuset.cpus.effective").write_text("0-3")
    monkeypatch.setattr(capacity, "PROC_MEMINFO", meminfo)
    monkeypatch.setattr(capacity, "PROC_CGROUP", membership)
    monkeypatch.setattr(capacity, "CGROUP_ROOT", root)
    monkeypatch.setattr(capacity.os, "sched_getaffinity", lambda _: set(range(8)))
    assert capacity._host_limits() == {
        "memory_total_bytes": 1000,
        "memory_available_bytes": 800,
        "cpus": 0.25,
    }
    (leaf / "memory.current").write_text("1100")
    assert capacity._host_limits()["memory_available_bytes"] == 0


def test_real_host_memory_cpu_and_filesystem_probes_are_read_only(tmp_path, monkeypatch):
    measured = capacity._host_limits()
    assert measured["memory_total_bytes"] > 0 and measured["cpus"] > 0
    assert measured["memory_available_bytes"] >= 0
    tiny = CapacityBudget(
        candidate_memory_bytes=1,
        candidate_cpus=0.001,
        memory_reserve_bytes=1,
        cpu_reserve=0,
        image_unpack_bytes=1,
        docker_free_reserve_bytes=1,
        spool_growth_bytes=1,
        spool_free_reserve_bytes=1,
    )
    monkeypatch.setattr(
        HostCapacity,
        "_allocations",
        lambda self: {"running_containers": 0, "allocated_memory_bytes": 0, "allocated_cpus": 0},
    )
    result = HostCapacity(tmp_path, tmp_path, budget=tiny).assess()
    assert result["allowed"] and result["measurements"]["docker_free_bytes"] > 0
