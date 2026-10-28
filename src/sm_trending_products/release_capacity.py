"""Conservative host admission for one additional acquisition container.

Budgets and paths are trusted host configuration, never release-manifest input.
This is a measured precondition, not a reservation against unrelated host writers.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path

from .release_control import ReleaseBlocked

GIB = 1024**3
PROC_MEMINFO = Path("/proc/meminfo")
PROC_CGROUP = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")


@dataclass(frozen=True)
class CapacityBudget:
    candidate_memory_bytes: int = 2 * GIB
    candidate_cpus: float = 1
    memory_reserve_bytes: int = GIB
    cpu_reserve: float = 1
    image_unpack_bytes: int = 4 * GIB
    docker_free_reserve_bytes: int = 4 * GIB
    spool_growth_bytes: int = 256 * 1024**2
    spool_free_reserve_bytes: int = 768 * 1024**2

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in {"candidate_cpus", "cpu_reserve"}:
                if (
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(value)
                    or value < 0
                ):
                    raise ValueError("Capacity CPU budgets must be finite and nonnegative")
            elif isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError("Capacity byte budgets must be positive integers")
        if self.candidate_cpus <= 0:
            raise ValueError("Candidate CPU budget must be positive")


def _cpu_count(value: str) -> int:
    count = 0
    for item in value.strip().split(","):
        if not re.fullmatch(r"\d+(?:-\d+)?", item):
            raise ValueError("Invalid CPU set")
        bounds = [int(part) for part in item.split("-")]
        first, last = bounds[0], bounds[-1]
        if last < first:
            raise ValueError("Invalid CPU set")
        count += last - first + 1
    return count


def _host_limits() -> dict:
    memory = {}
    for line in PROC_MEMINFO.read_text().splitlines():
        name, value = line.split(":", 1)
        if name in {"MemTotal", "MemAvailable"}:
            amount, unit = value.split()
            if unit != "kB":
                raise ValueError("Unknown memory unit")
            memory[name] = int(amount) * 1024
    total, available = memory["MemTotal"], memory["MemAvailable"]
    cpus = float(len(os.sched_getaffinity(0)))
    membership = [
        line[3:] for line in PROC_CGROUP.read_text().splitlines() if line.startswith("0::")
    ]
    if len(membership) != 1 or not (CGROUP_ROOT / "cgroup.controllers").is_file():
        raise ValueError("Unified cgroup measurements unavailable")
    relative = Path(membership[0]).relative_to("/")
    if ".." in relative.parts:
        raise ValueError("Invalid cgroup membership")
    leaf = CGROUP_ROOT / relative
    if not leaf.is_dir():
        raise ValueError("Current cgroup unavailable")
    for node in (leaf, *leaf.parents):
        if not node.is_relative_to(CGROUP_ROOT):
            break
        maximum = node / "memory.max"
        quota = node / "cpu.max"
        # The v2 hierarchy root may omit limit files because it has no parent.
        if node != CGROUP_ROOT and (not maximum.is_file() or not quota.is_file()):
            raise ValueError("Cgroup limits unavailable")
        if maximum.is_file():
            bound = maximum.read_text().strip()
            if bound != "max":
                limit = int(bound)
                used = int((node / "memory.current").read_text())
                total, available = min(total, limit), min(available, max(0, limit - used))
        if quota.is_file():
            bound, period = quota.read_text().split()
            if bound != "max":
                cpus = min(cpus, int(bound) / int(period))
        cpuset = node / "cpuset.cpus.effective"
        if cpuset.is_file() and (value := cpuset.read_text().strip()):
            cpus = min(cpus, _cpu_count(value))
    if total <= 0 or available < 0 or available > total or not math.isfinite(cpus) or cpus <= 0:
        raise ValueError("Invalid host measurements")
    return {"memory_total_bytes": total, "memory_available_bytes": available, "cpus": cpus}


@dataclass(frozen=True)
class HostCapacity:
    docker_data_dir: Path
    spool_dir: Path
    budget: CapacityBudget = CapacityBudget()
    docker: str = "/usr/bin/docker"

    def __post_init__(self):
        if not all(path.is_absolute() for path in (self.docker_data_dir, self.spool_dir)):
            raise ValueError("Capacity paths must be absolute host paths")
        if not Path(self.docker).is_absolute():
            raise ValueError("Docker executable must be an absolute host path")

    def _command(self, arguments: list[str], deadline: float) -> str:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Capacity measurement deadline exceeded")
        environment = dict(os.environ)
        for key in (
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "DOCKER_TLS",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
        ):
            environment.pop(key, None)
        result = subprocess.run(
            [self.docker, "--host=unix:///var/run/docker.sock", *arguments],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=remaining,
        )
        if result.returncode or len(result.stdout) > 256 * 1024:
            raise ValueError("Docker capacity measurement unavailable")
        return result.stdout

    def _allocations(self) -> dict:
        deadline = time.monotonic() + 10
        root = self._command(["info", "--format", "{{.DockerRootDir}}"], deadline).strip()
        if Path(root).resolve(strict=True) != self.docker_data_dir.resolve(strict=True):
            raise ValueError("Docker data directory mismatch")
        containers = self._command(["ps", "--quiet", "--no-trunc"], deadline).split()
        if len(containers) > 256 or any(
            not re.fullmatch(r"[0-9a-f]{64}", value) for value in containers
        ):
            raise ValueError("Unsupported Docker inventory")
        memory, cpus = 0, 0.0
        if containers:
            template = (
                "{{.Id}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}} "
                "{{.HostConfig.CpuQuota}} {{.HostConfig.CpuPeriod}} "
                "{{json .HostConfig.CpusetCpus}}"
            )
            rows = self._command(
                ["inspect", "--format", template, *containers], deadline
            ).splitlines()
            seen = set()
            for row in rows:
                identifier, mem, nano, quota, period, cpuset = row.split(maxsplit=5)
                if identifier not in containers or identifier in seen or int(mem) <= 0:
                    raise ValueError("Unbounded or inconsistent Docker allocation")
                seen.add(identifier)
                limits = []
                if int(nano) > 0:
                    limits.append(int(nano) / 1_000_000_000)
                if int(quota) > 0 and int(period) > 0:
                    limits.append(int(quota) / int(period))
                value = json.loads(cpuset)
                if not isinstance(value, str):
                    raise ValueError("Invalid Docker CPU set")
                if value:
                    limits.append(float(_cpu_count(value)))
                if not limits:
                    raise ValueError("Unbounded Docker CPU allocation")
                memory += int(mem)
                cpus += min(limits)
            if seen != set(containers):
                raise ValueError("Docker inventory changed during measurement")
        after = self._command(["ps", "--quiet", "--no-trunc"], deadline).split()
        if sorted(after) != sorted(containers):
            raise ValueError("Docker inventory changed during measurement")
        return {
            "running_containers": len(containers),
            "allocated_memory_bytes": memory,
            "allocated_cpus": cpus,
        }

    def assess(self) -> dict:
        measurements, reasons = {}, []
        budget = self.budget
        try:
            measurements.update(_host_limits())
        except (OSError, ValueError, KeyError, ZeroDivisionError):
            reasons.append("host_capacity_unavailable")
        try:
            measurements.update(self._allocations())
        except (OSError, ValueError, TypeError, subprocess.TimeoutExpired, TimeoutError):
            reasons.append("docker_capacity_unavailable")
        try:
            for name, path in (("docker", self.docker_data_dir), ("spool", self.spool_dir)):
                if not path.is_dir():
                    raise ValueError("Capacity filesystem unavailable")
                measurements[f"{name}_free_bytes"] = shutil.disk_usage(path).free
            shared = self.docker_data_dir.stat().st_dev == self.spool_dir.stat().st_dev
            measurements["shared_filesystem"] = shared
            docker_need = budget.image_unpack_bytes + budget.docker_free_reserve_bytes
            spool_need = budget.spool_growth_bytes + budget.spool_free_reserve_bytes
            if shared:
                if (
                    min(measurements["docker_free_bytes"], measurements["spool_free_bytes"])
                    < docker_need + spool_need
                ):
                    reasons.append("shared_disk_headroom_insufficient")
            else:
                if measurements["docker_free_bytes"] < docker_need:
                    reasons.append("docker_disk_headroom_insufficient")
                if measurements["spool_free_bytes"] < spool_need:
                    reasons.append("spool_disk_headroom_insufficient")
        except (OSError, ValueError):
            reasons.append("filesystem_capacity_unavailable")
        if "host_capacity_unavailable" not in reasons:
            if (
                measurements["memory_available_bytes"]
                < budget.candidate_memory_bytes + budget.memory_reserve_bytes
            ):
                reasons.append("available_memory_insufficient")
            if "docker_capacity_unavailable" not in reasons:
                if (
                    measurements["memory_total_bytes"]
                    < measurements["allocated_memory_bytes"]
                    + budget.candidate_memory_bytes
                    + budget.memory_reserve_bytes
                ):
                    reasons.append("allocated_memory_headroom_insufficient")
                if (
                    measurements["cpus"]
                    < measurements["allocated_cpus"] + budget.candidate_cpus + budget.cpu_reserve
                ):
                    reasons.append("cpu_headroom_insufficient")
        return {
            "allowed": not reasons,
            "reasons": reasons,
            "measurements": measurements,
            "requirements": asdict(budget),
        }

    def admit_cpu_increase(self, increase_nanocpus: int) -> None:
        """Admit only additional live CPU, including all existing host allocations."""
        if (
            isinstance(increase_nanocpus, bool)
            or not isinstance(increase_nanocpus, int)
            or increase_nanocpus < 0
        ):
            raise ValueError("CPU increase must be a nonnegative integer")
        if increase_nanocpus == 0:
            return
        try:
            allocated = self._allocations()["allocated_cpus"]
            required = allocated + increase_nanocpus / 10**9 + self.budget.cpu_reserve
            limit = _host_limits()["cpus"]
            if (
                not all(math.isfinite(value) for value in (required, limit))
                or limit <= 0
                or required > limit
            ):
                raise ReleaseBlocked("cpu_headroom_insufficient")
        except (OSError, ValueError, KeyError, TypeError):
            raise ReleaseBlocked("cpu_capacity_unavailable") from None

    def admit(self) -> dict:
        assessment = self.assess()
        if not assessment["allowed"]:
            unavailable = any(reason.endswith("unavailable") for reason in assessment["reasons"])
            raise ReleaseBlocked(
                "release_capacity_unavailable" if unavailable else "insufficient_release_capacity"
            )
        return assessment
