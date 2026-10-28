"""Private, durable CPU policy; only three analytical environment pins are exposed."""

from __future__ import annotations

import os
import stat
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator

from .contracts import Contract, canonical, utc
from .release_control import ReleaseBlocked

CPU_VARIABLES = {
    "dagster-daemon": "DAGSTER_DAEMON_CPUS",
    "dagster-webserver": "DAGSTER_WEBSERVER_CPUS",
    "dashboard": "DASHBOARD_CPUS",
}
Mode = Literal["standard", "economy"]
Quota = Annotated[int, Field(strict=True, ge=1, le=1024 * 10**9)]


def validate_quotas(quotas: dict[str, Quota]) -> None:
    if set(quotas) != set(CPU_VARIABLES) or any(
        isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1024 * 10**9
        for value in quotas.values()
    ):
        raise ValueError("Invalid analytical CPU quotas")


def mode_quotas(baseline: dict[str, int], mode: Mode) -> dict[str, int]:
    floors = {
        "dagster-daemon": 250_000_000,
        "dagster-webserver": 100_000_000,
        "dashboard": 100_000_000,
    }
    return {
        key: value if mode == "standard" else min(value, max(floors[key], value // 2))
        for key, value in baseline.items()
    }


class CpuContainer(Contract):
    identifier: str = Field(pattern=r"^[a-f0-9]{64}$")
    service: str
    pid: int = Field(gt=0, strict=True)
    started_at: str
    restarts: int = Field(ge=0, strict=True)
    image: str
    resource_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    nanocpus: int = Field(gt=0, strict=True)


class CpuTransition(Contract):
    mode: Mode
    quotas: dict[str, Quota]
    containers: tuple[CpuContainer, ...] = Field(min_length=4, max_length=32)

    @model_validator(mode="after")
    def valid(self):
        validate_quotas(self.quotas)
        identifiers = [row.identifier for row in self.containers]
        services = [row.service for row in self.containers]
        if len(set(identifiers)) != len(identifiers) or len(set(services)) != len(services):
            raise ValueError("Ambiguous transition inventory")
        if not set(CPU_VARIABLES) <= set(services):
            raise ValueError("Incomplete transition inventory")
        return self


class CpuPolicy(Contract):
    schema_version: Literal[1] = 1
    target: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    mode: Mode
    baseline: dict[str, Quota]
    quotas: dict[str, Quota]
    activated_at: datetime
    pending: CpuTransition | None = None

    @model_validator(mode="after")
    def valid(self):
        validate_quotas(self.baseline)
        validate_quotas(self.quotas)
        utc(self.activated_at)
        if any(self.quotas[key] > value for key, value in self.baseline.items()):
            raise ValueError("CPU policy exceeds baseline")
        if self.quotas != mode_quotas(self.baseline, self.mode):
            raise ValueError("CPU policy must match its mode")
        if self.pending and any(
            self.pending.quotas[key] > value for key, value in self.baseline.items()
        ):
            raise ValueError("Pending CPU policy exceeds baseline")
        if self.pending:
            if self.pending.quotas != mode_quotas(self.baseline, self.pending.mode):
                raise ValueError("Pending CPU quotas must match mode")
            actual = {
                row.service: row.nanocpus
                for row in self.pending.containers
                if row.service in CPU_VARIABLES
            }
            if actual != self.quotas:
                raise ValueError("Pending CPU intent differs from stable quotas")
        return self


def load_cpu_policy(directory: Path, target: str) -> CpuPolicy | None:
    try:
        descriptor = os.open(directory / "cpu-mode.json", os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError:
        raise ReleaseBlocked("cpu_policy_unavailable") from None
    try:
        with os.fdopen(descriptor, "rb") as handle:
            info = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise ReleaseBlocked("cpu_policy_permissions_invalid")
            body = handle.read(128 * 1024 + 1)
            if len(body) > 128 * 1024:
                raise ReleaseBlocked("cpu_policy_too_large")
        policy = CpuPolicy.model_validate_json(body)
        if policy.target != target:
            raise ValueError("Target mismatch")
        return policy
    except (OSError, ValueError):
        raise ReleaseBlocked("cpu_policy_invalid") from None


def write_cpu_policy(directory: Path, policy: CpuPolicy) -> None:
    policy = CpuPolicy.model_validate(policy.model_dump())
    with tempfile.NamedTemporaryFile(
        mode="w", dir=directory, prefix=".cpu-mode-", delete=False
    ) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(canonical(policy.model_dump(mode="json")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary, directory / "cpu-mode.json")
            descriptor = os.open(directory, os.O_DIRECTORY | os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)


def cpu_environment(directory: Path, target: str) -> dict[str, str]:
    policy = load_cpu_policy(directory, target)
    return (
        {
            variable: format(policy.quotas[service] / 10**9, ".9f")
            for service, variable in CPU_VARIABLES.items()
        }
        if policy
        else {}
    )


def cpu_transition_pending(directory: Path, target: str) -> bool:
    policy = load_cpu_policy(directory, target)
    return bool(policy and policy.pending)
