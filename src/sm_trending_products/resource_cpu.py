"""Verified live analytical CPU changes; acquisition resources are immutable here."""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from .contracts import digest
from .release_capacity import _host_limits
from .release_control import ReleaseBlocked, ReleaseManifest
from .resource_cpu_state import (
    CPU_VARIABLES,
    CpuContainer,
    CpuPolicy,
    CpuTransition,
    Mode,
    load_cpu_policy,
    mode_quotas,
    write_cpu_policy,
)

if TYPE_CHECKING:
    from .catalog import Catalog
    from .release_host import ComposeReleaseHost

PROTECTED_SERVICES = {
    "worker",
    "worker-green",
    "capture-planner",
    "capture-planner-green",
    "normalizer",
    "postgres",
    "objectstore",
}
# Resource fields are projected explicitly: logging credentials and environment never enter output.
RESOURCE_FIELDS = (
    "Memory",
    "MemorySwap",
    "MemoryReservation",
    "MemorySwappiness",
    "OomKillDisable",
    "NanoCpus",
    "CpuQuota",
    "CpuPeriod",
    "CpuShares",
    "CpuRealtimePeriod",
    "CpuRealtimeRuntime",
    "CpusetCpus",
    "CpusetMems",
    "PidsLimit",
    "BlkioWeight",
)
INSPECT_FORMAT = (
    "\t".join(
        "{{json " + field + "}}"
        for field in (
            ".Id",
            '(index .Config.Labels "com.docker.compose.project")',
            '(index .Config.Labels "com.docker.compose.service")',
            '(index .Config.Labels "com.docker.compose.oneoff")',
            ".Config.Image",
            ".State.Running",
            ".State.Pid",
            ".State.StartedAt",
            ".RestartCount",
        )
    )
    + "\t{"
    + ",".join('"' + key + '":{{json .HostConfig.' + key + "}}" for key in RESOURCE_FIELDS)
    + "}"
)


class CpuModeController:
    def __init__(self, host: ComposeReleaseHost, catalog: Catalog, freshness_seconds: int = 300):
        self.host, self.catalog, self.freshness_seconds = host, catalog, freshness_seconds
        if not 1 <= freshness_seconds <= 3600:
            raise ValueError("Invalid CPU health freshness")

    def status(self) -> dict:
        policy = load_cpu_policy(self.host.lock_directory, self.host.project)
        return {
            "target": self.host.project,
            "mode": policy.mode if policy else "unconfigured",
            "status": "recovery_required" if policy and policy.pending else "stable",
            "analytical_cpus": {
                key: value / 10**9 for key, value in (policy.quotas.items() if policy else ())
            },
        }

    def _inventory(self) -> tuple[CpuContainer, ...]:
        identifiers = self.host._command(
            [
                "ps",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={self.host.project}",
            ]
        ).split()
        if not 4 <= len(identifiers) <= 32 or any(
            not re.fullmatch(r"[a-f0-9]{64}", value) for value in identifiers
        ):
            raise ReleaseBlocked("cpu_inventory_unavailable")
        try:
            body = self.host._command(["inspect", "--format", INSPECT_FORMAT, *identifiers])
            if len(body) > 128 * 1024:
                raise ValueError("CPU inventory too large")
            rows = body.splitlines()
            result = []
            for line in rows:
                (
                    identifier,
                    project,
                    service,
                    oneoff,
                    image,
                    running,
                    pid,
                    started,
                    restarts,
                    resources,
                ) = [json.loads(value) for value in line.split("\t")]
                if (
                    identifier not in identifiers
                    or project != self.host.project
                    or service not in PROTECTED_SERVICES | CPU_VARIABLES.keys()
                    or oneoff not in {"False", "false", None}
                    or running is not True
                    or not resources["Memory"] > 0
                    or not resources["NanoCpus"] > 0
                    or resources.get("CpuQuota", 0) != 0
                    or resources.get("CpuPeriod", 0) != 0
                    or resources.get("CpusetCpus", "")
                ):
                    raise ValueError("Unqualified CPU inventory")
                config = dict(resources)
                nanocpus = config.pop("NanoCpus")
                result.append(
                    CpuContainer(
                        identifier=identifier,
                        service=service,
                        pid=pid,
                        started_at=started,
                        restarts=restarts,
                        image=image,
                        resource_sha256=digest(config),
                        nanocpus=nanocpus,
                    )
                )
            services = [row.service for row in result]
            if (
                {row.identifier for row in result} != set(identifiers)
                or len(set(services)) != len(services)
                or not set(CPU_VARIABLES) <= set(services)
                or "postgres" not in services
                or not {"worker", "worker-green"} & set(services)
            ):
                raise ValueError("Incomplete CPU inventory")
            after = self.host._command(
                [
                    "ps",
                    "--quiet",
                    "--no-trunc",
                    "--filter",
                    f"label=com.docker.compose.project={self.host.project}",
                ]
            ).split()
            if sorted(after) != sorted(identifiers):
                raise ValueError("CPU inventory changed")
            return tuple(sorted(result, key=lambda row: row.service))
        except (ValueError, KeyError, TypeError):
            raise ReleaseBlocked("cpu_inventory_invalid") from None

    def _idle_release(self) -> None:
        if self.catalog.one(
            "SELECT 1 FROM deployment_runs WHERE target=%s "
            "AND status NOT IN ('completed','aborted') LIMIT 1",
            (self.host.project,),
        ):
            raise ReleaseBlocked("cpu_release_in_progress")
        path = self.host.lock_directory / "processing-refresh.json"
        # Reuse the existing trusted journal parser rather than accepting a divergent format.
        if path.exists() or path.is_symlink():
            from .release_control import ReleaseTrust
            from .release_processing import ProcessingRefresh

            refresh = ProcessingRefresh(
                self.catalog, self.host, ReleaseTrust("", ()), self.host.lock_directory
            )
            if refresh.pending():
                raise ReleaseBlocked("cpu_processing_in_progress")

    def _healthy(self) -> bool:
        row = self.catalog.one(
            "SELECT t.active_manifest,t.active_slot,t.active_worker_id,t.synthetic,"
            "w.state,w.heartbeat_at,clock_timestamp() AS checked_at "
            "FROM deployment_targets t JOIN collection_workers w "
            "ON w.worker_id=t.active_worker_id AND w.synthetic=t.synthetic WHERE t.target=%s",
            (self.host.project,),
        )
        if (
            not row
            or row["state"] != "running"
            or not 0
            <= (row["checked_at"] - row["heartbeat_at"]).total_seconds()
            <= self.freshness_seconds
        ):
            return False
        if not self.host.healthy(
            self.host.project,
            ReleaseManifest.model_validate(row["active_manifest"]),
            row["active_slot"],
            row["active_worker_id"],
        ):
            return False
        pending = self.catalog.one(
            "WITH pending AS MATERIALIZED (SELECT deadline_at FROM capture_obligations "
            "WHERE synthetic=%s AND capture_dataset_id IS NULL "
            "AND original_due_at<=statement_timestamp() "
            "ORDER BY original_due_at,obligation_id LIMIT 1001) "
            "SELECT count(*)<=1000 AND coalesce(bool_and(deadline_at IS NOT NULL "
            "AND deadline_at>=clock_timestamp()),true) AS healthy FROM pending",
            (row["synthetic"],),
        )
        return bool(pending and pending["healthy"])

    @staticmethod
    def _quotas(containers: tuple[CpuContainer, ...]) -> dict[str, int]:
        return {row.service: row.nanocpus for row in containers if row.service in CPU_VARIABLES}

    def _admit(self, current: tuple[CpuContainer, ...], quotas: dict[str, int]) -> None:
        increase = sum(
            max(0, quotas[row.service] - row.nanocpus) for row in current if row.service in quotas
        )
        if increase == 0:
            return  # Reducing verified live quotas needs no additional host capacity.
        if self.host.capacity is None:
            return  # Explicit controlled-test host, matching the existing release adapter.
        try:
            allocated = self.host.capacity._allocations()["allocated_cpus"]
            required = allocated + increase / 10**9 + self.host.capacity.budget.cpu_reserve
            limit = _host_limits()["cpus"]
            if (
                not all(math.isfinite(value) for value in (required, limit))
                or limit <= 0
                or required > limit
            ):
                raise ReleaseBlocked("cpu_headroom_insufficient")
        except (OSError, ValueError, KeyError, TypeError):
            raise ReleaseBlocked("cpu_capacity_unavailable") from None

    def _qualify_pins(self, quotas: dict[str, int]) -> None:
        # The adapter permits prospective pins only for this read-only config command.
        try:
            configured = json.loads(
                self.host._command(
                    [
                        *self.host._compose(),
                        "config",
                        "--format",
                        "json",
                    ],
                    cpu_overrides=quotas,
                )
            )["services"]
            values = {key: round(float(configured[key]["cpus"]) * 10**9) for key in quotas}
            if values != quotas:
                raise ValueError("Compose ignores CPU pins")
        except (KeyError, ValueError, TypeError, OverflowError):
            raise ReleaseBlocked("cpu_compose_pins_unsupported") from None

    def _verify(self, original: tuple[CpuContainer, ...], quotas: dict[str, int]) -> None:
        current = self._inventory()
        expected = tuple(
            row.model_copy(update={"nanocpus": quotas.get(row.service, row.nanocpus)})
            for row in original
        )
        if current != expected:
            raise ReleaseBlocked("cpu_runtime_changed")

    def _apply(self, original: tuple[CpuContainer, ...], quotas: dict[str, int]) -> None:
        expected = self._quotas(original)
        for row in original:
            if row.service not in quotas or quotas[row.service] == expected[row.service]:
                continue
            self._verify(original, expected)
            self.host._command(
                [
                    "update",
                    "--cpus",
                    format(quotas[row.service] / 10**9, ".9f"),
                    row.identifier,
                ],
                timeout=10,
            )
            expected[row.service] = quotas[row.service]
            self._verify(original, expected)
        self._verify(original, quotas)

    def _restore(self, policy: CpuPolicy) -> None:
        assert policy.pending is not None
        original = policy.pending.containers
        current = self._inventory()
        allowed = self._quotas(original)
        actual = self._quotas(current)
        if any(actual[key] not in {allowed[key], policy.pending.quotas[key]} for key in allowed):
            raise ReleaseBlocked("cpu_recovery_runtime_changed")
        self._verify(original, actual)
        self._admit(current, policy.quotas)
        self._apply(current, policy.quotas)
        write_cpu_policy(self.host.lock_directory, policy.model_copy(update={"pending": None}))

    def switch(self, mode: str, dry_run: bool = False) -> dict:
        if mode not in {"standard", "economy"}:
            raise ReleaseBlocked("cpu_mode_invalid")
        with self.host.exclusive(self.host.project):
            policy = load_cpu_policy(self.host.lock_directory, self.host.project)
            if policy and policy.pending:
                raise ReleaseBlocked("cpu_recovery_required")
            self._idle_release()
            containers = self._inventory()
            actual = self._quotas(containers)
            if policy is None:
                try:
                    configured = json.loads(
                        self.host._command(
                            [
                                *self.host._compose(),
                                "config",
                                "--format",
                                "json",
                            ]
                        )
                    )["services"]
                    baseline = {
                        key: round(float(configured[key]["cpus"]) * 10**9) for key in CPU_VARIABLES
                    }
                    if baseline != actual:
                        raise ValueError("Live CPU differs from configured baseline")
                except (KeyError, ValueError, TypeError):
                    raise ReleaseBlocked("cpu_baseline_mismatch") from None
                policy = CpuPolicy(
                    target=self.host.project,
                    mode="standard",
                    baseline=baseline,
                    quotas=baseline,
                    activated_at=datetime.now(UTC),
                )
            if actual != policy.quotas:
                raise ReleaseBlocked("cpu_runtime_drift")
            quotas = mode_quotas(policy.baseline, cast(Mode, mode))
            self._qualify_pins(quotas)
            self._admit(containers, quotas)
            if mode == "economy" and not self._healthy():
                raise ReleaseBlocked("cpu_capture_unhealthy")
            if dry_run:
                return {
                    "target": self.host.project,
                    "status": "preview",
                    "mode": mode,
                    "analytical_cpus": {key: value / 10**9 for key, value in quotas.items()},
                }
            if policy.mode == mode and policy.quotas == quotas:
                write_cpu_policy(self.host.lock_directory, policy)
                return self.status()
            transition = CpuTransition(mode=cast(Mode, mode), quotas=quotas, containers=containers)
            activated_at = datetime.now(UTC)
            pending = policy.model_copy(update={"pending": transition})
            write_cpu_policy(self.host.lock_directory, pending)
            try:
                self._apply(containers, quotas)
                if mode == "economy" and not self._healthy():
                    raise ReleaseBlocked("cpu_capture_unhealthy")
                committed = policy.model_copy(
                    update={
                        "mode": mode,
                        "quotas": quotas,
                        "pending": None,
                        "activated_at": activated_at,
                    }
                )
                write_cpu_policy(self.host.lock_directory, committed)
            except Exception:
                try:
                    self._restore(pending)
                except Exception:
                    raise ReleaseBlocked("cpu_recovery_required") from None
                raise ReleaseBlocked("cpu_update_rolled_back") from None
            return self.status()

    def reconcile(self) -> dict | None:
        policy = load_cpu_policy(self.host.lock_directory, self.host.project)
        if policy is None:
            return None
        with self.host.exclusive(self.host.project):
            policy = load_cpu_policy(self.host.lock_directory, self.host.project)
            assert policy is not None
            if policy.pending:
                self._restore(policy)
                return {**self.status(), "recovered": True}
            if policy.mode != "economy":
                return None
            try:
                self._idle_release()
            except ReleaseBlocked as error:
                if str(error) in {"cpu_release_in_progress", "cpu_processing_in_progress"}:
                    return None  # Deliberate release draining is not a new capture failure.
                raise
            if self._healthy():
                return None
        result = self.switch("standard")
        return {**result, "reason": "cpu_capture_guard_restored_standard"}
