"""Verified live analytical CPU changes; acquisition resources are immutable here."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from .release_control import ReleaseBlocked, ReleaseManifest
from .resource_cpu_state import (
    CPU_VARIABLES,
    INDEPENDENT_SERVICES,
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
        if increase and self.host.capacity is not None:
            self.host.capacity.admit_cpu_increase(increase)

    def _verify(self, original: tuple[CpuContainer, ...], quotas: dict[str, int]) -> None:
        current = self.host.cpu_inventory()
        expected = tuple(
            row.model_copy(update={"nanocpus": quotas.get(row.service, row.nanocpus)})
            for row in original
        )
        if current != expected:
            raise ReleaseBlocked("cpu_runtime_changed")

    @staticmethod
    def _independent(containers: tuple[CpuContainer, ...]) -> bool:
        return all(
            row.independent_acquisition for row in containers if row.service in INDEPENDENT_SERVICES
        )

    def _apply(self, original: tuple[CpuContainer, ...], quotas: dict[str, int]) -> None:
        if not self._independent(original) and any(
            quotas.get(row.service, row.nanocpus) < row.nanocpus
            for row in original
            if row.service in INDEPENDENT_SERVICES
        ):
            raise ReleaseBlocked("cpu_legacy_collection_active")
        expected = self._quotas(original)
        for row in original:
            if row.service not in quotas or quotas[row.service] == expected[row.service]:
                continue
            self._verify(original, expected)
            self.host.update_cpu_quota(row, quotas[row.service])
            expected[row.service] = quotas[row.service]
            self._verify(original, expected)
        self._verify(original, quotas)

    def _recover(self, policy: CpuPolicy) -> None:
        assert policy.pending is not None
        original = policy.pending.containers
        current = self.host.cpu_inventory()
        allowed = self._quotas(original)
        actual = self._quotas(current)
        if any(actual[key] not in {allowed[key], policy.pending.quotas[key]} for key in allowed):
            raise ReleaseBlocked("cpu_recovery_runtime_changed")
        self._verify(original, actual)
        # A later legacy-role recreation can require restoration of larger standard quotas.
        # If that authorized increase was interrupted, rolling back would throttle collection.
        # Finish only its remaining increases instead; insufficient headroom retains the intent.
        if (
            policy.pending.mode == "standard"
            and not self._independent(current)
            and any(
                policy.quotas[row.service] < row.nanocpus
                for row in current
                if row.service in INDEPENDENT_SERVICES
            )
        ):
            policy = policy.model_copy(
                update={
                    "mode": "standard",
                    "quotas": policy.pending.quotas,
                    "activated_at": datetime.now(UTC),
                }
            )
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
            containers = self.host.cpu_inventory()
            actual = self._quotas(containers)
            if policy is None:
                try:
                    baseline = self.host.configured_cpu_quotas()
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
            if mode == "economy" and not self._independent(containers):
                raise ReleaseBlocked("cpu_legacy_collection_active")
            self.host.qualify_cpu_pins(quotas, require_independent=mode == "economy")
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
                    self._recover(pending)
                except Exception:
                    raise ReleaseBlocked("cpu_recovery_required") from None
                recovered = self.status()
                if recovered["mode"] == mode:
                    return {**recovered, "recovered": True}
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
                self._recover(policy)
                return {**self.status(), "recovered": True}
            if policy.mode != "economy":
                return None
            try:
                self._idle_release()
            except ReleaseBlocked as error:
                if str(error) in {"cpu_release_in_progress", "cpu_processing_in_progress"}:
                    return None  # Deliberate release draining is not a new capture failure.
                raise
            try:
                containers = self.host.cpu_inventory()
            except ReleaseBlocked as error:
                if str(error) == "cpu_inventory_incomplete":
                    return None  # Let ordinary role repair restore missing/stopped services.
                raise
            if self._independent(containers):
                try:
                    self.host.qualify_cpu_pins(policy.quotas, require_independent=True)
                except ReleaseBlocked as error:
                    if str(error) != "cpu_compose_pins_unsupported":
                        raise
                else:
                    if self._healthy():
                        return None
        result = self.switch("standard")
        return {**result, "reason": "cpu_capture_guard_restored_standard"}
