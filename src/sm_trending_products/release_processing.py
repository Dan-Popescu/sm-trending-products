"""Resumable host-only refresh of replayable roles and overlapping planners."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from .catalog import Catalog
from .contracts import canonical
from .release_control import ReleaseBlocked, ReleaseManifest, ReleaseTrust, VerifiedRelease
from .release_host import ComposeReleaseHost

PROCESSING_SERVICES = ("normalizer", "dagster-daemon", "dagster-webserver", "dashboard")
PLANNERS = {"blue": "capture-planner", "green": "capture-planner-green"}
ROLE_VARIABLES = {
    "normalizer": "NORMALIZER_IMAGE",
    "dagster-daemon": "ANALYTICS_IMAGE",
    "dagster-webserver": "ANALYTICS_IMAGE",
    "dashboard": "DASHBOARD_IMAGE",
}


class ProcessingRefresh:
    def __init__(
        self,
        catalog: Catalog,
        host: ComposeReleaseHost,
        trust: ReleaseTrust,
        state_directory: Path,
        *,
        freshness_seconds: int = 300,
        allow_synthetic: bool = False,
    ):
        self.catalog, self.host, self.trust = catalog, host, trust
        self.directory = state_directory
        self.freshness_seconds, self.allow_synthetic = freshness_seconds, allow_synthetic
        if not state_directory.is_absolute() or not 1 <= freshness_seconds <= 3600:
            raise ValueError("Invalid processing controller configuration")
        info = state_directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ReleaseBlocked("host_state_permissions_invalid")

    @property
    def journal(self) -> Path:
        return self.directory / "processing-refresh.json"

    def _load(self) -> dict | None:
        try:
            descriptor = os.open(self.journal, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as handle:
            info = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise ReleaseBlocked("processing_journal_permissions_invalid")
            body = handle.read(4 * 1024 * 1024 + 1)
            if len(body) > 4 * 1024 * 1024:
                raise ReleaseBlocked("processing_journal_too_large")
        row = json.loads(body)
        if row.get("target") != self.host.project or row.get("schema_version") != 1:
            raise ReleaseBlocked("processing_journal_invalid")
        return row

    def pending(self) -> bool:
        row = self._load()
        return bool(row and row.get("status") != "completed")

    def _write(self, path: Path, body: str) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=self.directory, prefix=".processing-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
                os.replace(temporary, path)
                directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                temporary.unlink(missing_ok=True)

    def _persist(self, row: dict) -> None:
        # Intent precedes pins; both are synchronized before any host mutation.
        self._write(self.journal, canonical(row) + "\n")
        self._write(
            self.directory / "processing.env",
            "".join(f"{key}={value}\n" for key, value in sorted(row["pins"].items())),
        )

    def _acquisition(self, manifest: ReleaseManifest, expected: dict | None = None) -> dict:
        row = self.catalog.one(
            "SELECT t.active_manifest,t.active_proof,t.active_slot,t.active_worker_id,t.synthetic,"
            "w.generation,w.state,w.heartbeat_at,clock_timestamp() AS checked_at,"
            "(SELECT max(version) FROM schema_migrations) AS schema_version "
            "FROM deployment_targets t JOIN collection_workers w "
            "ON w.worker_id=t.active_worker_id AND w.synthetic=t.synthetic WHERE t.target=%s",
            (self.host.project,),
        )
        if (
            not row
            or row["state"] != "running"
            or (row["synthetic"] and not self.allow_synthetic)
            or not 0
            <= (row["checked_at"] - row["heartbeat_at"]).total_seconds()
            <= self.freshness_seconds
        ):
            raise ReleaseBlocked("processing_acquisition_unavailable")
        active = ReleaseManifest.model_validate(row["active_manifest"])
        self.trust.verify(active, row["active_proof"])
        if (
            manifest.capture_protocol != active.capture_protocol
            or not manifest.schema_min <= row["schema_version"] <= manifest.schema_max
            or not active.schema_min <= row["schema_version"] <= active.schema_max
        ):
            raise ReleaseBlocked("processing_release_incompatible")
        identity = {
            "worker_id": row["active_worker_id"],
            "generation": row["generation"],
            "slot": row["active_slot"],
            "image": active.image,
        }
        if expected is not None and identity != expected:
            raise ReleaseBlocked("processing_acquisition_changed")
        if not self.host.healthy(
            self.host.project, active, row["active_slot"], row["active_worker_id"]
        ):
            raise ReleaseBlocked("processing_acquisition_container_unavailable")
        return identity

    def _container(self, service: str) -> str | None:
        if service not in (*PROCESSING_SERVICES, *PLANNERS.values()):
            raise ReleaseBlocked("processing_service_not_allowed")
        containers = self.host._command(
            [*self.host._compose(), "ps", "--all", "--quiet", service]
        ).split()
        if len(containers) > 1 or any(
            not re.fullmatch(r"[a-f0-9]{12,64}", value) for value in containers
        ):
            raise ReleaseBlocked("processing_container_ambiguous")
        return containers[0] if containers else None

    def _running(self, service: str, image: str) -> str | None:
        container = self._container(service)
        if container and self.host._identity(container)[:2] == (image, True):
            return container
        return None

    def _start(self, service: str, image: str, manifest: ReleaseManifest, watch: dict) -> str:
        self._acquisition(manifest, watch)
        if not self._running(service, image):
            if self.host.capacity is not None:
                self.host.capacity.admit()
            self.host._command(
                [
                    *self.host._compose(),
                    "up",
                    "--detach",
                    "--no-deps",
                    "--no-build",
                    "--pull",
                    "never",
                    service,
                ]
            )
        container = self._running(service, image)
        if not container:
            raise ReleaseBlocked("processing_container_unavailable")
        self._acquisition(manifest, watch)
        return container

    def _prepare(self, manifest: ReleaseManifest, watch: dict) -> None:
        self._acquisition(manifest, watch)
        command = ["image", "inspect", "--format", "{{json .RepoDigests}}", manifest.image]
        try:
            images = json.loads(self.host._command(command))
        except ReleaseBlocked as error:
            if str(error) != "deployment_host_command_failed":
                raise
            images = None
        if not isinstance(images, list) or manifest.image not in images:
            if self.host.capacity is not None:
                self.host.capacity.admit()
            self.host._command(["pull", manifest.image], timeout=300)
            images = json.loads(self.host._command(command))
            if not isinstance(images, list) or manifest.image not in images:
                raise ReleaseBlocked("image_digest_mismatch")
        self._acquisition(manifest, watch)

    def _planner(self, row: dict, manifest: ReleaseManifest, watch: dict) -> None:
        slot = row["planner_candidate"]
        container = self._start(PLANNERS[slot], manifest.image, manifest, watch)
        # Use the configured scheduler budget. A zero-budget tick would erase due work.
        for _ in range(10):
            self._acquisition(manifest, watch)
            probe = json.loads(
                self.host._command(
                    ["exec", container, "python", "-m", "sm_trending_products.capture_coordinator"]
                )
            )
            if probe.get("status") in {"planned", "not_due"}:
                break
            if probe.get("status") != "coordinator_busy":
                raise ReleaseBlocked("processing_planner_probe_failed")
            time.sleep(0.1)
        else:
            raise ReleaseBlocked("processing_planner_busy")
        if not self._running(PLANNERS[slot], manifest.image):
            raise ReleaseBlocked("processing_planner_unavailable")
        self._acquisition(manifest, watch)
        row["active_planner_slot"] = slot
        self._persist(row)
        previous = row["planner_previous"]
        if previous != slot and (old := self._container(PLANNERS[previous])):
            self._acquisition(manifest, watch)
            self.host._command(["stop", "--time=-1", old])
            self._acquisition(manifest, watch)

    def apply(self, release: VerifiedRelease | None = None) -> dict:
        with self.host.exclusive(self.host.project):
            row = self._load()
            if release is None:
                if row is None:
                    raise ReleaseBlocked("processing_refresh_not_found")
                release = VerifiedRelease(
                    ReleaseManifest.model_validate(row["manifest"]), row["proof"]
                )
            self.trust.verify(release.manifest, release.proof)
            manifest = release.manifest
            watch = self._acquisition(manifest)
            if (
                row
                and row["manifest"] != manifest.model_dump(mode="json")
                and row["status"] != "completed"
            ):
                raise ReleaseBlocked("processing_refresh_pending")
            if not row or row["manifest"] != manifest.model_dump(mode="json"):
                previous = row["active_planner_slot"] if row else "blue"
                old = self._container(PLANNERS[previous])
                if not old:
                    raise ReleaseBlocked("processing_planner_bootstrap_required")
                image, running, _ = self.host._identity(old)
                if not running:
                    raise ReleaseBlocked("processing_planner_unavailable")
                ReleaseManifest.model_validate({**manifest.model_dump(mode="json"), "image": image})
                slot = "green" if previous == "blue" else "blue"
                pins = {name: manifest.image for name in set(ROLE_VARIABLES.values())}
                pins[f"ACQUISITION_PLANNER_{previous.upper()}_IMAGE"] = image
                pins[f"ACQUISITION_PLANNER_{slot.upper()}_IMAGE"] = manifest.image
                row = {
                    "schema_version": 1,
                    "target": self.host.project,
                    "manifest": manifest.model_dump(mode="json"),
                    "proof": release.proof,
                    "pins": pins,
                    "planner_previous": previous,
                    "planner_candidate": slot,
                    "active_planner_slot": previous,
                    "completed_roles": [],
                    "status": "running",
                }
            assert row is not None
            self.host = replace(self.host, role_images=row["pins"])
            row.update(status="running", reason=None, acquisition=watch)
            self._persist(row)
            try:
                # Pull only a verified immutable digest; preserve the prior capture plane.
                self._prepare(manifest, watch)
                self._planner(row, manifest, watch)
                for service in PROCESSING_SERVICES:
                    self._start(service, manifest.image, manifest, watch)
                    if service not in row["completed_roles"]:
                        row["completed_roles"].append(service)
                    self._persist(row)
                self._acquisition(manifest, watch)
                row["status"] = "completed"
                self._persist(row)
            except ReleaseBlocked as error:
                row.update(status="blocked", reason=str(error))
                self._persist(row)
            except Exception:
                row.update(status="blocked", reason="processing_refresh_failed")
                self._persist(row)
            return {
                key: row.get(key)
                for key in (
                    "target",
                    "status",
                    "reason",
                    "active_planner_slot",
                    "completed_roles",
                    "acquisition",
                )
            }
