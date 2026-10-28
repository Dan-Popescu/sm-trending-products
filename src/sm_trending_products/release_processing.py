"""Resumable host-only refresh of replayable roles and overlapping planners."""

from __future__ import annotations

import json
import os
import stat
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from .catalog import Catalog
from .contracts import canonical, utc
from .release_control import ReleaseBlocked, ReleaseManifest, ReleaseTrust, VerifiedRelease
from .release_host import (
    PLANNERS,
    PROCESSING_SERVICES,
    ROLE_VARIABLES,
    ComposeReleaseHost,
    RoleContainer,
)


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
        self._active_release = VerifiedRelease(active, row["active_proof"])
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

    def _start(
        self, service: str, image: str, manifest: ReleaseManifest, watch: dict
    ) -> RoleContainer:
        self._acquisition(manifest, watch)
        container = self.host.start_role(service, image)
        self._acquisition(manifest, watch)
        return container

    def _prepare(self, manifest: ReleaseManifest, watch: dict) -> None:
        self._acquisition(manifest, watch)
        self.host.ensure_image(manifest.image)
        self._acquisition(manifest, watch)

    def _planner_current(self, service: str, image: str, identity: dict) -> bool:
        container = self.host.running_role(service, image)
        return bool(container and self.host.planner_identity(container) == identity)

    def _restore_planner(self, row: dict, manifest: ReleaseManifest, watch: dict) -> None:
        prior = ReleaseManifest.model_validate(row["planner_previous_manifest"])
        self.trust.verify(prior, row["planner_previous_proof"])
        self._acquisition(prior, watch)
        slot = row["planner_previous"]
        self._start(PLANNERS[slot], prior.image, manifest, watch)
        row["active_planner_slot"] = slot
        row.pop("planner_qualification", None)
        self._persist(row)

    def _ensure_planner(self, row: dict, manifest: ReleaseManifest, watch: dict) -> None:
        identity = row.get("planner_qualification", {}).get("identity", {})
        if not self._planner_current(PLANNERS[row["planner_candidate"]], manifest.image, identity):
            self._restore_planner(row, manifest, watch)
            raise ReleaseBlocked("processing_planner_qualification_lost")

    def _previous_release(self, row: dict | None, image: str, watch: dict) -> VerifiedRelease:
        if row and row["manifest"]["image"] == image:
            previous = VerifiedRelease(
                ReleaseManifest.model_validate(row["manifest"]), row["proof"]
            )
        elif self._active_release.manifest.image == image:
            previous = self._active_release
        elif self._previous_supplied is not None:
            previous = self._previous_supplied
        else:
            raise ReleaseBlocked("processing_planner_bootstrap_required")
        if previous.manifest.image != image:
            raise ReleaseBlocked("processing_planner_bootstrap_required")
        self.trust.verify(previous.manifest, previous.proof)
        self._acquisition(previous.manifest, watch)
        return previous

    def _planner(self, row: dict, manifest: ReleaseManifest, watch: dict) -> None:
        try:
            self._qualify_planner(row, manifest, watch)
        except Exception:
            self._restore_planner(row, manifest, watch)
            raise

    def _qualify_planner(self, row: dict, manifest: ReleaseManifest, watch: dict) -> None:
        slot = row["planner_candidate"]
        container = self._start(PLANNERS[slot], manifest.image, manifest, watch)
        identity = self.host.planner_identity(container)
        previous = self.host.running_role(
            PLANNERS[row["planner_previous"]], row["planner_previous_manifest"]["image"]
        )
        if (
            previous
            and previous != container
            and self.host.planner_identity(previous)["hostname"] == identity["hostname"]
        ):
            raise ReleaseBlocked("processing_planner_identity_not_unique")
        qualified = row.get("planner_qualification", {})
        if qualified.get("identity") != identity:
            qualified = {}
        # Use the configured scheduler budget. A zero-budget tick would erase due work.
        for _ in range(10 if not qualified else 0):
            self._acquisition(manifest, watch)
            probe = self.host.planner_probe(container)
            completed = probe.get("last_completed_at")
            if (
                probe.get("status") in {"planned", "not_due"}
                and probe.get("last_planner_id") == identity["hostname"]
                and completed is not None
                and utc(completed) >= utc(identity["started_at"])
            ):
                qualified = {"identity": identity, "completed_at": completed}
                break
            if probe.get("status") not in {"coordinator_busy", "not_due"}:
                raise ReleaseBlocked("processing_planner_probe_failed")
            if probe.get("status") == "not_due":
                break
            time.sleep(0.1)
        if not qualified:
            raise ReleaseBlocked("processing_planner_unqualified")
        if not self._planner_current(PLANNERS[slot], manifest.image, identity):
            raise ReleaseBlocked("processing_planner_unavailable")
        self._acquisition(manifest, watch)
        row["planner_qualification"] = qualified
        row["active_planner_slot"] = slot
        self._persist(row)
        retire = "green" if slot == "blue" else "blue"
        if old := self.host.role_container(PLANNERS[retire]):
            self._acquisition(manifest, watch)
            self.host.stop_planner(old)
            self._acquisition(manifest, watch)
        self._ensure_planner(row, manifest, watch)

    def apply(
        self,
        release: VerifiedRelease | None = None,
        *,
        previous: VerifiedRelease | None = None,
        supersede: bool = False,
    ) -> dict:
        self._previous_supplied = previous
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
                and not supersede
            ):
                raise ReleaseBlocked("processing_refresh_pending")
            if not row or row["manifest"] != manifest.model_dump(mode="json"):
                previous_slot = row["active_planner_slot"] if row else "blue"
                image = self.host.planner_image(PLANNERS[previous_slot])
                previous_release = self._previous_release(row, image, watch)
                slot = (
                    previous_slot
                    if image == manifest.image
                    else ("green" if previous_slot == "blue" else "blue")
                )
                pins = {name: manifest.image for name in set(ROLE_VARIABLES.values())}
                if row:
                    pins.update(
                        {
                            key: value
                            for key, value in row["pins"].items()
                            if key.startswith("ACQUISITION_PLANNER_")
                        }
                    )
                pins[f"ACQUISITION_PLANNER_{previous_slot.upper()}_IMAGE"] = image
                pins[f"ACQUISITION_PLANNER_{slot.upper()}_IMAGE"] = manifest.image
                row = {
                    "schema_version": 1,
                    "target": self.host.project,
                    "manifest": manifest.model_dump(mode="json"),
                    "proof": release.proof,
                    "pins": pins,
                    "planner_previous": previous_slot,
                    "planner_previous_manifest": previous_release.manifest.model_dump(mode="json"),
                    "planner_previous_proof": previous_release.proof,
                    "planner_candidate": slot,
                    "active_planner_slot": previous_slot,
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
                    self._ensure_planner(row, manifest, watch)
                    self._start(service, manifest.image, manifest, watch)
                    self._ensure_planner(row, manifest, watch)
                    if service not in row["completed_roles"]:
                        row["completed_roles"].append(service)
                    self._persist(row)
                if any(
                    not self.host.running_role(service, manifest.image)
                    for service in PROCESSING_SERVICES
                ):
                    raise ReleaseBlocked("processing_container_unavailable")
                self._ensure_planner(row, manifest, watch)
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
