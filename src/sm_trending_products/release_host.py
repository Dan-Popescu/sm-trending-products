"""Host-only, allowlisted Compose operations for two preconfigured collector slots."""

from __future__ import annotations

import fcntl
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .release_control import IMAGE_DIGEST_PATTERN, ReleaseBlocked, ReleaseManifest

if TYPE_CHECKING:
    from .release_capacity import HostCapacity

SLOT_SERVICES = {"blue": "worker", "green": "worker-green"}
ROLE_IMAGE_VARIABLES = frozenset(
    {
        "NORMALIZER_IMAGE",
        "ANALYTICS_IMAGE",
        "DASHBOARD_IMAGE",
        "ACQUISITION_PLANNER_BLUE_IMAGE",
        "ACQUISITION_PLANNER_GREEN_IMAGE",
    }
)


@dataclass(frozen=True)
class ComposeReleaseHost:
    """Configuration belongs to the host, never to an untrusted release manifest.

    Only capture slots are mutable. PostgreSQL, network, volumes, planner and
    analytical services remain untouched. No shell, compose-down, volume removal
    or arbitrary migration command is exposed through this interface.
    """

    project: str
    compose_file: Path
    env_file: Path
    lock_directory: Path
    slot_images: Mapping[str, str]
    slot_workers: Mapping[str, str]
    docker: str = "/usr/bin/docker"
    slot_state_file: Path | None = None
    capacity: HostCapacity | None = None
    role_images: Mapping[str, str] | None = None

    def __post_init__(self):
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", self.project):
            raise ValueError("Invalid Compose project")
        if any(
            not path.is_absolute()
            for path in (self.compose_file, self.env_file, self.lock_directory)
        ):
            raise ValueError("Host configuration paths must be absolute")
        if set(self.slot_images) != set(SLOT_SERVICES) or set(self.slot_workers) != set(
            SLOT_SERVICES
        ):
            raise ValueError("Both capture slots must be explicitly configured")
        for image in self.slot_images.values():
            if not re.fullmatch(IMAGE_DIGEST_PATTERN, image):
                raise ValueError("Capture slots require immutable image digests")
        if any(
            not re.fullmatch(r"[A-Za-z0-9._:-]{1,160}", value)
            for value in self.slot_workers.values()
        ):
            raise ValueError("Invalid capture worker identity")
        if self.slot_state_file is not None and not self.slot_state_file.is_absolute():
            raise ValueError("Slot pin path must be absolute")
        if self.role_images is not None:
            if not set(self.role_images) <= ROLE_IMAGE_VARIABLES:
                raise ValueError("Unexpected processing image override")
            if any(
                not re.fullmatch(IMAGE_DIGEST_PATTERN, image) for image in self.role_images.values()
            ):
                raise ValueError("Processing roles require immutable image digests")

    def persist_slots(self) -> None:
        """Publish only immutable image/worker pins, never Compose credentials."""
        if self.slot_state_file is None:
            return
        path = self.slot_state_file
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, prefix=".slots-", delete=False
        ) as handle:
            temporary = Path(handle.name)
            try:
                for slot in SLOT_SERVICES:
                    handle.write(f"CAPTURE_{slot.upper()}_IMAGE={self.slot_images[slot]}\n")
                    handle.write(f"CAPTURE_{slot.upper()}_WORKER_ID={self.slot_workers[slot]}\n")
                handle.flush()
                os.fsync(handle.fileno())
                os.replace(temporary, path)
                descriptor = os.open(path.parent, os.O_DIRECTORY | os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            finally:
                temporary.unlink(missing_ok=True)

    @contextmanager
    def exclusive(self, target: str) -> Iterator[None]:
        self._target(target)
        self.lock_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = self.lock_directory / f"{self.project}.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ReleaseBlocked("deployment_host_busy") from None
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _target(self, target: str) -> None:
        if target != self.project:
            raise ReleaseBlocked("deployment_target_mismatch")

    def _command(self, arguments: list[str], *, timeout: int = 45) -> str:
        environment = dict(os.environ)
        # The controller is local. Inherited selectors must not redirect writes
        # to another Docker daemon or a remote endpoint.
        for key in (
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "DOCKER_TLS",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
        ):
            environment.pop(key, None)
        for slot in SLOT_SERVICES:
            environment[f"CAPTURE_{slot.upper()}_IMAGE"] = self.slot_images[slot]
            environment[f"CAPTURE_{slot.upper()}_WORKER_ID"] = self.slot_workers[slot]
        for variable in ROLE_IMAGE_VARIABLES:
            environment.pop(variable, None)
        environment.update(self.role_images or {})
        try:
            result = subprocess.run(
                [self.docker, "--host=unix:///var/run/docker.sock", *arguments],
                env=environment,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise ReleaseBlocked("deployment_host_command_unavailable") from None
        if result.returncode:
            # Compose interpolation and remote errors can include secrets. Never
            # persist or return raw stdout/stderr, even on a failed operation.
            raise ReleaseBlocked("deployment_host_command_failed")
        return result.stdout

    def _compose(self) -> list[str]:
        return [
            "compose",
            "--project-name",
            self.project,
            "--file",
            str(self.compose_file),
            "--env-file",
            str(self.env_file),
            "--profile",
            "rollout",
        ]

    def _container(self, slot: str) -> str | None:
        service = SLOT_SERVICES[slot]
        values = self._command([*self._compose(), "ps", "--all", "--quiet", service]).split()
        if len(values) > 1 or any(not re.fullmatch(r"[a-f0-9]{12,64}", value) for value in values):
            raise ReleaseBlocked("unexpected_capture_slot_containers")
        return values[0] if values else None

    def _identity(self, container: str) -> tuple[str, bool, str | None]:
        template = (
            "{{json .Config.Image}}\n{{json .State.Running}}\n"
            '{{json (index .Config.Labels "io.sm-trends.worker-id")}}'
        )
        try:
            values = self._command(["inspect", "--format", template, container]).splitlines()
            image, running, worker = [json.loads(value) for value in values]
            if (
                not isinstance(image, str)
                or not isinstance(running, bool)
                or not isinstance(worker, (str, type(None)))
            ):
                raise ValueError("Invalid Docker identity")
            return image, running, worker
        except (ValueError, TypeError):
            raise ReleaseBlocked("capture_slot_identity_unavailable") from None

    def prepare(self, target: str, manifest: ReleaseManifest, slot: str) -> None:
        self._target(target)
        if self.slot_images.get(slot) != manifest.image:
            raise ReleaseBlocked("capture_slot_configuration_mismatch")
        if self.capacity is not None:
            self.capacity.admit()
        self._command(["pull", manifest.image], timeout=300)
        try:
            digests = json.loads(
                self._command(
                    ["image", "inspect", "--format", "{{json .RepoDigests}}", manifest.image]
                )
            )
        except (ValueError, TypeError):
            raise ReleaseBlocked("image_digest_unavailable") from None
        if not isinstance(digests, list) or manifest.image not in digests:
            raise ReleaseBlocked("image_digest_mismatch")

    def start(self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str) -> None:
        self._target(target)
        if self.slot_images.get(slot) != manifest.image or self.slot_workers.get(slot) != worker_id:
            raise ReleaseBlocked("capture_slot_configuration_mismatch")
        container = self._container(slot)
        if container:
            image, running, worker = self._identity(container)
            if running:
                if (image, worker) != (manifest.image, worker_id):
                    raise ReleaseBlocked("candidate_slot_occupied")
                return
        if self.capacity is not None:
            self.capacity.admit()
        self.persist_slots()
        self._command(
            [*self._compose(), "up", "--detach", "--no-deps", "--no-build", SLOT_SERVICES[slot]]
        )

    def healthy(self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str) -> bool:
        self._target(target)
        container = self._container(slot)
        return bool(container and self._identity(container) == (manifest.image, True, worker_id))

    def retire(self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str) -> None:
        self._target(target)
        container = self._container(slot)
        if not container:
            return
        image, running, worker = self._identity(container)
        if (image, worker) != (manifest.image, worker_id):
            raise ReleaseBlocked("retirement_slot_identity_changed")
        if running:
            # Drain is already acknowledged in SQL. Never use Compose's 60s
            # implicit SIGKILL deadline or remove a container with active work.
            self._command(["stop", "--time=-1", container])


class CatalogWorkerGate:
    """Translate the registered worker's acknowledged operation barrier."""

    def __init__(self, control):
        self.control = control

    def status(self, worker_id: str) -> dict | None:
        return self.control.status(worker_id)

    def request_drain(self, worker_id: str, generation: int) -> dict:
        status = self.control.status(worker_id)
        if not status or status["generation"] != generation:
            raise ReleaseBlocked("worker_generation_changed")
        if status["state"] != "stopped":
            self.control.request_drain(worker_id, generation, timeout_seconds=5.0)
        status = self.control.status(worker_id)
        settled = bool(
            status
            and status["generation"] == generation
            and status["owned_leases"] == 0
            and (
                (status["state"] == "drained" and status["drain_acknowledged_at"] is not None)
                or (status["state"] == "stopped" and status["stopped_at"] is not None)
            )
        )
        return {"drained": settled}

    def resume(self, worker_id: str, generation: int) -> dict | None:
        status = self.control.status(worker_id)
        if not status or status["generation"] != generation:
            raise ReleaseBlocked("worker_control_unavailable")
        if status["state"] in {"draining", "drained"}:
            return self.control.resume(worker_id, generation)
        if status["state"] != "running":
            raise ReleaseBlocked("predecessor_cannot_resume")
        return status
