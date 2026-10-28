"""Host-only role operations, qualified identities and analytical CPU controls."""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from .contracts import digest, utc
from .release_control import IMAGE_DIGEST_PATTERN, ReleaseBlocked, ReleaseManifest
from .resource_cpu_state import (
    CPU_VARIABLES,
    INDEPENDENT_SERVICES,
    CpuContainer,
    independent_environment,
    load_cpu_policy,
)

if TYPE_CHECKING:
    from .release_capacity import HostCapacity

SLOT_SERVICES = {"blue": "worker", "green": "worker-green"}
PROCESSING_SERVICES = ("normalizer", "dagster-daemon", "dagster-webserver", "dashboard")
PLANNERS = {"blue": "capture-planner", "green": "capture-planner-green"}
ROLE_VARIABLES = {
    "normalizer": "NORMALIZER_IMAGE",
    "dagster-daemon": "ANALYTICS_IMAGE",
    "dagster-webserver": "ANALYTICS_IMAGE",
    "dashboard": "DASHBOARD_IMAGE",
}
ROLE_IMAGE_VARIABLES = frozenset(
    (*ROLE_VARIABLES.values(), *(f"ACQUISITION_PLANNER_{slot.upper()}_IMAGE" for slot in PLANNERS))
)
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
# Inspect one boolean only, never Config.Env. Duplicate/case-variant keys are ambiguous
# because Python settings are case-insensitive; refuse them even if one value is true.
INDEPENDENT_FORMAT = (
    "{{ $independent := false }}{{ $seen := false }}{{ range .Config.Env }}"
    '{{ if eq (lower (index (split . "=") 0)) "acquisition_enabled" }}'
    "{{ if $seen }}{{ $independent = false }}"
    '{{ else }}{{ $independent = eq . "ACQUISITION_ENABLED=true" }}{{ end }}'
    "{{ $seen = true }}{{ end }}{{ end }}{{json $independent}}"
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
    + "}\t"
    + INDEPENDENT_FORMAT
)


@dataclass(frozen=True)
class ComposeReleaseHost:
    """Configuration belongs to the host, never to an untrusted release manifest.

    Capture replacement, replayable-role refresh and analytical CPU changes use
    fixed operations. PostgreSQL, networks and volumes remain untouched. No shell,
    compose-down, arbitrary command or migration is exposed by this interface.
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

    def _command(
        self,
        arguments: list[str],
        *,
        timeout: int = 45,
        cpu_overrides: dict[str, int] | None = None,
    ) -> str:
        from .resource_cpu_state import (
            CPU_VARIABLES,
            cpu_environment,
            cpu_transition_pending,
            validate_quotas,
        )

        pins = cpu_environment(self.lock_directory, self.project)
        if cpu_overrides is not None:
            # Prospective pins may only inspect the effective Compose config;
            # they cannot authorize creation with uncommitted policy settings.
            if arguments != [*self._compose(), "config", "--format", "json"]:
                raise ValueError("CPU overrides are restricted to Compose configuration preview")
            validate_quotas(cpu_overrides)
            pins = {
                variable: format(cpu_overrides[service] / 10**9, ".9f")
                for service, variable in CPU_VARIABLES.items()
            }
        mutating_release = arguments[0] in {"pull", "stop"} or (
            arguments[0] == "compose"
            and any(value in {"up", "down", "restart", "stop", "rm"} for value in arguments)
        )
        if mutating_release and cpu_transition_pending(self.lock_directory, self.project):
            raise ReleaseBlocked("cpu_transition_recovery_required")
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
        # Host-private CPU pins are authoritative even when a console or service
        # manager inherited conflicting Compose interpolation variables.
        environment.update(pins)
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

    def role_container(self, service: str) -> str | None:
        self._role_service(service)
        containers = self._command([*self._compose(), "ps", "--all", "--quiet", service]).split()
        if len(containers) > 1 or any(
            not re.fullmatch(r"[a-f0-9]{12,64}", value) for value in containers
        ):
            raise ReleaseBlocked("processing_container_ambiguous")
        return containers[0] if containers else None

    def running_role(self, service: str, image: str) -> str | None:
        container = self.role_container(service)
        if container and self._identity(container)[:2] == (image, True):
            return container
        return None

    def admit_role(self, service: str) -> None:
        self._role_service(service)
        capacity = self.capacity
        policy = load_cpu_policy(self.lock_directory, self.project)
        require_independent = bool(
            policy and policy.mode == "economy" and service in INDEPENDENT_SERVICES
        )
        if capacity is None and not require_independent:
            return
        # Compose is private host configuration. Discard its raw output, which
        # can include environment secrets; retain only explicit numeric limits.
        try:
            config = json.loads(
                self._command([*self._compose(), "config", "--format", "json", service])
            )["services"][service]
            # The trusted configuration can change between resumable refresh
            # attempts. Never create a legacy source collector with economy pins.
            if require_independent and not independent_environment(config.get("environment")):
                raise ReleaseBlocked("cpu_legacy_collection_active")
            if capacity is None:
                return
            memory, cpus = int(config["mem_limit"]), float(config["cpus"])
            if memory <= 0 or not math.isfinite(cpus) or cpus <= 0:
                raise ValueError("Unbounded role")
        except (KeyError, ValueError, TypeError):
            raise ReleaseBlocked("processing_role_limits_unavailable") from None
        budget = replace(
            capacity.budget,
            candidate_memory_bytes=max(capacity.budget.candidate_memory_bytes, memory),
            candidate_cpus=max(capacity.budget.candidate_cpus, cpus),
        )
        replace(capacity, budget=budget).admit()

    def planner_identity(self, container: str) -> dict:
        values = self._command(
            [
                "inspect",
                "--format",
                "{{json .Config.Hostname}}\n{{json .State.StartedAt}}",
                container,
            ]
        ).splitlines()
        try:
            hostname, started = (json.loads(value) for value in values)
            if not isinstance(hostname, str) or not re.fullmatch(
                r"[A-Za-z0-9_.-]{1,253}", hostname
            ):
                raise ValueError("Invalid planner identity")
            utc(started)
        except (ValueError, TypeError):
            raise ReleaseBlocked("processing_planner_identity_unavailable") from None
        return {"container": container, "hostname": hostname, "started_at": started}

    @staticmethod
    def _role_service(service: str) -> None:
        if service not in (*PROCESSING_SERVICES, *PLANNERS.values()):
            raise ReleaseBlocked("processing_service_not_allowed")

    def start_role(self, service: str, image: str) -> str:
        """Start one pinned replayable role, preserving an already running identity."""
        self._role_service(service)
        variable = ROLE_VARIABLES.get(service)
        if variable is None:
            slot = "BLUE" if service == PLANNERS["blue"] else "GREEN"
            variable = f"ACQUISITION_PLANNER_{slot}_IMAGE"
        if (self.role_images or {}).get(variable) != image:
            raise ReleaseBlocked("processing_role_configuration_mismatch")
        if not self.running_role(service, image):
            self.admit_role(service)
            self._command(
                [
                    *self._compose(),
                    "up",
                    "--detach",
                    "--no-deps",
                    "--no-build",
                    "--pull",
                    "never",
                    service,
                ]
            )
        container = self.running_role(service, image)
        if not container:
            raise ReleaseBlocked("processing_container_unavailable")
        return container

    def ensure_image(self, image: str) -> None:
        """Reuse a verified local digest, or admit, pull and verify the same image."""
        if not re.fullmatch(IMAGE_DIGEST_PATTERN, image):
            raise ReleaseBlocked("processing_role_configuration_mismatch")
        command = ["image", "inspect", "--format", "{{json .RepoDigests}}", image]
        try:
            images = json.loads(self._command(command))
        except ReleaseBlocked as error:
            if str(error) != "deployment_host_command_failed":
                raise
            images = None
        if not isinstance(images, list) or image not in images:
            if self.capacity is not None:
                self.capacity.admit()
            self._command(["pull", image], timeout=300)
            images = json.loads(self._command(command))
            if not isinstance(images, list) or image not in images:
                raise ReleaseBlocked("image_digest_mismatch")

    def planner_image(self, service: str) -> str:
        if service not in PLANNERS.values():
            raise ReleaseBlocked("processing_service_not_allowed")
        container = self.role_container(service)
        if not container:
            raise ReleaseBlocked("processing_planner_bootstrap_required")
        image, running, _ = self._identity(container)
        if not running:
            raise ReleaseBlocked("processing_planner_unavailable")
        return image

    def planner_probe(self, container: str) -> dict:
        """Run the fixed coordinator probe without exposing a command surface."""
        return json.loads(
            self._command(
                ["exec", container, "python", "-m", "sm_trending_products.capture_coordinator"]
            )
        )

    def stop_planner(self, service: str, container: str) -> None:
        if service not in PLANNERS.values() or not re.fullmatch(r"[a-f0-9]{12,64}", container):
            raise ReleaseBlocked("processing_service_not_allowed")
        self._command(["stop", "--time=-1", container])

    def _running_role_identifiers(self) -> list[str]:
        identifiers = self._command(
            [
                "ps",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={self.project}",
            ]
        ).split()
        if len(identifiers) > 32 or any(
            not re.fullmatch(r"[a-f0-9]{64}", value) for value in identifiers
        ):
            raise ReleaseBlocked("cpu_inventory_unavailable")
        return identifiers

    def cpu_inventory(self) -> tuple[CpuContainer, ...]:
        identifiers = self._running_role_identifiers()
        if not identifiers:
            if self._running_role_identifiers():
                raise ReleaseBlocked("cpu_inventory_invalid")
            raise ReleaseBlocked("cpu_inventory_incomplete")
        try:
            body = self._command(["inspect", "--format", INSPECT_FORMAT, *identifiers])
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
                    independent,
                ) = [json.loads(value) for value in line.split("\t")]
                if (
                    identifier not in identifiers
                    or project != self.project
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
                        independent_acquisition=independent,
                    )
                )
            services = [row.service for row in result]
            if {row.identifier for row in result} != set(identifiers) or len(set(services)) != len(
                services
            ):
                raise ValueError("Ambiguous CPU inventory")
            after = self._running_role_identifiers()
            if sorted(after) != sorted(identifiers):
                raise ValueError("CPU inventory changed")
            if (
                not set(CPU_VARIABLES) <= set(services)
                or "postgres" not in services
                or not {"worker", "worker-green"} & set(services)
            ):
                raise ReleaseBlocked("cpu_inventory_incomplete")
            return tuple(sorted(result, key=lambda row: row.service))
        except (ValueError, KeyError, TypeError):
            raise ReleaseBlocked("cpu_inventory_invalid") from None

    def qualify_cpu_pins(self, quotas: dict[str, int], *, require_independent: bool) -> None:
        # The adapter permits prospective pins only for this read-only config command.
        try:
            configured = json.loads(
                self._command(
                    [
                        *self._compose(),
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
            if require_independent and not all(
                independent_environment(configured[service].get("environment"))
                for service in INDEPENDENT_SERVICES
            ):
                raise ValueError("Independent acquisition is required for CPU reduction")
        except (KeyError, ValueError, TypeError, OverflowError):
            raise ReleaseBlocked("cpu_compose_pins_unsupported") from None

    def configured_cpu_quotas(self) -> dict[str, int]:
        configured = json.loads(self._command([*self._compose(), "config", "--format", "json"]))[
            "services"
        ]
        return {key: round(float(configured[key]["cpus"]) * 10**9) for key in CPU_VARIABLES}

    def update_cpu_quota(self, container: CpuContainer, nanocpus: int) -> None:
        if (
            container.service not in CPU_VARIABLES
            or isinstance(nanocpus, bool)
            or not isinstance(nanocpus, int)
            or not 1 <= nanocpus <= 1024 * 10**9
        ):
            raise ValueError("Invalid analytical CPU update")
        self._command(
            ["update", "--cpus", format(nanocpus / 10**9, ".9f"), container.identifier], timeout=10
        )

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

    def absent(self, target: str, slot: str) -> bool:
        self._target(target)
        return self._container(slot) is None

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

    def cancel_unregistered(self, worker_id: str, synthetic: bool) -> bool:
        return self.control.cancel_unregistered(worker_id, synthetic=synthetic)

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
