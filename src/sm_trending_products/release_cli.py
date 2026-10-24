"""Host-local signed release puller; no listener, SSH or application Docker socket."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb
from pydantic import Field

from .catalog import Catalog
from .contracts import Contract, canonical
from .release_authorization import HostAuthorization
from .release_capacity import CapacityBudget, HostCapacity
from .release_control import (
    DeploymentController,
    DeploymentRequest,
    ReleaseBlocked,
    ReleaseManifest,
    ReleaseTrust,
    VerifiedRelease,
    release_manifest_bytes,
)
from .release_evidence import CatalogCaptureVerifier
from .release_host import CatalogWorkerGate, ComposeReleaseHost
from .worker_control import WorkerControl


class HostConfig(Contract):
    target: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")
    repository: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    image_repository: str
    certificate_identity: str
    compose_file: Path
    env_file: Path
    database_url_file: Path
    state_directory: Path
    docker: Path = Path("/usr/bin/docker")
    cosign: Path = Path("/usr/local/bin/cosign")
    artifact_base_url: str = "https://github.com"
    freshness_seconds: int = Field(default=300, ge=1, le=3600)
    docker_data_dir: Path = Path("/var/lib/docker")
    spool_dir: Path
    capacity_budget: dict[str, Any] = Field(default_factory=dict)


def _trusted_parents(path: Path, controlled_test: bool) -> None:
    for parent in path.parents:
        info = parent.lstat()
        if stat.S_ISLNK(info.st_mode) or (
            info.st_uid not in {0, os.geteuid()}
            and not (controlled_test and parent in {Path("/"), Path("/tmp")})
        ):
            raise ReleaseBlocked("host_parent_permissions_invalid")
        if info.st_mode & 0o022 and not (controlled_test and info.st_mode & stat.S_ISVTX):
            raise ReleaseBlocked("host_parent_permissions_invalid")


def private_file(
    path: Path, *, executable: bool = False, secret: bool = True, controlled_test: bool = False
) -> bytes:
    """Host trust/secrets must not be replaceable by an application user."""
    if not path.is_absolute():
        raise ReleaseBlocked("host_path_not_absolute")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as handle:
            metadata = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_mode & 0o022
                or (secret and not executable and metadata.st_mode & 0o077)
            ):
                raise ReleaseBlocked("host_file_permissions_invalid")
            _trusted_parents(path, controlled_test)
            if executable:
                if not metadata.st_mode & 0o111:
                    raise ReleaseBlocked("host_executable_unavailable")
                return b""
            value = handle.read(2 * 1024 * 1024 + 1)
            if len(value) > 2 * 1024 * 1024:
                raise ReleaseBlocked("host_file_too_large")
            return value
    except OSError:
        raise ReleaseBlocked("host_file_unavailable") from None


def load_config(path: Path, *, controlled_test: bool = False) -> HostConfig:
    config = HostConfig.model_validate_json(private_file(path, controlled_test=controlled_test))
    for file in (config.env_file, config.database_url_file):
        private_file(file, controlled_test=controlled_test)
    private_file(config.compose_file, secret=False, controlled_test=controlled_test)
    for binary in (config.docker, config.cosign):
        private_file(binary, executable=True, controlled_test=controlled_test)
    if not config.state_directory.is_absolute():
        raise ReleaseBlocked("host_path_not_absolute")
    config.state_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    state = config.state_directory.lstat()
    if not stat.S_ISDIR(state.st_mode) or state.st_uid != os.geteuid() or state.st_mode & 0o077:
        raise ReleaseBlocked("host_state_permissions_invalid")
    _trusted_parents(config.state_directory, controlled_test)
    endpoint = urllib.parse.urlsplit(config.artifact_base_url)
    if config.artifact_base_url != "https://github.com" and not (
        controlled_test
        and endpoint.scheme == "http"
        and endpoint.hostname in {"127.0.0.1", "localhost"}
        and not endpoint.username
        and not endpoint.password
        and endpoint.path in {"", "/"}
    ):
        raise ReleaseBlocked("artifact_origin_not_allowed")
    if not controlled_test and config.certificate_identity != (
        f"https://github.com/{config.repository}/.github/workflows/release.yml@refs/heads/main"
    ):
        raise ReleaseBlocked("release_workflow_identity_mismatch")
    return config


class ArtifactReader:
    def __init__(self, config: HostConfig):
        self.config = config

    def read(self, tag: str, name: str) -> bytes:
        if not re.fullmatch(r"(?:candidate-[0-9a-f]{40}|pilot-deploy-request)", tag):
            raise ReleaseBlocked("artifact_tag_invalid")
        if name not in {"manifest.json", "bundle.json", "request.json", "request-bundle.json"}:
            raise ReleaseBlocked("artifact_name_invalid")
        config = self.config
        url = (
            f"{config.artifact_base_url.rstrip('/')}/{config.repository}"
            f"/releases/download/{tag}/{name}"
        )
        allowed = {
            "github.com",
            "release-assets.githubusercontent.com",
            urllib.parse.urlsplit(config.artifact_base_url).hostname,
        }

        class Redirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, fp, code, msg, headers, newurl):
                parsed = urllib.parse.urlsplit(newurl)
                if (
                    parsed.scheme != "https"
                    or parsed.hostname not in allowed
                    or parsed.username
                    or parsed.password
                ):
                    raise ReleaseBlocked("artifact_redirect_not_allowed")
                return super().redirect_request(request, fp, code, msg, headers, newurl)

        opener = urllib.request.build_opener(Redirect())
        try:
            with opener.open(
                urllib.request.Request(url, headers={"User-Agent": "sm-trends-release-controller"}),
                timeout=15,
            ) as response:
                body = response.read(2 * 1024 * 1024 + 1)
                if len(body) > 2 * 1024 * 1024:
                    raise ReleaseBlocked("artifact_too_large")
                return body
        except (OSError, urllib.error.URLError):
            raise ReleaseBlocked("artifact_download_unavailable") from None

    def release(self, revision: str, trust: ReleaseTrust) -> VerifiedRelease:
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ReleaseBlocked("artifact_revision_invalid")
        tag = f"candidate-{revision}"
        manifest = ReleaseManifest.model_validate_json(self.read(tag, "manifest.json"))
        if manifest.revision != revision:
            raise ReleaseBlocked("artifact_revision_mismatch")
        return trust.verify(
            manifest,
            {"method": "cosign_bundle", "bundle": json.loads(self.read(tag, "bundle.json"))},
        )

    def request(self, trust: ReleaseTrust) -> tuple[DeploymentRequest, dict]:
        request = DeploymentRequest.model_validate_json(
            self.read("pilot-deploy-request", "request.json")
        )
        proof = {
            "method": "cosign_bundle",
            "bundle": json.loads(self.read("pilot-deploy-request", "request-bundle.json")),
        }
        trust.verify_request(request, proof)
        if request.target != self.config.target:
            raise ReleaseBlocked("deployment_request_target_mismatch")
        return request, proof


class HostRuntime:
    def __init__(self, config: HostConfig, catalog: Catalog, *, controlled_test: bool = False):
        self.config, self.catalog = config, catalog
        self.controlled_test = controlled_test
        self.trust = ReleaseTrust(
            config.certificate_identity, (config.image_repository,), str(config.cosign)
        )
        self.reader = ArtifactReader(config)
        self.workers = CatalogWorkerGate(WorkerControl(catalog))
        self.authorization = HostAuthorization(config.state_directory, catalog, self.trust)
        self.capacity: HostCapacity | None = None
        if not controlled_test:
            self.capacity = HostCapacity(
                config.docker_data_dir,
                config.spool_dir,
                CapacityBudget(**config.capacity_budget),
                docker=str(config.docker),
            )

    def active(self) -> dict:
        row = self.catalog.one(
            "SELECT * FROM deployment_targets WHERE target=%s", (self.config.target,)
        )
        if not row:
            raise ReleaseBlocked("deployment_bootstrap_required")
        return row

    def host(self, active: dict, run: dict | None = None) -> ComposeReleaseHost:
        slots = {
            "blue": active["active_manifest"]["image"],
            "green": active["active_manifest"]["image"],
        }
        workers = {
            "blue": f"{self.config.target}-unused-blue",
            "green": f"{self.config.target}-unused-green",
        }
        workers[active["active_slot"]] = active["active_worker_id"]
        if run:
            prior = run["prior_slot"]
            candidate = "green" if prior == "blue" else "blue"
            slots[prior] = run["prior_manifest"]["image"]
            slots[candidate] = run["candidate_manifest"]["image"]
            workers[prior] = run["prior_worker_id"]
            workers[candidate] = run["candidate_worker_id"]
        return ComposeReleaseHost(
            project=self.config.target,
            compose_file=self.config.compose_file,
            env_file=self.config.env_file,
            lock_directory=self.config.state_directory,
            slot_images=slots,
            slot_workers=workers,
            docker=str(self.config.docker),
            slot_state_file=self.config.state_directory / "slots.env",
            capacity=self.capacity,
        )

    def controller(self, run: dict) -> DeploymentController:
        return DeploymentController(
            self.catalog,
            self.host(self.active(), run),
            self.workers,
            CatalogCaptureVerifier(self.catalog, freshness_seconds=self.config.freshness_seconds),
            trust=self.trust,
        )

    def bootstrap(
        self, revision: str, slot: str, worker_id: str, *, controlled_test: bool = False
    ) -> dict:
        signed = self.reader.release(revision, self.trust)
        worker = self.workers.status(worker_id)
        schema = self.catalog.one("SELECT max(version) AS version FROM schema_migrations")
        if (
            not worker
            or worker["state"] != "running"
            or (worker["synthetic"] and not controlled_test)
        ):
            raise ReleaseBlocked("bootstrap_worker_unavailable")
        if (
            not schema
            or not signed.manifest.schema_min <= schema["version"] <= signed.manifest.schema_max
        ):
            raise ReleaseBlocked("incompatible_schema")
        active = {
            "active_manifest": signed.manifest.model_dump(mode="json"),
            "active_proof": signed.proof,
            "active_slot": slot,
            "active_worker_id": worker_id,
            "synthetic": worker["synthetic"],
        }
        host = self.host(active)
        with host.exclusive(self.config.target):
            if not host.healthy(self.config.target, signed.manifest, slot, worker_id):
                raise ReleaseBlocked("bootstrap_container_unavailable")
            with self.catalog.transaction() as connection:
                created = connection.execute(
                    "INSERT INTO deployment_targets(target,active_manifest,active_proof,"
                    "active_slot,active_worker_id,synthetic) "
                    "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING target",
                    (
                        self.config.target,
                        Jsonb(active["active_manifest"]),
                        Jsonb(signed.proof),
                        slot,
                        worker_id,
                        worker["synthetic"],
                    ),
                ).fetchone()
                stored = connection.execute(
                    "SELECT * FROM deployment_targets WHERE target=%s", (self.config.target,)
                ).fetchone()
                if not stored or any(
                    stored[key] != active[key]
                    for key in ("active_manifest", "active_slot", "active_worker_id", "synthetic")
                ):
                    raise ReleaseBlocked("bootstrap_target_already_exists")
            # Conflict rejection must be read-only. Preserve paired-slot pins on
            # repeated adoption; repair a missing file after a post-commit crash.
            if created or (host.slot_state_file is not None and not host.slot_state_file.exists()):
                host.persist_slots()
        return {
            "target": self.config.target,
            "status": "bootstrapped",
            "worker_id": worker_id,
            "generation": worker["generation"],
        }

    def run(self, release_id: str) -> dict:
        row = self.catalog.one(
            "SELECT * FROM deployment_runs WHERE release_id=%s AND target=%s",
            (release_id, self.config.target),
        )
        if not row:
            raise ReleaseBlocked("deployment_run_not_found")
        return row

    def apply(self, release_id: str) -> dict:
        row = self.run(release_id)
        action = self.authorization.verify(row)
        if action == "abort":
            return self.rollback(release_id)
        controller = self.controller(row)
        result = controller.apply(release_id)
        if result["status"] == "completed" and result["phase"] == "retired":
            # Deployment requests authorize the whole application. Persist the
            # private processing intent immediately after qualified acquisition;
            # repeating this hook closes the crash gap between those journals.
            refreshed = self.refresh_processing_release(
                VerifiedRelease(
                    ReleaseManifest.model_validate(row["candidate_manifest"]),
                    row["candidate_proof"],
                )
            )
            return {**refreshed, "release_id": release_id, "capture_phase": "retired"}
        return result

    def rollback(self, release_id: str) -> dict:
        row = self.run(release_id)
        self.authorization.verify(row)
        if row["phase"] not in {"committed", "retired"}:
            self.authorization.cancel(row)
        result = self.controller(row).rollback(release_id)
        if result["release_id"] != release_id:
            self.authorization.rollback(self.run(result["release_id"]), row)
        return result

    def refresh_processing(self, manifest: Path | None, bundle: Path | None) -> dict:
        if (manifest is None) != (bundle is None):
            raise ReleaseBlocked("processing_manifest_and_bundle_required")
        release = None
        if manifest is not None and bundle is not None:
            release = self.trust.verify(
                ReleaseManifest.model_validate_json(
                    private_file(manifest, controlled_test=self.controlled_test)
                ),
                {
                    "method": "cosign_bundle",
                    "bundle": json.loads(
                        private_file(bundle, controlled_test=self.controlled_test)
                    ),
                },
            )
        return self.refresh_processing_release(release)

    def processing_refresh(self):
        from .release_processing import ProcessingRefresh

        return ProcessingRefresh(
            self.catalog,
            self.host(self.active()),
            self.trust,
            self.config.state_directory,
            freshness_seconds=self.config.freshness_seconds,
            allow_synthetic=self.controlled_test,
        )

    def pending_processing(self) -> bool:
        return self.processing_refresh().pending()

    def refresh_processing_release(
        self, release: VerifiedRelease | None = None, *, previous: VerifiedRelease | None = None
    ) -> dict:
        return self.processing_refresh().apply(release, previous=previous)

    def pull(self) -> dict:
        if self.pending_processing():
            return self.refresh_processing_release()
        active = self.active()
        # Resume accepted unfinished work even when outbound network is down.
        pending = self.catalog.one(
            "SELECT r.* FROM deployment_runs r "
            "WHERE r.target=%s AND r.status NOT IN ('completed','aborted') "
            "AND (r.rollback_of IS NOT NULL OR EXISTS(SELECT 1 FROM deployment_requests q "
            "WHERE q.release_id=r.release_id)) ORDER BY r.created_at,r.release_id LIMIT 1",
            (self.config.target,),
        )
        if pending:
            return self.apply(pending["release_id"])
        request, proof = self.reader.request(self.trust)
        self.authorization.sequence(request)
        previous = self.catalog.one(
            "SELECT * FROM deployment_requests WHERE target=%s ORDER BY sequence DESC LIMIT 1",
            (self.config.target,),
        )
        if previous and request.sequence < previous["sequence"]:
            raise ReleaseBlocked("deployment_request_replayed")
        if previous and request.sequence == previous["sequence"]:
            if previous["request"] != request.model_dump(mode="json"):
                raise ReleaseBlocked("deployment_request_sequence_conflict")
            return self.apply(previous["release_id"])
        signed = self.reader.release(request.revision, self.trust)
        if (
            hashlib.sha256(release_manifest_bytes(signed.manifest)).hexdigest()
            != request.manifest_sha256
        ):
            raise ReleaseBlocked("deployment_request_manifest_mismatch")
        schema = self.catalog.one("SELECT max(version) AS version FROM schema_migrations")
        if not schema:
            raise ReleaseBlocked("deployment_schema_unavailable")
        current = VerifiedRelease(
            ReleaseManifest.model_validate(active["active_manifest"]), active["active_proof"]
        )
        controller = DeploymentController(
            self.catalog,
            self.host(active),
            self.workers,
            CatalogCaptureVerifier(self.catalog),
            trust=self.trust,
        )
        # Plan + monotonic acceptance serialize independently of host mutations.
        with self.catalog.transaction() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371215))",
                (self.config.target,),
            )
            pending = connection.execute(
                "SELECT r.* FROM deployment_runs r WHERE r.target=%s "
                "AND r.status NOT IN ('completed','aborted') "
                "AND (r.rollback_of IS NOT NULL OR EXISTS(SELECT 1 FROM deployment_requests q "
                "WHERE q.release_id=r.release_id)) ORDER BY r.created_at,r.release_id LIMIT 1",
                (self.config.target,),
            ).fetchone()
            if pending:
                accepted_id = pending["release_id"]
            else:
                accepted_id = self._accept_request(
                    connection,
                    request,
                    proof,
                    signed,
                    current,
                    active,
                    schema["version"],
                    controller,
                )
        return self.apply(accepted_id)

    def _accept_request(
        self,
        connection,
        request: DeploymentRequest,
        proof: dict,
        signed: VerifiedRelease,
        current: VerifiedRelease,
        active: dict,
        schema_version: int,
        controller: DeploymentController,
    ) -> str:
        self.authorization.sequence(request)
        latest = connection.execute(
            "SELECT * FROM deployment_requests WHERE target=%s ORDER BY sequence DESC LIMIT 1",
            (self.config.target,),
        ).fetchone()
        if latest and latest["sequence"] >= request.sequence:
            raise ReleaseBlocked("deployment_request_concurrent")
        plan = controller.plan(
            signed,
            target=self.config.target,
            current=current,
            current_slot=active["active_slot"],
            current_worker_id=active["active_worker_id"],
            schema_version=schema_version,
            synthetic=active["synthetic"],
            request_id=f"signed-{request.sequence}",
        )
        connection.execute(
            "INSERT INTO deployment_requests(target,sequence,request,proof,release_id) "
            "VALUES(%s,%s,%s,%s,%s)",
            (
                self.config.target,
                request.sequence,
                Jsonb(request.model_dump(mode="json")),
                Jsonb(proof),
                plan["release_id"],
            ),
        )
        self.authorization.record_request(self.run(plan["release_id"]), request, proof)
        return plan["release_id"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("pull", "apply", "status", "bootstrap", "rollback", "refresh-processing"),
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--release-id")
    parser.add_argument("--revision")
    parser.add_argument("--slot", choices=("blue", "green"), default="blue")
    parser.add_argument("--worker-id")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--controlled-test", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config, controlled_test=args.controlled_test)
        catalog = Catalog(
            private_file(config.database_url_file, controlled_test=args.controlled_test)
            .decode()
            .strip()
        )
        try:
            runtime = HostRuntime(config, catalog, controlled_test=args.controlled_test)
            if args.command == "bootstrap":
                if not args.revision or not args.worker_id:
                    parser.error("bootstrap requires --revision and --worker-id")
                result = runtime.bootstrap(
                    args.revision, args.slot, args.worker_id, controlled_test=args.controlled_test
                )
            elif args.command == "pull":
                result = runtime.pull()
            elif args.command == "refresh-processing":
                result = runtime.refresh_processing(args.manifest, args.bundle)
            elif args.command == "status" and not args.release_id:
                active = runtime.active()
                result = {
                    "target": config.target,
                    "active_slot": active["active_slot"],
                    "active_worker_id": active["active_worker_id"],
                    "image": active["active_manifest"]["image"],
                }
            else:
                if not args.release_id:
                    parser.error("command requires --release-id")
                if args.command in {"apply", "rollback"}:
                    result = getattr(runtime, args.command)(args.release_id)
                else:
                    result = runtime.controller(runtime.run(args.release_id)).status(
                        args.release_id
                    )
            print(canonical(result))
            return 2 if result.get("status") == "blocked" else 0
        finally:
            catalog.close()
    except ReleaseBlocked as error:
        print(canonical({"status": "blocked", "reason": str(error)}))
        return 2
    except Exception:
        # Parser, database and Compose diagnostics can contain credential URLs.
        print(canonical({"status": "blocked", "reason": "controller_operation_failed"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
