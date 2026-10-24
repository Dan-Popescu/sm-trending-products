"""Host-private deployment intent authority, separate from the app-writable journal."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path

from .catalog import Catalog
from .contracts import canonical
from .release_control import (
    DeploymentRequest,
    ReleaseBlocked,
    ReleaseManifest,
    ReleaseTrust,
    release_manifest_bytes,
)


class HostAuthorization:
    def __init__(self, directory: Path, catalog: Catalog, trust: ReleaseTrust):
        self.directory, self.catalog, self.trust = directory, catalog, trust
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ReleaseBlocked("deployment_authority_permissions_invalid")

    @staticmethod
    def intent(row: dict) -> dict:
        return {
            key: row[key]
            for key in (
                "release_id",
                "target",
                "candidate_manifest",
                "prior_manifest",
                "prior_slot",
                "prior_worker_id",
                "prior_generation",
                "candidate_worker_id",
                "schema_version",
                "synthetic",
                "rollback_of",
            )
        }

    def _path(self, release_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{64}", release_id):
            raise ReleaseBlocked("deployment_identity_invalid")
        return self.directory / f"authorization-{release_id}.json"

    def _read(self, path: Path, *, missing: bool = False) -> dict | None:
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as handle:
                info = os.fstat(handle.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077
                ):
                    raise ReleaseBlocked("deployment_authority_permissions_invalid")
                body = handle.read(4 * 1024 * 1024 + 1)
                if len(body) > 4 * 1024 * 1024:
                    raise ReleaseBlocked("deployment_authority_invalid")
                value = json.loads(body)
                if not isinstance(value, dict):
                    raise ReleaseBlocked("deployment_authority_invalid")
                return value
        except FileNotFoundError:
            if missing:
                return None
            raise ReleaseBlocked("deployment_authorization_missing") from None
        except (OSError, ValueError):
            raise ReleaseBlocked("deployment_authority_invalid") from None

    def _write(self, path: Path, value: dict) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=self.directory, prefix=".authorization-", delete=False
        ) as handle:
            temporary = Path(handle.name)
            try:
                handle.write(canonical(value) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
                os.replace(temporary, path)
                descriptor = os.open(self.directory, os.O_DIRECTORY | os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            finally:
                temporary.unlink(missing_ok=True)

    def sequence(self, request: DeploymentRequest) -> dict | None:
        highest = self._read(self.directory / "accepted-request.json", missing=True)
        if highest:
            previous = DeploymentRequest.model_validate(highest["request"])
            if previous.target != request.target or request.sequence < previous.sequence:
                raise ReleaseBlocked("deployment_request_replayed")
            if request.sequence == previous.sequence and request != previous:
                raise ReleaseBlocked("deployment_request_sequence_conflict")
        return highest

    def record_request(self, row: dict, request: DeploymentRequest, proof: dict) -> None:
        self.trust.verify_request(request, proof)
        self._bound_request(row, request)
        highest = self.sequence(request)
        if (
            highest
            and request.sequence == highest["request"]["sequence"]
            and highest["release_id"] != row["release_id"]
        ):
            raise ReleaseBlocked("deployment_request_intent_changed")
        path = self._path(row["release_id"])
        existing = self._read(path, missing=True)
        intent = self.intent(row)
        if existing and existing["intent"] != intent:
            raise ReleaseBlocked("deployment_intent_changed")
        if not existing:
            self._write(
                path,
                {
                    "intent": intent,
                    "action": "apply",
                    "authority": {
                        "kind": "request",
                        "request": request.model_dump(mode="json"),
                        "proof": proof,
                    },
                },
            )
        self._write(
            self.directory / "accepted-request.json",
            {"request": request.model_dump(mode="json"), "release_id": row["release_id"]},
        )

    @staticmethod
    def _bound_request(row: dict, request: DeploymentRequest) -> None:
        manifest = ReleaseManifest.model_validate(row["candidate_manifest"])
        if (
            row["target"] != request.target
            or manifest.revision != request.revision
            or hashlib.sha256(release_manifest_bytes(manifest)).hexdigest()
            != request.manifest_sha256
        ):
            raise ReleaseBlocked("deployment_request_run_mismatch")

    def verify(self, row: dict, *, parent: bool = False, depth: int = 0) -> str:
        if depth > 32:
            raise ReleaseBlocked("deployment_authority_chain_too_deep")
        value = self._read(self._path(row["release_id"]))
        assert value is not None
        if value.get("intent") != self.intent(row):
            raise ReleaseBlocked("deployment_intent_changed")
        authority = value.get("authority", {})
        if authority.get("kind") == "request":
            request = DeploymentRequest.model_validate(authority["request"])
            proof = authority["proof"]
            self.trust.verify_request(request, proof)
            self._bound_request(row, request)
            stored = self.catalog.one(
                "SELECT * FROM deployment_requests WHERE target=%s AND sequence=%s",
                (request.target, request.sequence),
            )
            if (
                not stored
                or stored["release_id"] != row["release_id"]
                or stored["request"] != authority["request"]
                or stored["proof"] != proof
            ):
                raise ReleaseBlocked("deployment_request_journal_changed")
            if not parent:
                self.sequence(request)
        elif authority.get("kind") == "host_rollback":
            original = self.catalog.one(
                "SELECT * FROM deployment_runs WHERE release_id=%s",
                (authority.get("parent_release_id"),),
            )
            if not original or row["rollback_of"] != original["release_id"]:
                raise ReleaseBlocked("deployment_rollback_authorization_invalid")
            self.verify(original, parent=True, depth=depth + 1)
        else:
            raise ReleaseBlocked("deployment_authority_invalid")
        action = value.get("action")
        if action not in {"apply", "abort"}:
            raise ReleaseBlocked("deployment_authority_invalid")
        return action

    def cancel(self, row: dict) -> None:
        self.verify(row)
        value = self._read(self._path(row["release_id"]))
        assert value is not None
        value["action"] = "abort"
        self._write(self._path(row["release_id"]), value)

    def rollback(self, row: dict, original: dict) -> None:
        self.verify(original)
        self._write(
            self._path(row["release_id"]),
            {
                "intent": self.intent(row),
                "action": "apply",
                "authority": {"kind": "host_rollback", "parent_release_id": original["release_id"]},
            },
        )
