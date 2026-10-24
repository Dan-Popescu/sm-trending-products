"""Host-private deployment intent authority, separate from the app-writable journal."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path

from psycopg.types.json import Jsonb

from .catalog import Catalog
from .contracts import canonical, now
from .release_control import (
    DeploymentRequest,
    ReleaseBlocked,
    ReleaseManifest,
    ReleaseTrust,
    release_intent_id,
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
        return json.loads(
            canonical(
                {
                    key: row[key]
                    for key in (
                        "release_id",
                        "target",
                        "candidate_manifest",
                        "candidate_proof",
                        "prior_manifest",
                        "prior_proof",
                        "prior_slot",
                        "prior_worker_id",
                        "prior_generation",
                        "candidate_worker_id",
                        "schema_version",
                        "synthetic",
                        "rollback_of",
                    )
                }
            )
        )

    @contextmanager
    def _exclusive(self):
        descriptor = os.open(
            self.directory / "authorization.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def execution(self, row: dict) -> AuthorizedExecution:
        self.verify(row)
        return AuthorizedExecution(self, row)

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
                body = handle.read(8 * 1024 * 1024 + 1)
                if len(body) > 8 * 1024 * 1024:
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
        with self._exclusive():
            self._record_request(row, request, proof)

    def _record_request(self, row: dict, request: DeploymentRequest, proof: dict) -> None:
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
                    "progress": AuthorizedExecution.initial_progress(),
                    "authorized_at": now().isoformat(),
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
        # This atomic high-water pointer is supersession authority. An old
        # rollback cache never needs a second independently durable flag update.

    def recover_accepted_request(self) -> None:
        highest = self._read(self.directory / "accepted-request.json", missing=True)
        if not highest:
            return
        row = self.catalog.one(
            "SELECT * FROM deployment_runs WHERE release_id=%s", (highest["release_id"],)
        )
        value = self._read(self._path(highest["release_id"]))
        if not row or not value or value.get("intent") != self.intent(row):
            raise ReleaseBlocked("deployment_execution_authority_invalid")
        authority = value.get("authority", {})
        request = DeploymentRequest.model_validate(highest["request"])
        if authority.get("kind") != "request" or authority.get("request") != highest["request"]:
            raise ReleaseBlocked("deployment_authority_invalid")
        proof = authority["proof"]
        self.trust.verify_request(request, proof)
        self._bound_request(row, request)
        # The private pointer can commit immediately before its SQL acceptance
        # transaction rolls back. Restore only that exact sealed signed intent.
        with self.catalog.transaction() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371215))", (request.target,)
            )
            if self._read(self.directory / "accepted-request.json") != highest:
                raise ReleaseBlocked("deployment_request_concurrent")
            stored = connection.execute(
                "SELECT * FROM deployment_requests WHERE target=%s AND sequence=%s",
                (request.target, request.sequence),
            ).fetchone()
            if stored:
                if (
                    stored["request"] != highest["request"]
                    or stored["proof"] != proof
                    or stored["release_id"] != row["release_id"]
                ):
                    raise ReleaseBlocked("deployment_request_journal_changed")
            else:
                connection.execute(
                    "INSERT INTO deployment_requests(target,sequence,request,proof,release_id) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (
                        request.target,
                        request.sequence,
                        Jsonb(highest["request"]),
                        Jsonb(proof),
                        row["release_id"],
                    ),
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
        HostAuthorization._bound_identity(row, request_id=f"signed-{request.sequence}")

    @staticmethod
    def _bound_identity(row: dict, *, request_id: str = "") -> None:
        expected = release_intent_id(row, request_id=request_id)
        worker = (
            f"{row['target']}-{'green' if row['prior_slot'] == 'blue' else 'blue'}-{expected[:12]}"
        )
        if row["release_id"] != expected or row["candidate_worker_id"] != worker:
            raise ReleaseBlocked("deployment_plan_identity_mismatch")

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
            highest = self._read(self.directory / "accepted-request.json", missing=True)
            if not parent and highest and highest["request"]["sequence"] > authority["sequence"]:
                raise ReleaseBlocked("deployment_rollback_superseded")
        else:
            raise ReleaseBlocked("deployment_authority_invalid")
        action = value.get("action")
        if action not in {"apply", "abort", "rollback"}:
            raise ReleaseBlocked("deployment_authority_invalid")
        return action

    def cancel(self, row: dict) -> None:
        self.verify(row)
        with self._exclusive():
            value = self._read(self._path(row["release_id"]))
            assert value is not None
            value["action"] = "abort"
            self._write(self._path(row["release_id"]), value)

    def begin_rollback(self, row: dict) -> None:
        self.verify(row)
        with self._exclusive():
            self.verify(row)
            highest = self._read(self.directory / "accepted-request.json")
            assert highest is not None
            existing = self._read(self.directory / "rollback-intent.json", missing=True)
            if (
                existing
                and existing["sequence"] == highest["request"]["sequence"]
                and existing["origin"] == row["release_id"]
            ):
                return
            value = self._read(self._path(row["release_id"]))
            assert value is not None
            value["action"] = "rollback"
            self._write(self._path(row["release_id"]), value)
            self._write(
                self.directory / "rollback-intent.json",
                {
                    "origin": row["release_id"],
                    "release_id": row["release_id"],
                    "sequence": highest["request"]["sequence"],
                    "completed": False,
                },
            )

    def rollback_state(self) -> dict | None:
        state = self._read(self.directory / "rollback-intent.json", missing=True)
        highest = self._read(self.directory / "accepted-request.json", missing=True)
        if not highest:
            return None
        sequence = highest["request"]["sequence"]
        if state and state["sequence"] < sequence:
            state = None  # Effective supersession precedes obsolete reverse verification.
        selected = state["release_id"] if state else highest["release_id"]
        seal = self._read(self._path(selected))
        if not seal:
            raise ReleaseBlocked("deployment_authority_invalid")
        if seal.get("action") == "rollback" and (
            not state or state["origin"] != selected or state["completed"]
        ):
            # Per-release action is authority; the routing cache is derivable
            # after a crash between the action write and global marker write.
            state = {
                "origin": selected,
                "release_id": selected,
                "sequence": sequence,
                "completed": False,
            }
            self._write(self.directory / "rollback-intent.json", state)
        if not state:
            return None
        row = self.catalog.one(
            "SELECT * FROM deployment_runs WHERE release_id=%s", (state["release_id"],)
        )
        if not row:
            raise ReleaseBlocked("deployment_rollback_authorization_invalid")
        self.verify(row)
        return state

    def complete_rollback(self, release_id: str) -> None:
        with self._exclusive():
            state = self._read(self.directory / "rollback-intent.json", missing=True)
            if (
                state
                and (highest := self._read(self.directory / "accepted-request.json", missing=True))
                and state["sequence"] == highest["request"]["sequence"]
                and state["release_id"] == release_id
                and state["release_id"] != state["origin"]
            ):
                self._write(self.directory / "rollback-intent.json", {**state, "completed": True})

    def rollback(self, row: dict, original: dict) -> None:
        self.verify(original)
        self._bound_identity(row)
        if (
            row["rollback_of"] != original["release_id"]
            or row["candidate_manifest"] != original["prior_manifest"]
            or row["prior_manifest"] != original["candidate_manifest"]
        ):
            raise ReleaseBlocked("deployment_rollback_authorization_invalid")
        with self._exclusive():
            self.verify(original)
            highest = self._read(self.directory / "accepted-request.json")
            assert highest is not None
            sequence = highest["request"]["sequence"]
            existing = self._read(self._path(row["release_id"]), missing=True)
            if existing and existing["intent"] != self.intent(row):
                raise ReleaseBlocked("deployment_intent_changed")
            if not existing:
                self._write(
                    self._path(row["release_id"]),
                    {
                        "intent": self.intent(row),
                        "action": "apply",
                        "progress": AuthorizedExecution.initial_progress(),
                        "authorized_at": now().isoformat(),
                        "authority": {
                            "kind": "host_rollback",
                            "parent_release_id": original["release_id"],
                            "sequence": sequence,
                        },
                    },
                )
            # Preserve private progress on retries, including a crash between
            # writing reverse authority and advancing this durable override.
            self._write(
                self.directory / "rollback-intent.json",
                {
                    "origin": original["release_id"],
                    "release_id": row["release_id"],
                    "sequence": sequence,
                    "completed": False,
                },
            )


class AuthorizedExecution:
    """Pinned identities and private progress replace app-writable execution input."""

    def __init__(self, authority: HostAuthorization, row: dict):
        self.authority, self.intent = authority, authority.intent(row)

    @staticmethod
    def initial_progress() -> dict:
        return {
            "phase": "planned",
            "status": "pending",
            "reason": None,
            "evidence": {},
            "candidate_generation": None,
            "abort_requested": False,
        }

    @staticmethod
    def progress(row: dict) -> dict:
        return {
            key: row[key]
            for key in (
                "phase",
                "status",
                "reason",
                "evidence",
                "candidate_generation",
                "abort_requested",
            )
        }

    def bind(self, row: dict) -> dict:
        if self.authority.intent(row) != self.intent:
            raise ReleaseBlocked("deployment_intent_changed")
        value = self.authority._read(self.authority._path(row["release_id"]))
        if (
            not value
            or value.get("intent") != self.intent
            or not isinstance(value.get("progress"), dict)
        ):
            raise ReleaseBlocked("deployment_execution_authority_invalid")
        # SQL progress is an operational mirror, never authority to skip checks,
        # cancel a candidate or manufacture a terminal source handover.
        return {
            **row,
            **self.intent,
            **value["progress"],
            "abort_requested": value.get("action") == "abort",
            "created_at": value["authorized_at"],
        }

    def checkpoint(self, row: dict) -> None:
        with self.authority._exclusive():
            value = self.authority._read(self.authority._path(row["release_id"]))
            if not value or value.get("intent") != self.intent:
                raise ReleaseBlocked("deployment_execution_authority_invalid")
            value["progress"] = self.progress(row)
            self.authority._write(self.authority._path(row["release_id"]), value)
