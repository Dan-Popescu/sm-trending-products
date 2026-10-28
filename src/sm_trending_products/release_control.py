"""Fenced, resumable rolling releases; source task/publication ownership stays in Catalog."""

from __future__ import annotations

import hashlib
import re
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from psycopg.types.json import Jsonb
from pydantic import Field, model_validator

from .catalog import Catalog
from .contracts import Contract, canonical, digest

IMAGE_DIGEST_PATTERN = (
    r"^[a-z0-9][a-z0-9._-]*(?::[0-9]{1,5})?"
    r"(?:/[a-z0-9][a-z0-9._-]*)+@sha256:[0-9a-f]{64}$"
)


class ReleaseBlocked(RuntimeError):
    """A safe precondition or external action failed; acquisition must remain available."""

    def __init__(self, reason: str):
        super().__init__(
            reason if re.fullmatch(r"[a-z_]{1,80}", reason) else "release_precondition_failed"
        )


class ReleaseManifest(Contract):
    image: str = Field(pattern=IMAGE_DIGEST_PATTERN, max_length=320)
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    schema_min: int = Field(ge=1)
    schema_max: int = Field(ge=1)
    capture_protocol: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_schema_range(self):
        if self.schema_max < self.schema_min:
            raise ValueError("Invalid schema compatibility range")
        return self


def release_manifest_bytes(manifest: ReleaseManifest) -> bytes:
    """The exact signed payload shared by CI and the host verifier."""
    return (canonical(manifest.model_dump(mode="json")) + "\n").encode()


class DeploymentRequest(Contract):
    target: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")
    sequence: int = Field(gt=0, strict=True)
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def deployment_request_bytes(request: DeploymentRequest) -> bytes:
    return (canonical(request.model_dump(mode="json")) + "\n").encode()


def release_intent_id(intent: dict, *, request_id: str = "") -> str:
    return digest(
        {
            **{
                key: intent[key]
                for key in (
                    "target",
                    "candidate_manifest",
                    "prior_manifest",
                    "prior_slot",
                    "prior_worker_id",
                    "prior_generation",
                    "schema_version",
                    "synthetic",
                    "rollback_of",
                )
            },
            "request_id": request_id,
        }
    )


@dataclass(frozen=True)
class VerifiedRelease:
    manifest: ReleaseManifest
    proof: dict[str, Any]

    @classmethod
    def for_controlled_test(cls, manifest: ReleaseManifest) -> VerifiedRelease:
        """Explicit synthetic fixtures only; production controllers reject this proof."""
        return cls(manifest, {"method": "controlled_fixture"})


@dataclass(frozen=True)
class ReleaseTrust:
    certificate_identity: str
    allowed_repositories: tuple[str, ...]
    cosign: str = "/usr/local/bin/cosign"
    issuer: str = "https://token.actions.githubusercontent.com"

    def verify(self, manifest: ReleaseManifest, proof: dict) -> VerifiedRelease:
        if not self.certificate_identity or not self.allowed_repositories:
            raise ReleaseBlocked("release_trust_unconfigured")
        if manifest.image.split("@", 1)[0] not in self.allowed_repositories:
            raise ReleaseBlocked("image_repository_not_allowed")
        self._verify_payload(release_manifest_bytes(manifest), proof)
        return VerifiedRelease(manifest, proof)

    def verify_request(self, request: DeploymentRequest, proof: dict) -> None:
        if not self.certificate_identity or not self.allowed_repositories:
            raise ReleaseBlocked("release_trust_unconfigured")
        self._verify_payload(deployment_request_bytes(request), proof)

    def _verify_payload(self, payload_bytes: bytes, proof: dict) -> None:
        if proof.get("method") != "cosign_bundle" or not isinstance(proof.get("bundle"), dict):
            raise ReleaseBlocked("release_signature_missing")
        if len(canonical(proof["bundle"]).encode()) > 2 * 1024 * 1024:
            raise ReleaseBlocked("release_signature_too_large")
        with tempfile.TemporaryDirectory(prefix="sm-release-verification-") as directory:
            root = Path(directory)
            payload, bundle = root / "manifest.json", root / "bundle.json"
            payload.write_bytes(payload_bytes)
            bundle.write_text(canonical(proof["bundle"]) + "\n")
            try:
                result = subprocess.run(
                    [
                        self.cosign,
                        "verify-blob",
                        "--offline",
                        "--bundle",
                        str(bundle),
                        "--certificate-identity",
                        self.certificate_identity,
                        "--certificate-oidc-issuer",
                        self.issuer,
                        str(payload),
                    ],
                    capture_output=True,
                    timeout=45,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                raise ReleaseBlocked("release_verifier_unavailable") from None
            if result.returncode:
                raise ReleaseBlocked("release_signature_invalid")


class ReleaseHost(Protocol):
    def exclusive(self, target: str) -> Any: ...
    def prepare(self, target: str, manifest: ReleaseManifest, slot: str) -> None: ...
    def start(self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str) -> None: ...
    def healthy(
        self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str
    ) -> bool: ...
    def retire(self, target: str, manifest: ReleaseManifest, slot: str, worker_id: str) -> None: ...
    def absent(self, target: str, slot: str) -> bool: ...


class WorkerGate(Protocol):
    def status(self, worker_id: str) -> dict | None: ...
    def request_drain(self, worker_id: str, generation: int) -> dict: ...
    def resume(self, worker_id: str, generation: int) -> Any: ...
    def cancel_unregistered(self, worker_id: str, synthetic: bool) -> bool: ...


class CaptureVerifier(Protocol):
    def verify(
        self, *, worker_id: str, generation: int, since: Any, synthetic: bool
    ) -> dict | None: ...


class ReleaseExecution(Protocol):
    def bind(self, row: dict) -> dict: ...
    def checkpoint(self, row: dict) -> None: ...


class DeploymentController:
    def __init__(
        self,
        catalog: Catalog,
        host: ReleaseHost,
        workers: WorkerGate,
        evidence: CaptureVerifier,
        *,
        trust: ReleaseTrust | None = None,
        allow_synthetic: bool = False,
        execution: ReleaseExecution | None = None,
    ):
        self.catalog, self.host, self.workers, self.evidence = catalog, host, workers, evidence
        self.trust, self.allow_synthetic = trust, allow_synthetic
        self._execution = execution

    def _bind(self, row: dict, *, mutation: bool = False) -> dict:
        if self._execution is not None:
            return self._execution.bind(row)
        if mutation and not (self.allow_synthetic and row["synthetic"]):
            raise ReleaseBlocked("deployment_execution_unconfigured")
        return row

    def _verify(self, manifest: ReleaseManifest, proof: dict, synthetic: bool) -> None:
        if synthetic and self.allow_synthetic and proof == {"method": "controlled_fixture"}:
            return
        if self.trust is None:
            raise ReleaseBlocked("release_trust_unconfigured")
        self.trust.verify(manifest, proof)

    def plan(
        self,
        release: VerifiedRelease,
        *,
        target: str,
        current: VerifiedRelease,
        current_slot: str,
        current_worker_id: str,
        schema_version: int,
        current_generation: int | None = None,
        synthetic: bool = False,
        rollback_of: str | None = None,
        request_id: str = "",
    ) -> dict:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", target):
            raise ValueError("Invalid deployment target")
        if request_id and not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", request_id):
            raise ValueError("Invalid deployment request identity")
        if current_slot not in {"blue", "green"} or not re.fullmatch(
            r"[A-Za-z0-9._:-]{1,160}", current_worker_id
        ):
            raise ValueError("Invalid active worker identity")
        self._verify(release.manifest, release.proof, synthetic)
        self._verify(current.manifest, current.proof, synthetic)
        for manifest in (release.manifest, current.manifest):
            if not manifest.schema_min <= schema_version <= manifest.schema_max:
                raise ReleaseBlocked("incompatible_schema")
        if release.manifest.capture_protocol != current.manifest.capture_protocol:
            raise ReleaseBlocked("incompatible_capture_protocol")
        if release.manifest.image == current.manifest.image:
            raise ReleaseBlocked("release_already_active")
        worker = self.workers.status(current_worker_id)
        if not worker or (
            current_generation is not None and worker["generation"] != current_generation
        ):
            raise ReleaseBlocked("predecessor_generation_changed")
        current_generation = worker["generation"]
        value, previous = (
            release.manifest.model_dump(mode="json"),
            current.manifest.model_dump(mode="json"),
        )
        release_id = release_intent_id(
            {
                "target": target,
                "candidate_manifest": value,
                "prior_manifest": previous,
                "prior_slot": current_slot,
                "prior_worker_id": current_worker_id,
                "prior_generation": current_generation,
                "schema_version": schema_version,
                "synthetic": synthetic,
                "rollback_of": rollback_of,
            },
            request_id=request_id,
        )
        candidate_worker = (
            f"{target}-{'green' if current_slot == 'blue' else 'blue'}-{release_id[:12]}"
        )
        with self.catalog.transaction() as connection:
            if rollback_of is not None:
                original = connection.execute(
                    "SELECT * FROM deployment_runs WHERE release_id=%s", (rollback_of,)
                ).fetchone()
                if (
                    not original
                    or original["target"] != target
                    or original["phase"] not in {"committed", "retired"}
                    or original["prior_manifest"] != value
                    or original["candidate_manifest"] != previous
                ):
                    raise ReleaseBlocked("invalid_rollback_reference")
            connection.execute(
                "INSERT INTO deployment_targets(target,active_manifest,active_proof,active_slot,"
                "active_worker_id,synthetic) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    target,
                    Jsonb(previous),
                    Jsonb(current.proof),
                    current_slot,
                    current_worker_id,
                    synthetic,
                ),
            )
            active = connection.execute(
                "SELECT * FROM deployment_targets WHERE target=%s FOR UPDATE", (target,)
            ).fetchone()
            if (
                not active
                or active["active_manifest"] != previous
                or active["active_slot"] != current_slot
                or active["active_worker_id"] != current_worker_id
                or active["synthetic"] != synthetic
            ):
                raise ReleaseBlocked("active_release_changed")
            connection.execute(
                "INSERT INTO deployment_runs(release_id,target,candidate_manifest,candidate_proof,"
                "prior_manifest,prior_proof,prior_slot,prior_worker_id,prior_generation,candidate_worker_id,"
                "schema_version,synthetic,rollback_of) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    release_id,
                    target,
                    Jsonb(value),
                    Jsonb(release.proof),
                    Jsonb(previous),
                    Jsonb(current.proof),
                    current_slot,
                    current_worker_id,
                    current_generation,
                    candidate_worker,
                    schema_version,
                    synthetic,
                    rollback_of,
                ),
            )
        return self._status(release_id, bind=False)

    def status(self, release_id: str) -> dict:
        return self._status(release_id, bind=True)

    def _status(self, release_id: str, *, bind: bool) -> dict:
        row = self.catalog.one("SELECT * FROM deployment_runs WHERE release_id=%s", (release_id,))
        if not row:
            raise ValueError("Unknown release")
        if bind:
            row = self._bind(row)
        # Public status omits signature bundles, host paths and credentials.
        return {
            key: row[key]
            for key in (
                "release_id",
                "target",
                "phase",
                "status",
                "reason",
                "abort_requested",
                "fence",
                "created_at",
                "updated_at",
                "candidate_worker_id",
                "candidate_generation",
                "prior_worker_id",
                "prior_generation",
                "evidence",
            )
        }

    @contextmanager
    def _ownership(self, target: str) -> Iterator[tuple[Any, int]]:
        # Host lock outlives DB connectivity: a disconnected controller cannot
        # overlap host mutations with its successor while an operation finishes.
        with self.host.exclusive(target), self.catalog.pool.connection() as connection:
            key = int.from_bytes(
                hashlib.sha256(f"deployment:{target}".encode()).digest()[:8], "big", signed=True
            )
            locked = connection.execute(
                "SELECT pg_try_advisory_lock(%s) AS locked", (key,)
            ).fetchone()
            connection.commit()
            if not locked or not locked["locked"]:
                raise ReleaseBlocked("deployment_busy")
            try:
                with connection.transaction():
                    row = connection.execute(
                        "UPDATE deployment_targets SET fence=fence+1 "
                        "WHERE target=%s RETURNING fence",
                        (target,),
                    ).fetchone()
                    if not row:
                        raise ReleaseBlocked("deployment_target_missing")
                yield connection, row["fence"]
            finally:
                # Pool reset must never leak a session lock to an unrelated use.
                if not connection.closed:
                    connection.rollback()
                    connection.execute("SELECT pg_advisory_unlock(%s)", (key,))
                    connection.commit()

    def _record(self, connection, row: dict, fence: int, **changes) -> None:
        phase = changes.get("phase", row["phase"])
        status = changes.get("status", "running")
        reason = changes.get("reason")
        evidence = changes.get("evidence", row["evidence"])
        candidate_generation = changes.get("candidate_generation", row["candidate_generation"])
        abort_requested = changes.get("abort_requested", row["abort_requested"])
        with connection.transaction():
            if self._execution is not None:
                current = connection.execute(
                    "SELECT * FROM deployment_runs WHERE release_id=%s FOR UPDATE",
                    (row["release_id"],),
                ).fetchone()
                if not current:
                    raise ReleaseBlocked("deployment_execution_authority_invalid")
                self._execution.bind(current)
            changed = connection.execute(
                "UPDATE deployment_runs r SET phase=%s,status=%s,reason=%s,evidence=%s,fence=%s,"
                "candidate_generation=%s,abort_requested=%s,"
                "updated_at=clock_timestamp() FROM deployment_targets t "
                "WHERE r.release_id=%s AND t.target=r.target AND t.fence=%s RETURNING r.release_id",
                (
                    phase,
                    status,
                    reason,
                    Jsonb(evidence),
                    fence,
                    candidate_generation,
                    abort_requested,
                    row["release_id"],
                    fence,
                ),
            ).fetchone()
            if not changed:
                raise ReleaseBlocked("deployment_ownership_lost")
        row.update(
            phase=phase,
            status=status,
            reason=reason,
            evidence=evidence,
            fence=fence,
            candidate_generation=candidate_generation,
            abort_requested=abort_requested,
        )
        if self._execution is not None:
            self._execution.checkpoint(row)

    def _abort(self, connection, row: dict, fence: int) -> None:
        active = connection.execute(
            "SELECT active_manifest,active_worker_id FROM deployment_targets WHERE target=%s",
            (row["target"],),
        ).fetchone()
        connection.commit()
        if (
            not active
            or active["active_manifest"] != row["prior_manifest"]
            or active["active_worker_id"] != row["prior_worker_id"]
        ):
            raise ReleaseBlocked("active_release_changed")
        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
        if row["phase"] == "planned":
            self._record(connection, row, fence, status="aborted")
            return
        candidate = ReleaseManifest.model_validate(row["candidate_manifest"])
        slot = "green" if row["prior_slot"] == "blue" else "blue"
        worker = self.workers.status(row["candidate_worker_id"])
        if (
            (worker is None or worker["state"] == "stopped")
            and self.host.absent(row["target"], slot)
            and self.workers.cancel_unregistered(row["candidate_worker_id"], row["synthetic"])
        ):
            self._record(connection, row, fence, status="aborted")
            return
        if not worker:
            raise ReleaseBlocked("candidate_worker_unavailable")
        if worker["generation"] != row["candidate_generation"]:
            if not self.host.healthy(row["target"], candidate, slot, row["candidate_worker_id"]):
                raise ReleaseBlocked("candidate_identity_unavailable")
            self._record(connection, row, fence, candidate_generation=worker["generation"])
        drained = self.workers.request_drain(
            row["candidate_worker_id"], row["candidate_generation"]
        )
        if not drained.get("drained"):
            raise ReleaseBlocked("candidate_busy")
        self.host.retire(row["target"], candidate, slot, row["candidate_worker_id"])
        self._record(connection, row, fence, status="aborted")

    def _require_replacement(self, row: dict, manifest: ReleaseManifest, slot: str) -> None:
        worker = self.workers.status(row["candidate_worker_id"])
        reason = None
        if not worker or worker["state"] != "running":
            reason = "replacement_worker_unavailable"
        elif worker["generation"] != row["candidate_generation"]:
            reason = "replacement_generation_changed"
        elif not self.host.healthy(row["target"], manifest, slot, row["candidate_worker_id"]):
            reason = "replacement_unhealthy"
        if reason:
            self.workers.resume(row["prior_worker_id"], row["prior_generation"])
            raise ReleaseBlocked(reason)

    def apply(self, release_id: str) -> dict:
        row = self.catalog.one("SELECT * FROM deployment_runs WHERE release_id=%s", (release_id,))
        if not row:
            raise ValueError("Unknown release")
        row = self._bind(row, mutation=True)
        candidate, prior = (
            ReleaseManifest.model_validate(row[key])
            for key in ("candidate_manifest", "prior_manifest")
        )
        # Trust policy comes from the host, never from editable journal fields.
        self._verify(candidate, row["candidate_proof"], row["synthetic"])
        self._verify(prior, row["prior_proof"], row["synthetic"])
        if row["status"] in {"completed", "aborted"}:
            return self.status(release_id)
        slot = "green" if row["prior_slot"] == "blue" else "blue"
        with self._ownership(row["target"]) as (connection, fence):
            row = connection.execute(
                "SELECT * FROM deployment_runs WHERE release_id=%s", (release_id,)
            ).fetchone()
            connection.commit()
            assert row is not None
            row = self._bind(row, mutation=True)
            if row["status"] in {"completed", "aborted"}:
                return self.status(release_id)
            if row["abort_requested"]:
                try:
                    self._abort(connection, row, fence)
                except ReleaseBlocked as error:
                    self._record(connection, row, fence, status="blocked", reason=str(error))
                return self.status(release_id)
            try:
                active = connection.execute(
                    "SELECT * FROM deployment_targets WHERE target=%s", (row["target"],)
                ).fetchone()
                schema = connection.execute(
                    "SELECT max(version) AS version FROM schema_migrations"
                ).fetchone()
                connection.commit()
                expected_active = (
                    row["candidate_manifest"]
                    if row["phase"] in {"committed", "retired"}
                    else row["prior_manifest"]
                )
                expected_worker = (
                    row["candidate_worker_id"]
                    if row["phase"] in {"committed", "retired"}
                    else row["prior_worker_id"]
                )
                committed_recovery = (
                    row["phase"] in {"ready", "draining", "verified"}
                    and active is not None
                    and active["active_manifest"] == row["candidate_manifest"]
                    and active["active_worker_id"] == row["candidate_worker_id"]
                )
                if not committed_recovery and (
                    not active
                    or active["active_manifest"] != expected_active
                    or active["active_worker_id"] != expected_worker
                ):
                    raise ReleaseBlocked("active_release_changed")
                predecessor = self.workers.status(row["prior_worker_id"])
                if not predecessor or predecessor["generation"] != row["prior_generation"]:
                    raise ReleaseBlocked("predecessor_generation_changed")
                if not schema or any(
                    not m.schema_min <= schema["version"] <= m.schema_max
                    for m in (prior, candidate)
                ):
                    raise ReleaseBlocked("incompatible_schema")
                self._record(connection, row, fence)
                if row["phase"] == "planned":
                    if row["rollback_of"] is None and not self.host.healthy(
                        row["target"], prior, row["prior_slot"], row["prior_worker_id"]
                    ):
                        raise ReleaseBlocked("predecessor_unhealthy")
                    self.host.prepare(row["target"], candidate, slot)
                    self._record(connection, row, fence, phase="prepared")
                if row["phase"] in {"prepared", "started"}:
                    self.host.start(row["target"], candidate, slot, row["candidate_worker_id"])
                    self._record(connection, row, fence, phase="started")
                replacement = self.workers.status(row["candidate_worker_id"])
                if not replacement or replacement["state"] != "running":
                    self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                    raise ReleaseBlocked("replacement_worker_unavailable")
                if replacement["generation"] != row["candidate_generation"]:
                    if predecessor["state"] != "stopped":
                        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                    changes = {"candidate_generation": replacement["generation"], "evidence": {}}
                    if row["phase"] not in {"committed", "retired"}:
                        changes["phase"] = "started"
                    self._record(connection, row, fence, **changes)
                if row["phase"] not in {"committed", "retired"}:
                    self._require_replacement(row, candidate, slot)
                    evidence = self.evidence.verify(
                        worker_id=row["candidate_worker_id"],
                        generation=row["candidate_generation"],
                        since=row["created_at"],
                        synthetic=row["synthetic"],
                    )
                    if evidence is None:
                        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                        raise ReleaseBlocked("capture_proof_missing")
                    self._record(connection, row, fence, phase="ready", evidence=evidence)
                    drained = self.workers.request_drain(
                        row["prior_worker_id"], row["prior_generation"]
                    )
                    self._record(connection, row, fence, phase="draining")
                    if not drained.get("drained"):
                        raise ReleaseBlocked("predecessor_busy")
                    # Recheck after the drain barrier before committing the pointer.
                    self._require_replacement(row, candidate, slot)
                    evidence = self.evidence.verify(
                        worker_id=row["candidate_worker_id"],
                        generation=row["candidate_generation"],
                        since=row["created_at"],
                        synthetic=row["synthetic"],
                    )
                    if evidence is None:
                        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                        raise ReleaseBlocked("capture_proof_missing")
                    self._require_replacement(row, candidate, slot)
                    self._record(connection, row, fence, phase="verified", evidence=evidence)
                    with connection.transaction():
                        changed = connection.execute(
                            "UPDATE deployment_targets SET active_manifest=%s,active_proof=%s,"
                            "active_slot=%s,active_worker_id=%s,updated_at=clock_timestamp() "
                            "WHERE target=%s AND fence=%s RETURNING target",
                            (
                                Jsonb(row["candidate_manifest"]),
                                Jsonb(row["candidate_proof"]),
                                slot,
                                row["candidate_worker_id"],
                                row["target"],
                                fence,
                            ),
                        ).fetchone()
                        if not changed:
                            raise ReleaseBlocked("deployment_ownership_lost")
                        # Active pointer and committed phase are one transaction.
                        connection.execute(
                            "UPDATE deployment_runs SET phase='committed',"
                            "updated_at=clock_timestamp() WHERE release_id=%s",
                            (release_id,),
                        )
                    row["phase"] = "committed"
                    if self._execution is not None:
                        self._execution.checkpoint(row)
                if row["phase"] == "committed":
                    self._require_replacement(row, candidate, slot)
                    evidence = self.evidence.verify(
                        worker_id=row["candidate_worker_id"],
                        generation=row["candidate_generation"],
                        since=row["created_at"],
                        synthetic=row["synthetic"],
                    )
                    if evidence is None:
                        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                        raise ReleaseBlocked("capture_proof_missing")
                    drained = self.workers.request_drain(
                        row["prior_worker_id"], row["prior_generation"]
                    )
                    if not drained.get("drained"):
                        raise ReleaseBlocked("predecessor_busy")
                    self._require_replacement(row, candidate, slot)
                    self.host.retire(
                        row["target"], prior, row["prior_slot"], row["prior_worker_id"]
                    )
                    self._record(connection, row, fence, phase="retired", status="completed")
            except ReleaseBlocked as error:
                self._record(connection, row, fence, status="blocked", reason=str(error))
            except Exception:
                if row["phase"] not in {"committed", "retired"}:
                    try:
                        self.workers.resume(row["prior_worker_id"], row["prior_generation"])
                    except Exception:
                        self._record(
                            connection,
                            row,
                            fence,
                            status="blocked",
                            reason="predecessor_recovery_required",
                        )
                        return self.status(release_id)
                self._record(
                    connection, row, fence, status="blocked", reason="host_operation_failed"
                )
        return self.status(release_id)

    def rollback(self, release_id: str) -> dict:
        row = self.catalog.one("SELECT * FROM deployment_runs WHERE release_id=%s", (release_id,))
        if not row:
            raise ValueError("Unknown release")
        row = self._bind(row, mutation=True)
        self._verify(
            ReleaseManifest.model_validate(row["candidate_manifest"]),
            row["candidate_proof"],
            row["synthetic"],
        )
        self._verify(
            ReleaseManifest.model_validate(row["prior_manifest"]),
            row["prior_proof"],
            row["synthetic"],
        )
        with self._ownership(row["target"]) as (connection, fence):
            row = connection.execute(
                "SELECT * FROM deployment_runs WHERE release_id=%s", (release_id,)
            ).fetchone()
            connection.commit()
            assert row is not None
            row = self._bind(row, mutation=True)
            if row["phase"] not in {"committed", "retired"}:
                self._record(connection, row, fence, abort_requested=True)
                try:
                    self._abort(connection, row, fence)
                except ReleaseBlocked as error:
                    self._record(connection, row, fence, status="blocked", reason=str(error))
                return self.status(release_id)
        rollback_release = VerifiedRelease(
            ReleaseManifest.model_validate(row["prior_manifest"]), row["prior_proof"]
        )
        current = VerifiedRelease(
            ReleaseManifest.model_validate(row["candidate_manifest"]), row["candidate_proof"]
        )
        return self.plan(
            rollback_release,
            target=row["target"],
            current=current,
            current_slot="green" if row["prior_slot"] == "blue" else "blue",
            current_worker_id=row["candidate_worker_id"],
            current_generation=row["candidate_generation"],
            schema_version=row["schema_version"],
            synthetic=row["synthetic"],
            rollback_of=release_id,
        )
