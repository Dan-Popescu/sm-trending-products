"""Acquisition and replay share immutable datasets, not a second evidence store."""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Callable
from typing import Any

from .catalog import InvalidationPending, LeaseLost
from .collection import SourceCollection
from .collector import CollectionError
from .contracts import digest, now, utc
from .replay import ReplayService


class RawCaptureService:
    def __init__(self, settings, catalog, history, collector, worker_id):
        self.settings, self.catalog, self.history = settings, catalog, history
        self.collector, self.worker_id = collector, worker_id
        self.worker_generation: int | None = None
        self.collection = SourceCollection(settings, catalog, history, collector, worker_id)
        self.replay = ReplayService(catalog, history, collector)

    @staticmethod
    def identity(task: dict) -> str:
        return digest(
            {"capture_of": task["task_id"], "page_index": task["page_index"], "protocol": "1"}
        )

    def capture_batch(
        self,
        batch_size: int = 20,
        synthetic: bool = False,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict:
        if not 1 <= batch_size <= 1000:
            raise ValueError("Batch size must be between 1 and 1000")
        result: dict[str, Any] = {
            "processed": 0,
            "datasets": [],
            "errors": [],
            "synthetic": synthetic,
        }
        from .source_contracts import credential_keys

        self.cleanup_committed_spool(batch_size)
        for _ in range(batch_size):
            if should_stop and should_stop():
                break
            task = self.catalog.claim(
                self.worker_id,
                self.settings.lease_seconds,
                synthetic,
                self.settings.max_attempts,
                credential_keys=credential_keys(synthetic),
                **(
                    {"worker_generation": self.worker_generation}
                    if self.worker_generation is not None
                    else {}
                ),
            )
            if task is None:
                waiting = self.catalog.quota_waiting(
                    synthetic,
                    self.settings.max_attempts,
                    credential_keys=credential_keys(synthetic),
                )
                if waiting:
                    result.update(quota_waiting=waiting, waiting=True)
                break
            try:
                if not synthetic:
                    self.collection._require_access(task["source"], task["account_id"])
                request = self.collection._request(task)
                capture_id = self.identity(task)
                prior = self.catalog.one(
                    "SELECT * FROM datasets WHERE dataset_id=%s", (capture_id,)
                )
                if prior and prior["state"] == "committed":
                    if (
                        prior["manifest"].get("kind") != "raw_capture"
                        or prior["manifest"].get("acquisition_protocol") != "1"
                    ):
                        raise CollectionError("capture_identity_mismatch", 300)
                    receipt = self.history.raw(prior)
                    self._validate_receipt(receipt, request, task)
                else:
                    # A legacy delivery must be recovered by the legacy worker; never
                    # replace its already observed response with a new source request.
                    legacy_id = digest({"task": task["task_id"], "page_index": task["page_index"]})
                    if self.catalog.one("SELECT 1 FROM datasets WHERE dataset_id=%s", (legacy_id,)):
                        raise CollectionError("legacy_delivery_requires_recovery", 300)
                    receipt = self.collector.capture(
                        request,
                        task,
                        max_spool_bytes=getattr(
                            self.settings, "capture_spool_max_bytes", 268_435_456
                        ),
                        worker_generation=self.worker_generation,
                    )
                    self._validate_receipt(receipt, request, task)
                error = receipt.get("continuation_error")
                next_payload = receipt.get("next_payload")
                if next_payload is not None and synthetic:
                    next_payload = dict(next_payload)
                    for key in ("endpoint_override", "fixture_observed_at"):
                        if key in task["payload"]:
                            next_payload[key] = task["payload"][key]
                if (
                    next_payload is not None
                    and task["page_index"] + 1 >= self.settings.collection_max_pages
                ):
                    task["pagination_limit_reached"] = True
                    next_payload = None
                self.catalog.renew(task, self.settings.lease_seconds)
                self.history.publish(
                    [],
                    receipt,
                    source=task["source"],
                    synthetic=synthetic,
                    dataset_id=capture_id,
                    task=task,
                    next_payload=next_payload,
                    advance_task=not error,
                    kind="raw_capture",
                    capture=receipt["capture"],
                    quarantine=[{"reason": "captured_envelope_invalid"}] if error else (),
                )
                if error:
                    self.catalog.reschedule(task, "captured_envelope_invalid", 300, quarantine=True)
                    result["errors"].append(
                        {"task_id": task["task_id"], "category": "captured_envelope_invalid"}
                    )
                else:
                    result["processed"] += 1
                result["datasets"].append(capture_id)
                try:
                    self._remove_committed_spool(capture_id, receipt)
                except (OSError, ValueError, CollectionError):
                    result["cleanup_pending"] = result.get("cleanup_pending", 0) + 1
            except InvalidationPending:
                try:
                    self.catalog.reschedule(task, "invalidation_pending", 1, no_attempt=True)
                except LeaseLost:
                    pass
                raise
            except LeaseLost:
                result["errors"].append({"task_id": task["task_id"], "category": "lease_lost"})
            except Exception as error:
                category = (
                    error.category
                    if isinstance(error, CollectionError)
                    else "capture_publication_failed"
                )
                delay = (
                    error.retry_after_seconds
                    if isinstance(error, CollectionError)
                    else min(300, 2 ** task["attempts"])
                )
                try:
                    self.catalog.reschedule(task, category, delay)
                except LeaseLost:
                    category = "lease_lost"
                result["errors"].append({"task_id": task["task_id"], "category": category})
        return result

    def _validate_receipt(self, receipt: dict, request: dict, task: dict) -> None:
        capture = receipt.get("capture", {})
        recorded_request = receipt.get("request", {})
        if (
            receipt.get("capture_version") != "1"
            or capture.get("capture_id") != self.identity(task)
            or capture.get("task_id") != task["task_id"]
            or capture.get("page_index") != task["page_index"]
            or capture.get("request_fingerprint") != digest(request)
            or recorded_request.get("source") != task["source"]
            or recorded_request.get("account_id") != task["account_id"]
            or recorded_request.get("synthetic") is not task["synthetic"]
            or not isinstance(capture.get("worker_id"), str)
            or not capture["worker_id"]
            or utc(capture["captured_at"]) > now()
        ):
            raise CollectionError("capture_identity_mismatch", 300)
        utc(receipt["observed_at"])
        if "response" not in receipt:
            raise CollectionError("invalid_capture_receipt", 300)

    def _remove_committed_spool(self, capture_id: str, receipt: dict) -> None:
        directory = self.settings.spool_dir / "captures"
        path = directory / f"{capture_id}.json"
        if not path.exists():
            return
        with (directory / f"capture-{capture_id[:2]}.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if path.is_symlink():
                raise CollectionError("capture_spool_symlink")
            try:
                stored = json.loads(path.read_bytes())
            except FileNotFoundError:
                return
            if stored != receipt:
                raise CollectionError("capture_identity_mismatch")
            path.unlink()
            handle = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(handle)
            finally:
                os.close(handle)

    def cleanup_committed_spool(self, limit: int = 20) -> int:
        """Retry local cleanup only after verifying the committed remote receipt."""
        directory = self.settings.spool_dir / "captures"
        if not directory.exists():
            return 0
        processed = 0
        for path in directory.iterdir():
            if processed >= limit:
                break
            if path.suffix != ".json" or len(path.stem) != 64:
                continue
            processed += 1
            dataset = self.catalog.one(
                "SELECT * FROM datasets WHERE dataset_id=%s AND state='committed' "
                "AND manifest->>'kind'='raw_capture' AND manifest->>'acquisition_protocol'='1'",
                (path.stem,),
            )
            if dataset:
                try:
                    self._remove_committed_spool(path.stem, self.history.raw(dataset))
                except (OSError, ValueError, CollectionError):
                    # Preserve the receipt until an exact remote copy is readable.
                    continue
        return processed

    def _claim_publication(self, synthetic: bool) -> dict | None:
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            row = connection.execute(
                "SELECT d.dataset_id FROM datasets d LEFT JOIN capture_publication_leases l "
                "USING(dataset_id) WHERE d.synthetic=%s AND d.state='committed' "
                "AND d.manifest->>'kind'='raw_capture' "
                "AND d.manifest->>'acquisition_protocol'='1' "
                "AND (l.dataset_id IS NULL OR (l.lease_until<clock_timestamp() "
                "AND l.retry_at<=clock_timestamp())) "
                "AND NOT EXISTS(SELECT 1 FROM datasets child WHERE child.supersedes=d.dataset_id "
                "AND child.transform_version='1' AND child.state='committed') "
                "ORDER BY COALESCE(l.retry_at,d.created_at),d.created_at,d.dataset_id "
                "FOR UPDATE OF d SKIP LOCKED LIMIT 1",
                (synthetic,),
            ).fetchone()
            if not row:
                return None
            return connection.execute(
                "INSERT INTO capture_publication_leases(dataset_id,worker_id,lease_until) "
                "VALUES(%s,%s,clock_timestamp()+%s*interval '1 second') "
                "ON CONFLICT(dataset_id) DO UPDATE SET worker_id=EXCLUDED.worker_id,"
                "fence=capture_publication_leases.fence+1,lease_until=EXCLUDED.lease_until,"
                "attempts=capture_publication_leases.attempts+1,last_error=NULL RETURNING *",
                (row["dataset_id"], self.worker_id, self.settings.lease_seconds),
            ).fetchone()

    def _publication_guard(self, lease: dict):
        def guard(connection):
            current = connection.execute(
                "SELECT 1 FROM capture_publication_leases WHERE dataset_id=%s AND worker_id=%s "
                "AND fence=%s AND lease_until>clock_timestamp() FOR UPDATE",
                (lease["dataset_id"], lease["worker_id"], lease["fence"]),
            ).fetchone()
            if not current:
                raise LeaseLost("Capture publication lease expired or replaced")
            self.catalog.guard_inputs(connection, [lease["dataset_id"]])

        return guard

    def publish_pending(
        self,
        limit: int = 20,
        synthetic: bool = False,
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict:
        if not 1 <= limit <= 1000:
            raise ValueError("Publication limit must be between 1 and 1000")
        result: dict[str, Any] = {
            "processed": 0,
            "datasets": [],
            "errors": [],
            "synthetic": synthetic,
        }
        for _ in range(limit):
            if should_stop and should_stop():
                break
            lease = self._claim_publication(synthetic)
            if lease is None:
                break
            try:
                published = self.replay.replay(
                    lease["dataset_id"], publication_guard=self._publication_guard(lease)
                )
                self.catalog.execute(
                    "DELETE FROM capture_publication_leases WHERE dataset_id=%s "
                    "AND worker_id=%s AND fence=%s",
                    (lease["dataset_id"], lease["worker_id"], lease["fence"]),
                )
                result["processed"] += 1
                result["datasets"].append(published["dataset_id"])
                if published["quarantine_count"]:
                    result["quarantined"] = result.get("quarantined", 0) + 1
            except InvalidationPending:
                raise
            except Exception as error:
                category = (
                    error.category
                    if isinstance(error, CollectionError)
                    else "lease_lost"
                    if isinstance(error, LeaseLost)
                    else "capture_normalization_failed"
                )
                self.catalog.execute(
                    "UPDATE capture_publication_leases SET lease_until=clock_timestamp(),"
                    "retry_at=clock_timestamp()+%s*interval '1 second',last_error=%s "
                    "WHERE dataset_id=%s AND worker_id=%s AND fence=%s",
                    (
                        min(300, 2 ** min(lease["attempts"], 8)),
                        category,
                        lease["dataset_id"],
                        lease["worker_id"],
                        lease["fence"],
                    ),
                )
                result["errors"].append({"dataset_id": lease["dataset_id"], "category": category})
        return result
