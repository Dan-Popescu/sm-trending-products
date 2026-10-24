"""Acquisition and replay share immutable datasets, not a second evidence store."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from functools import partial
from typing import Any

from psycopg.types.json import Jsonb

from .capture_spool import CaptureSpool
from .catalog import InvalidationPending, LeaseLost
from .collection import SourceCollection
from .collector import CollectionError
from .contracts import digest, now, utc
from .replay import ReplayService
from .source_contracts import raw_capture_identity


class RawCaptureService:
    def __init__(self, settings, catalog, history, collector, worker_id):
        self.settings, self.catalog, self.history = settings, catalog, history
        self.collector, self.worker_id = collector, worker_id
        self.worker_generation: int | None = None
        self.spool = CaptureSpool(settings, catalog, history)
        self.collection = SourceCollection(settings, catalog, history, collector, worker_id)
        self.replay = ReplayService(catalog, history, collector)

    @staticmethod
    def identity(task: dict) -> str:
        return raw_capture_identity(task)

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
                        prior["manifest"].get("kind") not in {"raw_capture", "raw_capture_error"}
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
                business_error = bool(
                    error and error.get("category") in {"source", "authorization", "rate_limit"}
                )
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
                    kind="raw_capture_error" if business_error else "raw_capture",
                    capture=receipt["capture"],
                    quarantine=[{"reason": "captured_envelope_invalid"}] if error else (),
                    publication_guard=partial(self.spool.assert_publishable, receipt=receipt),
                )
                if error:
                    if business_error:
                        category = {
                            "authorization": "access_denied",
                            "rate_limit": "rate_limited",
                        }.get(error["category"], "source")
                        self._reschedule_failure(
                            task,
                            category,
                            error.get("retry_after_seconds") or 60,
                            error_dataset_id=capture_id,
                        )
                    else:
                        category = "captured_envelope_invalid"
                        self.catalog.reschedule(task, category, 300, quarantine=True)
                    result["errors"].append({"task_id": task["task_id"], "category": category})
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
                    self._reschedule_failure(task, category, delay)
                except LeaseLost:
                    category = "lease_lost"
                result["errors"].append({"task_id": task["task_id"], "category": category})
        return result

    def _reschedule_failure(
        self, task: dict, category: str, delay: float, *, error_dataset_id: str | None = None
    ) -> None:
        denied = category in {
            "source_access_unverified",
            "credentials_missing",
            "missing_token",
            "unauthorized",
            "access_denied",
            "invalid_request",
            "schema",
            "invalid_retry_hint",
            "capture_retention_expired",
            "capture_source_removed",
        }
        no_attempt = category in {"rate_limited", "quota_exhausted"}
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, task["synthetic"])
            self.catalog.assert_lease(connection, task)
            if error_dataset_id:
                archived = connection.execute(
                    "SELECT 1 FROM datasets WHERE dataset_id=%s AND state='committed' "
                    "AND task_id=%s AND manifest->>'kind'='raw_capture_error' "
                    "AND (manifest->'capture'->>'page_index')::bigint=%s "
                    "AND COALESCE((manifest->'capture'->>'capture_attempt')::bigint,0)=%s",
                    (
                        error_dataset_id,
                        task["task_id"],
                        task["page_index"],
                        task.get("capture_attempt", 0),
                    ),
                ).fetchone()
                if not archived:
                    raise ValueError("Capture attempt can advance only after its error is archived")
            if not task["synthetic"] and category == "access_denied":
                connection.execute(
                    "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
                    "VALUES(%s,%s,'collect','denied',%s)",
                    (task["source"], task["account_id"], Jsonb({"category": category})),
                )
            if no_attempt:
                connection.execute(
                    "SELECT block_request_budget(%s,%s,%s)",
                    (task["source"], task["account_id"], delay),
                )
            updated = connection.execute(
                "UPDATE collection_tasks SET state=%s,"
                "due_at=clock_timestamp()+%s*interval '1 second',"
                "last_error=%s,lease_owner=NULL,lease_until=NULL,attempts=GREATEST(0,attempts-%s),"
                "capture_attempt=capture_attempt+%s WHERE task_id=%s "
                "AND capture_attempt=%s RETURNING 1",
                (
                    "quarantined" if denied else "pending",
                    max(1, delay),
                    category,
                    int(no_attempt),
                    int(error_dataset_id is not None),
                    task["task_id"],
                    task.get("capture_attempt", 0),
                ),
            ).fetchone()
            if not updated:
                raise LeaseLost("Capture attempt was superseded")

    def _validate_receipt(self, receipt: dict, request: dict, task: dict) -> None:
        capture = receipt.get("capture", {})
        recorded_request = receipt.get("request", {})
        if (
            receipt.get("capture_version") != "1"
            or capture.get("capture_id") != self.identity(task)
            or capture.get("task_id") != task["task_id"]
            or capture.get("page_index") != task["page_index"]
            or capture.get("capture_attempt", 0) != task.get("capture_attempt", 0)
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
        self.spool.remove_committed(capture_id, receipt)

    def cleanup_committed_spool(self, limit: int = 20) -> int:
        """Retry local cleanup only after verifying the committed remote receipt."""
        return self.spool.scan(
            limit, apply=True, retention=False, remove_committed=self._remove_committed_spool
        )["committed"]

    def maintain_spool(self, limit: int = 20, *, apply: bool = False) -> dict:
        return self.spool.scan(limit, apply=apply)

    def close(self) -> None:
        self.spool.close()

    def request_spool_removal(
        self,
        source: str,
        account_id: str,
        synthetic: bool,
        reason: str,
        *,
        captured_before: datetime | None = None,
    ) -> dict:
        return self.spool.request_removal(
            source, account_id, synthetic, reason, captured_before=captured_before
        )

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
            self.catalog.guard_inputs(connection, [lease["dataset_id"]])
            # Claims lock the parent before its lease. Match that order before
            # child publication/FK checks so concurrent takeover cannot deadlock.
            connection.execute(
                "SELECT dataset_id FROM datasets WHERE dataset_id=%s FOR KEY SHARE",
                (lease["dataset_id"],),
            )
            current = connection.execute(
                "SELECT 1 FROM capture_publication_leases WHERE dataset_id=%s AND worker_id=%s "
                "AND fence=%s AND lease_until>clock_timestamp() FOR UPDATE",
                (lease["dataset_id"], lease["worker_id"], lease["fence"]),
            ).fetchone()
            if not current:
                raise LeaseLost("Capture publication lease expired or replaced")

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
