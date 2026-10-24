"""Collection use cases: access gates, task recovery, and source acceptance."""

from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from .catalog import Catalog, InvalidationPending, LeaseLost
from .collector import CollectionError, Collector, redact
from .contracts import Observation, digest, now
from .history import History
from .settings import Settings
from .source_contracts import canonical_operation, credential_keys


class SourceCollection:
    def __init__(
        self,
        settings: Settings,
        catalog: Catalog,
        history: History,
        collector: Collector,
        worker_id: str,
    ):
        self.settings, self.catalog, self.history = settings, catalog, history
        self.collector, self.worker_id = collector, worker_id

    def _require_access(self, source: str, account_id: str) -> None:
        cap = self.catalog.one(
            "SELECT state FROM source_capabilities WHERE source=%s AND account_id=%s "
            "AND capability='collect' ORDER BY available_at DESC,id DESC LIMIT 1",
            (source, account_id),
        )
        policy = self.catalog.one("SELECT verified FROM source_policies WHERE source=%s", (source,))
        if not cap or cap["state"] != "available" or not policy or not policy["verified"]:
            raise CollectionError("source_access_unverified", 3600)

    def enqueue(
        self,
        source: str,
        account_id: str,
        operation: str,
        payload: dict,
        synthetic: bool = False,
        idempotency_key: str | None = None,
    ) -> str:
        operation = canonical_operation(source, operation)
        if not synthetic:
            self._require_access(source, account_id)
            if any(
                key in payload
                for key in ("endpoint_override", "observed_at", "fixture_observed_at")
            ):
                raise ValueError("Fixture overrides cannot enter live collection")
        return self.catalog.enqueue(
            source,
            account_id,
            operation,
            redact(payload),
            synthetic,
            due_at=now(),
            idempotency_key=idempotency_key,
        )

    def _request(self, task: dict) -> dict:
        payload = dict(task["payload"])
        request = {
            "source": task["source"],
            "account_id": task["account_id"],
            "operation": canonical_operation(task["source"], task["operation"]),
            "synthetic": task["synthetic"],
            "payload": payload,
        }
        if task["synthetic"]:
            for source, target in (
                ("endpoint_override", "endpoint_override"),
                ("fixture_observed_at", "observed_at"),
            ):
                if source in payload:
                    request[target] = payload.pop(source)
        return request

    def collect_batch(self, batch_size: int = 20, synthetic: bool = False) -> dict:
        if not 1 <= batch_size <= 1000:
            raise ValueError("Batch size must be between 1 and 1000")
        result: dict[str, Any] = {
            "processed": 0,
            "datasets": [],
            "errors": [],
            "synthetic": synthetic,
        }
        for _ in range(batch_size):
            current_credentials = credential_keys(synthetic)
            task = self.catalog.claim(
                self.worker_id,
                self.settings.lease_seconds,
                synthetic,
                self.settings.max_attempts,
                credential_keys=current_credentials,
            )
            if task is None:
                waiting = self.catalog.quota_waiting(
                    synthetic, self.settings.max_attempts, credential_keys=current_credentials
                )
                if waiting:
                    result["quota_waiting"] = waiting
                    result["waiting"] = True
                break
            dataset_id = digest({"task": task["task_id"], "page_index": task["page_index"]})
            try:
                if not synthetic:
                    self._require_access(task["source"], task["account_id"])
                prior = self.catalog.one(
                    "SELECT * FROM datasets WHERE dataset_id=%s", (dataset_id,)
                )
                if prior and prior["state"] == "committed":
                    raw = self.history.raw(prior)
                    self.catalog.commit_dataset(
                        dataset_id,
                        prior["manifest"],
                        (),
                        task=task,
                        next_payload=raw.get("next_payload"),
                    )
                else:
                    raw = None
                    if prior and prior["state"] == "staged":
                        try:
                            raw = self.history.raw(prior)
                        except InvalidationPending:
                            raise
                        except Exception:
                            # No completed observation exists; a bounded refetch is allowed.
                            raw = None
                    if raw is None:
                        request = self._request(task)
                        envelope = self.collector.run(request)
                        raw = {
                            "schema_version": "1",
                            "request": request,
                            "response": envelope["raw"],
                            "records": envelope["records"],
                            "observed_at": envelope["observed_at"],
                            "next_payload": envelope.get("next_payload"),
                            "quarantine": envelope.get("quarantine", []),
                        }
                    observations = [Observation.model_validate(item) for item in raw["records"]]
                    if task["source"] == "tiktok_display" and task["operation"] == "query":
                        requested = set(task["payload"].get("filters", {}).get("video_ids", []))
                        returned = {obs.entity_id for obs in observations}
                        raw["requested_entity_ids"] = sorted(requested)
                        raw["missing_entity_ids"] = sorted(requested - returned)
                    if any(
                        obs.source != task["source"] or obs.synthetic != synthetic
                        for obs in observations
                    ):
                        raise CollectionError("collector_scope_mismatch")
                    self.catalog.renew(task, self.settings.lease_seconds)
                    next_payload = raw.get("next_payload")
                    if (
                        next_payload is not None
                        and task["page_index"] + 1 >= self.settings.collection_max_pages
                    ):
                        task["pagination_limit_reached"] = True
                        next_payload = None
                    if next_payload is not None and synthetic:
                        next_payload = dict(next_payload)
                        for key in ("endpoint_override", "fixture_observed_at"):
                            if key in task["payload"]:
                                next_payload[key] = task["payload"][key]
                    self.history.publish(
                        observations,
                        redact(raw),
                        source=task["source"],
                        synthetic=synthetic,
                        dataset_id=dataset_id,
                        task=task,
                        next_payload=next_payload,
                        quarantine=raw.get("quarantine", ()),
                    )
                result["processed"] += 1
                result["datasets"].append(dataset_id)
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
                    error.category if isinstance(error, CollectionError) else "publication_failed"
                )
                delay = (
                    error.retry_after_seconds
                    if isinstance(error, CollectionError)
                    else min(300, 2 ** task["attempts"])
                )
                if isinstance(error, CollectionError) and error.artifact is not None:
                    artifact = {
                        "kind": "schema_quarantine",
                        "request": self._request(task),
                        "observed_at": error.artifact.get("observed_at", now().isoformat()),
                        "response": error.artifact.get("raw"),
                        "records": [],
                        "category": category,
                    }
                    try:
                        artifact_id = digest(
                            {
                                "task": task["task_id"],
                                "page": task["page_index"],
                                "artifact": artifact,
                            }
                        )
                        self.history.publish(
                            [],
                            redact(artifact),
                            source=task["source"],
                            synthetic=synthetic,
                            dataset_id=artifact_id,
                            task=task,
                            advance_task=False,
                            quarantine=[{"reason": category}],
                        )
                        result.setdefault("quarantine_datasets", []).append(artifact_id)
                    except LeaseLost:
                        category = "lease_lost"
                denied = category in {
                    "source_access_unverified",
                    "credentials_missing",
                    "missing_token",
                    "unauthorized",
                    "access_denied",
                    "invalid_request",
                    "schema",
                    "invalid_retry_hint",
                }
                if not synthetic and category == "access_denied":
                    self.catalog.execute(
                        "INSERT INTO "
                        "source_capabilities(source,account_id,capability,state,details) "
                        "VALUES(%s,%s,'collect','denied',%s)",
                        (task["source"], task["account_id"], Jsonb({"category": category})),
                    )
                if category in {"rate_limited", "quota_exhausted"}:
                    self.catalog.block_budget(task["source"], task["account_id"], delay)
                try:
                    self.catalog.reschedule(
                        task,
                        category,
                        delay,
                        quarantine=denied,
                        no_attempt=category in {"rate_limited", "quota_exhausted"},
                    )
                except LeaseLost:
                    category = "lease_lost"
                result["errors"].append({"task_id": task["task_id"], "category": category})
        self.history.flush_usage()
        return result

    def verify_live(
        self,
        source: str,
        account_id: str,
        operation: str,
        payload: dict,
        *,
        evidence: str,
        retention_days: int,
    ) -> dict:
        operation = canonical_operation(source, operation)
        if not evidence.strip() or retention_days < 1:
            raise ValueError("Source approval evidence and positive retention are required")
        request = {
            "source": source,
            "account_id": account_id,
            "operation": operation,
            "synthetic": False,
            "payload": payload,
        }
        if any(
            key in payload for key in ("endpoint_override", "observed_at", "fixture_observed_at")
        ):
            raise ValueError("Fixture configuration is forbidden for live acceptance")
        try:
            envelope = self.collector.run(request)
            state, details = (
                "available",
                {
                    "approval_evidence": evidence,
                    "intended_use_approval": "operator_attested_explicit_permission",
                    "verified_operation": operation,
                    "scope": "authorized_account"
                    if source == "tiktok_display"
                    else "commercial_content_only",
                    "normalizer_version": envelope.get("normalizer_version", "1"),
                },
            )
            observations = [Observation.model_validate(record) for record in envelope["records"]]
            details["endpoint_accessible"] = True
            details["acceptance_observation_count"] = len(observations)
            if not observations:
                state = "unverified"
                details["category"] = "no_eligible_observations"
            raw = {
                "request": request,
                "response": envelope["raw"],
                "records": envelope["records"],
                "observed_at": envelope["observed_at"],
                "next_payload": envelope.get("next_payload"),
            }
            dataset_id = self.history.publish(
                observations, redact(raw), source=source, synthetic=False
            )
            details["acceptance_dataset_id"] = dataset_id
        except CollectionError as error:
            state = (
                "unverified"
                if error.category in {"missing_token", "credentials_missing"}
                else "denied"
            )
            details = {"category": error.category, "approval_evidence": evidence}
        with self.catalog.transaction() as connection:
            connection.execute(
                "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
                "VALUES(%s,%s,'collect',%s,%s)",
                (source, account_id, state, Jsonb(redact(details))),
            )
            if (source == "tiktok_display" and operation == "list") or (
                source in {"tiktok_ads", "tiktok_commercial"} and operation == "query"
            ):
                connection.execute(
                    "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
                    "VALUES(%s,%s,'discovery',%s,%s)",
                    (source, account_id, state, Jsonb(redact(details))),
                )
            connection.execute(
                "INSERT INTO source_policies(source,retention_days,verified,details) "
                "VALUES(%s,%s,%s,%s) ON CONFLICT(source) DO UPDATE SET "
                "retention_days=EXCLUDED.retention_days,"
                "verified=source_policies.verified OR EXCLUDED.verified,"
                "details=EXCLUDED.details,updated_at=clock_timestamp()",
                (source, retention_days, state == "available", Jsonb(redact(details))),
            )
        return {
            "source": source,
            "account_id": account_id,
            "state": state,
            "details": details,
            "ok": state == "available",
        }
