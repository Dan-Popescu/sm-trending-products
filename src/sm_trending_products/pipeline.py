"""Application boundary: source evidence through durable publication to operator decisions."""

from __future__ import annotations

import json
import os
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

from .catalog import Catalog, LeaseLost
from .collector import CollectionError, Collector, redact
from .contracts import (
    AssessmentPolicy,
    Capability,
    FamilyMembership,
    Observation,
    Product,
    ProductLink,
    canonical,
    digest,
    now,
    utc,
)
from .history import History
from .settings import Settings


class Pipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        os.environ.setdefault("POLARS_MAX_THREADS", str(settings.polars_max_threads))
        self.catalog = Catalog(settings.database_url.get_secret_value())
        self.history = History(settings, self.catalog)
        self.collector = Collector(settings)
        self.worker_id = settings.worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"

    @classmethod
    def from_env(cls) -> Pipeline:
        return cls(Settings())  # type: ignore[call-arg]

    def __enter__(self) -> Pipeline:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        try:
            self.history.flush_usage()
        finally:
            self.catalog.close()

    @property
    def identity(self):
        from .identity import Identity

        return Identity(self.catalog)

    def initialize(self, create_bucket: bool = False) -> dict:
        self.catalog.initialize()
        self.history.initialize(create_bucket)
        return {"ok": True, "schema_version": 1, "live_access": "unverified_until_probe"}

    def status(self) -> dict:
        self.history.flush_usage()
        queues = self.catalog.all(
            "SELECT state,synthetic,count(*) AS count,min(due_at) AS oldest_due_at,"
            "count(*) FILTER(WHERE lease_until<clock_timestamp()) AS expired_leases "
            "FROM collection_tasks GROUP BY state,synthetic ORDER BY synthetic,state"
        )
        sources = self.catalog.all(
            "SELECT source,synthetic,count(*) AS entity_count,max(last_observed_at) AS "
            "last_observed_at,"
            "min(next_due_at) AS oldest_refresh_due_at FROM entities GROUP BY source,synthetic"
        )
        datasets = self.catalog.all(
            "SELECT state,synthetic,count(*) AS dataset_count,sum(row_count) AS observation_count,"
            "sum(quarantine_count) AS quarantined_records FROM datasets GROUP BY state,synthetic"
        )
        return {
            "ok": True,
            "checked_at": now(),
            "queues": queues,
            "queue_errors": self.catalog.all(
                "SELECT source,account_id,synthetic,state,last_error,count(*) AS count "
                "FROM collection_tasks WHERE last_error IS NOT NULL "
                "GROUP BY source,account_id,synthetic,state,last_error ORDER BY source,state"
            ),
            "sources": sources,
            "datasets": datasets,
            "capabilities": self.capabilities(),
            "budgets": self.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id"),
            "storage_metrics": {"scope": "current_process", **self.history.metrics},
            "persisted_storage_metrics": self.catalog.one(
                "SELECT COALESCE(sum((counters->>'get_requests')::bigint),0) AS get_operations,"
                "COALESCE(sum((counters->>'put_requests')::bigint),0) AS put_operations,"
                "COALESCE(sum((counters->>'http_attempts')::bigint),0) AS http_attempts,"
                "COALESCE(sum((counters->>'download_bytes')::bigint),0) AS download_bytes,"
                "COALESCE(sum((counters->>'upload_bytes')::bigint),0) AS upload_bytes "
                "FROM operational_usage"
            ),
            "coverage_warning": "Only granted sources and actually sampled entities are covered",
            "pending_live_acceptance": not any(
                cap["state"] == "available" for cap in self.capabilities()
            ),
        }

    def products(
        self, synthetic: bool = False, *, limit: int = 1000, offset: int = 0
    ) -> list[dict]:
        return self.identity.products(synthetic=synthetic, limit=limit, offset=offset)

    def _all_products(self, synthetic: bool, as_of: datetime) -> list[dict]:
        result: list[dict] = []
        last_id = ""
        while True:
            rows = self.catalog.all(
                "SELECT * FROM products WHERE synthetic=%s AND created_at<=%s AND product_id>%s "
                "ORDER BY product_id LIMIT 1000",
                (synthetic, as_of, last_id),
            )
            result.extend(rows)
            if len(rows) < 1000:
                return result
            last_id = rows[-1]["product_id"]

    def links(self, as_of: datetime, synthetic: bool = False) -> list[dict]:
        return self.identity.links(as_of, synthetic=synthetic)

    def create_product(
        self, product_id: str, name: str, synthetic: bool = False, market: str | None = None
    ) -> dict:
        return self.identity.create_product(product_id, name, synthetic=synthetic, market=market)

    def review_link(
        self,
        product_id: str,
        entity_key: str,
        status: str = "confirmed",
        confidence: float = 1,
        method: str = "manual",
        evidence_refs=(),
    ) -> dict:
        return self.identity.review_link(
            product_id,
            entity_key,
            status=status,
            confidence=confidence,
            method=method,
            evidence_refs=evidence_refs,
        )

    def set_family(self, product_id: str, family_id: str) -> dict:
        return self.identity.set_family(product_id, family_id)

    def suggest_links(self, query: str, synthetic: bool = False, limit: int = 20) -> list[dict]:
        return self.identity.suggest_links(query, synthetic=synthetic, limit=limit)

    def datasets(self, limit: int = 100) -> list[dict]:
        return self.catalog.all(
            "SELECT dataset_id,source,synthetic,state,transform_version,"
            "row_count,quarantine_count,created_at,committed_at,invalid_reason FROM datasets "
            "ORDER BY created_at DESC LIMIT %s",
            (min(max(limit, 1), 1000),),
        )

    def capabilities(self) -> list[dict]:
        return self.catalog.all(
            "SELECT DISTINCT ON(source,account_id,capability) source,account_id,"
            "capability,state,available_at,details FROM source_capabilities "
            "ORDER BY source,account_id,capability,available_at DESC,id DESC"
        )

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
        if source not in {"tiktok_display", "tiktok_ads", "tiktok_commercial"}:
            raise ValueError("Unsupported source")
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
            "operation": task["operation"],
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
            task = self.catalog.claim(
                self.worker_id, self.settings.lease_seconds, synthetic, self.settings.max_attempts
            )
            if task is None:
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

    def import_records(
        self, records: list[dict], synthetic: bool = False, origin: str = "explicit_import"
    ) -> list[str]:
        groups: dict[str, list[Observation]] = defaultdict(list)
        acquired = now()
        original_digest = digest(redact(records))
        for index, record in enumerate(records):
            clean = redact(dict(record))
            if bool(clean.get("synthetic", False)) != synthetic:
                raise ValueError("Imported classification differs from selected workspace")
            clean["synthetic"] = synthetic
            if not synthetic:
                clean["available_at"] = acquired
                clean.setdefault("observed_at", acquired.isoformat())
            clean.setdefault("observation_id", digest({"import": original_digest, "row": index}))
            clean.setdefault("attributes", {})
            clean["attributes"] = {**clean["attributes"], "acquisition_context": origin}
            obs = Observation.model_validate(clean)
            groups[obs.source].append(obs)
        ids = []
        for source, rows in groups.items():
            raw = {
                "schema_version": "1",
                "kind": "import",
                "origin": origin,
                "observed_at": min(obs.observed_at for obs in rows).isoformat(),
                "records": [obs.model_dump(mode="json") for obs in rows],
            }
            dataset_id = digest(
                {"source": source, "input": original_digest, "synthetic": synthetic}
            )
            ids.append(
                self.history.publish(
                    rows, raw, source=source, synthetic=synthetic, dataset_id=dataset_id
                )
            )
        return ids

    def import_file(self, path: Path, synthetic: bool = False, batch_size: int = 500) -> dict:
        if not 1 <= batch_size <= 5000:
            raise ValueError("Import batch size must be between 1 and 5000")
        datasets, chunk = [], []
        with path.open() as handle:
            for line_number, line in enumerate(handle, 1):
                if len(line) > 1024 * 1024:
                    raise ValueError("Import record exceeds size limit")
                if not line.strip():
                    continue
                try:
                    chunk.append(json.loads(line))
                except ValueError:
                    raise ValueError(f"Invalid JSONL record at line {line_number}") from None
                if len(chunk) >= batch_size:
                    datasets.extend(self.import_records(chunk, synthetic=synthetic))
                    chunk = []
        if chunk:
            datasets.extend(self.import_records(chunk, synthetic=synthetic))
        return {"datasets": datasets, "synthetic": synthetic}

    def replay(self, dataset_id: str, transform_version: str = "1") -> dict:
        if transform_version != "1":
            raise ValueError("This binary implements normalizer version 1 only")
        dataset = self.catalog.one(
            "SELECT * FROM datasets WHERE dataset_id=%s AND state='committed'", (dataset_id,)
        )
        if not dataset:
            raise ValueError("No completed dataset found")
        raw = self.history.raw(dataset)
        if raw.get("kind") in {"import", "compaction"}:
            observations = [Observation.model_validate(item) for item in raw["records"]]
        else:
            result = self.collector.run(
                {
                    "request": raw["request"],
                    "raw": raw["response"],
                    "observed_at": raw["observed_at"],
                },
                normalize=True,
            )
            observations = [Observation.model_validate(item) for item in result["records"]]
        replay_id = digest({"replay_of": dataset_id, "version": transform_version})
        self.history.publish(
            observations,
            raw,
            source=dataset["source"],
            synthetic=dataset["synthetic"],
            dataset_id=replay_id,
            transform_version=transform_version,
            supersedes=dataset_id,
        )
        return {"dataset_id": replay_id, "replayed_from": dataset_id, "records": len(observations)}

    def _observations(
        self,
        as_of: datetime,
        synthetic: bool,
        *,
        entity_keys: set[str] | None = None,
        since: datetime | None = None,
    ) -> tuple[list[Observation], list[dict]]:
        manifests = self.catalog.manifests(synthetic=synthetic, as_of=as_of, since=since)
        observations = self.history.read(
            manifests,
            as_of=as_of,
            since=since,
            entity_keys=entity_keys,
            max_rows=self.settings.analysis_max_rows_per_batch,
        )
        return observations, manifests

    def assess(
        self,
        as_of: datetime | None = None,
        synthetic: bool = False,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        from .analytics import assess, normalize_assessments

        as_of, policy = utc(as_of or now()), policy or AssessmentPolicy()
        links = [ProductLink.model_validate(item) for item in self.links(as_of, synthetic)]
        products = [
            Product.model_validate(item)
            for item in self._all_products(synthetic, as_of)
            if utc(item["created_at"]) <= as_of
        ]
        rows: list[dict] = []
        dataset_ids: set[str] = set()
        step = self.settings.analysis_product_batch_size
        for start in range(0, len(products), step):
            batch = products[start : start + step]
            product_ids = {item.product_id for item in batch}
            entity_keys = {link.entity_key for link in links if link.product_id in product_ids}
            observations, manifests = self._observations(
                as_of,
                synthetic,
                entity_keys=entity_keys,
                since=as_of - timedelta(days=policy.lookback_days),
            )
            dataset_ids.update(
                item["dataset_id"]
                for item in manifests
                if any(
                    set(obj["entity_keys"]) & entity_keys for obj in item["manifest"]["normalized"]
                )
            )
            rows.extend(
                assess(observations, batch, links, as_of, synthetic, policy, normalize=False)
            )
        rows = normalize_assessments(rows, policy)
        body = {
            "as_of": as_of,
            "synthetic": synthetic,
            "policy": policy.model_dump(mode="json"),
            "rows": rows,
            "datasets": sorted(dataset_ids),
        }
        run_id = digest(body)
        key = f"assessments/{'synthetic' if synthetic else 'live'}/{run_id}.json"
        self.catalog.stage_derivation(key, sorted(dataset_ids))
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, sorted(dataset_ids))
            self.history.put(
                key,
                canonical(body).encode(),
                content_type="application/json",
                connection=connection,
            )
            connection.execute(
                "INSERT INTO assessment_runs(run_id,as_of,synthetic,policy,object_key,"
                "evidence_datasets) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    run_id,
                    as_of,
                    synthetic,
                    Jsonb(policy.model_dump(mode="json")),
                    key,
                    Jsonb(sorted(dataset_ids)),
                ),
            )
            for row in rows:
                connection.execute(
                    "INSERT INTO current_assessments(product_id,run_id,synthetic,score,payload) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(product_id) DO UPDATE SET "
                    "run_id=EXCLUDED.run_id,"
                    "score=EXCLUDED.score,payload=EXCLUDED.payload WHERE "
                    "(SELECT as_of FROM assessment_runs "
                    "WHERE run_id=current_assessments.run_id)<=%s",
                    (
                        row["product_id"],
                        run_id,
                        synthetic,
                        row.get("score"),
                        Jsonb(redact(row)),
                        as_of,
                    ),
                )
                if row.get("score") is None or row["score"] < policy.alert_threshold:
                    continue
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))", ("alert:" + row["product_id"],)
                )
                recent = connection.execute(
                    "SELECT 1 FROM alerts WHERE product_id=%s AND NOT invalidated "
                    "AND created_at>clock_timestamp()-%s*interval '1 hour'",
                    (row["product_id"], policy.cooldown_hours),
                ).fetchone()
                if not recent:
                    alert_id = digest({"run_id": run_id, "product_id": row["product_id"]})
                    connection.execute(
                        "INSERT INTO alerts(alert_id,product_id,run_id,synthetic,payload) "
                        "VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (alert_id, row["product_id"], run_id, synthetic, Jsonb(row)),
                    )
            connection.execute(
                "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (key,)
            )
        return {"run_id": run_id, **body}

    def latest_assessments(self, synthetic: bool = False) -> list[dict]:
        return [
            row["payload"]
            for row in self.catalog.all(
                "SELECT c.payload FROM current_assessments c "
                "JOIN assessment_runs r USING(run_id) WHERE c.synthetic=%s AND NOT r.invalidated "
                "ORDER BY c.score DESC NULLS LAST,c.product_id LIMIT 10000",
                (synthetic,),
            )
        ]

    def alerts(self, synthetic: bool = False) -> list[dict]:
        return self.catalog.all(
            "SELECT * FROM alerts WHERE synthetic=%s ORDER BY created_at DESC LIMIT 1000",
            (synthetic,),
        )

    def evidence(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        limit, offset = min(max(limit, 1), 1000), max(offset, 0)
        keys = {
            link["entity_key"]
            for link in self.links(as_of, synthetic)
            if link["product_id"] == product_id and link["status"] == "confirmed"
        }
        records, _ = self._observations(as_of, synthetic, entity_keys=keys)
        return [obs.model_dump(mode="json") for obs in records[offset : offset + limit]]

    def schedule(
        self, budget: int = 100, synthetic: bool = False, policy: dict | None = None
    ) -> dict:
        from .scheduler import Scheduler

        return Scheduler(self.catalog).schedule(budget, synthetic=synthetic, policy=policy)

    def verify(self, dataset_id: str | None = None) -> dict:
        result = self.history.verify(dataset_id)
        invalid = self.catalog.all(
            "SELECT count(*) AS count FROM current_assessments c "
            "JOIN assessment_runs r USING(run_id) WHERE r.invalidated"
        )
        result["invalidated_current_assessments"] = invalid[0]["count"]
        return result

    def evaluations(self, synthetic: bool = False) -> list[dict]:
        return self.catalog.all(
            "SELECT * FROM evaluation_runs WHERE synthetic=%s ORDER BY created_at DESC LIMIT 1000",
            (synthetic,),
        )

    def _evaluation_inputs(self, as_of: datetime, synthetic: bool) -> dict:
        observations, manifests = self._observations(as_of, synthetic)
        caps = self.catalog.all(
            "SELECT source,account_id,capability,state,available_at,details "
            "FROM source_capabilities WHERE available_at<=%s ORDER BY available_at,id",
            (as_of,),
        )
        families = self.identity.family_history(as_of, synthetic=synthetic)
        return {
            "observations": observations,
            "products": [
                Product.model_validate(item) for item in self._all_products(synthetic, as_of)
            ],
            "links": [
                ProductLink.model_validate(item)
                for item in self.identity.link_history(as_of, synthetic)
            ],
            "families": [FamilyMembership.model_validate(item) for item in families],
            "capabilities": [Capability.model_validate(item) for item in caps],
            "dataset_ids": [item["dataset_id"] for item in manifests],
        }

    def _save_evaluation(
        self,
        payload: dict,
        kind: str,
        synthetic: bool,
        decision_time: datetime | None = None,
        parent_id: str | None = None,
        dataset_ids: list[str] | None = None,
    ) -> str:
        evaluation_id = digest({"kind": kind, "synthetic": synthetic, "payload": payload})
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, dataset_ids or [])
            if parent_id:
                parent = connection.execute(
                    "SELECT invalidated FROM evaluation_runs WHERE evaluation_id=%s", (parent_id,)
                ).fetchone()
                if not parent or parent["invalidated"]:
                    raise ValueError("Frozen decision was invalidated during computation")
            connection.execute(
                "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,decision_time,payload,"
                "parent_id) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (evaluation_id, kind, synthetic, decision_time, Jsonb(redact(payload)), parent_id),
            )
            for dataset_id in sorted(set(dataset_ids or [])):
                connection.execute(
                    "INSERT INTO evaluation_dependencies(evaluation_id,dataset_id) "
                    "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                    (evaluation_id, dataset_id),
                )
        return evaluation_id

    def backtest(
        self,
        decision_times: list[datetime],
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
    ) -> dict:
        from .evaluation import EvaluationConfig, backtest

        inputs = self._evaluation_inputs(as_of, synthetic)
        dataset_ids = inputs.pop("dataset_ids")
        payload = backtest(
            **inputs,
            decision_times=decision_times,
            as_of=as_of,
            config=EvaluationConfig.model_validate(config or {}),
            synthetic=synthetic,
        )
        return {
            "evaluation_id": self._save_evaluation(
                payload, "backtest", synthetic, dataset_ids=dataset_ids
            ),
            "result": payload,
        }

    def freeze(self, as_of: datetime, synthetic: bool = False, config: dict | None = None) -> dict:
        from .evaluation import EvaluationConfig, freeze_decision

        if not synthetic and as_of < now() - timedelta(minutes=5):
            raise ValueError("A live forward decision cannot be retrospectively backdated")
        if not synthetic and as_of > now() + timedelta(seconds=30):
            raise ValueError("A live forward decision cannot be future dated")
        inputs = self._evaluation_inputs(as_of, synthetic)
        dataset_ids = inputs.pop("dataset_ids")
        payload = freeze_decision(
            **inputs,
            decision_time=as_of,
            config=EvaluationConfig.model_validate(config or {}),
            synthetic=synthetic,
        )
        return {
            "evaluation_id": self._save_evaluation(
                payload, "forward", synthetic, as_of, dataset_ids=dataset_ids
            ),
            "result": payload,
        }

    def mature(self, evaluation_id: str, as_of: datetime) -> dict:
        from .evaluation import mature_decision

        row = self.catalog.one(
            "SELECT * FROM evaluation_runs WHERE evaluation_id=%s AND kind='forward' "
            "AND NOT invalidated",
            (evaluation_id,),
        )
        if not row:
            raise ValueError("No valid frozen decision found")
        observations, manifests = self._observations(as_of, row["synthetic"])
        payload = mature_decision(row["payload"], observations, as_of)
        return {
            "evaluation_id": self._save_evaluation(
                payload,
                "outcome",
                row["synthetic"],
                parent_id=evaluation_id,
                dataset_ids=[item["dataset_id"] for item in manifests],
            ),
            "result": payload,
        }

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

    def explore(
        self,
        sql_query: str,
        synthetic: bool = False,
        *,
        dataset_ids: list[str] | None = None,
        max_rows: int = 1000,
    ) -> dict:
        from .exploration import explore

        manifests = self.catalog.manifests(synthetic=synthetic)
        if dataset_ids:
            manifests = [item for item in manifests if item["dataset_id"] in set(dataset_ids)]
            if {item["dataset_id"] for item in manifests} != set(dataset_ids):
                raise ValueError("Selected datasets are absent, compacted, or unavailable")
        return explore(self.history, manifests, sql_query, max_rows=max_rows)

    def compact(self, synthetic: bool = False, max_datasets: int = 100) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).compact(synthetic, max_datasets)

    def retention(self, apply: bool = False) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).retention(apply=apply)
