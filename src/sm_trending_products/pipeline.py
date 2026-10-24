"""Application boundary: source evidence through durable publication to operator decisions."""

from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from psycopg.types.json import Jsonb

from .assessment_service import AssessmentService
from .catalog import Catalog
from .collection import SourceCollection
from .collector import Collector, redact
from .contracts import (
    AssessmentPolicy,
    Observation,
    Product,
    ProductLink,
    canonical,
    digest,
    now,
    utc,
)
from .evaluation_service import EvaluationService
from .history import History, HistoryIndexPending
from .provenance import Provenance
from .replay import ReplayService
from .settings import Settings


def _filesystem_space(directory: Path) -> dict:
    """Read filesystem counters without traversing cache or evidence files."""
    probe = directory
    while True:
        try:
            usage = shutil.disk_usage(probe)
            return {
                "status": "measured",
                "scope": "directory_filesystem" if probe == directory else "ancestor_filesystem",
                "total_bytes": usage.total,
                "used_bytes": usage.used,
                "available_bytes": usage.free,
            }
        except FileNotFoundError:
            if probe == probe.parent:
                return {"status": "unavailable"}
            probe = probe.parent
        except OSError:
            return {"status": "unavailable"}


class Pipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        os.environ.setdefault("POLARS_MAX_THREADS", str(settings.polars_max_threads))
        self.catalog = Catalog(settings.database_url.get_secret_value())
        self.history = History(settings, self.catalog)
        self.provenance = Provenance(self.catalog, self.history)
        self.collector = Collector(settings)
        self.worker_id = settings.worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
        self._collection = SourceCollection(
            settings, self.catalog, self.history, self.collector, self.worker_id
        )
        self._evaluation = EvaluationService(self.catalog, self.history, self.identity)
        self._replay = ReplayService(self.catalog, self.history, self.collector)

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
        capabilities = self.capabilities()
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
            "capabilities": capabilities,
            "budgets": self.catalog.all("SELECT * FROM request_budgets ORDER BY source,account_id"),
            "storage_metrics": {"scope": "current_process", **self.history.metrics},
            "storage_capacity": {
                "cache": {
                    **_filesystem_space(self.settings.cache_dir),
                    "configured_cache_limit_bytes": self.settings.cache_max_bytes,
                },
                "spool": _filesystem_space(self.settings.spool_dir),
            },
            "persisted_storage_metrics": self.catalog.one(
                "SELECT COALESCE(sum((counters->>'get_requests')::bigint),0) AS get_operations,"
                "COALESCE(sum((counters->>'put_requests')::bigint),0) AS put_operations,"
                "COALESCE(sum((counters->>'http_attempts')::bigint),0) AS http_attempts,"
                "COALESCE(sum((counters->>'download_bytes')::bigint),0) AS download_bytes,"
                "COALESCE(sum((counters->>'upload_bytes')::bigint),0) AS upload_bytes "
                "FROM operational_usage"
            ),
            "coverage_warning": "Only granted sources and actually sampled entities are covered",
            "pending_live_acceptance": not any(cap["state"] == "available" for cap in capabilities),
        }

    def products(
        self, synthetic: bool = False, *, limit: int = 1000, offset: int = 0
    ) -> list[dict]:
        return self.identity.products(synthetic=synthetic, limit=limit, offset=offset)

    def _all_products(self, synthetic: bool, as_of: datetime) -> list[dict]:
        return self.identity.all_products(as_of, synthetic=synthetic)

    def links(self, as_of: datetime, synthetic: bool = False) -> list[dict]:
        return self.identity.links(as_of, synthetic=synthetic)

    def product_links(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        return self.identity.product_links(
            product_id, as_of, synthetic=synthetic, limit=limit, offset=offset
        )

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

    def enqueue(
        self,
        source: str,
        account_id: str,
        operation: str,
        payload: dict,
        synthetic: bool = False,
        idempotency_key: str | None = None,
    ) -> str:
        return self._collection.enqueue(
            source=source,
            account_id=account_id,
            operation=operation,
            payload=payload,
            synthetic=synthetic,
            idempotency_key=idempotency_key,
        )

    def collect_batch(self, batch_size: int = 20, synthetic: bool = False) -> dict:
        return self._collection.collect_batch(batch_size=batch_size, synthetic=synthetic)

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
        return self._replay.replay(dataset_id, transform_version)

    def _observations(
        self,
        as_of: datetime,
        synthetic: bool,
        *,
        entity_keys: set[str] | None = None,
        since: datetime | None = None,
    ) -> tuple[list[Observation], list[dict]]:
        return self.history.observations(as_of, synthetic, entity_keys=entity_keys, since=since)

    def assess(
        self,
        as_of: datetime | None = None,
        synthetic: bool = False,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        from .analytics import AssessmentIdentity

        as_of, policy = utc(as_of or now()), policy or AssessmentPolicy()
        links = [ProductLink.model_validate(item) for item in self.links(as_of, synthetic)]
        products = [
            Product.model_validate(item)
            for item in self._all_products(synthetic, as_of)
            if utc(item["created_at"]) <= as_of
        ]
        identity = AssessmentIdentity.resolve(
            products, links, as_of, synthetic, policy.min_link_confidence
        )
        prepared = AssessmentService(self.history, self.provenance).prepare(
            products, links, identity, as_of, synthetic, policy
        )
        rows, input_scopes = prepared.rows, prepared.input_scopes
        publication_scope = self.provenance.publication_scope(input_scopes, rows, synthetic)

        body = {
            "schema_version": "2",
            "as_of": as_of,
            "synthetic": synthetic,
            "policy": policy.model_dump(mode="json"),
            "rows": rows,
            "datasets": {"location": "sharded_provenance", "scope": "source_deliveries"},
            "input_provenance": publication_scope.input_provenance,
        }
        run_id = digest(body)
        key = f"assessments/{'synthetic' if synthetic else 'live'}/{run_id}.json"
        self.catalog.stage_derivation(key, ())
        with self.catalog.transaction() as connection:
            self.provenance.attach(connection, key, publication_scope.roots)
            connection.execute(
                "UPDATE derived_publications SET object_sha256=%s,object_bytes=%s "
                "WHERE object_key=%s",
                (run_id, len(canonical(body).encode()), key),
            )
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
                    Jsonb([]),
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

    def assessment_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[dict]:
        """Project list fields before transferring any full assessment evidence."""
        limit, offset = self._review_page(limit, offset)
        fields = ["product_id", "name", "market", "score", "as_of", "policy_version"]
        projection = ",".join(f"'{field}',c.payload->'{field}'" for field in fields)
        for dimension in ("attention", "demand", "competition", "feasibility"):
            projection += (
                f",'{dimension}',jsonb_build_object("
                f"'score',c.payload#>'{{{dimension},score}}',"
                f"'status',c.payload#>'{{{dimension},status}}')"
            )
        return [
            row["summary"]
            for row in self.catalog.all(
                f"SELECT jsonb_build_object({projection}) AS summary "
                "FROM current_assessments c JOIN assessment_runs r USING(run_id) "
                "WHERE c.synthetic=%s AND NOT r.invalidated "
                "ORDER BY c.score DESC NULLS LAST,c.product_id LIMIT %s OFFSET %s",
                (synthetic, limit, offset),
            )
        ]

    def assessment(self, product_id: str, synthetic: bool = False) -> dict | None:
        row = self.catalog.one(
            "SELECT c.payload FROM current_assessments c JOIN assessment_runs r USING(run_id) "
            "WHERE c.product_id=%s AND c.synthetic=%s AND NOT r.invalidated",
            (product_id, synthetic),
        )
        return row["payload"] if row else None

    def _analytical_value(
        self,
        kind: str,
        identifier: str,
        synthetic: bool,
        path: Sequence[str | int],
    ):
        if len(path) > 32:
            raise ValueError("Analytical paths may contain at most 32 selectors")
        if kind == "assessment":
            value = self.assessment(identifier, synthetic)
        elif kind == "evaluation":
            evaluation = self.evaluation(identifier, synthetic)
            value = evaluation["payload"] if evaluation else None
        else:
            raise ValueError("Analytical kind must be assessment or evaluation")
        if value is None:
            raise ValueError("No analytical result exists in the selected workspace")
        for part in path:
            if isinstance(value, dict) and value.get("format") == "sharded-records-v1":
                value = self.provenance.record(value, str(part))
            elif isinstance(value, dict) and isinstance(part, str):
                value = value.get(part)
            elif isinstance(value, list) and isinstance(part, int) and 0 <= part < len(value):
                value = value[part]
            else:
                raise ValueError("Analytical path does not select an available value")
            if value is None:
                raise ValueError("Analytical record is unavailable")
        return value

    def analytical_page(
        self,
        kind: str,
        identifier: str,
        synthetic: bool = False,
        *,
        path: Sequence[str | int] = ("evidence_refs",),
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """Read one reference/record page from a selected stored result."""
        return self.provenance.page(
            self._analytical_value(kind, identifier, synthetic, path),
            limit=limit,
            offset=offset,
        )

    def analytical_record(
        self,
        kind: str,
        identifier: str,
        record_id: str,
        synthetic: bool = False,
        *,
        path: Sequence[str | int],
    ):
        """Select one immutable record through its hash index, without sibling scans."""
        return self.provenance.record(
            self._analytical_value(kind, identifier, synthetic, path), record_id
        )

    @staticmethod
    def _review_page(limit: int, offset: int) -> tuple[int, int]:
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError(
                "Review pages require a limit between 1 and 200 and nonnegative offset"
            )
        return limit, offset

    def collection_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[dict]:
        limit, offset = self._review_page(limit, offset)
        return self.catalog.all(
            "SELECT task_id,source,account_id,operation,synthetic,state,created_at,due_at,"
            "completed_at,last_error,schedule_reason->>'policy_version' AS policy_version,"
            "schedule_reason->>'bucket' AS bucket,schedule_reason->>'tier' AS tier,"
            "schedule_reason->>'scope' AS scope,"
            "schedule_reason->'planned_interval_hours' AS planned_interval_hours,"
            "CASE WHEN jsonb_typeof(schedule_reason->'entity_keys')='array' "
            "THEN jsonb_array_length(schedule_reason->'entity_keys') "
            "WHEN schedule_reason ? 'entity_key' THEN 1 ELSE 0 END AS entity_count "
            "FROM collection_tasks WHERE synthetic=%s AND schedule_reason<>'{}'::jsonb "
            "ORDER BY created_at DESC,task_id LIMIT %s OFFSET %s",
            (synthetic, limit, offset),
        )

    def collection_decision(
        self, task_id: str, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> dict | None:
        """Read one decision and a bounded member page in the same SQL snapshot."""
        limit, offset = self._review_page(limit, offset)
        row = self.catalog.one(
            "WITH planned AS (SELECT *,CASE "
            "WHEN jsonb_typeof(schedule_reason->'entity_reasons')='array' "
            "THEN schedule_reason->'entity_reasons' "
            "WHEN schedule_reason ? 'entity_key' THEN jsonb_build_array(schedule_reason) "
            "ELSE '[]'::jsonb END AS reasons FROM collection_tasks "
            "WHERE task_id=%s AND synthetic=%s AND schedule_reason<>'{}'::jsonb) "
            "SELECT p.task_id,p.source,p.account_id,p.operation,p.synthetic,p.state,"
            "p.created_at,p.due_at,p.completed_at,p.last_error,p.page_index,"
            "p.payload AS request_context,"
            "p.schedule_reason-'entity_reasons'-'entity_keys' AS decision,"
            "jsonb_array_length(p.reasons) AS member_count,"
            "COALESCE((SELECT jsonb_agg(jsonb_build_object('reason',page.reason,"
            "'candidate',jsonb_build_object('discovered_at',e.discovered_at,"
            "'last_observed_at',e.last_observed_at,'next_due_at',e.next_due_at,"
            "'origin_dataset_id',e.origin->>'dataset_id')) ORDER BY page.position) "
            "FROM (SELECT value AS reason,ordinality AS position "
            "FROM jsonb_array_elements(p.reasons) WITH ORDINALITY "
            "ORDER BY ordinality LIMIT %s OFFSET %s) page "
            "LEFT JOIN entities e ON e.entity_key=(page.reason->>'entity_key') "
            "AND e.synthetic=p.synthetic),'[]'::jsonb) AS members FROM planned p",
            (task_id, synthetic, limit, offset),
        )
        return redact(row) if row else None

    def alerts(self, synthetic: bool = False) -> list[dict]:
        return self.catalog.all(
            "SELECT * FROM alerts WHERE synthetic=%s ORDER BY created_at DESC LIMIT 1000",
            (synthetic,),
        )

    def alert_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[dict]:
        limit, offset = self._review_page(limit, offset)
        return self.catalog.all(
            "SELECT alert_id,product_id,run_id,synthetic,created_at,invalidated "
            "FROM alerts WHERE synthetic=%s ORDER BY created_at DESC,alert_id LIMIT %s OFFSET %s",
            (synthetic, limit, offset),
        )

    def alert(self, alert_id: str, synthetic: bool = False) -> dict | None:
        return self.catalog.one(
            "SELECT * FROM alerts WHERE alert_id=%s AND synthetic=%s", (alert_id, synthetic)
        )

    def evidence(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Compatibility offset surface; every internal page remains bounded."""
        limit, offset = min(max(limit, 1), 1000), max(offset, 0)
        cursor = None
        while offset:
            skipped = self.evidence_page(
                product_id, as_of, synthetic, limit=min(offset, 1000), cursor=cursor
            )
            if skipped["status"] == "indexing":
                raise HistoryIndexPending(
                    "Historical evidence index is being prepared; retry this page"
                )
            offset -= len(skipped["records"])
            cursor = skipped["next_cursor"]
            if not cursor:
                return []
        page = self.evidence_page(product_id, as_of, synthetic, limit=limit, cursor=cursor)
        if page["status"] == "indexing":
            raise HistoryIndexPending(
                "Historical evidence index is being prepared; retry this page"
            )
        return page["records"]

    def evidence_page(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict:
        cutoff = utc(as_of)
        scope = digest({"product_id": product_id, "cutoff": cutoff, "synthetic": synthetic})
        after = None
        if cursor:
            try:
                decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if (
                    not isinstance(decoded, dict)
                    or not isinstance(decoded.get("observed_at"), str)
                    or not isinstance(decoded.get("observation_id"), str)
                    or not decoded["observation_id"]
                ):
                    raise ValueError("Invalid evidence cursor fields")
                if decoded["scope"] != scope:
                    raise ValueError("Evidence cursor belongs to a different query")
                after = (utc(decoded["observed_at"]), str(decoded["observation_id"]))
            except (ValueError, KeyError, TypeError, UnicodeError) as error:
                raise ValueError("Invalid evidence cursor") from error
        keys = self.identity.entity_keys_for_product(product_id, cutoff, synthetic)
        try:
            observations, next_after = self.history.page(
                cutoff, synthetic, keys, limit=limit, after=after
            )
        except HistoryIndexPending:
            return {"records": [], "next_cursor": cursor, "status": "indexing"}

        next_cursor = None
        if next_after:
            next_cursor = base64.urlsafe_b64encode(
                canonical(
                    {
                        "scope": scope,
                        "observed_at": next_after[0],
                        "observation_id": next_after[1],
                    }
                ).encode()
            ).decode()
        return {
            "records": [row.model_dump(mode="json") for row in observations],
            "next_cursor": next_cursor,
            "status": "ready",
        }

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

    def evaluation_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[dict]:
        limit, offset = self._review_page(limit, offset)
        return self.catalog.all(
            "SELECT evaluation_id,kind,synthetic,decision_time,parent_id,invalidated,created_at,"
            "payload->>'status' AS status,"
            "COALESCE(payload->>'target',payload#>>'{config,target}') AS target "
            "FROM evaluation_runs WHERE synthetic=%s "
            "ORDER BY created_at DESC,evaluation_id LIMIT %s OFFSET %s",
            (synthetic, limit, offset),
        )

    def evaluation(self, evaluation_id: str, synthetic: bool = False) -> dict | None:
        return self.catalog.one(
            "SELECT * FROM evaluation_runs WHERE evaluation_id=%s AND synthetic=%s",
            (evaluation_id, synthetic),
        )

    def backtest(
        self,
        decision_times: list[datetime],
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
        *,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        return self._evaluation.backtest(
            decision_times=decision_times,
            as_of=as_of,
            synthetic=synthetic,
            config=config,
            policy=policy,
        )

    def freeze(
        self,
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
        *,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        return self._evaluation.freeze(
            as_of=as_of, synthetic=synthetic, config=config, policy=policy
        )

    def mature(self, evaluation_id: str, as_of: datetime) -> dict:
        return self._evaluation.mature(evaluation_id=evaluation_id, as_of=as_of)

    def mature_due(
        self, limit: int = 100, synthetic: bool = False, as_of: datetime | None = None
    ) -> dict:
        return self._evaluation.mature_due(limit=limit, synthetic=synthetic, as_of=as_of)

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
        return self._collection.verify_live(
            source=source,
            account_id=account_id,
            operation=operation,
            payload=payload,
            evidence=evidence,
            retention_days=retention_days,
        )

    def explore(
        self,
        sql_query: str,
        synthetic: bool = False,
        *,
        dataset_ids: list[str] | None = None,
        max_rows: int = 1000,
    ) -> dict:
        from .exploration import MAX_INPUT_FILES, explore

        manifests: list[dict] = []
        objects: set[str] = set()
        for item in self.catalog.iter_manifests(
            synthetic=synthetic,
            dataset_ids=set(dataset_ids) if dataset_ids else None,
            page_size=min(100, MAX_INPUT_FILES + 1),
        ):
            objects.update(obj["key"] for obj in item["manifest"]["normalized"])
            if len(objects) > MAX_INPUT_FILES or len(manifests) >= MAX_INPUT_FILES:
                raise ValueError(
                    "Too many input files or datasets; narrow the selected dataset range"
                )
            manifests.append(item)
        if dataset_ids and {item["dataset_id"] for item in manifests} != set(dataset_ids):
            raise ValueError("Selected datasets are absent, compacted, or unavailable")
        return explore(self.history, manifests, sql_query, max_rows=max_rows)

    def compact(self, synthetic: bool = False, max_datasets: int = 100) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).compact(synthetic, max_datasets)

    def retention(self, apply: bool = False) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).retention(apply=apply)

    def retention_drain(
        self,
        *,
        max_batches: int | None = None,
        max_objects: int | None = None,
        max_seconds: float | None = None,
        max_operations: int | None = None,
    ) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).retention_drain(
            max_batches=self.settings.retention_max_batches if max_batches is None else max_batches,
            max_objects=self.settings.retention_max_objects if max_objects is None else max_objects,
            max_seconds=self.settings.maintenance_max_seconds
            if max_seconds is None
            else max_seconds,
            max_operations=self.settings.retention_max_operations
            if max_operations is None
            else max_operations,
        )

    def compact_drain(
        self,
        synthetic: bool = False,
        max_datasets: int = 100,
        *,
        max_batches: int | None = None,
        max_seconds: float | None = None,
    ) -> dict:
        from .maintenance import Maintenance

        return Maintenance(self).compact_drain(
            synthetic,
            max_datasets,
            max_batches=self.settings.compaction_max_batches
            if max_batches is None
            else max_batches,
            max_seconds=self.settings.maintenance_max_seconds
            if max_seconds is None
            else max_seconds,
        )
