"""Adaptive, quota-aware collection planning with protected exploration.

This planner chooses bounded work; Rust reserves the shared request budget before
performing each HTTP attempt. Scheduling never consumes request tokens in advance.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .catalog import Catalog
from .contracts import digest
from .identity_rules import LINK_STATE_CTES

SOURCES = {"tiktok_display", "tiktok_ads", "tiktok_commercial"}


def _reject_secrets(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower()
            if (
                any(
                    word in lowered
                    for word in ("token", "password", "secret", "authorization", "cookie")
                )
                or lowered == "headers"
            ):
                raise ValueError(
                    "Source credentials belong in environment secrets, not policy payloads"
                )
            _reject_secrets(child)
    elif isinstance(value, list):
        for child in value:
            _reject_secrets(child)


class DiscoveryQuery(BaseModel):
    """An explicit permitted discovery request, never an implicit global search."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source: Literal["tiktok_display", "tiktok_ads", "tiktok_commercial"]
    account_id: str = Field(min_length=1)
    operation: Literal["list", "query"]
    payload: dict[str, Any] = Field(default_factory=dict)
    interval_hours: float = Field(default=24, gt=0, le=8760)

    @model_validator(mode="after")
    def valid_query(self) -> DiscoveryQuery:
        if self.source == "tiktok_display" and self.operation != "list":
            raise ValueError(
                "Display discovery uses an authorized account's list, not public search"
            )
        if self.source != "tiktok_display" and self.operation != "query":
            raise ValueError("Commercial-content discovery uses the documented query operation")
        if not math.isfinite(self.interval_hours):
            raise ValueError("Discovery interval must be finite")
        if self.payload.get("cursor") or self.payload.get("search_id"):
            raise ValueError(
                "Discovery policy starts at the first page; continuation belongs to tasks"
            )
        _reject_secrets(self.payload)
        return self

    @property
    def query_id(self) -> str:
        return digest(
            {
                "source": self.source,
                "account_id": self.account_id,
                "operation": self.operation,
                "payload": self.payload,
            }
        )


class SchedulerPolicy(BaseModel):
    """Versioned bounds and priority rules accepted at the scheduling boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: str = Field(default="adaptive-v1", min_length=1)
    exploration_fraction: float = Field(default=0.2, gt=0, le=1)
    intervals_hours: dict[str, float] = Field(
        default_factory=lambda: {"hot": 1.0, "monitoring": 6.0, "exploration": 24.0}
    )
    hot_threshold: float = Field(default=70, ge=0, le=100)
    monitoring_threshold: float = Field(default=35, ge=0, le=100)
    min_link_confidence: float = Field(default=0.9, ge=0, le=1)
    max_candidate_scan: int = Field(default=10000, ge=1, le=100000)
    candidate_retry_seconds: int = Field(default=300, ge=1, le=86400)
    max_pending_tasks: int = Field(default=10000, ge=1, le=1000000)
    request_costs: dict[str, float] = Field(default_factory=dict)
    discovery_queries: list[DiscoveryQuery] = Field(default_factory=list, max_length=1000)

    @model_validator(mode="after")
    def valid_policy(self) -> SchedulerPolicy:
        if set(self.intervals_hours) != {"hot", "monitoring", "exploration"}:
            raise ValueError("All three scheduling tier intervals must be specified")
        if any(
            not math.isfinite(value) or not 0 < value <= 8760
            for value in self.intervals_hours.values()
        ):
            raise ValueError("Scheduling intervals must be finite and between 0 and 8760 hours")
        if self.monitoring_threshold > self.hot_threshold:
            raise ValueError("Monitoring threshold must not exceed the hot threshold")
        if set(self.request_costs) - SOURCES or any(
            not math.isfinite(cost) or not 0 < cost <= 100000
            for cost in self.request_costs.values()
        ):
            raise ValueError(
                "Request-cost estimates require a supported source and finite positive units"
            )
        identities = [query.query_id for query in self.discovery_queries]
        if len(set(identities)) != len(identities):
            raise ValueError("Duplicate discovery queries are not allowed")
        return self


class Scheduler:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def schedule(
        self, budget: int = 100, synthetic: bool = False, policy: dict | None = None
    ) -> dict:
        """Queue at most ``budget`` tasks and explain every selection atomically."""
        if isinstance(budget, bool) or not isinstance(budget, int) or not 0 <= budget <= 10000:
            raise ValueError("Scheduling budget must be an integer between 0 and 10000")
        supplied = SchedulerPolicy.model_validate(policy) if policy is not None else None
        with self.catalog.transaction() as connection:
            # One planner per workspace; collectors retain independent row leases.
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (492371210 + int(synthetic),))
            clock = connection.execute("SELECT clock_timestamp() AS checked_at").fetchone()
            if clock is None:
                raise RuntimeError("Database did not provide its clock")
            checked_at = clock["checked_at"]
            prior = connection.execute(
                "SELECT p.policy_id,p.payload FROM scheduler_policy_activations a "
                "JOIN scheduler_policies p USING(policy_id) WHERE a.synthetic=%s "
                "ORDER BY a.available_at DESC,a.activation_id DESC LIMIT 1",
                (synthetic,),
            ).fetchone()
            if supplied is None:
                supplied = SchedulerPolicy.model_validate(
                    prior["payload"]["configuration"] if prior else {}
                )
            config = supplied
            for query in config.discovery_queries:
                self._validate_workspace_payload(query.payload, synthetic)
            configuration = config.model_dump(mode="json")
            policy_body = {"synthetic": synthetic, "configuration": configuration}
            policy_id = digest(policy_body)
            connection.execute(
                "INSERT INTO scheduler_policies(policy_id,payload) VALUES(%s,%s) "
                "ON CONFLICT DO NOTHING",
                (policy_id, Jsonb(policy_body)),
            )
            if prior is None or prior["policy_id"] != policy_id:
                connection.execute(
                    "INSERT INTO scheduler_policy_activations(policy_id,synthetic) VALUES(%s,%s)",
                    (policy_id, synthetic),
                )
            capabilities = connection.execute(
                "SELECT DISTINCT ON(source,account_id,capability) "
                "source,account_id,capability,state "
                "FROM source_capabilities WHERE available_at<=%s "
                "ORDER BY source,account_id,capability,available_at DESC,id DESC",
                (checked_at,),
            ).fetchall()
            granted = {
                (row["source"], row["account_id"], row["capability"]): row["state"]
                for row in capabilities
            }
            verified = {
                row["source"]
                for row in connection.execute(
                    "SELECT source FROM source_policies WHERE verified"
                ).fetchall()
            }
            budgets = {
                (row["source"], row["account_id"]): row
                for row in connection.execute("SELECT * FROM request_budgets").fetchall()
            }
            warnings: set[tuple[str, str, str]] = set()
            pending_row = connection.execute(
                "SELECT count(*) AS count FROM collection_tasks "
                "WHERE synthetic=%s AND state IN ('pending','leased')",
                (synthetic,),
            ).fetchone()
            if pending_row is None:
                raise RuntimeError("Database did not provide the pending task count")
            pending = pending_row["count"]
            effective_budget = min(budget, max(0, config.max_pending_tasks - pending))
            if effective_budget < budget:
                warnings.add(("all", "all", "pending_task_limit_reached"))

            def allowed(source: str, account_id: str, discovery: bool = False) -> bool:
                if (source, account_id) not in budgets:
                    warnings.add((source, account_id, "request_budget_missing"))
                    return False
                if synthetic:
                    return True
                if source not in verified:
                    warnings.add((source, account_id, "source_policy_unverified"))
                    return False
                required = ("collect", "discovery") if discovery else ("collect",)
                if any(
                    granted.get((source, account_id, capability)) != "available"
                    for capability in required
                ):
                    warnings.add(
                        (
                            source,
                            account_id,
                            "discovery_unavailable" if discovery else "collection_unavailable",
                        )
                    )
                    return False
                return True

            # Inspect a blocked row once, then let later due rows advance before
            # revisiting it. Its original due time still drives overdue priority.
            entity_rows = connection.execute(
                "WITH "
                + LINK_STATE_CTES
                + ", identity AS (SELECT entity_key,product_id FROM resolved_link_owners "
                "WHERE confidence>=%s) "
                "SELECT e.*,c.score AS assessment_score,c.payload AS assessment_payload,"
                "d.manifest AS origin_manifest FROM entities e "
                "LEFT JOIN identity i ON i.entity_key=e.entity_key "
                "LEFT JOIN current_assessments c ON c.product_id=i.product_id "
                "AND c.synthetic=e.synthetic AND EXISTS(SELECT 1 FROM assessment_runs r "
                "WHERE r.run_id=c.run_id AND NOT r.invalidated AND r.as_of<=%s) "
                "LEFT JOIN datasets d ON d.dataset_id=e.origin->>'dataset_id' "
                "AND d.state='committed' "
                "WHERE e.synthetic=%s AND e.next_due_at<=%s AND e.source=ANY(%s) "
                "AND e.entity_type IN ('video','ad','commercial_content') "
                "AND COALESCE((e.schedule_reason->'deferred'->>'retry_after')::timestamptz,"
                "'-infinity'::timestamptz)<=%s "
                "ORDER BY GREATEST(e.next_due_at,e.last_scheduler_inspected_at),e.entity_key "
                "FOR UPDATE OF e SKIP LOCKED LIMIT %s",
                (
                    synthetic,
                    checked_at,
                    checked_at,
                    config.min_link_confidence,
                    checked_at,
                    synthetic,
                    checked_at,
                    sorted(SOURCES),
                    checked_at,
                    config.max_candidate_scan,
                ),
            ).fetchall()
            if len(entity_rows) == config.max_candidate_scan:
                warnings.add(("all", "all", "candidate_scan_limit_reached"))
            if not config.discovery_queries:
                warnings.add(("all", "all", "discovery_not_configured"))
            active_entities = {
                row["entity_key"]
                for row in connection.execute(
                    "SELECT schedule_reason->>'entity_key' AS entity_key FROM collection_tasks "
                    "WHERE synthetic=%s AND state IN ('pending','leased') "
                    "AND schedule_reason ? 'entity_key'",
                    (synthetic,),
                ).fetchall()
            }
            deferred_count = 0
            deferred_candidates: list[dict[str, Any]] = []

            def defer(entity: dict, category: str) -> None:
                nonlocal deferred_count
                detail = {
                    "category": category,
                    "checked_at": checked_at.isoformat(),
                    "retry_after": (
                        checked_at + timedelta(seconds=config.candidate_retry_seconds)
                    ).isoformat(),
                    "original_due_at": entity["next_due_at"].isoformat(),
                    "policy_id": policy_id,
                }
                connection.execute(
                    "UPDATE entities SET schedule_reason=schedule_reason || %s,"
                    "last_scheduler_inspected_at=%s "
                    "WHERE entity_key=%s AND synthetic=%s",
                    (Jsonb({"deferred": detail}), checked_at, entity["entity_key"], synthetic),
                )
                deferred_count += 1
                if len(deferred_candidates) < 100:
                    deferred_candidates.append({"entity_key": entity["entity_key"], **detail})

            candidates = []
            for entity in entity_rows:
                source, account = entity["source"], entity["account_id"]
                if not allowed(source, account):
                    defer(entity, "source_unavailable")
                    continue
                try:
                    refresh = self._refresh_request(entity, synthetic)
                except ValueError:
                    warnings.add((source, account, "refresh_origin_invalid"))
                    defer(entity, "refresh_origin_invalid")
                    continue
                if refresh is None:
                    warnings.add((source, account, "refresh_origin_unavailable"))
                    defer(entity, "refresh_origin_unavailable")
                    continue
                operation, payload = refresh
                if entity["entity_key"] in active_entities:
                    defer(entity, "refresh_already_pending")
                    continue
                score = entity["assessment_score"]
                previous_inputs = entity["schedule_reason"].get("priority_inputs", {})
                base_priority = previous_inputs.get("base_priority", entity["priority"])
                hint = float(score if score is not None else base_priority)
                if not math.isfinite(hint):
                    hint = 0.0
                hint = min(100.0, max(0.0, hint))
                tier = (
                    "hot"
                    if hint >= config.hot_threshold
                    else "monitoring"
                    if hint >= config.monitoring_threshold
                    else "exploration"
                )
                interval = config.intervals_hours[tier]
                overdue = max(0.0, (checked_at - entity["next_due_at"]).total_seconds() / 3600)
                uncertainty = 1.0 if score is None else 0.0
                request_cost = config.request_costs.get(source, 1.0)
                priority = (
                    hint + min(100.0, overdue / interval * 10) + uncertainty * 10
                ) / request_cost
                candidates.append(
                    {
                        "entity": entity,
                        "operation": operation,
                        "payload": payload,
                        "tier": tier,
                        "interval_hours": interval,
                        "priority": priority,
                        "inputs": {
                            "assessment_score": score,
                            "previous_priority": entity["priority"],
                            "base_priority": base_priority,
                            "overdue_hours": overdue,
                            "uncertainty": uncertainty,
                            "estimated_request_cost": request_cost,
                        },
                    }
                )
            query_history = connection.execute(
                "SELECT schedule_reason->>'query_id' AS query_id,"
                "max(created_at) AS last_created_at,"
                "bool_or(state IN ('pending','leased')) AS active FROM collection_tasks "
                "WHERE synthetic=%s AND schedule_reason->>'query_id'=ANY(%s) "
                "GROUP BY schedule_reason->>'query_id'",
                (synthetic, [query.query_id for query in config.discovery_queries]),
            ).fetchall()
            query_state = {row["query_id"]: row for row in query_history}
            queries = []
            for query in config.discovery_queries:
                if not allowed(query.source, query.account_id, discovery=True):
                    continue
                self._validate_workspace_payload(query.payload, synthetic)
                prior = query_state.get(query.query_id)
                if prior and (
                    prior["active"]
                    or prior["last_created_at"] + timedelta(hours=query.interval_hours) > checked_at
                ):
                    continue
                queries.append(
                    (query, prior["last_created_at"] if prior else datetime.min.replace(tzinfo=UTC))
                )
            queries.sort(key=lambda item: (item[1], item[0].query_id))
            candidates.sort(
                key=lambda item: (
                    -item["priority"],
                    item["entity"]["next_due_at"],
                    item["entity"]["entity_key"],
                )
            )
            low = sorted(
                (item for item in candidates if item["tier"] == "exploration"),
                key=lambda item: (
                    item["entity"]["next_due_at"],
                    item["priority"],
                    item["entity"]["entity_key"],
                ),
            )
            slots = min(effective_budget, math.ceil(effective_budget * config.exploration_fraction))
            query_slots = slots if not low else (slots + 1) // 2
            if slots == 1 and queries and low:
                # With a single available exploration slot, alternate discovery and
                # low-priority monitoring by their last actual scheduling time.
                previous = connection.execute(
                    "SELECT schedule_reason->>'bucket' AS bucket,max(created_at) AS at "
                    "FROM collection_tasks WHERE synthetic=%s AND schedule_reason->>'bucket' "
                    "IN ('exploration_query','exploration_entity') "
                    "GROUP BY schedule_reason->>'bucket'",
                    (synthetic,),
                ).fetchall()
                activity = {row["bucket"]: row["at"] for row in previous}
                minimum = datetime.min.replace(tzinfo=UTC)
                if activity.get("exploration_query", minimum) > activity.get(
                    "exploration_entity", minimum
                ):
                    query_slots = 0
            selected_queries = queries[:query_slots]
            low_slots = max(0, slots - len(selected_queries))
            selected_entities = [(item, "exploration_entity") for item in low[:low_slots]]
            chosen_keys = {item["entity"]["entity_key"] for item, _ in selected_entities}
            remaining = effective_budget - len(selected_queries) - len(selected_entities)
            for item in candidates:
                if remaining <= 0:
                    break
                if item["entity"]["entity_key"] not in chosen_keys:
                    selected_entities.append((item, "priority_refresh"))
                    chosen_keys.add(item["entity"]["entity_key"])
                    remaining -= 1
            if remaining > 0:
                selected_queries.extend(
                    queries[len(selected_queries) : len(selected_queries) + remaining]
                )
            decisions = []
            for query, _ in selected_queries:
                reason = {
                    "policy_id": policy_id,
                    "policy_version": config.version,
                    "query_id": query.query_id,
                    "bucket": "exploration_query",
                    "tier": "exploration",
                    "scope": "explicit_discovery_query",
                    "planned_interval_hours": query.interval_hours,
                    "scheduled_at": checked_at.isoformat(),
                    "execution_priority": 1000.0,
                }
                task_id = self._insert(
                    connection,
                    source=query.source,
                    account_id=query.account_id,
                    operation=query.operation,
                    payload=query.payload,
                    synthetic=synthetic,
                    priority=1000,
                    key={
                        "query_id": query.query_id,
                        "previous": query_state.get(query.query_id, {}).get("last_created_at"),
                    },
                    reason=reason,
                )
                decisions.append({"task_id": task_id, **reason})
            for candidate, bucket in selected_entities:
                entity = candidate["entity"]
                reason = {
                    "policy_id": policy_id,
                    "policy_version": config.version,
                    "entity_key": entity["entity_key"],
                    "bucket": bucket,
                    "tier": candidate["tier"],
                    "scope": "known_entity_refresh",
                    "priority_inputs": candidate["inputs"],
                    "planned_interval_hours": candidate["interval_hours"],
                    "scheduled_at": checked_at.isoformat(),
                    "execution_priority": candidate["priority"]
                    + (1000 if bucket == "exploration_entity" else 0),
                }
                task_id = self._insert(
                    connection,
                    source=entity["source"],
                    account_id=entity["account_id"],
                    operation=candidate["operation"],
                    payload=candidate["payload"],
                    synthetic=synthetic,
                    priority=reason["execution_priority"],
                    key={"entity_key": entity["entity_key"], "due_at": entity["next_due_at"]},
                    reason=reason,
                )
                connection.execute(
                    "UPDATE entities SET next_due_at=%s,priority=%s,tier=%s,schedule_reason=%s "
                    "WHERE entity_key=%s AND synthetic=%s",
                    (
                        checked_at + timedelta(hours=candidate["interval_hours"]),
                        candidate["priority"],
                        candidate["tier"],
                        Jsonb(reason),
                        entity["entity_key"],
                        synthetic,
                    ),
                )
                decisions.append({"task_id": task_id, **reason})
            return {
                "scheduled": len(decisions),
                "task_ids": [row["task_id"] for row in decisions],
                "decisions": decisions,
                "budget": budget,
                "effective_budget": effective_budget,
                "synthetic": synthetic,
                "policy_id": policy_id,
                "policy": configuration,
                "checked_at": checked_at.isoformat(),
                "scanned_candidates": len(entity_rows),
                "deferred_count": deferred_count,
                "deferred_candidates": deferred_candidates,
                "deferred_candidates_truncated": deferred_count > len(deferred_candidates),
                "exploration_slots": slots,
                "exploration_scheduled": sum(
                    row["bucket"].startswith("exploration_") for row in decisions
                ),
                "refresh_scheduled": len(selected_entities),
                "discovery_scheduled": len(selected_queries),
                "warnings": [
                    {"source": source, "account_id": account, "category": category}
                    for source, account, category in sorted(warnings)
                ],
                "coverage_scope": "configured_queries_and_known_entities_only",
            }

    @staticmethod
    def _validate_workspace_payload(payload: dict, synthetic: bool) -> None:
        _reject_secrets(payload)
        if not synthetic and any(
            key in payload for key in ("endpoint_override", "observed_at", "fixture_observed_at")
        ):
            raise ValueError("Fixture overrides cannot enter a live scheduling policy")

    @staticmethod
    def _refresh_request(entity: dict, synthetic: bool) -> tuple[str, dict] | None:
        source = entity["source"]
        if source == "tiktok_display" and entity["entity_type"] == "video":
            request: dict[str, Any] = {
                "filters": {"video_ids": [entity["entity_key"].split(":", 2)[-1]]}
            }
            origin = (entity.get("origin_manifest") or {}).get("task_request") or {}
            if synthetic:
                for key in ("endpoint_override", "fixture_observed_at"):
                    if key in origin.get("payload", {}):
                        request[key] = origin["payload"][key]
                if origin.get("endpoint_override"):
                    request["endpoint_override"] = origin["endpoint_override"]
            return "query", request
        origin = (entity.get("origin_manifest") or {}).get("task_request") or {}
        if source not in {"tiktok_ads", "tiktok_commercial"} or origin.get("operation") != "query":
            return None
        if (
            origin.get("source") != source
            or str(origin.get("account_id", "")) != entity["account_id"]
        ):
            return None
        payload = dict(origin.get("payload", {}))
        payload.pop("search_id", None)
        payload.pop("cursor", None)
        Scheduler._validate_workspace_payload(payload, synthetic)
        if synthetic and origin.get("endpoint_override"):
            payload["endpoint_override"] = origin["endpoint_override"]
        return "query", payload

    @staticmethod
    def _insert(
        connection: Any,
        *,
        source: str,
        account_id: str,
        operation: str,
        payload: dict,
        synthetic: bool,
        priority: float,
        key: dict,
        reason: dict,
    ) -> str:
        task_id = digest(
            {
                "source": source,
                "account_id": account_id,
                "operation": operation,
                "synthetic": synthetic,
                "scheduling_key": key,
            }
        )
        connection.execute(
            "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,"
            "synthetic,due_at,priority,schedule_reason) "
            "VALUES(%s,%s,%s,%s,%s,%s,clock_timestamp(),%s,%s) ON CONFLICT DO NOTHING",
            (
                task_id,
                source,
                account_id,
                operation,
                Jsonb(payload),
                synthetic,
                priority,
                Jsonb(reason),
            ),
        )
        return task_id
