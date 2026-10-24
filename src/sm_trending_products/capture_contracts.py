"""Original capture obligations and conservative, quota-aware pause assessment.

An admission assessment is conditional on the declared measured catch-up rate;
it never grants source access or replaces final atomic request reservations.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Literal

import psycopg
from psycopg.types.json import Jsonb
from pydantic import Field, model_validator

from .catalog import Catalog
from .collector import redact
from .contracts import Contract, digest, utc
from .source_contracts import canonical_operation, credential_keys

# A diagnostic attempt's retirement does not withdraw a later physical attempt
# of the same original window. A missing receipt needs exact cancelled ownership;
# an acknowledged receipt needs its exact retired capture identity.
_WITHDRAWAL_JOIN = (
    "LEFT JOIN LATERAL (SELECT r.* FROM capture_spool_retirements r "
    "JOIN collection_tasks t ON t.task_id=r.task_id "
    "WHERE r.task_id=o.task_id AND r.page_index=o.page_index "
    "AND r.source=o.source AND r.account_id=o.account_id AND r.synthetic=o.synthetic "
    "AND t.operation=o.operation AND r.retired_at<=%s "
    "AND (r.capture_id=o.capture_dataset_id OR (o.capture_dataset_id IS NULL "
    "AND t.state='cancelled' AND t.page_index=r.page_index "
    "AND t.capture_attempt=r.capture_attempt)) "
    "ORDER BY r.retired_at DESC,r.capture_id LIMIT 1) w ON true "
)


class CapturePolicy(Contract):
    source: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    operation: str
    scope_id: str = Field(default="*", min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    cadence_seconds: float = Field(gt=0, le=315_360_000)
    lateness_seconds: float = Field(default=0, ge=0, le=315_360_000)
    requests_per_interval: int = Field(default=1, ge=1, le=1_000_000, strict=True)
    recoverability: Literal["unknown", "snapshot", "ephemeral", "historical"] = "unknown"
    replay_horizon_seconds: float | None = Field(default=None, gt=0, le=315_360_000)
    cursor_lifetime_seconds: float | None = Field(default=None, gt=0, le=315_360_000)
    verification_ref: str | None = Field(default=None, min_length=1, max_length=1000)
    verified_at: datetime | None = None

    @model_validator(mode="after")
    def validate_contract(self):
        if canonical_operation(self.source, self.operation) != self.operation:
            raise ValueError("Capture policy requires the canonical source operation")
        if self.verified_at is not None:
            utc(self.verified_at)
        if self.recoverability == "historical" and (
            self.replay_horizon_seconds is None
            or self.verification_ref is None
            or self.verified_at is None
        ):
            raise ValueError("Historical replay requires a verified horizon and evidence")
        return self


class CaptureContracts:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def register(self, policy: CapturePolicy, *, synthetic: bool = False) -> str:
        payload = policy.model_dump(mode="json")
        if redact(payload) != payload:
            raise ValueError("Capture policy must not contain credentials")
        identity = digest(
            {
                **{
                    key: payload[key]
                    for key in ("source", "account_id", "operation", "scope_id", "version")
                },
                "synthetic": synthetic,
            }
        )
        with self.catalog.transaction() as connection:
            measured = connection.execute("SELECT clock_timestamp() AS time").fetchone()
            if measured is None:
                raise RuntimeError("Database did not provide its clock")
            clock = measured["time"]
            if policy.verified_at is not None and utc(policy.verified_at) > clock:
                raise ValueError("Capture verification cannot be in the future")
            connection.execute(
                "INSERT INTO capture_policies(contract_id,source,account_id,operation,scope_id,"
                "synthetic,version,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    identity,
                    policy.source,
                    policy.account_id,
                    policy.operation,
                    policy.scope_id,
                    synthetic,
                    policy.version,
                    Jsonb(payload),
                ),
            )
            existing = connection.execute(
                "SELECT payload FROM capture_policies WHERE contract_id=%s", (identity,)
            ).fetchone()
            if existing is None or existing["payload"] != payload:
                raise ValueError("Capture policy versions are immutable")
        return identity

    def declare(self, task_id: str, page_index: int | None = None) -> dict:
        row = self.catalog.one(
            "SELECT o.* FROM capture_obligations o JOIN collection_tasks t USING(task_id) "
            "WHERE o.task_id=%s AND o.page_index=COALESCE(%s,t.page_index)",
            (task_id, page_index),
        )
        if row is None:
            raise ValueError("No admitted capture obligation for this task and page")
        return row

    def acknowledge_capture(self, task_id: str, page_index: int, dataset_id: str) -> dict:
        try:
            self.catalog.execute(
                "SELECT acknowledge_raw_capture(%s,%s,%s)", (task_id, page_index, dataset_id)
            )
        except psycopg.Error as error:
            raise ValueError("A committed matching raw capture is required") from error
        return self.declare(task_id, page_index)

    def status(self, *, as_of: datetime, synthetic: bool, limit: int = 200) -> dict:
        if isinstance(limit, bool) or not 1 <= limit <= 200:
            raise ValueError("Capture status limit must be between 1 and 200")
        cutoff = utc(as_of)
        rows = self.catalog.all(
            "SELECT o.*,d.state AS dataset_state,p.version AS contract_version,"
            "CASE WHEN w.capture_id IS NOT NULL THEN to_jsonb(w) END AS withdrawal "
            "FROM capture_obligations o LEFT JOIN datasets d "
            "ON d.dataset_id=o.capture_dataset_id LEFT JOIN capture_policies p USING(contract_id) "
            + _WITHDRAWAL_JOIN
            + "WHERE o.synthetic=%s AND o.original_due_at<=%s AND o.created_at<=%s "
            "ORDER BY o.original_due_at,o.obligation_id LIMIT %s",
            (cutoff, synthetic, cutoff, cutoff, limit),
        )
        for row in rows:
            captured = row["durable_at"] is not None and row["durable_at"] <= cutoff
            qualified = row["deadline_at"] is not None
            row["capture_status"] = (
                "not_captured"
                if not captured
                else "captured_unqualified"
                if not qualified
                else "captured_late"
                if row["captured_at"] > row["deadline_at"]
                else "captured"
            )
            row["status"] = (
                "authorized_withdrawal"
                if row["withdrawal"] is not None
                else "retired"
                if captured and row["dataset_state"] != "committed"
                else "captured_unqualified"
                if captured and not qualified
                else "unqualified"
                if not qualified
                else "captured_late"
                if captured and row["captured_at"] > row["deadline_at"]
                else "captured"
                if captured
                else "overdue"
                if row["deadline_at"] < cutoff
                else "pending"
            )
            row["late_seconds"] = (
                max(
                    0.0,
                    (
                        (
                            row["captured_at"]
                            if captured
                            else utc(row["withdrawal"]["retired_at"])
                            if row["withdrawal"] is not None
                            else cutoff
                        )
                        - row["deadline_at"]
                    ).total_seconds(),
                )
                if qualified
                else None
            )
            if not captured:
                for key in ("capture_dataset_id", "captured_at", "durable_at", "dataset_state"):
                    row[key] = None
        return {"as_of": cutoff, "synthetic": synthetic, "obligations": rows}

    def assess_pause(
        self,
        *,
        as_of: datetime,
        synthetic: bool,
        pause_seconds: float,
        rollback_seconds: float,
        safety_seconds: float,
        catchup_requests_per_second: float,
        request_budget: int,
    ) -> dict:
        values = (pause_seconds, rollback_seconds, safety_seconds, catchup_requests_per_second)
        if any(
            isinstance(value, bool) or not math.isfinite(value) or value < 0 for value in values
        ):
            raise ValueError("Pause timing and catch-up rate must be finite nonnegative numbers")
        if (
            isinstance(request_budget, bool)
            or not isinstance(request_budget, int)
            or request_budget < 0
        ):
            raise ValueError("Request budget must be a nonnegative integer")
        cutoff = utc(as_of)
        if pause_seconds == rollback_seconds == 0:
            return {
                "allowed": True,
                "mode": "continuous",
                "requires_live_replacement": True,
                "basis": "no_acquisition_blackout_permitted",
                "as_of": cutoff,
                "synthetic": synthetic,
                "reasons": [],
                "required_requests": 0,
                "catchup_seconds": 0,
                "required_seconds": 0,
            }
        reasons: set[str] = set()
        policies = self.catalog.all(
            "SELECT DISTINCT ON(source,account_id,operation,scope_id) * FROM capture_policies "
            "WHERE synthetic=%s AND created_at<=%s "
            "ORDER BY source,account_id,operation,scope_id,created_at DESC,contract_id DESC",
            (synthetic, cutoff),
        )
        if not policies:
            reasons.add("source_contract_missing")
        pending = self.catalog.all(
            "SELECT o.contract_id,min(o.original_due_at) AS oldest_due_at,count(*) AS count,"
            "bool_or(o.due_origin='legacy_created_bound') AS unknown_window "
            "FROM capture_obligations o LEFT JOIN datasets d ON d.dataset_id=o.capture_dataset_id "
            + _WITHDRAWAL_JOIN
            + "WHERE o.synthetic=%s AND o.original_due_at<=%s AND o.created_at<=%s "
            "AND w.capture_id IS NULL "
            "AND (o.durable_at IS NULL OR o.durable_at>%s "
            "OR d.state IS DISTINCT FROM 'committed') GROUP BY o.contract_id",
            (cutoff, synthetic, cutoff, cutoff, cutoff),
        )
        by_id = {row["contract_id"]: row for row in policies}
        # Frozen older contracts constrain their pending windows even after a new
        # version is registered; policy updates never rewrite the obligation.
        for item in pending:
            if item["contract_id"] is None:
                reasons.add("unknown_recoverability")
            elif item["contract_id"] not in by_id:
                old = self.catalog.one(
                    "SELECT * FROM capture_policies WHERE contract_id=%s", (item["contract_id"],)
                )
                if old:
                    by_id[old["contract_id"]] = old
            if item["unknown_window"]:
                reasons.add("original_window_unknown")
        parsed = {
            identity: CapturePolicy.model_validate(row["payload"])
            for identity, row in by_id.items()
        }
        for policy in parsed.values():
            if policy.recoverability != "historical":
                reasons.add(
                    "unknown_recoverability"
                    if policy.recoverability == "unknown"
                    else "non_replayable_source"
                )
        latest = [CapturePolicy.model_validate(row["payload"]) for row in policies]
        arrival_rate = sum(
            policy.requests_per_interval / policy.cadence_seconds for policy in latest
        )
        rate, quota_wait, quota_reasons = self._quota_rate(
            latest, synthetic, cutoff, catchup_requests_per_second
        )
        reasons.update(quota_reasons)
        blackout = pause_seconds + rollback_seconds + quota_wait
        ratios = [blackout / policy.cadence_seconds for policy in latest]
        if not all(
            math.isfinite(value)
            for value in (blackout, blackout + safety_seconds, arrival_rate, *ratios)
        ):
            raise ValueError("Pause arithmetic exceeds the supported finite range")
        # Include one burst at every scope's interval boundary, even when the
        # pause is shorter than its cadence; future work cannot be priced as zero.
        pending_requests = sum(
            item["count"]
            * (
                parsed[item["contract_id"]].requests_per_interval
                if item["contract_id"] in parsed
                else 1
            )
            for item in pending
        )
        new_requests = sum(
            policy.requests_per_interval * (math.ceil(blackout / policy.cadence_seconds) + 1)
            for policy in latest
        )
        net_rate = rate - arrival_rate
        catchup = (pending_requests + new_requests) / net_rate if net_rate > 0 else None
        if catchup is not None and not math.isfinite(catchup):
            catchup = None
        if catchup is None and (parsed or pending):
            reasons.add("catchup_capacity_insufficient")
        required_requests = math.ceil(rate * catchup) if catchup is not None else None
        if required_requests is not None and required_requests > request_budget:
            reasons.add("insufficient_request_budget")
        needed = blackout + safety_seconds + (catchup or 0)
        oldest = {item["contract_id"]: item["oldest_due_at"] for item in pending}
        remaining = []
        for identity, policy in parsed.items():
            if policy.recoverability != "historical":
                continue
            horizon = min(
                value
                for value in (policy.replay_horizon_seconds, policy.cursor_lifetime_seconds)
                if value is not None
            )
            remaining.append(
                horizon - max(0, (cutoff - oldest.get(identity, cutoff)).total_seconds())
            )
        if remaining and min(remaining) <= needed:
            reasons.add("replay_window_exhausted")
        if not synthetic and self.catalog.one(
            "SELECT 1 FROM (SELECT DISTINCT ON(source,account_id) * FROM source_capabilities "
            "WHERE capability='collect' AND available_at<=%s "
            "ORDER BY source,account_id,available_at DESC,id DESC) c WHERE c.capability='collect' "
            "AND c.state='available' AND c.available_at<=%s AND NOT EXISTS("
            "SELECT 1 FROM capture_policies p WHERE p.source=c.source "
            "AND p.account_id=c.account_id AND NOT p.synthetic AND p.created_at<=%s) LIMIT 1",
            (cutoff, cutoff, cutoff),
        ):
            reasons.add("source_contract_missing")
        return {
            "allowed": not reasons,
            "mode": "bounded_pause",
            "requires_live_replacement": False,
            "as_of": cutoff,
            "synthetic": synthetic,
            "basis": "declared_catchup_rate_capped_by_catalog",
            "reasons": sorted(reasons),
            "required_requests": required_requests,
            "catchup_seconds": catchup,
            "oldest_uncaptured_due_at": min(oldest.values()) if oldest else None,
            "replay_seconds_remaining": min(remaining) if remaining else None,
            "required_seconds": needed,
            "quota_wait_seconds": quota_wait,
            "effective_requests_per_second": rate,
            "arrival_requests_per_second": arrival_rate,
        }

    def _quota_rate(self, policies, synthetic, cutoff, declared_rate):
        rate, wait, reasons = declared_rate, 0.0, set()
        current = credential_keys(synthetic)
        for source, account in sorted({(p.source, p.account_id) for p in policies}):
            budget = self.catalog.one(
                "SELECT * FROM request_budgets WHERE source=%s AND account_id=%s", (source, account)
            )
            budgets = [budget] if budget else []
            if budget is None:
                reasons.add("request_budget_unknown")
            if not synthetic:
                access = self.catalog.one(
                    "SELECT c.state,p.verified FROM source_capabilities c "
                    "LEFT JOIN source_policies p USING(source) WHERE c.source=%s "
                    "AND c.account_id=%s AND c.capability='collect' AND c.available_at<=%s "
                    "ORDER BY c.available_at DESC,c.id DESC LIMIT 1",
                    (source, account, cutoff),
                )
                if not access or access["state"] != "available" or not access["verified"]:
                    reasons.add("source_access_unverified")
                credential = self.catalog.one(
                    "SELECT * FROM credential_budgets WHERE credential_key=%s",
                    (current.get(source),),
                )
                if credential is None:
                    reasons.add("credential_budget_unverified")
                else:
                    budgets.append(credential)
            for item in budgets:
                rate = min(rate, item["refill_per_second"])
                available = min(
                    item["capacity"],
                    item["tokens"]
                    + max(0, (cutoff - item["updated_at"]).total_seconds())
                    * item["refill_per_second"],
                )
                wait = max(wait, (1 - available) / item["refill_per_second"], 0)
                if item["blocked_until"]:
                    wait = max(wait, (item["blocked_until"] - cutoff).total_seconds(), 0)
        return rate, wait, reasons
