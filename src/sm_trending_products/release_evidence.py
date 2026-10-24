"""Catalog-backed replacement qualification from actual committed raw captures."""

from __future__ import annotations

from .catalog import Catalog
from .contracts import now, utc

# Certificates are validated and made immutable by migration035. A retired old
# diagnostic attempt cannot withdraw an independently active retry of the page.
ACTIVE_OBLIGATION = (
    "NOT EXISTS(SELECT 1 FROM capture_spool_retirements r "
    "JOIN collection_tasks t ON t.task_id=r.task_id "
    "WHERE r.task_id=o.task_id AND r.page_index=o.page_index "
    "AND r.source=o.source AND r.account_id=o.account_id AND r.synthetic=o.synthetic "
    "AND r.retired_at<=%s AND ((o.capture_dataset_id IS NOT NULL "
    "AND r.capture_id=o.capture_dataset_id) OR (o.capture_dataset_id IS NULL "
    "AND t.state='cancelled' AND t.page_index=r.page_index "
    "AND t.capture_attempt=r.capture_attempt)))"
)


class CatalogCaptureVerifier:
    """A process heartbeat, old capture or adopting worker is never capture proof.

    Qualification waits for naturally due captures. A fresh controller can safely
    remain blocked with both versions alive until the source supplies due work.
    Rows are bounded; unsupported oversized scope sets remain unqualified.
    """

    def __init__(self, catalog: Catalog, *, freshness_seconds: int = 300, scope_limit: int = 200):
        if not 1 <= freshness_seconds <= 3600 or not 1 <= scope_limit <= 200:
            raise ValueError("Invalid release qualification bounds")
        self.catalog = catalog
        self.freshness_seconds, self.scope_limit = freshness_seconds, scope_limit

    def verify(self, *, worker_id: str, generation: int, since, synthetic: bool) -> dict | None:
        cutoff = now()
        start = utc(since)
        with self.catalog.transaction() as connection:
            unqualified = connection.execute(
                "SELECT 1 FROM capture_obligations o WHERE o.synthetic=%s "
                "AND (o.original_due_at>=%s OR o.durable_at>=%s OR o.capture_dataset_id IS NULL) "
                "AND o.original_due_at<=%s AND (o.contract_id IS NULL OR o.deadline_at IS NULL) "
                "AND " + ACTIVE_OBLIGATION + " LIMIT 1",
                (synthetic, start, start, cutoff, cutoff),
            ).fetchone()
            if unqualified:
                return None
            required = connection.execute(
                "SELECT DISTINCT o.source,o.account_id,o.operation,o.scope_id "
                "FROM capture_obligations o WHERE o.synthetic=%s "
                "AND (o.original_due_at>=%s OR o.durable_at>=%s OR o.capture_dataset_id IS NULL) "
                "AND o.original_due_at<=%s AND "
                + ACTIVE_OBLIGATION
                + " ORDER BY o.source,o.account_id,o.operation,o.scope_id LIMIT %s",
                (synthetic, start, start, cutoff, cutoff, self.scope_limit + 1),
            ).fetchall()
            policies = connection.execute(
                "SELECT DISTINCT ON(source,account_id,operation,scope_id) "
                "source,account_id,operation,scope_id,contract_id FROM capture_policies "
                "WHERE synthetic=%s AND created_at<=%s "
                "ORDER BY source,account_id,operation,scope_id,created_at DESC,contract_id DESC "
                "LIMIT %s",
                (synthetic, cutoff, self.scope_limit + 1),
            ).fetchall()
            if (not required and not policies) or max(
                len(required), len(policies)
            ) > self.scope_limit:
                return None
            breaches = connection.execute(
                "SELECT count(*) AS count FROM capture_obligations o LEFT JOIN datasets d "
                "ON d.dataset_id=o.capture_dataset_id AND d.synthetic=o.synthetic "
                "AND d.source=o.source WHERE o.synthetic=%s "
                "AND (o.original_due_at>=%s OR o.durable_at>=%s OR o.capture_dataset_id IS NULL) "
                "AND o.deadline_at<%s AND (o.captured_at IS NULL OR o.captured_at>o.deadline_at "
                "OR o.durable_at IS NULL OR d.state IS DISTINCT FROM 'committed') "
                "AND " + ACTIVE_OBLIGATION,
                (synthetic, start, start, cutoff, cutoff),
            ).fetchone()
            if not breaches or breaches["count"]:
                return None
            captures = connection.execute(
                "SELECT DISTINCT ON(o.source,o.account_id,o.operation,o.scope_id,o.contract_id) "
                "o.source,o.account_id,o.operation,o.scope_id,o.contract_id,o.obligation_id,"
                "o.capture_dataset_id,o.captured_at,o.durable_at,d.manifest "
                "FROM capture_obligations o JOIN datasets d "
                "ON d.dataset_id=o.capture_dataset_id AND d.synthetic=o.synthetic "
                "AND d.source=o.source "
                "JOIN capture_policies p ON p.contract_id=o.contract_id "
                "WHERE o.synthetic=%s AND o.captured_at>=%s "
                "-GREATEST(%s,(p.payload->>'cadence_seconds')::double precision+"
                "(p.payload->>'lateness_seconds')::double precision)*interval '1 second' "
                "AND o.captured_at>=%s "
                "AND o.durable_at IS NOT NULL AND o.durable_at<=%s "
                "AND o.captured_at<=%s "
                "AND o.captured_at<=o.deadline_at AND d.state='committed' "
                "AND d.quarantine_count=0 "
                "AND d.manifest->>'kind'='raw_capture' "
                "AND d.manifest->>'acquisition_protocol'='1' "
                "AND d.manifest#>>'{capture,worker_id}'=%s "
                "AND d.manifest#>>'{capture,worker_generation}'=%s "
                "AND d.manifest#>>'{capture,task_id}'=o.task_id "
                "AND d.manifest#>>'{capture,page_index}'=o.page_index::text "
                "AND "
                + ACTIVE_OBLIGATION
                + " ORDER BY o.source,o.account_id,o.operation,o.scope_id,o.contract_id,"
                "o.captured_at DESC,o.obligation_id LIMIT %s",
                (
                    synthetic,
                    cutoff,
                    self.freshness_seconds,
                    start,
                    cutoff,
                    cutoff,
                    worker_id,
                    str(generation),
                    cutoff,
                    self.scope_limit + 1,
                ),
            ).fetchall()
        keys = ("source", "account_id", "operation", "scope_id")
        covered = {tuple(row[key] for key in keys) for row in captures}
        expected = {tuple(row[key] for key in keys) for row in required}
        if not expected <= covered:
            return None
        if len(captures) > self.scope_limit:
            return None
        for policy in policies:
            if not any(
                all(
                    capture[key] == policy[key]
                    for key in ("source", "account_id", "operation", "contract_id")
                )
                and (policy["scope_id"] == "*" or capture["scope_id"] == policy["scope_id"])
                for capture in captures
            ):
                return None
        return {
            "method": "committed_raw_capture_obligations",
            "worker_id": worker_id,
            "worker_generation": generation,
            "checked_at": cutoff.isoformat(),
            "qualified_scope_count": len(expected),
            "registered_policy_count": len(policies),
            "deadline_breaches": 0,
            "capture_dataset_ids": sorted({row["capture_dataset_id"] for row in captures}),
            "obligation_ids": sorted({row["obligation_id"] for row in captures}),
        }
