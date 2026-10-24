"""Catalog-backed replacement qualification from actual committed raw captures."""

from __future__ import annotations

from datetime import timedelta

from .catalog import Catalog
from .contracts import now, utc


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
        fresh = max(start, cutoff - timedelta(seconds=self.freshness_seconds))
        with self.catalog.transaction() as connection:
            unqualified = connection.execute(
                "SELECT 1 FROM capture_obligations WHERE synthetic=%s "
                "AND (original_due_at>=%s OR durable_at>=%s OR capture_dataset_id IS NULL) "
                "AND original_due_at<=%s "
                "AND (contract_id IS NULL OR deadline_at IS NULL) LIMIT 1",
                (synthetic, start, start, cutoff),
            ).fetchone()
            if unqualified:
                return None
            required = connection.execute(
                "SELECT DISTINCT source,account_id,operation,scope_id FROM capture_obligations "
                "WHERE synthetic=%s "
                "AND (original_due_at>=%s OR durable_at>=%s OR capture_dataset_id IS NULL) "
                "AND original_due_at<=%s "
                "ORDER BY source,account_id,operation,scope_id LIMIT %s",
                (synthetic, start, start, cutoff, self.scope_limit + 1),
            ).fetchall()
            if not required or len(required) > self.scope_limit:
                return None
            breaches = connection.execute(
                "SELECT count(*) AS count FROM capture_obligations o LEFT JOIN datasets d "
                "ON d.dataset_id=o.capture_dataset_id AND d.synthetic=o.synthetic "
                "AND d.source=o.source WHERE o.synthetic=%s "
                "AND (o.original_due_at>=%s OR o.durable_at>=%s OR o.capture_dataset_id IS NULL) "
                "AND o.deadline_at<%s AND (o.captured_at IS NULL OR o.captured_at>o.deadline_at "
                "OR o.durable_at IS NULL OR d.state IS DISTINCT FROM 'committed')",
                (synthetic, start, start, cutoff),
            ).fetchone()
            if not breaches or breaches["count"]:
                return None
            captures = connection.execute(
                "SELECT o.source,o.account_id,o.operation,o.scope_id,o.obligation_id,"
                "o.capture_dataset_id,o.captured_at,o.durable_at,d.manifest "
                "FROM capture_obligations o JOIN datasets d "
                "ON d.dataset_id=o.capture_dataset_id AND d.synthetic=o.synthetic "
                "AND d.source=o.source "
                "WHERE o.synthetic=%s AND o.original_due_at>=%s AND o.captured_at>=%s "
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
                "ORDER BY o.captured_at DESC,o.obligation_id LIMIT %s",
                (
                    synthetic,
                    start,
                    fresh,
                    cutoff,
                    cutoff,
                    worker_id,
                    str(generation),
                    self.scope_limit * 5,
                ),
            ).fetchall()
        keys = ("source", "account_id", "operation", "scope_id")
        covered = {tuple(row[key] for key in keys) for row in captures}
        expected = {tuple(row[key] for key in keys) for row in required}
        if not expected <= covered:
            return None
        return {
            "method": "committed_raw_capture_obligations",
            "worker_id": worker_id,
            "worker_generation": generation,
            "checked_at": cutoff.isoformat(),
            "qualified_scope_count": len(expected),
            "deadline_breaches": 0,
            "capture_dataset_ids": sorted({row["capture_dataset_id"] for row in captures}),
            "obligation_ids": sorted({row["obligation_id"] for row in captures}),
        }
