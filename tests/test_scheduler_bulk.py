"""Grouped planning preserves each member's policy and workspace across write batches."""

from datetime import timedelta

import pytest
from test_scheduler import catalog as catalog

from sm_trending_products.contracts import utc
from sm_trending_products.scheduler import Scheduler

pytestmark = pytest.mark.integration


def test_more_than_one_write_batch_keeps_member_schedules_and_deferrals(catalog):
    catalog.configure_budget("tiktok_display", "creator", 1000, 1000)
    catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,priority) "
        "SELECT 'tiktok_display:video:bulk-'||i,true,'tiktok_display','creator','video',90 "
        "FROM generate_series(1,1001) AS s(i)"
    )
    catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,priority) "
        "VALUES('tiktok_display:video:bulk-1',false,'tiktok_display','creator','video',12)"
    )
    sentinel = catalog.one("SELECT * FROM entities WHERE NOT synthetic")
    scheduled = Scheduler(catalog).schedule(100, synthetic=True)
    assert scheduled["scheduled"] == 51
    assert scheduled["refresh_entities_scheduled"] == 1001
    rows = catalog.all("SELECT * FROM entities WHERE synthetic")
    tasks = {
        row["task_id"]: row for row in catalog.all("SELECT * FROM collection_tasks WHERE synthetic")
    }
    for row in rows:
        reason = row["schedule_reason"]
        task = tasks[reason["task_id"]]
        member = next(
            item
            for item in task["schedule_reason"]["entity_reasons"]
            if item["entity_key"] == row["entity_key"]
        )
        assert reason == {**member, "task_id": task["task_id"]}
        assert row["tier"] == "hot"
        assert row["next_due_at"] == utc(scheduled["checked_at"]) + timedelta(hours=1)
        assert row["priority"] == member["execution_priority"]
    assert catalog.one("SELECT * FROM entities WHERE NOT synthetic") == sentinel

    catalog.execute("UPDATE entities SET next_due_at=clock_timestamp()-interval '1 hour'")
    original = {row["entity_key"]: row["schedule_reason"] for row in rows}
    sentinel = catalog.one("SELECT * FROM entities WHERE NOT synthetic")
    deferred = Scheduler(catalog).schedule(100, synthetic=True)
    assert deferred["scheduled"] == 0 and deferred["deferred_count"] == 1001
    assert deferred["deferred_candidates_truncated"] is True
    for row in catalog.all("SELECT * FROM entities WHERE synthetic"):
        reason = row["schedule_reason"]
        detail = reason.pop("deferred")
        assert detail["category"] == "refresh_already_pending"
        assert reason == original[row["entity_key"]]
        assert row["last_scheduler_inspected_at"] == utc(deferred["checked_at"])
    assert catalog.one("SELECT * FROM entities WHERE NOT synthetic") == sentinel
