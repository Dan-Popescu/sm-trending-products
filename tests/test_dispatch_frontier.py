"""Native dispatch keeps quota and lease fencing across a large pending frontier."""

from concurrent.futures import ThreadPoolExecutor

import pytest
from test_native_concurrency import native_catalog as native_catalog

pytestmark = pytest.mark.integration


def test_large_dispatch_frontier_skips_future_blocked_locked_and_exhausted_work(native_catalog):
    catalog = native_catalog
    for source, account in (
        ("tiktok_display", "healthy"),
        ("tiktok_display", "blocked-account"),
        ("tiktok_ads", "blocked-credential"),
    ):
        catalog.configure_budget(source, account, 1000, 1000)
    catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
        "due_at,priority) SELECT 'backlog-'||i,'tiktok_display','healthy','query','{}',false,"
        "clock_timestamp()-interval '1 hour',1 FROM generate_series(1,10000) AS s(i)"
    )
    with catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
            "due_at,priority,state,attempts,lease_until) VALUES "
            "('first','tiktok_display','healthy','query','{}',false,clock_timestamp(),90,"
            "'pending',0,NULL),"
            "('second','tiktok_display','healthy','query','{}',false,clock_timestamp(),80,"
            "'pending',0,NULL),"
            "('future','tiktok_display','healthy','query','{}',false,"
            "clock_timestamp()+interval '1 hour',1000,'pending',0,NULL),"
            "('active','tiktok_display','healthy','query','{}',false,clock_timestamp(),900,"
            "'leased',0,clock_timestamp()+interval '1 hour'),"
            "('exhausted','tiktok_display','healthy','query','{}',false,clock_timestamp(),800,"
            "'pending',3,NULL),"
            "('expired-exhausted','tiktok_display','healthy','query','{}',false,clock_timestamp(),"
            "790,'leased',3,clock_timestamp()-interval '1 hour'),"
            "('blocked','tiktok_display','blocked-account','query','{}',false,clock_timestamp(),"
            "700,'pending',0,NULL),"
            "('credential','tiktok_ads','blocked-credential','query','{}',false,clock_timestamp(),"
            "600,'pending',0,NULL),"
            "('other-workspace','tiktok_display','healthy','query','{}',true,clock_timestamp(),"
            "2000,'pending',0,NULL)"
        )
        connection.execute(
            "UPDATE request_budgets SET blocked_until=clock_timestamp()+interval '1 hour' "
            "WHERE account_id='blocked-account'"
        )
        connection.execute(
            "INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens,"
            "blocked_until) VALUES('current-blocked',1000,1000,1000,"
            "clock_timestamp()+interval '1 hour')"
        )
    preserved = catalog.all(
        "SELECT * FROM collection_tasks WHERE task_id IN "
        "('future','active','blocked','credential','other-workspace') ORDER BY task_id"
    )
    hints = {"tiktok_ads": "current-blocked"}
    with ThreadPoolExecutor(max_workers=1) as executor:
        with catalog.transaction() as connection:
            connection.execute(
                "SELECT task_id FROM collection_tasks WHERE task_id='first' FOR UPDATE"
            )
            selected = executor.submit(
                catalog.claim, "second-worker", 300, False, 3, credential_keys=hints
            ).result(timeout=5)
            assert selected["task_id"] == "second"
    first = catalog.claim("first-worker", 300, False, 3, credential_keys=hints)
    assert first["task_id"] == "first"
    assert first["attempts"] == first["lease_version"] == 1
    assert (
        catalog.all(
            "SELECT * FROM collection_tasks WHERE task_id IN "
            "('future','active','blocked','credential','other-workspace') ORDER BY task_id"
        )
        == preserved
    )
    exhausted = catalog.all(
        "SELECT state,last_error,lease_owner,lease_until FROM collection_tasks "
        "WHERE task_id IN ('exhausted','expired-exhausted')"
    )
    assert len(exhausted) == 2
    assert all(
        row
        == {
            "state": "quarantined",
            "last_error": "attempt_limit",
            "lease_owner": None,
            "lease_until": None,
        }
        for row in exhausted
    )
    assert catalog.claim("backlog-worker", 300, False, 3, credential_keys=hints)[
        "task_id"
    ].startswith("backlog-")


def test_dispatch_index_upgrade_preserves_existing_tasks_and_quota_consumption(native_catalog):
    catalog = native_catalog
    catalog.configure_budget("tiktok_display", "account", 10, 0.001)
    catalog.one("SELECT * FROM reserve_request('tiktok_display','account')")
    catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic,"
        "attempts,priority) VALUES('before-upgrade','tiktok_display','account','query','{}',"
        "true,2,90)"
    )
    before = catalog.one("SELECT * FROM collection_tasks WHERE task_id='before-upgrade'")
    budget = catalog.one("SELECT * FROM request_budgets")
    catalog.execute("DROP INDEX collection_active_dispatch")
    catalog.execute("DROP INDEX collection_attempt_frontier")
    catalog.execute("DELETE FROM schema_migrations WHERE version=18")
    catalog.initialize()
    catalog.initialize()
    assert catalog.one("SELECT * FROM collection_tasks WHERE task_id='before-upgrade'") == before
    assert catalog.one("SELECT * FROM request_budgets") == budget
    assert catalog.claim("upgraded-worker", 300, True, 3)["task_id"] == "before-upgrade"
