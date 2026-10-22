"""Native PostgreSQL contention checks; embedded SQL cannot certify these semantics."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from sm_trending_products.catalog import Catalog
from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration
WORKERS = 8


@pytest.fixture
def native_catalog(sql_engine) -> Iterator[Catalog]:
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip(
            "Requires native PostgreSQL backends: PGLite multiplexes one backend and cannot "
            "certify concurrent row locks, shared credential limits, or SKIP LOCKED claims"
        )
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.fail("Native concurrency checks require an isolated TEST_DATABASE_URL")
    schema = "native_concurrency_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    catalog = Catalog(
        make_conninfo(database_url, options=f"-csearch_path={schema}"), max_connections=WORKERS
    )
    try:
        catalog.initialize()
        yield catalog
    finally:
        catalog.close()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_shared_credential_quota_is_strict_across_parallel_account_aliases(native_catalog):
    capacity = 7
    accounts = ("account-alias-a", "account-alias-b")
    for account in accounts:
        native_catalog.configure_budget("tiktok_ads", account, capacity, 0.000001)
    gate = Barrier(WORKERS, timeout=15)

    def reserve(worker: int) -> list[dict]:
        outcomes = []
        for _round in range(4):
            # Each participant owns a different real connection before contention begins.
            with native_catalog.transaction() as connection:
                gate.wait()
                outcome = connection.execute(
                    "SELECT pg_backend_pid() AS backend_pid,r.allowed,r.retry_after_seconds "
                    "FROM reserve_request(%s,%s,%s) r",
                    ("tiktok_ads", accounts[worker % len(accounts)], "one-shared-credential"),
                ).fetchone()
                assert outcome is not None
                outcomes.append(outcome)
        return outcomes

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(reserve, range(WORKERS)))
    outcomes = [outcome for worker in results for outcome in worker]
    accepted = [outcome for outcome in outcomes if outcome["allowed"]]
    rejected = [outcome for outcome in outcomes if not outcome["allowed"]]
    assert len({outcome["backend_pid"] for outcome in outcomes}) == WORKERS
    assert len(accepted) == capacity
    assert len(rejected) == WORKERS * 4 - capacity
    assert all(outcome["retry_after_seconds"] > 0 for outcome in rejected)


def test_quota_reservation_does_not_wait_for_another_alias_binding(native_catalog):
    for account in ("collecting-account", "binding-account"):
        native_catalog.configure_budget("tiktok_ads", account, 3, 0.000001)
    native_catalog.execute(
        "INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens) "
        "VALUES('established-credential',3,0.000001,3)"
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        with native_catalog.transaction() as connection:
            # A new alias holds the normal foreign-key KEY SHARE lock on its
            # established parent. Reserving tokens does not change that key.
            connection.execute(
                "INSERT INTO credential_bindings VALUES"
                "('tiktok_ads','binding-account','established-credential')"
            )
            future = pool.submit(
                native_catalog.one,
                "SELECT * FROM reserve_request('tiktok_ads','collecting-account',"
                "'established-credential')",
            )
            assert future.result(timeout=2)["allowed"] is True
    assert (
        native_catalog.one(
            "SELECT request_count FROM credential_budgets "
            "WHERE credential_key='established-credential'"
        )["request_count"]
        == 1
    )


def test_existing_credential_is_strict_when_many_new_aliases_bind_in_parallel(native_catalog):
    for index in range(WORKERS):
        native_catalog.configure_budget("tiktok_ads", f"new-alias-{index}", 3, 0.000001)
    native_catalog.execute(
        "INSERT INTO credential_budgets(credential_key,capacity,refill_per_second,tokens) "
        "VALUES('existing-shared-token',3,0.000001,3)"
    )
    gate = Barrier(WORKERS, timeout=15)

    def reserve(index):
        with native_catalog.transaction() as connection:
            gate.wait()
            return connection.execute(
                "SELECT * FROM reserve_request('tiktok_ads',%s,'existing-shared-token')",
                (f"new-alias-{index}",),
            ).fetchone()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        outcomes = list(pool.map(reserve, range(WORKERS)))
    assert sum(result["allowed"] for result in outcomes) == 3
    assert (
        native_catalog.one(
            "SELECT count(*) AS count FROM credential_bindings "
            "WHERE credential_key='existing-shared-token'"
        )["count"]
        == WORKERS
    )


def test_credential_lock_upgrade_preserves_existing_limits_and_consumption(native_catalog):
    original_function = files("sm_trending_products").joinpath(
        "sql", "004_credential_budgets_and_search.sql"
    )
    with native_catalog.transaction() as connection:
        connection.execute(original_function.read_text())
        connection.execute("DELETE FROM schema_migrations WHERE version=12")
    native_catalog.configure_budget("tiktok_ads", "upgrade-account", 7, 0.000001)
    assert (
        native_catalog.one(
            "SELECT * FROM reserve_request('tiktok_ads','upgrade-account','upgrade-credential')"
        )["allowed"]
        is True
    )
    native_catalog.block_budget("tiktok_ads", "upgrade-account", 120)
    tables = ("request_budgets", "credential_budgets", "credential_bindings")
    before = {table: native_catalog.all(f"SELECT * FROM {table}") for table in tables}
    native_catalog.initialize()
    native_catalog.initialize()
    assert {table: native_catalog.all(f"SELECT * FROM {table}") for table in tables} == before
    result = native_catalog.one(
        "SELECT * FROM reserve_request('tiktok_ads','upgrade-account','upgrade-credential')"
    )
    assert result["allowed"] is False
    assert 0 < result["retry_after_seconds"] <= 120


def test_parallel_workers_claim_each_due_task_once(native_catalog):
    due_at = now()
    expected = {
        native_catalog.enqueue(
            "tiktok_display",
            "authorized-account",
            "query",
            {"index": index},
            True,
            due_at=due_at,
            idempotency_key=f"native-task-{index}",
        )
        for index in range(37)
    }
    gate = Barrier(WORKERS, timeout=15)

    def claim(worker: int) -> list[dict]:
        owned = []
        owner = f"parallel-worker-{worker}"
        for _round in range(5):
            gate.wait()
            task = native_catalog.claim(owner, lease_seconds=300, synthetic=True, max_attempts=3)
            if task is not None:
                assert task["lease_owner"] == owner
                assert task["state"] == "leased"
                assert task["lease_version"] == 1
                owned.append(task)
        return owned

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(claim, range(WORKERS)))
    tasks = [task for worker in results for task in worker]
    identifiers = [task["task_id"] for task in tasks]
    assert len(identifiers) == len(expected)
    assert len(set(identifiers)) == len(expected)
    assert set(identifiers) == expected
    assert len({task["lease_owner"] for task in tasks}) == WORKERS
    assert native_catalog.claim("extra-worker", 300, True, 3) is None


@pytest.mark.parametrize("synthetic", [False, True])
@pytest.mark.parametrize("state", ["pending", "leased"])
def test_attempt_exhaustion_only_changes_the_claimed_workspace(native_catalog, synthetic, state):
    tasks = {}
    for workspace in (False, True):
        task_id = native_catalog.enqueue(
            "tiktok_display", "workspace-account", "query", {}, workspace, due_at=now()
        )
        native_catalog.execute(
            "UPDATE collection_tasks SET attempts=3,state=%s,lease_owner='old-worker',"
            "lease_until=clock_timestamp()-interval '1 second' WHERE task_id=%s",
            (state, task_id),
        )
        tasks[workspace] = task_id
    other_before = native_catalog.one(
        "SELECT * FROM collection_tasks WHERE task_id=%s", (tasks[not synthetic],)
    )
    active = native_catalog.enqueue(
        "tiktok_display", "active-account", "query", {}, synthetic, due_at=now()
    )
    native_catalog.execute(
        "UPDATE collection_tasks SET attempts=3,state='leased',lease_owner='active-worker',"
        "lease_until=clock_timestamp()+interval '5 minutes' WHERE task_id=%s",
        (active,),
    )
    active_before = native_catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (active,))
    assert native_catalog.claim("isolated-worker", 300, synthetic, 3) is None
    selected = native_catalog.one(
        "SELECT * FROM collection_tasks WHERE task_id=%s", (tasks[synthetic],)
    )
    assert selected["state"] == "quarantined"
    assert selected["last_error"] == "attempt_limit"
    assert selected["lease_owner"] is None
    assert (
        native_catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (active,))
        == active_before
    )
    assert (
        native_catalog.one(
            "SELECT * FROM collection_tasks WHERE task_id=%s", (tasks[not synthetic],)
        )
        == other_before
    )


def test_claim_skips_locked_exhausted_work_and_reaches_other_due_tasks(native_catalog):
    exhausted = native_catalog.enqueue(
        "tiktok_display", "account", "query", {"old": True}, True, due_at=now()
    )
    due = native_catalog.enqueue(
        "tiktok_display", "account", "query", {"new": True}, True, due_at=now()
    )
    native_catalog.execute("UPDATE collection_tasks SET attempts=3 WHERE task_id=%s", (exhausted,))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with native_catalog.transaction() as connection:
            connection.execute(
                "SELECT task_id FROM collection_tasks WHERE task_id=%s FOR UPDATE", (exhausted,)
            )
            future = pool.submit(native_catalog.claim, "available-worker", 300, True, 3)
            claimed = future.result(timeout=2)
    assert claimed["task_id"] == due
    assert claimed["state"] == "leased"
    assert native_catalog.claim("cleanup-worker", 300, True, 3) is None
    assert (
        native_catalog.one("SELECT state FROM collection_tasks WHERE task_id=%s", (exhausted,))[
            "state"
        ]
        == "quarantined"
    )
