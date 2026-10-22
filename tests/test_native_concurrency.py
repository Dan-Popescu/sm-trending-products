"""Native PostgreSQL contention checks; embedded SQL cannot certify these semantics."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
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
