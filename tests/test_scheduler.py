"""Scheduling behavior at the public policy and PostgreSQL queue boundary."""

from __future__ import annotations

import math
import os
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg.conninfo import make_conninfo


def test_policy_rejects_unbounded_or_unsupported_collection() -> None:
    from sm_trending_products.scheduler import SchedulerPolicy

    with pytest.raises(ValueError):
        SchedulerPolicy.model_validate({"exploration_fraction": 0})
    with pytest.raises(ValueError):
        SchedulerPolicy.model_validate(
            {
                "discovery_queries": [
                    {
                        "source": "research_video",
                        "account_id": "a",
                        "operation": "query",
                        "payload": {},
                    }
                ]
            }
        )
    with pytest.raises(ValueError):
        SchedulerPolicy.model_validate(
            {
                "discovery_queries": [
                    {
                        "source": "tiktok_display",
                        "account_id": "a",
                        "operation": "query",
                        "payload": {},
                    }
                ]
            }
        )


@pytest.mark.integration
def test_reactivating_a_policy_persists_without_rewriting_its_first_availability(catalog):
    from sm_trending_products.scheduler import Scheduler

    scheduler = Scheduler(catalog)
    first = scheduler.schedule(0, synthetic=True, policy={"version": "policy-a"})
    original = catalog.one(
        "SELECT available_at FROM scheduler_policies WHERE policy_id=%s", (first["policy_id"],)
    )
    scheduler.schedule(0, synthetic=True, policy={"version": "policy-b"})
    scheduler.schedule(0, synthetic=True, policy={"version": "policy-a"})
    restored = scheduler.schedule(0, synthetic=True)
    assert restored["policy"]["version"] == "policy-a"
    assert (
        catalog.one(
            "SELECT available_at FROM scheduler_policies WHERE policy_id=%s", (first["policy_id"],)
        )
        == original
    )
    assert catalog.one("SELECT count(*) AS count FROM scheduler_policy_activations")["count"] == 3
    # Activating the live workspace cannot change the synthetic selection.
    scheduler.schedule(0, synthetic=False, policy={"version": "live-policy"})
    assert scheduler.schedule(0, synthetic=True)["policy"]["version"] == "policy-a"


@pytest.mark.integration
def test_request_cost_estimate_affects_persisted_priority(catalog):
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,priority) "
        "VALUES('tiktok_display:video:costed',true,'tiktok_display','creator','video',90)"
    )
    result = Scheduler(catalog).schedule(
        1, synthetic=True, policy={"request_costs": {"tiktok_display": 2}}
    )
    reason = result["decisions"][0]
    inputs = reason["priority_inputs"]
    assert inputs["estimated_request_cost"] == 2
    expected = (90 + min(100, inputs["overdue_hours"] * 10) + 10) / 2
    assert reason["execution_priority"] == pytest.approx(expected)


@pytest.mark.integration
@pytest.mark.parametrize("request_cost", [0.01, 5e-324])
def test_cost_scaling_preserves_exploration_at_the_collector_claim_boundary(catalog, request_cost):
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for index in range(10):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,priority) "
            "VALUES(%s,true,'tiktok_display','creator','video',%s)",
            (f"tiktok_display:video:cost-{index}", 90 if index < 9 else 0),
        )
    result = Scheduler(catalog).schedule(
        10,
        synthetic=True,
        policy={
            "request_costs": {"tiktok_display": request_cost},
            "discovery_queries": [
                {"source": "tiktok_display", "account_id": "creator", "operation": "list"}
            ],
        },
    )
    assert result["scheduled"] == 3
    assert result["refresh_entities_scheduled"] == 10
    assert result["exploration_scheduled"] == 2
    claims = [catalog.claim(f"cost-worker-{index}", 60, True, 3) for index in range(3)]
    assert {task["schedule_reason"]["bucket"] for task in claims[:2]} == {
        "exploration_query",
        "exploration_entity",
    }
    assert all(task["schedule_reason"]["bucket"] == "priority_refresh" for task in claims[2:])
    assert all(math.isfinite(task["priority"]) for task in claims)


@pytest.mark.integration
def test_parallel_schedulers_do_not_duplicate_or_lose_the_due_frontier(catalog):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Scheduler contention requires independent native PostgreSQL backends")
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for index in range(40):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
            "VALUES(%s,true,'tiktok_display','creator','video')",
            (f"tiktok_display:video:parallel-{index}",),
        )
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(lambda _: Scheduler(catalog).schedule(10, synthetic=True), range(4))
        )
    task_ids = [task for result in results for task in result["task_ids"]]
    assert len(task_ids) == len(set(task_ids)) == 2
    assert sum(result["refresh_entities_scheduled"] for result in results) == 40
    tasks = catalog.all("SELECT schedule_reason,payload FROM collection_tasks")
    assert all(len(task["payload"]["filters"]["video_ids"]) == 20 for task in tasks)
    assert len({key for task in tasks for key in task["schedule_reason"]["entity_keys"]}) == 40


@pytest.fixture
def catalog() -> Iterator[object]:
    from sm_trending_products.catalog import Catalog

    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for real PostgreSQL scheduler scenarios")
    schema = "scheduler_test_" + uuid.uuid4().hex
    with psycopg.connect(url, autocommit=True) as connection:
        connection.execute(
            psycopg.sql.SQL("CREATE SCHEMA {}").format(psycopg.sql.Identifier(schema))
        )
    database = Catalog(make_conninfo(url, options=f"-csearch_path={schema}"))
    database.initialize()
    try:
        yield database
    finally:
        database.close()
        with psycopg.connect(url, autocommit=True) as connection:
            connection.execute(
                psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(psycopg.sql.Identifier(schema))
            )


@pytest.mark.integration
def test_scheduler_preserves_exploration_and_does_not_duplicate_due_work(catalog: object) -> None:
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for index in range(40):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,"
            "priority,next_due_at) VALUES(%s,true,%s,%s,%s,%s,"
            "clock_timestamp()-interval '1 day')",
            (
                f"tiktok_display:video:v{index}",
                "tiktok_display",
                "creator",
                "video",
                99 if index < 35 else 0,
            ),
        )
    result = Scheduler(catalog).schedule(10, synthetic=True)
    assert result["scheduled"] == 3
    assert result["exploration_scheduled"] >= 1
    assert result["refresh_scheduled"] == 3
    assert result["refresh_entities_scheduled"] == 40
    assert len(set(result["task_ids"])) == 3
    repeated = Scheduler(catalog).schedule(10, synthetic=True)
    assert set(repeated["task_ids"]).isdisjoint(result["task_ids"])
    assert all(decision["policy_id"] == result["policy_id"] for decision in result["decisions"])


@pytest.mark.integration
def test_discovery_is_due_based_and_shares_protected_exploration(catalog: object) -> None:
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for index in range(5):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
            "VALUES(%s,true,%s,%s,%s)",
            (f"tiktok_display:video:old{index}", "tiktok_display", "creator", "video"),
        )
    policy = {
        "discovery_queries": [
            {
                "source": "tiktok_display",
                "account_id": "creator",
                "operation": "list",
                "payload": {"max_count": 20},
                "interval_hours": 24,
            }
        ]
    }
    result = Scheduler(catalog).schedule(5, synthetic=True, policy=policy)
    assert result["discovery_scheduled"] == 1
    assert result["exploration_scheduled"] >= 1
    query_id = next(
        decision["query_id"] for decision in result["decisions"] if "query_id" in decision
    )
    repeated = Scheduler(catalog).schedule(5, synthetic=True)
    assert not any(decision.get("query_id") == query_id for decision in repeated["decisions"])
    task = catalog.claim("scheduler-test-worker", 60, True, 3)
    assert task["schedule_reason"]["bucket"].startswith("exploration_")


@pytest.mark.integration
def test_live_discovery_requires_verified_policy_and_separate_capability(catalog: object) -> None:
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    policy = {
        "discovery_queries": [
            {
                "source": "tiktok_display",
                "account_id": "creator",
                "operation": "list",
                "payload": {"max_count": 20},
            }
        ]
    }
    missing = Scheduler(catalog).schedule(10, policy=policy)
    assert missing["scheduled"] == 0
    assert any(warning["category"] == "source_policy_unverified" for warning in missing["warnings"])
    catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('tiktok_display',7,true)"
    )
    catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state) "
        "VALUES('tiktok_display','creator','collect','available')"
    )
    still_missing = Scheduler(catalog).schedule(10)
    assert still_missing["discovery_scheduled"] == 0
    assert any(
        warning["category"] == "discovery_unavailable" for warning in still_missing["warnings"]
    )
    catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state) "
        "VALUES('tiktok_display','creator','discovery','available')"
    )
    accepted = Scheduler(catalog).schedule(10)
    assert accepted["discovery_scheduled"] == 1
    with pytest.raises(ValueError, match="Fixture overrides"):
        Scheduler(catalog).schedule(
            10,
            policy={
                "discovery_queries": [
                    {
                        "source": "tiktok_display",
                        "account_id": "creator",
                        "operation": "list",
                        "payload": {"endpoint_override": "http://127.0.0.1:8000"},
                    }
                ]
            },
        )


@pytest.mark.integration
def test_pending_queue_backpressure_is_visible(catalog: object) -> None:
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for index in range(4):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
            "VALUES(%s,true,%s,%s,%s)",
            (f"tiktok_display:video:v{index}", "tiktok_display", "creator", "video"),
        )
    first = Scheduler(catalog).schedule(10, synthetic=True, policy={"max_pending_tasks": 2})
    assert first["scheduled"] == 1
    # Saturation is measured in physical tasks, not the four entities they cover.
    catalog.execute(
        "INSERT INTO collection_tasks(task_id,source,account_id,operation,payload,synthetic) "
        "VALUES('other-pending','tiktok_display','creator','list','{}',true)"
    )
    second = Scheduler(catalog).schedule(10, synthetic=True)
    assert second["scheduled"] == 0
    assert second["effective_budget"] == 0
    assert any(
        warning["category"] == "pending_task_limit_reached" for warning in second["warnings"]
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("alternative_status", "alternative_confidence", "simultaneous", "expected_score"),
    [
        ("rejected", 1.0, False, 99),
        ("confirmed", 0.2, False, None),
        ("confirmed", 0.2, True, None),
    ],
)
def test_refresh_priority_uses_unique_latest_owner_before_confidence(
    catalog: object,
    alternative_status: str,
    alternative_confidence: float,
    simultaneous: bool,
    expected_score: int | None,
) -> None:
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", capacity=100, refill_per_second=1)
    for product_id in ["owner", "alternative"]:
        catalog.execute(
            "INSERT INTO products(product_id,name,synthetic) VALUES(%s,%s,true)",
            (product_id, product_id),
        )
    catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
        "VALUES('tiktok_display:video:v1',true,'tiktok_display','creator','video')"
    )
    catalog.execute(
        "INSERT INTO product_links(decision_id,product_id,entity_key,status,confidence,method) "
        "VALUES('confirmed','owner','tiktok_display:video:v1','confirmed',1,'manual')"
    )
    catalog.execute(
        "INSERT INTO product_links(decision_id,product_id,entity_key,status,confidence,"
        "method,available_at) SELECT 'alternative','alternative',entity_key,%s,%s,'manual',"
        "available_at+%s*interval '1 microsecond' FROM product_links WHERE decision_id='confirmed'",
        (alternative_status, alternative_confidence, 0 if simultaneous else 1),
    )
    catalog.execute(
        "INSERT INTO assessment_runs(run_id,as_of,synthetic,policy,object_key,evidence_datasets) "
        "VALUES('run',clock_timestamp(),true,'{}','unused','[]')"
    )
    catalog.execute(
        "INSERT INTO current_assessments(product_id,run_id,synthetic,score,payload) "
        "VALUES('owner','run',true,99,'{}')"
    )
    result = Scheduler(catalog).schedule(1, synthetic=True)
    assert result["scheduled"] == 1
    assert result["decisions"][0]["tier"] == (
        "hot" if expected_score is not None else "exploration"
    )
    assert result["decisions"][0]["priority_inputs"]["assessment_score"] == expected_score


@pytest.mark.integration
def test_bounded_scan_progresses_past_unrefreshable_old_candidates_and_retries_them(
    pipeline: object,
) -> None:
    from psycopg.types.json import Jsonb

    for source, account in [("tiktok_ads", "older-account"), ("tiktok_display", "later-account")]:
        pipeline.catalog.configure_budget(source, account, 100, 1)
    for key, source, account, kind, overdue_days in [
        ("tiktok_ads:ad:old", "tiktok_ads", "older-account", "ad", 2),
        ("tiktok_display:video:new", "tiktok_display", "later-account", "video", 1),
    ]:
        pipeline.catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,next_due_at) "
            "VALUES(%s,true,%s,%s,%s,clock_timestamp()-%s*interval '1 day')",
            (key, source, account, kind, overdue_days),
        )
    original_due = pipeline.catalog.one(
        "SELECT next_due_at FROM entities WHERE entity_key='tiktok_ads:ad:old' AND synthetic"
    )["next_due_at"]
    first = pipeline.schedule(
        1, synthetic=True, policy={"max_candidate_scan": 1, "candidate_retry_seconds": 1}
    )
    assert first["scheduled"] == 0
    assert first["deferred_count"] == 1
    assert first["deferred_candidates"][0]["category"] == "refresh_origin_unavailable"
    # Even after backoff expires, the scan frontier must reach later due work.
    time.sleep(1.1)
    second = pipeline.schedule(1, synthetic=True)
    assert second["scheduled"] == 1
    assert second["decisions"][0]["entity_key"] == "tiktok_display:video:new"
    assert (
        pipeline.catalog.one(
            "SELECT next_due_at FROM entities WHERE entity_key='tiktok_ads:ad:old' AND synthetic"
        )["next_due_at"]
        == original_due
    )

    # Repair the missing refresh origin. It must be retried, not silently dropped.
    pipeline.catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
        "VALUES('repaired-origin','tiktok_ads',true,'committed',%s)",
        (
            Jsonb(
                {
                    "task_request": {
                        "source": "tiktok_ads",
                        "account_id": "older-account",
                        "operation": "query",
                        "payload": {"filters": {"country_code": "FR"}},
                    }
                }
            ),
        ),
    )
    pipeline.catalog.execute(
        "UPDATE entities SET origin=%s WHERE entity_key='tiktok_ads:ad:old' AND synthetic",
        (Jsonb({"dataset_id": "repaired-origin"}),),
    )
    retried = pipeline.schedule(1, synthetic=True)
    assert retried["scheduled"] == 1
    assert retried["decisions"][0]["entity_key"] == "tiktok_ads:ad:old"


def test_legacy_commercial_list_origin_uses_canonical_refresh():
    from sm_trending_products.scheduler import Scheduler

    for source, kind in [("tiktok_ads", "ad"), ("tiktok_commercial", "commercial_content")]:
        entity = {
            "source": source,
            "account_id": "account",
            "entity_type": kind,
            "entity_key": f"{source}:{kind}:one",
            "origin_manifest": {
                "task_request": {
                    "source": source,
                    "account_id": "account",
                    "operation": "list",
                    "payload": {
                        "filters": {"country_code": "FR"},
                        "cursor": 2,
                        "search_id": "old-page",
                    },
                }
            },
        }
        assert Scheduler._refresh_request(entity, False) == (
            "query",
            {"filters": {"country_code": "FR"}},
        )


@pytest.mark.integration
def test_full_queue_preserves_policy_without_scanning_or_deferring_entities(catalog):
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", 100, 1)
    catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
        "VALUES('tiktok_display:video:full',true,'tiktok_display','creator','video')"
    )
    first = Scheduler(catalog).schedule(1, synthetic=True, policy={"max_pending_tasks": 1})
    assert first["scheduled"] == 1
    catalog.execute("UPDATE entities SET next_due_at=clock_timestamp()-interval '1 day'")
    before = catalog.one("SELECT last_scheduler_inspected_at FROM entities")
    result = Scheduler(catalog).schedule(
        100, synthetic=True, policy={"version": "full-queue-v2", "max_pending_tasks": 1}
    )
    assert result["scheduled"] == result["scanned_candidates"] == result["deferred_count"] == 0
    assert result["policy"]["version"] == "full-queue-v2"
    entity = catalog.one("SELECT last_scheduler_inspected_at,schedule_reason FROM entities")
    assert entity["last_scheduler_inspected_at"] == before["last_scheduler_inspected_at"]
    assert "deferred" not in entity["schedule_reason"]


@pytest.mark.integration
def test_display_refresh_budget_counts_physical_requests_and_covers_every_id(catalog):
    from sm_trending_products.scheduler import Scheduler

    catalog.configure_budget("tiktok_display", "creator", 100, 1)
    for index in range(40):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type) "
            "VALUES(%s,true,'tiktok_display','creator','video')",
            (f"tiktok_display:video:grouped-{index}",),
        )
    first = Scheduler(catalog).schedule(1, synthetic=True)
    second = Scheduler(catalog).schedule(1, synthetic=True)
    assert first["scheduled"] == second["scheduled"] == 1
    assert first["refresh_entities_scheduled"] == second["refresh_entities_scheduled"] == 20
    tasks = catalog.all("SELECT payload,schedule_reason FROM collection_tasks")
    assert len(tasks) == 2
    assert {key for task in tasks for key in task["schedule_reason"]["entity_keys"]} == {
        f"tiktok_display:video:grouped-{index}" for index in range(40)
    }
    for task in tasks:
        assert len(task["payload"]["filters"]["video_ids"]) == 20
        assert len(task["schedule_reason"]["entity_reasons"]) == 20
    assert Scheduler(catalog).schedule(10, synthetic=True)["scheduled"] == 0
    # A still-running grouped task protects every member, not just its representative.
    catalog.execute("UPDATE entities SET next_due_at=clock_timestamp()-interval '1 hour'")
    active = Scheduler(catalog).schedule(10, synthetic=True)
    assert active["scheduled"] == 0
    assert active["deferred_count"] == 40


@pytest.mark.integration
def test_commercial_refreshes_share_query_cycle_and_keep_entity_intentions(catalog):
    from psycopg.types.json import Jsonb

    from sm_trending_products.scheduler import Scheduler

    source, account = "tiktok_ads", "commercial-account"
    catalog.configure_budget(source, account, 100, 1)
    payload = {"filters": {"country_code": "FR"}}
    catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
        "VALUES('commercial-origin',%s,true,'committed',%s)",
        (
            source,
            Jsonb(
                {
                    "task_request": {
                        "source": source,
                        "account_id": account,
                        "operation": "list",
                        "payload": {
                            **payload,
                            "search_id": "previous-page",
                            "cursor": 2,
                        },
                    }
                }
            ),
        ),
    )
    for index in range(10):
        catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,account_id,entity_type,"
            "origin,priority) "
            "VALUES(%s,true,%s,%s,'ad',%s,%s)",
            (
                f"tiktok_ads:ad:grouped-{index}",
                source,
                account,
                Jsonb({"dataset_id": "commercial-origin"}),
                index * 10,
            ),
        )
    result = Scheduler(catalog).schedule(
        2,
        synthetic=True,
        policy={
            "discovery_queries": [
                {
                    "source": source,
                    "account_id": account,
                    "operation": "query",
                    "payload": payload,
                }
            ]
        },
    )
    assert result["scheduled"] == 1
    assert result["refresh_scheduled"] == result["discovery_scheduled"] == 1
    assert result["refresh_entities_scheduled"] == 10
    task = catalog.one("SELECT * FROM collection_tasks")
    assert task["payload"] == payload
    assert task["operation"] == "query"
    assert task["schedule_reason"]["bucket"] == "exploration_query"
    intentions = task["schedule_reason"]["entity_reasons"]
    assert len(intentions) == 10
    assert {reason["tier"] for reason in intentions} == {"hot", "monitoring", "exploration"}
    assert all(
        "priority_inputs" in reason and "planned_interval_hours" in reason for reason in intentions
    )
    assert Scheduler(catalog).schedule(2, synthetic=True)["scheduled"] == 0
    # Completing a cycle and making entities due again permits a new refresh;
    # request fingerprints alone must never suppress a legitimate later cycle.
    catalog.execute("UPDATE collection_tasks SET state='done'")
    catalog.execute("UPDATE entities SET next_due_at=clock_timestamp()-interval '1 hour'")
    next_cycle = Scheduler(catalog).schedule(2, synthetic=True)
    assert next_cycle["scheduled"] == 1
    assert next_cycle["task_ids"] != result["task_ids"]


def test_display_grouping_keeps_fixture_contexts_and_tiers_separate():
    from copy import deepcopy
    from datetime import UTC, datetime

    from sm_trending_products.source_contracts import group_refreshes

    candidates = []
    for index, (tier, endpoint) in enumerate(
        [
            ("hot", "http://127.0.0.1:1"),
            ("exploration", "http://127.0.0.1:1"),
            ("hot", "http://127.0.0.1:2"),
            ("hot", "http://127.0.0.1:1"),
        ]
    ):
        candidates.append(
            {
                "entity": {
                    "source": "tiktok_display",
                    "account_id": "a",
                    "entity_key": f"tiktok_display:video:{index}",
                    "next_due_at": datetime(2026, 1, 1, tzinfo=UTC),
                },
                "operation": "query",
                "tier": tier,
                "priority": 1,
                "payload": {"endpoint_override": endpoint, "filters": {"video_ids": [str(index)]}},
            }
        )
    original = deepcopy(candidates)
    groups = group_refreshes(candidates)
    assert sorted(len(group["members"]) for group in groups) == [1, 1, 2]
    assert candidates == original


def test_only_current_live_credentials_are_hinted_and_never_synthetic(monkeypatch):
    import hashlib

    from sm_trending_products.source_contracts import credential_keys

    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "previous-token")
    monkeypatch.setenv("TIKTOK_CCL_ACCESS_TOKEN", "shared-commercial-token")
    previous = credential_keys(False)
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", "rotated-token")
    current = credential_keys(False)
    assert current["tiktok_display"] == hashlib.sha256(b"rotated-token").hexdigest()
    assert current["tiktok_display"] != previous["tiktok_display"]
    assert current["tiktok_ads"] == current["tiktok_commercial"]
    assert credential_keys(True) == {}
    monkeypatch.delenv("TIKTOK_DISPLAY_ACCESS_TOKEN")
    assert "tiktok_display" not in credential_keys(False)
