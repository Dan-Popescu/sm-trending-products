"""Scheduling behavior at the public policy and PostgreSQL queue boundary."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator

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
    assert result["scheduled"] == 10
    assert result["exploration_scheduled"] >= 2
    assert result["refresh_scheduled"] > 0
    assert len(set(result["task_ids"])) == 10
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
    assert first["scheduled"] == 2
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
