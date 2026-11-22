"""Failure settlement owns the task lease and shared source mutations together."""

import pytest
from psycopg.errors import RaiseException

from sm_trending_products.collector import CollectionError

pytestmark = pytest.mark.integration
SOURCE = "tiktok_display"
ACCOUNT = "fixture-account"


@pytest.mark.parametrize("mode", ["legacy", "capture"])
@pytest.mark.parametrize("category", ["rate_limited", "access_denied"])
@pytest.mark.parametrize("case", ["current_owner", "lease_replaced", "settlement_rolled_back"])
def test_collection_failure_settles_shared_source_effects_with_task(
    pipeline, monkeypatch, mode, category, case
):
    # The access-denial case exercises live-mode policy handling entirely inside
    # a fixture schema. Both collector effects below are controlled local calls.
    synthetic = category == "rate_limited"
    catalog = pipeline.catalog
    if not synthetic:
        with catalog.transaction() as connection:
            connection.execute(
                "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,30,true)",
                (SOURCE,),
            )
            connection.execute(
                "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
                "VALUES(%s,%s,'collect','available','{}')",
                (SOURCE, ACCOUNT),
            )
    else:
        catalog.one("SELECT * FROM reserve_request(%s,%s,'fixture-shared-key')", (SOURCE, ACCOUNT))

    if case == "settlement_rolled_back":
        catalog.execute(
            "CREATE FUNCTION refuse_failure_settlement() RETURNS trigger LANGUAGE plpgsql "
            "AS $$ BEGIN RAISE EXCEPTION 'Controlled failure settlement rollback'; END $$"
        )
        catalog.execute(
            "CREATE TRIGGER refuse_failure_settlement BEFORE UPDATE OF last_error "
            "ON collection_tasks FOR EACH ROW "
            "WHEN (NEW.last_error IN ('rate_limited','access_denied')) "
            "EXECUTE FUNCTION refuse_failure_settlement()"
        )

    task_id = pipeline.enqueue(SOURCE, ACCOUNT, "list", {}, synthetic=synthetic)

    def shared_source_state():
        return (
            catalog.all(
                "SELECT blocked_until FROM request_budgets WHERE source=%s AND account_id=%s",
                (SOURCE, ACCOUNT),
            ),
            catalog.all("SELECT blocked_until FROM credential_budgets"),
            catalog.all("SELECT state FROM source_capabilities ORDER BY id"),
        )

    original = shared_source_state()
    calls = []

    def controlled_failure(*args, **kwargs):
        calls.append(category)
        if case == "lease_replaced":
            catalog.execute(
                "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE task_id=%s",
                (task_id,),
            )
            assert catalog.claim("replacement", 180, synthetic, 8)["task_id"] == task_id
        raise CollectionError(category, 60)

    collector_call, batch = (
        ("run", pipeline.collect_batch) if mode == "legacy" else ("capture", pipeline.capture_batch)
    )
    monkeypatch.setattr(pipeline.collector, collector_call, controlled_failure)
    if case == "settlement_rolled_back":
        with pytest.raises(RaiseException, match="Controlled failure settlement rollback"):
            batch(1, synthetic=synthetic)
    else:
        result = batch(1, synthetic=synthetic)
        expected_error = "lease_lost" if case == "lease_replaced" else category
        assert result["errors"] == [{"task_id": task_id, "category": expected_error}]

    assert calls == [category]
    task = catalog.one(
        "SELECT state,lease_owner,last_error,attempts,capture_attempt "
        "FROM collection_tasks WHERE task_id=%s",
        (task_id,),
    )
    if case == "current_owner":
        limits, credentials, capabilities = shared_source_state()
        if synthetic:
            assert limits[0]["blocked_until"] is not None
            assert credentials == limits
            assert capabilities == original[2]
        else:
            assert (limits, credentials) == original[:2]
            assert capabilities == [{"state": "available"}, {"state": "denied"}]
        assert task == {
            "state": "pending" if synthetic else "quarantined",
            "lease_owner": None,
            "last_error": category,
            "attempts": 0 if synthetic else 1,
            "capture_attempt": 0,
        }
    else:
        assert shared_source_state() == original
        replaced = case == "lease_replaced"
        assert task == {
            "state": "leased",
            "lease_owner": "replacement" if replaced else pipeline.worker_id,
            "last_error": None,
            "attempts": 2 if replaced else 1,
            "capture_attempt": 0,
        }
