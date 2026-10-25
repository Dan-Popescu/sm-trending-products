"""Pending publication is a disposable projection of authoritative datasets."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from threading import current_thread
from uuid import uuid4

import pytest
from test_raw_capture import datasets, enqueue, one_shot_pages, page, service

from sm_trending_products.raw_capture import _publication_claim_sql

pytestmark = pytest.mark.integration


def pending(pipeline):
    return pipeline.catalog.all(
        "SELECT dataset_id FROM capture_publication_pending ORDER BY dataset_id"
    )


def capture_pages(pipeline, count=1):
    with one_shot_pages(
        [page(str(index), more=index < count - 1, cursor=index + 1) for index in range(count)]
    ) as (endpoint, _):
        enqueue(pipeline, endpoint)
        assert service(pipeline).capture_batch(count, True)["processed"] == count
    return datasets(pipeline)


def remove_expansion(pipeline):
    with pipeline.catalog.transaction() as connection:
        connection.execute("DROP TRIGGER capture_publication_pending_changes ON datasets")
        connection.execute("DROP FUNCTION maintain_capture_publication_pending()")
        connection.execute("DROP FUNCTION refresh_capture_publication_pending(text)")
        connection.execute("DROP TABLE capture_publication_pending")
        connection.execute("DROP INDEX capture_committed_normalization_parent")
        connection.execute(
            "DROP TRIGGER capture_publication_takeover_guard ON capture_publication_leases"
        )
        connection.execute("DROP FUNCTION guard_capture_publication_takeover()")
        connection.execute("DELETE FROM schema_migrations WHERE version=37")


def test_frontier_follows_committed_child_not_lease_cleanup(pipeline):
    parent = capture_pages(pipeline)[0]
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    acquisition = service(pipeline)
    lease = acquisition._claim_publication(True)
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    acquisition.replay.replay(
        parent["dataset_id"], publication_guard=acquisition._publication_guard(lease)
    )
    assert pending(pipeline) == []
    assert pipeline.catalog.one("SELECT 1 FROM capture_publication_leases")
    assert service(pipeline, "other")._claim_publication(True) is None


def test_legacy_replay_and_child_retirement_update_frontier_without_new_worker(pipeline):
    parent = capture_pages(pipeline)[0]
    acquisition = service(pipeline)
    child_id = acquisition.replay.replay(parent["dataset_id"])["dataset_id"]
    assert pending(pipeline) == []
    # The source-free legacy replay path owns no new publisher lease. Database
    # mutation hooks must maintain eligibility even for that old-image writer.
    assert pipeline.catalog.one("SELECT 1 FROM capture_publication_leases") is None
    pipeline.catalog.execute(
        "UPDATE datasets SET state='invalidated' WHERE dataset_id=%s", (child_id,)
    )
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    pipeline.catalog.invalidate_dataset(parent["dataset_id"], "source_deleted")
    assert pending(pipeline) == []


def test_v2_child_does_not_complete_v1_and_staged_child_is_not_completion(pipeline):
    parent = capture_pages(pipeline)[0]
    acquisition = service(pipeline)
    acquisition.replay.replay(parent["dataset_id"], "2")
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    pipeline.catalog.stage_dataset(
        "staged-normalization",
        parent["source"],
        True,
        {"transform_version": "1"},
        supersedes=parent["dataset_id"],
    )
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    assert acquisition.publish_pending(1, True)["processed"] == 1
    assert pending(pipeline) == []


def test_pending_claim_skips_locked_parent_and_preserves_retry_order(pipeline):
    parents = capture_pages(pipeline, 3)
    first, second = service(pipeline, "first"), service(pipeline, "second")
    with pipeline.catalog.transaction() as transaction:
        transaction.execute(
            "SELECT 1 FROM datasets WHERE dataset_id=%s FOR UPDATE", (parents[0]["dataset_id"],)
        )
        with ThreadPoolExecutor(1) as pool:
            lease = pool.submit(second._claim_publication, True).result(timeout=5)
        assert lease["dataset_id"] == parents[1]["dataset_id"]
    pipeline.catalog.execute(
        "UPDATE capture_publication_leases SET lease_until=clock_timestamp()-interval '1 second',"
        "retry_at=clock_timestamp()+interval '1 hour' WHERE dataset_id=%s",
        (lease["dataset_id"],),
    )
    assert first._claim_publication(True)["dataset_id"] == parents[0]["dataset_id"]
    assert first._claim_publication(True)["dataset_id"] == parents[2]["dataset_id"]
    assert first._claim_publication(True) is None


def test_schema_expansion_backfills_existing_raw_parents_and_old_retry_metadata(pipeline):
    parents = capture_pages(pipeline, 3)
    acquisition = service(pipeline)
    acquisition.replay.replay(parents[0]["dataset_id"])
    pipeline.catalog.invalidate_dataset(parents[1]["dataset_id"], "source_deleted")
    lease = acquisition._claim_publication(True)
    assert lease["dataset_id"] == parents[2]["dataset_id"]
    pipeline.catalog.execute(
        "UPDATE capture_publication_leases SET lease_until=clock_timestamp()-interval '1 second',"
        "retry_at=clock_timestamp()+interval '1 hour' WHERE dataset_id=%s",
        (lease["dataset_id"],),
    )
    remove_expansion(pipeline)
    # A compatible bridge image still performs the original claim on schema 35.
    assert acquisition._claim_publication(True) is None
    pipeline.catalog.initialize()
    assert pending(pipeline) == [{"dataset_id": parents[2]["dataset_id"]}]
    assert acquisition._claim_publication(True) is None
    pipeline.catalog.initialize()
    assert pending(pipeline) == [{"dataset_id": parents[2]["dataset_id"]}]
    migration = files("sm_trending_products").joinpath("sql", "037_capture_publication_pending.sql")
    assert "LOCK TABLE datasets" in migration.read_text()


def test_missing_projection_after_expansion_fails_instead_of_silently_scanning_history(pipeline):
    pipeline.catalog.execute("DROP TABLE capture_publication_pending")
    with pytest.raises(RuntimeError, match="projection is missing after schema expansion"):
        service(pipeline)._claim_publication(True)


def test_stale_projection_namespace_does_not_admit_another_namespace(pipeline):
    parent = capture_pages(pipeline)[0]
    pipeline.catalog.execute("UPDATE capture_publication_pending SET synthetic=false")
    assert service(pipeline)._claim_publication(False) is None
    pipeline.catalog.execute(
        "UPDATE capture_publication_pending SET synthetic=true WHERE dataset_id=%s",
        (parent["dataset_id"],),
    )
    assert service(pipeline)._claim_publication(True)["dataset_id"] == parent["dataset_id"]


def test_candidate_query_locks_only_its_selected_parent(pipeline):
    parents = capture_pages(pipeline, 3)
    with pipeline.catalog.transaction() as claim:
        selected = claim.execute(_publication_claim_sql(True), (True, True)).fetchone()
        assert selected["dataset_id"] == parents[0]["dataset_id"]
        with pipeline.catalog.transaction() as other:
            for parent in parents[1:]:
                other.execute(
                    "SELECT 1 FROM datasets WHERE dataset_id=%s FOR UPDATE NOWAIT",
                    (parent["dataset_id"],),
                )


def test_empty_frontier_does_not_scan_retained_history_with_stale_backlog_statistics(pipeline):
    # Minimal metadata is sufficient to test query work. These are not captured
    # observations and make no assertion about valid source receipts or access.
    pipeline.catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
        "SELECT 'frontier-parent-'||n,'frontier_fixture',true,'committed',"
        '\'{"kind":"raw_capture","acquisition_protocol":"1"}\'::jsonb '
        "FROM generate_series(1,1000) n"
    )
    pipeline.catalog.execute("ANALYZE capture_publication_pending")
    pipeline.catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest,supersedes) "
        "SELECT 'frontier-child-'||n,'frontier_fixture',true,'committed','{}'::jsonb,"
        "'frontier-parent-'||n FROM generate_series(1,1000) n"
    )
    pipeline.catalog.execute("ANALYZE datasets")
    assert pending(pipeline) == []
    with pipeline.catalog.transaction() as connection:
        explained = connection.execute(
            "EXPLAIN (ANALYZE,FORMAT JSON) " + _publication_claim_sql(True), (True, True)
        ).fetchone()["QUERY PLAN"][0]["Plan"]

    def visits(node):
        if node.get("Relation Name") == "datasets":
            yield node["Actual Loops"]
        for child in node.get("Plans", []):
            yield from visits(child)

    actual_visits = list(visits(explained))
    assert actual_visits and all(count == 0 for count in actual_visits)


def test_claim_snapshot_cannot_overwrite_owner_that_committed_after_it_started(
    pipeline, monkeypatch
):
    import sm_trending_products.raw_capture as raw_capture

    parent = capture_pages(pipeline)[0]
    first, second = service(pipeline, "first"), service(pipeline, "second")
    gate_key = int(uuid4().hex[:8], 16)
    pipeline.catalog.execute(
        "CREATE FUNCTION capture_snapshot_gate() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$ "
        f"BEGIN PERFORM pg_advisory_xact_lock({gate_key}::bigint); RETURN true; END $$"
    )

    def gated_query(pending):
        query = _publication_claim_sql(pending)
        if current_thread().name.startswith("capture-snapshot"):
            return query.replace(
                "WHERE p.synthetic=%s", "WHERE p.synthetic=%s AND capture_snapshot_gate()"
            )
        return query

    monkeypatch.setattr(raw_capture, "_publication_claim_sql", gated_query)
    with ThreadPoolExecutor(1, thread_name_prefix="capture-snapshot") as pool:
        with pipeline.catalog.transaction() as gate:
            gate.execute("SELECT pg_advisory_xact_lock(%s::bigint)", (gate_key,))
            waiting = pool.submit(first._claim_publication, True)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if pipeline.catalog.one(
                    "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 "
                    "AND objid::bigint=%s AND objsubid=1 AND NOT granted",
                    (gate_key,),
                ):
                    break
                time.sleep(0.01)
            else:
                pytest.fail("The first claim did not reach its established-snapshot gate")
            winner = second._claim_publication(True)
            assert winner["dataset_id"] == parent["dataset_id"]
        assert waiting.result(timeout=5) is None
    current = pipeline.catalog.one("SELECT * FROM capture_publication_leases")
    assert current["worker_id"] == "second" and current["fence"] == winner["fence"]


@pytest.mark.parametrize("eligibility", ["active", "backoff", "ready"])
def test_old_image_unconditional_upsert_cannot_bypass_ownership_or_retry_gate(
    pipeline, eligibility
):
    parent = capture_pages(pipeline)[0]
    lease = service(pipeline, "current")._claim_publication(True)
    if eligibility != "active":
        pipeline.catalog.execute(
            "UPDATE capture_publication_leases SET "
            "lease_until=clock_timestamp()-interval '1 second',"
            "retry_at=clock_timestamp()+%s*interval '1 second' WHERE dataset_id=%s",
            (300 if eligibility == "backoff" else -1, parent["dataset_id"]),
        )
    # This is the unconditional schema-35 writer, intentionally without the new
    # UPSERT predicate. The database must also protect compatible old images.
    replacement = pipeline.catalog.one(
        "INSERT INTO capture_publication_leases(dataset_id,worker_id,lease_until) "
        "VALUES(%s,%s,clock_timestamp()+%s*interval '1 second') "
        "ON CONFLICT(dataset_id) DO UPDATE SET worker_id=EXCLUDED.worker_id,"
        "fence=capture_publication_leases.fence+1,lease_until=EXCLUDED.lease_until,"
        "attempts=capture_publication_leases.attempts+1,last_error=NULL RETURNING *",
        (parent["dataset_id"], "old-image", pipeline.settings.lease_seconds),
    )
    current = pipeline.catalog.one("SELECT * FROM capture_publication_leases")
    if eligibility == "ready":
        assert replacement["worker_id"] == "old-image"
        assert current["fence"] == lease["fence"] + 1
    else:
        assert replacement is None
        assert current["worker_id"] == lease["worker_id"]
        assert current["fence"] == lease["fence"]
        assert current["attempts"] == lease["attempts"]


def test_schema_expansion_drains_old_row_locking_claim_before_lease_ddl_and_backfill(pipeline):
    parent = capture_pages(pipeline)[0]
    remove_expansion(pipeline)
    gate_key = int(uuid4().hex[:12], 16)
    migration = files("sm_trending_products").joinpath("sql", "037_capture_publication_pending.sql")
    # Pause actual expansion after its DDL has taken table locks, before the
    # pending-row FK backfill. An old claim must wait before locking its parent.
    content = migration.read_text().replace(
        "-- This full-history reconciliation",
        f"SELECT pg_advisory_xact_lock({gate_key}::bigint);\n-- This full-history reconciliation",
        1,
    )
    active = {}

    def migrate():
        with pipeline.catalog.transaction() as connection:
            active["migration"] = connection.info.backend_pid
            connection.execute(content)

    def old_claim():
        with pipeline.catalog.transaction() as connection:
            active["claim"] = connection.info.backend_pid
            selected = connection.execute(_publication_claim_sql(False), (True,)).fetchone()
            assert selected["dataset_id"] == parent["dataset_id"]
            connection.execute(
                "INSERT INTO capture_publication_leases(dataset_id,worker_id,lease_until) "
                "VALUES(%s,'old-image',clock_timestamp()+interval '180 seconds')",
                (parent["dataset_id"],),
            )

    def wait_blocked(actor):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if (
                actor in active
                and pipeline.catalog.one(
                    "SELECT cardinality(pg_blocking_pids(%s)) AS count", (active[actor],)
                )["count"]
            ):
                return
            time.sleep(0.01)
        pytest.fail(f"{actor} did not reach the controlled migration lock gate")

    with ThreadPoolExecutor(2) as pool:
        with pipeline.catalog.transaction() as gate:
            gate.execute("SELECT pg_advisory_xact_lock(%s::bigint)", (gate_key,))
            expanding = pool.submit(migrate)
            wait_blocked("migration")
            claiming = pool.submit(old_claim)
            wait_blocked("claim")
            # The stronger migration lock still permits ordinary metadata reads.
            assert pipeline.catalog.one("SELECT count(*) AS count FROM datasets")["count"] == 1
        expanding.result(timeout=8)
        claiming.result(timeout=8)
    assert pending(pipeline) == [{"dataset_id": parent["dataset_id"]}]
    current = pipeline.catalog.one("SELECT worker_id FROM capture_publication_leases")
    assert current["worker_id"] == "old-image"
