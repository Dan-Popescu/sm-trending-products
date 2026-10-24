"""Bounded retired-artifact relation collection against the authoritative SQL DAG."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from test_maintenance import record

import sm_trending_products.maintenance as maintenance_module
from sm_trending_products.catalog import RETENTION_GUARD_KEY
from sm_trending_products.contracts import now
from sm_trending_products.maintenance import Maintenance

pytestmark = pytest.mark.integration


def relation_counts(pipeline):
    return pipeline.catalog.one(
        "SELECT (SELECT count(*) FROM derived_artifact_edges) AS edges,"
        "(SELECT count(*) FROM derived_artifact_inputs) AS inputs,"
        "(SELECT count(*) FROM evaluation_artifacts) AS evaluations"
    )


def seed_retired_parent(pipeline, key, *, edges=0, inputs=0, evaluations=0, live=False):
    """Use insertion guards before retiring a populated parent, with no object-store IO."""
    with pipeline.catalog.transaction() as connection, connection.cursor() as cursor:
        parent = connection.execute(
            "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
            "VALUES(%s,'[]','staged') RETURNING object_id",
            (key,),
        ).fetchone()["object_id"]
        if edges:
            cursor.executemany(
                "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
                "VALUES(%s,'[]','committed')",
                [(f"{key}/child-{index:05d}",) for index in range(edges)],
            )
            cursor.executemany(
                "INSERT INTO derived_artifact_edges(parent_id,child_id) "
                "VALUES(%s,(SELECT object_id FROM derived_publications WHERE object_key=%s))",
                [(parent, f"{key}/child-{index:05d}") for index in range(edges)],
            )
        if inputs:
            cursor.executemany(
                "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
                "VALUES(%s,'artifact_gc_fixture',true,'committed','{}')",
                [(f"{key}/input-{index:05d}",) for index in range(inputs)],
            )
            cursor.executemany(
                "INSERT INTO derived_artifact_inputs(object_id,dataset_id) VALUES(%s,%s)",
                [(parent, f"{key}/input-{index:05d}") for index in range(inputs)],
            )
        connection.execute(
            "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (key,)
        )
        if evaluations:
            cursor.executemany(
                "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,payload) "
                "VALUES(%s,'forward',true,'{}')",
                [(f"{key}/evaluation-{index:05d}",) for index in range(evaluations)],
            )
            cursor.executemany(
                "INSERT INTO evaluation_artifacts(evaluation_id,object_id) VALUES(%s,%s)",
                [(f"{key}/evaluation-{index:05d}", parent) for index in range(evaluations)],
            )
            if not live:
                connection.execute(
                    "UPDATE evaluation_runs SET invalidated=true WHERE evaluation_id LIKE %s",
                    (f"{key}/evaluation-%",),
                )
        connection.execute(
            "UPDATE derived_publications SET state='deleted' WHERE object_key=%s", (key,)
        )


def test_one_pass_caps_total_mixed_relations_and_restart_reports_remaining_work(pipeline):
    seed_retired_parent(pipeline, "mixed", edges=600, inputs=600, evaluations=3)
    assert relation_counts(pipeline) == {"edges": 600, "inputs": 600, "evaluations": 3}
    maintenance = Maintenance(pipeline)
    first = maintenance.retention_drain(max_batches=1)
    assert first["artifact_relations_deleted"] == 1000
    assert first["pending_artifact_gc_count"] == 1
    assert first["status"] == "partial" and first["stop_reason"] == "batch_limit"
    assert relation_counts(pipeline) == {"edges": 0, "inputs": 200, "evaluations": 3}
    resumed = maintenance.retention_drain()
    assert resumed["artifact_relations_deleted"] == 203
    assert resumed["artifact_gc_completed_parents"] == 1
    assert resumed["pending_artifact_gc_count"] == 0
    assert resumed["status"] == "complete" and resumed["stop_reason"] == "no_work"
    assert relation_counts(pipeline) == {"edges": 0, "inputs": 0, "evaluations": 0}
    assert pipeline.catalog.one(
        "SELECT state,relations_purged FROM derived_publications WHERE object_key='mixed'"
    ) == {"state": "deleted", "relations_purged": True}
    assert pipeline.catalog.one("SELECT count(*) AS n FROM evaluation_runs")["n"] == 3
    assert pipeline.catalog.one("SELECT count(*) AS n FROM datasets")["n"] == 600
    assert maintenance.retention_drain()["artifact_relations_deleted"] == 0


def test_private_gc_hard_caps_even_an_oversized_requested_batch(pipeline):
    seed_retired_parent(pipeline, "large", edges=1105)
    with pipeline.catalog.transaction() as connection:
        pipeline.catalog.guard_retention(connection)
        result = Maintenance._gc_artifact_relations(connection, 100_000)
    assert result["artifact_relations_deleted"] == 1000
    assert result["artifact_gc_next"] == ("large", True)
    assert relation_counts(pipeline)["edges"] == 105


def test_migration_backfills_existing_tombstones_without_object_store_reads(pipeline, monkeypatch):
    seed_retired_parent(pipeline, "old-deleted", inputs=1)
    pipeline.catalog.execute(
        "ALTER TABLE derived_publications DROP COLUMN relations_purged CASCADE"
    )
    pipeline.catalog.execute("DELETE FROM schema_migrations WHERE version=25")

    def unexpected_storage(*args, **kwargs):
        raise AssertionError("SQL migration must not access object storage")

    monkeypatch.setattr(pipeline.history.client, "get_object", unexpected_storage)
    pipeline.catalog.initialize()
    assert Maintenance(pipeline).retention_backlog()["pending_artifact_gc_count"] == 1
    assert pipeline.catalog.one(
        "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() "
        "AND indexname='retired_artifact_relation_work'"
    )
    result = Maintenance(pipeline).retention_drain()
    assert result["status"] == "complete" and result["artifact_relations_deleted"] == 1


def test_live_evaluation_link_is_kept_visible_and_does_not_starve_later_parents(
    pipeline, monkeypatch
):
    seed_retired_parent(pipeline, "a-blocked", evaluations=1, live=True)
    seed_retired_parent(pipeline, "z-removable", edges=1, evaluations=1)
    # Force separate parent pages, so a blocked prefix cannot hide later work.
    monkeypatch.setattr(maintenance_module, "ARTIFACT_RELATION_BATCH", 1)
    result = Maintenance(pipeline).retention_drain()
    assert result["artifact_relations_deleted"] == 2
    assert result["status"] == "blocked"
    assert result["stop_reason"] == "artifact_gc_live_evaluation"
    assert result["artifact_gc_blocked_parent_keys"] == ["a-blocked"]
    assert result["pending_artifact_gc_count"] == 1
    assert pipeline.catalog.all(
        "SELECT p.object_key FROM evaluation_artifacts e "
        "JOIN derived_publications p USING(object_id)"
    ) == [{"object_key": "a-blocked"}]
    assert pipeline.catalog.one(
        "SELECT relations_purged FROM derived_publications WHERE object_key='a-blocked'"
    ) == {"relations_purged": False}
    pipeline.catalog.execute("UPDATE evaluation_runs SET invalidated=true")
    resumed = Maintenance(pipeline).retention_drain()
    assert resumed["status"] == "complete"
    assert resumed["artifact_relations_deleted"] == 1


def test_gc_rollback_keeps_relation_rows_progress_and_finalization_atomic(pipeline, monkeypatch):
    seed_retired_parent(pipeline, "rollback", edges=2, inputs=1, evaluations=1)
    pipeline.catalog.execute(
        "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
        "VALUES('finalize','[]','invalidated')"
    )
    pipeline.catalog.execute(
        "INSERT INTO retention_objects(object_key,reason,state) "
        "VALUES('finalize','retention_expired','deleted')"
    )
    original = Maintenance._gc_artifact_relations

    def abort(connection, limit, *, after=None):
        assert original(connection, limit, after=after)["artifact_relations_deleted"] == 4
        raise RuntimeError("fail after SQL deletes")

    monkeypatch.setattr(Maintenance, "_gc_artifact_relations", staticmethod(abort))
    with pytest.raises(RuntimeError, match="after SQL deletes"):
        Maintenance(pipeline)._finish_retention()
    assert relation_counts(pipeline) == {"edges": 2, "inputs": 1, "evaluations": 1}
    assert pipeline.catalog.one(
        "SELECT state,relations_purged FROM derived_publications WHERE object_key='finalize'"
    ) == {"state": "invalidated", "relations_purged": False}
    assert pipeline.catalog.one(
        "SELECT relations_purged FROM derived_publications WHERE object_key='rollback'"
    ) == {"relations_purged": False}


def test_gc_skips_locked_parent_and_revisits_it_on_the_next_drain(pipeline):
    seed_retired_parent(pipeline, "a-locked", edges=1)
    seed_retired_parent(pipeline, "b-available", edges=1)
    with pipeline.catalog.transaction() as connection:
        pipeline.catalog.guard_retention(connection)
        connection.execute(
            "SELECT object_key FROM derived_publications WHERE object_key='a-locked' FOR UPDATE"
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(Maintenance(pipeline)._finish_retention).result(timeout=5)
        assert result["artifact_relations_deleted"] == 1
        assert Maintenance(pipeline).retention_backlog()["pending_artifact_gc_count"] == 1
        assert relation_counts(pipeline)["edges"] == 1
    resumed = Maintenance(pipeline).retention_drain()
    assert resumed["status"] == "complete" and resumed["artifact_relations_deleted"] == 1


def test_gc_transaction_keeps_its_own_publication_fence(pipeline, monkeypatch):
    seed_retired_parent(pipeline, "fenced", edges=1)
    entered, release = Event(), Event()
    owner = []
    original = Maintenance._gc_artifact_relations

    def paused(connection, limit, *, after=None):
        owner.append(connection.info.backend_pid)
        entered.set()
        assert release.wait(10)
        return original(connection, limit, after=after)

    monkeypatch.setattr(Maintenance, "_gc_artifact_relations", staticmethod(paused))
    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(Maintenance(pipeline)._finish_retention)
        try:
            assert entered.wait(5)
            assert pipeline.catalog.one(
                "SELECT count(*) AS n FROM pg_locks WHERE pid=%s "
                "AND locktype='advisory' AND mode='ShareLock' AND granted AND objid=%s",
                (owner[0], RETENTION_GUARD_KEY),
            ) == {"n": 1}
        finally:
            release.set()
        assert running.result(timeout=10)["artifact_relations_deleted"] == 1


def test_retirement_purges_dead_outgoing_relations_but_preserves_live_dag_and_tombstones(
    pipeline, monkeypatch
):
    timestamp = now()
    first = pipeline.import_records(
        [record(f"a-{i}", timestamp, i) for i in range(4)], synthetic=True
    )[0]
    second = pipeline.import_records([record("b", timestamp, 9)], synthetic=True)[0]
    rows, _ = pipeline.history.observations(timestamp + timedelta(seconds=1), True)
    by_entity = {row.entity_id: row.observation_id for row in rows}
    live = pipeline.provenance.references([by_entity["b"]], {by_entity["b"]: second}, True)
    current = live
    for index in range(4):
        reference = by_entity[f"a-{index}"]
        other = pipeline.provenance.references([reference], {reference: first}, True)
        current = pipeline.provenance.combine([current, other], True)
    before_live = pipeline.provenance.page(live)
    dead_key = current["root"]["key"]
    dead_descriptor = pipeline.catalog.one(
        "SELECT object_sha256,object_bytes FROM derived_publications WHERE object_key=%s",
        (dead_key,),
    )
    assert relation_counts(pipeline) == {"edges": 8, "inputs": 5, "evaluations": 0}
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('maintenance_fixture',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (first,),
    )
    initial = Maintenance(pipeline).retention_drain()
    assert initial["status"] == "complete"
    assert initial["artifact_relations_deleted"] == 12
    assert initial["artifact_gc_completed_parents"] == 8
    assert relation_counts(pipeline) == {"edges": 0, "inputs": 1, "evaluations": 0}
    assert pipeline.provenance.page(live) == before_live
    assert (
        pipeline.catalog.one(
            "SELECT object_sha256,object_bytes FROM derived_publications WHERE object_key=%s",
            (dead_key,),
        )
        == dead_descriptor
    )
    assert pipeline.catalog.one(
        "SELECT state FROM retention_objects WHERE object_key=%s", (dead_key,)
    ) == {"state": "deleted"}
    with pytest.raises(ValueError, match="retired"):
        pipeline.history.put(dead_key, b"resurrection")
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.provenance.page(current)
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (second,),
    )
    monkeypatch.setattr(maintenance_module, "RETENTION_TARGET_LIMIT", 4)
    # The remaining source has three objects and one live artifact. Dead ancestors
    # must not consume its future atomic closure budget.
    later = Maintenance(pipeline).retention_drain()
    assert later["status"] == "complete"
    assert relation_counts(pipeline) == {"edges": 0, "inputs": 0, "evaluations": 0}


def test_gc_honors_cooperative_deadline_without_deleting_relations(pipeline, monkeypatch):
    seed_retired_parent(pipeline, "deadline", edges=2)
    clock = iter([0.0, 2.0, 2.0, 2.0])
    monkeypatch.setattr(maintenance_module, "monotonic", lambda: next(clock))
    result = Maintenance(pipeline).retention_drain(max_seconds=1)
    assert result["stop_reason"] == "deadline" and result["status"] == "partial"
    assert result["pending_artifact_gc_count"] == 1
    assert result["artifact_relations_deleted"] == 0
    assert relation_counts(pipeline)["edges"] == 2
