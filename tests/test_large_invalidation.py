"""Native bounded source-removal continuation over large immutable graphs."""

import time

import pytest

from sm_trending_products.cache import _Cache


def assert_bounded_cursor(connection, kind, branch, key, positions):
    from sm_trending_products.invalidation import SUCCESSORS

    query = SUCCESSORS[kind][branch][1] + " LIMIT 1001"
    for after in positions:
        plan = connection.execute(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query,
            {"key": key, "after": str(after)},
        ).fetchone()["QUERY PLAN"][0]
        nodes = []
        pending = [plan["Plan"]]
        while pending:
            node = pending.pop()
            nodes.append(node)
            pending.extend(node.get("Plans", []))
        print(
            {
                "cursor": kind,
                "after": after,
                "ms": plan["Execution Time"],
                "nodes": [
                    (node["Node Type"], node.get("Relation Name"), node["Actual Rows"])
                    for node in nodes
                ],
            }
        )
        assert plan["Plan"]["Actual Rows"] <= 1001
        assert any("Index" in node["Node Type"] and node.get("Index Cond") for node in nodes)
        assert all(node["Actual Rows"] <= 1001 for node in nodes)
        assert all(node.get("Rows Removed by Filter", 0) <= 1001 for node in nodes)
        assert all(node.get("Rows Removed by Join Filter", 0) <= 1001 for node in nodes)


def seed_fanout(pipeline, count=12000):
    datasets = pipeline.import_records(
        [
            {
                "observation_id": "remove-me",
                "source": "large_remove",
                "entity_type": "video",
                "entity_id": "v",
                "observed_at": "2026-01-01T00:00:00Z",
                "available_at": "2026-01-01T00:00:00Z",
                "synthetic": True,
                "metrics": {"view_count": {"lower": 10, "upper": 10}},
            }
        ],
        synthetic=True,
    )
    source = datasets[0]
    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
            "SELECT 'provenance/synthetic/fanout-'||n,'[]','staged' "
            "FROM generate_series(1,%s) n",
            (count,),
        )
        connection.execute(
            "INSERT INTO derived_artifact_inputs(object_id,dataset_id) "
            "SELECT object_id,%s FROM derived_publications "
            "WHERE object_key LIKE 'provenance/synthetic/fanout-%%'",
            (source,),
        )
        connection.execute(
            "UPDATE derived_publications SET state='committed' "
            "WHERE object_key LIKE 'provenance/synthetic/fanout-%%'"
        )
    return source


@pytest.mark.integration
@pytest.mark.parametrize("count", [12000, 50000])
def test_large_artifact_removal_fences_then_resumes_without_partial_visibility(pipeline, count):
    from sm_trending_products.catalog import InvalidationPending

    source = seed_fanout(pipeline, count)
    if count == 50000:
        with pipeline.catalog.transaction() as connection:
            connection.execute("ANALYZE derived_artifact_inputs")
            connection.execute("ANALYZE derived_publications")
            assert_bounded_cursor(connection, "dataset", 2, source, ["", 25000, 49000])
    start = time.monotonic()
    progress = pipeline.catalog.invalidate_dataset(source, "source_deleted")
    assert progress["status"] == "partial"
    assert pipeline.status()["pending_invalidations"]
    with pytest.raises(InvalidationPending):
        pipeline.latest_assessments(True)
    assert pipeline.latest_assessments(False) == []
    resumed = pipeline.catalog.resume_invalidations(max_batches=1000, max_seconds=120)
    print({"artifacts": count, "resume": resumed, "seconds": time.monotonic() - start})
    assert resumed["status"] == "complete"
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (source,))["state"]
        == "invalidated"
    )
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS n FROM derived_publications WHERE state='invalidated'"
        )["n"]
        == count
    )
    assert pipeline.latest_assessments(True) == []
    assert pipeline.catalog.one("SELECT count(*) AS n FROM source_invalidation_work")["n"] == 0


def test_all_published_and_cached_reads_are_fenced_while_metadata_and_other_namespace_work(
    pipeline, monkeypatch
):
    from datetime import timedelta

    from test_sharded_results import seed

    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.invalidation import SourceInvalidation

    cutoff, _records, datasets = seed(pipeline)
    published = pipeline.assess(cutoff, True)
    frozen = pipeline.freeze(cutoff, True)
    manifests = pipeline.catalog.manifests(synthetic=True, as_of=cutoff)
    SourceInvalidation(pipeline.catalog).begin(datasets[0], "source_deleted")
    # Exercise each read's actual fence while retaining the normal persisted job;
    # separate tests exercise the real bounded automatic continuation.
    monkeypatch.setattr(pipeline.catalog, "resume_invalidations", lambda **kwargs: None)
    operations = [
        lambda: pipeline.latest_assessments(True),
        lambda: pipeline.assessment_summaries(True),
        lambda: pipeline.assessment("p0", True),
        lambda: pipeline.alerts(True),
        lambda: pipeline.alert_summaries(True),
        lambda: pipeline.alert("unknown", True),
        lambda: pipeline.evaluations(True),
        lambda: pipeline.evaluation_summaries(True),
        lambda: pipeline.evaluation(frozen["evaluation_id"], True),
        lambda: pipeline.analytical_page("assessment", "p0", True),
        lambda: pipeline.provenance.page(published["rows"][0]["evidence_refs"]),
        lambda: pipeline.evidence_page("p0", cutoff, True),
        lambda: pipeline.history.read(manifests, as_of=cutoff),
        lambda: pipeline.history.observations(cutoff, True),
        lambda: pipeline.history.raw(manifests[0]),
        lambda: pipeline.assess(cutoff + timedelta(minutes=1), True),
        lambda: pipeline.freeze(cutoff, True),
        lambda: pipeline.collect_batch(synthetic=True),
        lambda: pipeline.schedule(synthetic=True),
        lambda: pipeline.suggest_links("fixture", synthetic=True),
        lambda: pipeline.collection_summaries(True),
        lambda: pipeline.collection_decision("unknown", True),
    ]
    for operation in operations:
        with pytest.raises(InvalidationPending):
            operation()
    assert pipeline.status()["pending_invalidations"]
    assert pipeline.products(True)
    assert pipeline.datasets()
    assert pipeline.latest_assessments(False) == []
    assert pipeline.evaluations(False) == []
    assert pipeline.catalog.export_state()["source_invalidations"]


def test_failed_batch_rolls_back_progress_and_namespace_gate_never_expires(pipeline, monkeypatch):
    from test_sharded_results import seed

    from sm_trending_products.invalidation import SourceInvalidation

    cutoff, _records, datasets = seed(pipeline)
    pipeline.assess(cutoff, True)
    work = SourceInvalidation(pipeline.catalog)
    identifier = work.begin(datasets[0], "source_deleted")
    before = pipeline.catalog.all("SELECT * FROM source_invalidation_work ORDER BY kind,target_key")
    original = work._discover

    def fail_after_mutating(connection, job, deadline):
        original(connection, job, deadline)
        raise RuntimeError("Controlled process failure before commit")

    monkeypatch.setattr(work, "_discover", fail_after_mutating)
    with pytest.raises(RuntimeError, match="Controlled process failure"):
        work.resume()
    assert (
        pipeline.catalog.all("SELECT * FROM source_invalidation_work ORDER BY kind,target_key")
        == before
    )
    pipeline.catalog.execute(
        "UPDATE source_invalidations SET created_at=clock_timestamp()-interval '90 days'"
    )
    assert work.status()[0]["invalidation_id"] == identifier
    monkeypatch.setattr(work, "_discover", original)
    assert work.resume(max_batches=100, max_seconds=20)["status"] == "complete"
    assert pipeline.assessment("p0", True) is None


def test_namespace_gate_is_atomic_with_readers_and_resumer_does_not_queue_an_exclusive_lock(
    pipeline,
):
    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline, 2)
    work = SourceInvalidation(pipeline.catalog)
    work.begin(source, "source_deleted")
    with pipeline.catalog.transaction() as connection:
        pipeline.catalog.guard_retention(connection)
        start = time.monotonic()
        progress = work.resume(max_batches=100, max_seconds=0.08)
        assert progress["status"] == "partial"
        assert time.monotonic() - start < 0.5
    assert work.resume(max_batches=100, max_seconds=20)["status"] == "complete"


def test_sql_writers_cannot_publish_into_a_pending_namespace(pipeline):
    import psycopg

    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline, 2)
    SourceInvalidation(pipeline.catalog).begin(source, "source_deleted")
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="invalidation pending"):
        # A raw SQL client still receives PostgreSQL's authoritative denial;
        # Catalog callers receive its translated typed backpressure contract.
        with pipeline.catalog.pool.connection() as connection, connection.transaction():
            connection.execute(
                "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
                "VALUES('provenance/synthetic/new-during-removal','[]','staged')"
            )
    from sm_trending_products.catalog import InvalidationPending

    with pytest.raises(InvalidationPending):
        pipeline.catalog.stage_derivation("provenance/synthetic/new-during-removal", [])
    pipeline.catalog.stage_derivation("provenance/live/unrelated-live", [])
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' "
        "WHERE object_key='provenance/live/unrelated-live'"
    )

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="Different prerequisite"):
        pipeline.catalog.execute(
            "DO $$ BEGIN RAISE EXCEPTION 'Different prerequisite' USING ERRCODE='55000'; END $$"
        )


def test_exploration_gate_committed_after_manifest_selection_blocks_actual_file_read(
    pipeline, monkeypatch
):
    import sm_trending_products.exploration as module
    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline, 1)
    original = module.explore
    before = dict(pipeline.history.metrics)

    def begin_after_selection(history, manifests, sql_query, **kwargs):
        SourceInvalidation(pipeline.catalog).begin(source, "source_deleted")
        return original(history, manifests, sql_query, **kwargs)

    monkeypatch.setattr(module, "explore", begin_after_selection)
    with pytest.raises(InvalidationPending):
        pipeline.explore("SELECT observation_id FROM observations", synthetic=True)
    assert pipeline.history.metrics == before


@pytest.mark.parametrize("boundary", ["staging", "commit"])
def test_leaf_publication_preserves_typed_retry_at_both_sql_boundaries(
    pipeline, monkeypatch, boundary
):
    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline, 1)
    work = SourceInvalidation(pipeline.catalog)
    if boundary == "staging":
        work.begin(source, "source_deleted")
    else:
        original = pipeline.history.put

        def begin_after_upload(*args, **kwargs):
            result = original(*args, **kwargs)
            work.begin(source, "source_deleted")
            return result

        monkeypatch.setattr(pipeline.history, "put", begin_after_upload)
    with pytest.raises(InvalidationPending):
        pipeline.provenance._node(
            {"kind": "datasets", "count": 1, "datasets": [source]}, True, inputs=[source]
        )


@pytest.mark.parametrize("operation", ["suggest_links", "schedule"])
def test_source_consumers_recheck_gate_in_their_actual_sql_transaction(
    pipeline, monkeypatch, operation
):
    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.identity import Identity
    from sm_trending_products.invalidation import SourceInvalidation
    from sm_trending_products.scheduler import Scheduler

    source = seed_fanout(pipeline, 1)
    owner = Identity if operation == "suggest_links" else Scheduler
    original = getattr(owner, operation)

    def begin_after_preflight(self, *args, **kwargs):
        SourceInvalidation(pipeline.catalog).begin(source, "source_deleted")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(owner, operation, begin_after_preflight)
    with pytest.raises(InvalidationPending):
        if operation == "suggest_links":
            pipeline.suggest_links("desk lamp", True)
        else:
            pipeline.schedule(synthetic=True)


def test_cli_worker_continues_pending_removal_then_collects_without_restart(pipeline):
    import json
    import os
    import select
    import signal
    import subprocess
    import sys

    from test_pipeline_integration import ACCOUNT, controlled_source

    from sm_trending_products.contracts import now
    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline)
    SourceInvalidation(pipeline.catalog).begin(source, "source_deleted")
    response = (
        200,
        {"data": {"videos": [{"id": "after-removal", "view_count": 10}], "has_more": False}},
        {},
    )
    with controlled_source([response]) as (endpoint, requests):
        task = pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint, "fixture_observed_at": now().isoformat()},
            synthetic=True,
        )
        settings = pipeline.settings.model_dump(mode="json")
        settings["database_url"] = pipeline.settings.database_url.get_secret_value()
        process = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-c",
                "import sys; from sm_trending_products.pipeline import Pipeline; "
                "from sm_trending_products.settings import Settings; "
                "from sm_trending_products.cli import main; "
                "settings=Settings.model_validate_json(sys.stdin.read()); "
                "Pipeline.from_env=classmethod(lambda cls: cls(settings)); "
                "raise SystemExit(main(['worker','--loop','--synthetic','--poll-seconds','60']))",
            ],
            env=dict(os.environ),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        process.stdin.write(json.dumps(settings).encode())
        process.stdin.close()
        process.stdin = None
        outputs = []
        started = time.monotonic()
        try:
            while time.monotonic() - started < 30:
                assert select.select([process.stdout], [], [], 10)[0], "Worker made no progress"
                line = process.stdout.readline()
                assert line, "Worker exited instead of continuing the invalidation"
                result = json.loads(line)
                outputs.append(result)
                assert pipeline.latest_assessments(False) == []
                pipeline.status()
                if result.get("processed"):
                    break
            assert any(row.get("status") == "invalidation_pending" for row in outputs)
            assert outputs[-1]["processed"] == 1
            assert not pipeline.catalog.pending_invalidations()
            assert pipeline.catalog.one(
                "SELECT state,attempts FROM collection_tasks WHERE task_id=%s", (task,)
            ) == {"state": "done", "attempts": 0}
            assert len(requests) == 1
            process.send_signal(signal.SIGTERM)
            output, errors = process.communicate(timeout=5)
            assert process.returncode == 0, errors.decode()
            assert json.loads(output.splitlines()[-1]) == {"status": "stopped"}
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)


def test_late_collection_gate_releases_lease_without_consuming_failure_budget(
    pipeline, monkeypatch
):
    from test_pipeline_integration import ACCOUNT, controlled_source

    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.contracts import now
    from sm_trending_products.invalidation import SourceInvalidation

    source = seed_fanout(pipeline, 1)
    response = (200, {"data": {"videos": [{"id": "new"}], "has_more": False}}, {})
    with controlled_source([response]) as (endpoint, requests):
        task = pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint, "fixture_observed_at": now().isoformat()},
            synthetic=True,
        )
        original = pipeline.history.publish

        def begin_before_publication(*args, **kwargs):
            SourceInvalidation(pipeline.catalog).begin(source, "source_deleted")
            return original(*args, **kwargs)

        monkeypatch.setattr(pipeline.history, "publish", begin_before_publication)
        with pytest.raises(InvalidationPending):
            pipeline.collect_batch(1, True)
        assert len(requests) == 1
        assert pipeline.catalog.one(
            "SELECT state,attempts,lease_owner,last_error FROM collection_tasks WHERE task_id=%s",
            (task,),
        ) == {
            "state": "pending",
            "attempts": 0,
            "lease_owner": None,
            "last_error": "invalidation_pending",
        }
        assert pipeline.catalog.one("SELECT count(*) AS n FROM datasets")["n"] == 1


def test_late_maturation_gate_keeps_due_time_without_hour_failure_backoff(pipeline, monkeypatch):
    from test_sharded_results import seed

    from sm_trending_products.catalog import InvalidationPending
    from sm_trending_products.invalidation import SourceInvalidation

    cutoff, _, datasets = seed(pipeline)
    decision = pipeline.freeze(cutoff, True)["evaluation_id"]
    pipeline.catalog.execute(
        "UPDATE forward_frontier SET next_due_at=%s WHERE evaluation_id=%s",
        (cutoff, decision),
    )
    service = pipeline._evaluation
    original = service.mature

    def begin_after_claim(*args, **kwargs):
        SourceInvalidation(pipeline.catalog).begin(datasets[0], "source_deleted")
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "mature", begin_after_claim)
    with pytest.raises(InvalidationPending):
        pipeline.mature_due(1, True, cutoff)
    assert pipeline.catalog.one(
        "SELECT state,next_due_at,lease_token,attempts,last_error FROM forward_frontier "
        "WHERE evaluation_id=%s",
        (decision,),
    ) == {
        "state": "pending",
        "next_due_at": cutoff,
        "lease_token": None,
        "attempts": 0,
        "last_error": None,
    }


@pytest.mark.parametrize("count", [2500, 5000])
def test_shared_cluster_below_cardinality_cap_uses_bounded_work_preflight(pipeline, count):
    from sm_trending_products.maintenance import Maintenance

    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
            "SELECT 'dense-'||n,'dense',true,'committed',jsonb_build_object("
            "'raw',jsonb_build_object('key','raw/synthetic/shared-dense'),"
            "'normalized','[]'::jsonb,'manifest_key','manifests/synthetic/dense-'||n) "
            "FROM generate_series(1,%s) n",
            (count,),
        )
        connection.execute("ANALYZE dataset_object_refs")
        assert_bounded_cursor(
            connection,
            "shared_object",
            0,
            "raw/synthetic/shared-dense",
            ["", "dense-2500", "dense-9"],
        )
    # A real verified expired root also makes the read-only preview use the
    # bounded-work fallback rather than the quadratic shared-object closure.
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES('dense',1,true)"
    )
    pipeline.catalog.execute("UPDATE datasets SET created_at=clock_timestamp()-interval '2 days'")
    started = time.monotonic()
    preview = Maintenance(pipeline).retention()
    assert preview["dataset_count"] == count
    assert preview["dataset_count_is_lower_bound"]
    assert preview["preview_scope"] == "expired_roots_only"
    assert pipeline.catalog.one("SELECT source_invalidation_wide(ARRAY['dense-1']) AS wide")["wide"]
    result = pipeline.catalog.invalidate_dataset("dense-1", "source_deleted")
    assert result["status"] == "partial"
    assert time.monotonic() - started < 3
    # The cap is intentionally based on work, not on a 10,000-row result count:
    # the old recursive shared-object join visits count**2 candidate edges.
    assert pipeline.catalog.one("SELECT count(*) AS n FROM datasets")["n"] == count


def test_rare_evaluation_dependencies_seek_past_unrelated_history(pipeline):
    source = seed_fanout(pipeline, 1)
    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) VALUES "
            "('unrelated','index_probe',true,'committed',jsonb_build_object("
            "'raw',jsonb_build_object('key','raw/synthetic/unrelated'),"
            "'normalized','[]'::jsonb,'manifest_key','manifests/synthetic/unrelated'))"
        )
        connection.execute(
            "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,payload) "
            "SELECT 'run-'||lpad(n::text,6,'0'),'forward',true,'{}'::jsonb "
            "FROM generate_series(1,20000) n"
        )
        connection.execute(
            "INSERT INTO evaluation_dependencies(evaluation_id,dataset_id) "
            "SELECT 'run-'||lpad(n::text,6,'0'),CASE WHEN n%%1000=0 THEN %s "
            "ELSE 'unrelated' END FROM generate_series(1,20000) n",
            (source,),
        )
        connection.execute("ANALYZE evaluation_dependencies")
        assert_bounded_cursor(connection, "dataset", 5, source, ["", "run-010000", "run-019000"])


def test_deep_dense_cluster_falls_back_on_statement_budget_before_cardinality_cap(pipeline):
    with pipeline.catalog.transaction() as connection:
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
            "SELECT 'hidden-'||n,'hidden',true,'committed',jsonb_build_object("
            "'raw',jsonb_build_object('key',CASE WHEN n=0 THEN 'raw/synthetic/root' "
            "ELSE 'raw/synthetic/deep-shared' END),'normalized','[]'::jsonb,"
            "'manifest_key','manifests/synthetic/hidden-'||n,'parent_dataset_ids',"
            "CASE WHEN n=1 THEN '[\"hidden-0\"]'::jsonb ELSE '[]'::jsonb END) "
            "FROM generate_series(0,5000) n ORDER BY n"
        )
    assert not pipeline.catalog.one("SELECT source_invalidation_wide(ARRAY['hidden-0']) AS wide")[
        "wide"
    ]
    started = time.monotonic()
    result = pipeline.catalog.invalidate_dataset("hidden-0", "source_deleted")
    elapsed = time.monotonic() - started
    print({"hidden_shared_datasets": 5000, "accepted_seconds": elapsed})
    assert result["status"] == "partial" and elapsed < 3.5
    assert pipeline.catalog.pending_invalidations()


@pytest.mark.integration
@pytest.mark.parametrize("shape", ["chain", "shared"])
def test_large_dataset_graph_discovers_indexed_frontiers_and_preserves_source_semantics(
    pipeline, shape
):
    count = 12000
    with pipeline.catalog.transaction() as connection:
        # This is a real SQL metadata graph with empty-page datasets, not a claim
        # that twelve thousand payload files or source observations were fetched.
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
            "SELECT 'graph-'||n,'large_graph',true,'committed',jsonb_build_object("
            "'raw',jsonb_build_object('key',%s||CASE WHEN %s THEN n::text ELSE 'shared' END),"
            "'normalized','[]'::jsonb,'manifest_key','manifests/synthetic/graph-'||n,"
            "'parent_dataset_ids',CASE WHEN %s AND n>0 THEN jsonb_build_array('graph-'||(n-1)) "
            "ELSE '[]'::jsonb END) FROM generate_series(0,%s) n ORDER BY n",
            ("raw/synthetic/graph-", shape == "chain", shape == "chain", count - 1),
        )
    started = time.monotonic()
    initial = pipeline.catalog.invalidate_dataset("graph-0", "source_deleted")
    assert initial["status"] == "partial"
    progress = pipeline.catalog.resume_invalidations(max_batches=1000, max_seconds=120)
    print(
        {
            "shape": shape,
            "datasets": count,
            "progress": progress,
            "seconds": time.monotonic() - started,
        }
    )
    assert progress["status"] == "complete"
    assert (
        pipeline.catalog.one("SELECT count(*) AS n FROM datasets WHERE state='invalidated'")["n"]
        == count
    )


def test_retention_continuation_removes_real_files_and_sql_payload_and_restores_fresh_origin(
    pipeline, monkeypatch
):
    from datetime import timedelta

    from test_maintenance import record

    import sm_trending_products.maintenance as maintenance_module
    from sm_trending_products.contracts import now
    from sm_trending_products.maintenance import Maintenance

    cutoff = now() + timedelta(minutes=1)
    old = pipeline.import_records(
        [record("mixed", cutoff - timedelta(hours=2), 100)], synthetic=True
    )[0]
    fresh = pipeline.import_records(
        [record("mixed", cutoff - timedelta(hours=1), 200)], synthetic=True
    )[0]
    pipeline.create_product("p", "P", synthetic=True)
    pipeline.review_link("p", "maintenance_fixture:video:mixed")
    compacted = pipeline.compact(True)["dataset_id"]
    pipeline.assess(cutoff, True)
    frozen = pipeline.freeze(cutoff, True)
    window = pipeline.catalog.one(
        "SELECT descriptor FROM feature_product_windows WHERE product_id='p'"
    )["descriptor"]

    local = _Cache(pipeline.history).paths(window["key"])[0]
    assert local.exists()
    pipeline.catalog.execute(
        "UPDATE feature_product_projections SET payload=payload||'{\"private_price\":99.99}'::jsonb"
    )
    pipeline.catalog.execute(
        "UPDATE entities SET origin=jsonb_build_object('dataset_id',%s::text),title='retired',"
        "attributes='{\"removed_price\":99.99}' WHERE entity_key='maintenance_fixture:video:mixed'",
        (old,),
    )
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('maintenance_fixture',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (old,),
    )
    monkeypatch.setattr(maintenance_module, "RETENTION_TARGET_LIMIT", 2)
    first = Maintenance(pipeline).retention_drain(max_seconds=30)
    assert first["status"] == "partial" and first["stop_reason"] == "invalidation_pending"
    assert first["pending_source_invalidations"]
    result = Maintenance(pipeline).retention_drain(max_seconds=30)
    assert result["status"] == "complete"
    assert not local.exists()
    assert (
        pipeline.catalog.one("SELECT 1 FROM feature_product_projections WHERE product_id='p'")
        is None
    )
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (old,))["state"]
        == "deleted"
    )
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (compacted,))[
            "state"
        ]
        == "deleted"
    )
    survivor = pipeline.catalog.one(
        "SELECT state,compacted_into FROM datasets WHERE dataset_id=%s", (fresh,)
    )
    assert survivor == {"state": "committed", "compacted_into": None}
    entity = pipeline.catalog.one(
        "SELECT title,attributes,origin FROM entities "
        "WHERE entity_key='maintenance_fixture:video:mixed'"
    )
    assert entity["origin"]["dataset_id"] == fresh
    assert entity["title"] == "" and entity["attributes"] == {}
    assert pipeline.evaluation(frozen["evaluation_id"], True)["invalidated"]
    after = pipeline.assess(cutoff, True)
    assert after["rows"][0]["coverage"]["observations"] == 1
