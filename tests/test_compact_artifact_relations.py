"""Populated SQL019 upgrade and public artifact contracts without remote rewrites."""

from datetime import timedelta
from importlib.resources import files

import pytest
from psycopg.errors import RaiseException
from test_sharded_results import seed

from sm_trending_products.catalog import RETENTION_GUARD_KEY

pytestmark = pytest.mark.integration


def public_relations(catalog):
    return {
        "inputs": catalog.all(
            "SELECT p.object_key,i.dataset_id FROM derived_artifact_inputs i "
            "JOIN derived_publications p USING(object_id) ORDER BY p.object_key,i.dataset_id"
        ),
        "edges": catalog.all(
            "SELECT p.object_key AS parent_key,c.object_key AS child_key "
            "FROM derived_artifact_edges e JOIN derived_publications p ON p.object_id=e.parent_id "
            "JOIN derived_publications c ON c.object_id=e.child_id ORDER BY parent_key,child_key"
        ),
        "evaluations": catalog.all(
            "SELECT e.evaluation_id,p.object_key FROM evaluation_artifacts e "
            "JOIN derived_publications p USING(object_id) ORDER BY e.evaluation_id,p.object_key"
        ),
    }


def restore_sql019_layout(catalog):
    """Test-only reconstruction using the actual unchanged SQL019 DDL/closure.

    Preserve the whole native-produced DAG and public payloads as an old deployed
    catalog would contain them. Do not run this helper against deployed data.
    """
    with catalog.transaction() as connection:
        for name, query in (
            (
                "fixture_inputs",
                "SELECT p.object_key,i.dataset_id FROM derived_artifact_inputs i "
                "JOIN derived_publications p USING(object_id)",
            ),
            (
                "fixture_edges",
                "SELECT p.object_key AS parent_key,c.object_key AS child_key "
                "FROM derived_artifact_edges e "
                "JOIN derived_publications p ON p.object_id=e.parent_id "
                "JOIN derived_publications c ON c.object_id=e.child_id",
            ),
            (
                "fixture_evaluations",
                "SELECT e.evaluation_id,p.object_key FROM evaluation_artifacts e "
                "JOIN derived_publications p USING(object_id)",
            ),
        ):
            connection.execute(f"CREATE TEMP TABLE {name} ON COMMIT DROP AS {query}")
        connection.execute(
            "DROP TABLE derived_artifact_inputs,derived_artifact_edges,evaluation_artifacts"
        )
        connection.execute("DROP FUNCTION analytical_artifact_ancestor_ids(text[])")
        connection.execute("ALTER TABLE derived_publications DROP COLUMN object_id CASCADE")
        connection.execute(
            files("sm_trending_products").joinpath("sql/019_sharded_provenance.sql").read_text()
        )
        for table, source in (
            ("derived_artifact_inputs", "fixture_inputs"),
            ("derived_artifact_edges", "fixture_edges"),
            ("evaluation_artifacts", "fixture_evaluations"),
        ):
            connection.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
            connection.execute(f"INSERT INTO {table} SELECT * FROM {source}")
            connection.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")
        connection.execute("DELETE FROM schema_migrations WHERE version=26")


def test_populated_sql019_upgrade_preserves_entire_v2_results_and_source_closure(
    pipeline, monkeypatch
):
    cutoff, _records, datasets = seed(pipeline, products=2, videos=2, points=3)
    published = pipeline.assess(cutoff, True)
    frozen = pipeline.freeze(cutoff, True, {"top_k": 2})
    matured = pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(days=20))
    replayed = pipeline.backtest([cutoff], cutoff + timedelta(days=20), True, {"top_k": 2})
    roots = [row["evidence_refs"] for row in published["rows"]]
    pages = [pipeline.provenance.page(root) for root in roots]
    pipeline.catalog.execute(
        "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
        "VALUES('legacy/日本語/nullable descriptor','[]','deleted'),"
        "('legacy/invalidated','[]','invalidated')"
    )
    relations = public_relations(pipeline.catalog)
    publications = pipeline.catalog.all(
        "SELECT object_key,evidence_datasets,state,created_at,object_sha256,object_bytes,"
        "relations_purged FROM derived_publications ORDER BY object_key"
    )
    restore_sql019_layout(pipeline.catalog)
    old_ancestors = pipeline.catalog.all(
        "SELECT object_key FROM analytical_artifact_ancestors(%s) ORDER BY object_key", (datasets,)
    )

    def no_storage(*args, **kwargs):
        pytest.fail("SQL026 migration must not read or rewrite immutable objects")

    with monkeypatch.context() as migration:
        for method in ("get_object", "put_object", "head_object", "list_objects_v2"):
            migration.setattr(pipeline.history.client, method, no_storage)
        pipeline.catalog.initialize()
    assert public_relations(pipeline.catalog) == relations
    assert (
        pipeline.catalog.all(
            "SELECT object_key,evidence_datasets,state,created_at,object_sha256,object_bytes,"
            "relations_purged FROM derived_publications ORDER BY object_key"
        )
        == publications
    )
    assert (
        pipeline.catalog.all(
            "SELECT object_key FROM analytical_artifact_ancestors(%s) ORDER BY object_key",
            (datasets,),
        )
        == old_ancestors
    )
    identities = pipeline.catalog.all(
        "SELECT object_key,object_id FROM derived_publications ORDER BY object_key"
    )
    assert len({row["object_id"] for row in identities}) == len(publications)
    pipeline.catalog.initialize()
    assert (
        pipeline.catalog.all(
            "SELECT object_key,object_id FROM derived_publications ORDER BY object_key"
        )
        == identities
    )
    assert [pipeline.provenance.page(root) for root in roots] == pages
    assert pipeline.assess(cutoff, True) == published
    assert pipeline.freeze(cutoff, True, {"top_k": 2}) == frozen
    assert pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(days=20)) == matured
    assert pipeline.backtest([cutoff], cutoff + timedelta(days=20), True, {"top_k": 2}) == replayed
    pipeline.catalog.invalidate_dataset(datasets[0], "post-upgrade-removal")
    assert pipeline.assessment("p0", True) is None
    assert pipeline.evaluation(frozen["evaluation_id"], True)["invalidated"]
    assert pipeline.evaluation(matured["evaluation_id"], True)["invalidated"]
    assert pipeline.evaluation(replayed["evaluation_id"], True)["invalidated"]
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.provenance.page(roots[0])


def test_compact_association_triggers_keep_authoritative_state_and_retention_fence(pipeline):
    cutoff, _records, datasets = seed(pipeline)
    root = pipeline.assess(cutoff, True)["rows"][0]["evidence_refs"]["root"]
    with pipeline.catalog.transaction() as connection:
        child = connection.execute(
            "SELECT object_id FROM derived_publications WHERE object_key=%s", (root["key"],)
        ).fetchone()["object_id"]
        parent = connection.execute(
            "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
            "VALUES('guarded-parent','[]','staged') RETURNING object_id"
        ).fetchone()["object_id"]
        connection.execute(
            "INSERT INTO derived_artifact_edges(parent_id,child_id) VALUES(%s,%s)", (parent, child)
        )
        assert connection.execute(
            "SELECT count(*) AS n FROM pg_locks WHERE pid=pg_backend_pid() "
            "AND locktype='advisory' AND mode='ShareLock' AND granted AND objid=%s",
            (RETENTION_GUARD_KEY,),
        ).fetchone() == {"n": 1}
    pipeline.catalog.invalidate_dataset(datasets[0], "retired-child")
    with pytest.raises(RaiseException, match="child is unavailable"):
        with pipeline.catalog.transaction() as connection:
            fresh_parent = connection.execute(
                "INSERT INTO derived_publications(object_key,evidence_datasets,state) "
                "VALUES('fresh-parent','[]','staged') RETURNING object_id"
            ).fetchone()["object_id"]
            connection.execute(
                "INSERT INTO derived_artifact_edges(parent_id,child_id) VALUES(%s,%s)",
                (fresh_parent, child),
            )
