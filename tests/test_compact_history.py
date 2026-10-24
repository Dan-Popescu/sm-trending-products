"""Compact SQL references preserve public evidence and bounded period selection."""

from datetime import timedelta
from importlib.resources import files

import pytest

from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def record(entity, timestamp, identifier, *, attributes=None):
    return {
        "source": "compact_fixture",
        "entity_type": "video",
        "entity_id": entity,
        "observation_id": identifier,
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "synthetic": True,
        "metrics": {"view_count": {"lower": 100, "upper": 100}},
        "attributes": attributes or {},
    }


def restore_legacy_reference_schema(catalog):
    """Reconstruct populated v15 SQL references without touching immutable objects."""
    with catalog.transaction() as connection:
        original = connection.execute(
            "SELECT n.dataset_id,n.object_key,o.observation_id,o.entity_key,o.observed_at,"
            "o.available_at,encode(o.payload_sha256,'hex') AS payload_sha256 "
            "FROM observation_deliveries o JOIN normalized_objects n USING(object_id)"
        ).fetchall()
        connection.execute("DROP TRIGGER normalized_manifest_index ON datasets")
        connection.execute("DROP TABLE observation_deliveries")
        connection.execute("DROP TABLE normalized_object_entities")
        connection.execute("DROP TABLE normalized_objects")
        connection.execute("DELETE FROM schema_migrations WHERE version IN (14,15,20)")
        for name in ("014_object_selection.sql", "015_evidence_identifier_order.sql"):
            connection.execute(files("sm_trending_products").joinpath("sql", name).read_text())
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO observation_deliveries(dataset_id,object_key,observation_id,"
                "entity_key,observed_at,available_at,payload_sha256) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                [tuple(row.values()) for row in original],
            )
        connection.execute("UPDATE normalized_objects SET observations_indexed=true")
    return original


def test_populated_sql_upgrade_preserves_arbitrary_identifiers_payloads_and_oldest_delivery(
    pipeline, monkeypatch
):
    timestamp = now() - timedelta(days=1)
    identifiers = ["a", "B", "é", "中", "arbitrary:with/slashes?", "0" * 64]
    dataset = pipeline.import_records(
        [record("legacy", timestamp, identifier) for identifier in identifiers], synthetic=True
    )[0]
    pipeline.create_product("legacy-product", "Legacy", synthetic=True)
    pipeline.review_link("legacy-product", "compact_fixture:video:legacy")
    replay = pipeline.replay(dataset)
    before = pipeline.evidence_page("legacy-product", now(), True, limit=100)["records"]
    raw_manifests = pipeline.catalog.all(
        "SELECT dataset_id,manifest FROM datasets ORDER BY dataset_id"
    )
    original = restore_legacy_reference_schema(pipeline.catalog)
    metrics = dict(pipeline.history.metrics)

    class NoObjectStore:
        def __getattr__(self, name):
            pytest.fail(f"SQL migration accessed the object store: {name}")

    with monkeypatch.context() as blocked:
        blocked.setattr(pipeline.history, "_client", NoObjectStore())
        pipeline.catalog.initialize()
    assert pipeline.history.metrics == metrics
    after = pipeline.evidence_page("legacy-product", now(), True, limit=100)["records"]
    assert after == before
    assert [row["observation_id"] for row in after] == sorted(identifiers)
    assert {row["raw_dataset_id"] for row in after} == {dataset}
    assert (
        pipeline.catalog.all("SELECT dataset_id,manifest FROM datasets ORDER BY dataset_id")
        == raw_manifests
    )
    indexed = pipeline.catalog.all(
        "SELECT n.dataset_id,n.object_key,o.observation_id,o.entity_key,o.observed_at,"
        "o.available_at,encode(o.payload_sha256,'hex') AS payload_sha256 "
        "FROM observation_deliveries o JOIN normalized_objects n USING(object_id)"
    )
    assert sorted(indexed, key=str) == sorted(original, key=str)
    assert pipeline.catalog.one(
        "SELECT bool_and(octet_length(payload_sha256)=32) AS valid FROM observation_deliveries"
    )["valid"]
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS n FROM normalized_objects WHERE object_id IS NULL"
        )["n"]
        == 0
    )
    pipeline.catalog.initialize()  # Reopening never rebuilds or duplicates compact references.
    assert pipeline.verify(dataset)["ok"] and pipeline.verify(replay["dataset_id"])["ok"]


@pytest.mark.parametrize("period_kind", ["declared", "aliases", "ambiguous", "none"])
def test_period_index_prunes_old_counters_and_keeps_late_reports(pipeline, period_kind):
    timestamp = now()
    start, end = timestamp - timedelta(days=5), timestamp - timedelta(days=3)
    for day in range(31):
        pipeline.import_records(
            [record("history", timestamp - timedelta(days=day), f"day-{day}")], synthetic=True
        )
    attributes = {}
    if period_kind != "none":
        prefix = "" if period_kind == "declared" else "source_"
        attributes = {
            f"{prefix}period_start": start.isoformat(),
            f"{prefix}period_end": end.isoformat(),
        }
        if period_kind == "ambiguous":
            attributes["period_start"] = None
        pipeline.import_records(
            [record("history", timestamp, "late-report", attributes=attributes)], synthetic=True
        )
    before = pipeline.history.metrics["get_requests"]
    rows, manifests = pipeline.history.observations(
        timestamp,
        True,
        entity_keys={"compact_fixture:video:history"},
        since=start,
        until=end,
        period_window=(start, end),
    )
    expected = {"day-3", "day-4", "day-5"}
    if period_kind != "none":
        expected.add("late-report")
    assert {row.observation_id for row in rows} == expected
    assert len(manifests) == len(expected)
    assert pipeline.history.metrics["get_requests"] - before == len(expected)


def test_unknown_legacy_period_metadata_remains_a_conservative_match(pipeline):
    from psycopg.types.json import Jsonb

    timestamp = now()
    dataset = pipeline.import_records(
        [record("legacy-period", timestamp - timedelta(days=30), "legacy-old")], synthetic=True
    )[0]
    manifest = pipeline.catalog.one(
        "SELECT manifest FROM datasets WHERE dataset_id=%s", (dataset,)
    )["manifest"]
    for obj in manifest["normalized"]:
        for key in ("periods_indexed", "max_period_start", "min_period_end"):
            obj.pop(key)
    pipeline.catalog.execute(
        "UPDATE datasets SET manifest=%s WHERE dataset_id=%s", (Jsonb(manifest), dataset)
    )
    before = pipeline.history.metrics["get_requests"]
    rows, manifests = pipeline.history.observations(
        timestamp,
        True,
        entity_keys={"compact_fixture:video:legacy-period"},
        since=timestamp - timedelta(days=5),
        until=timestamp,
        period_window=(timestamp - timedelta(days=5), timestamp),
    )
    assert rows == [] and len(manifests) == 1
    assert pipeline.history.metrics["get_requests"] - before == 1


def test_committing_unchanged_manifest_preserves_compact_object_mapping(pipeline, monkeypatch):
    before = []
    commit = pipeline.catalog.commit_dataset

    def capture(dataset_id, *args, **kwargs):
        before.extend(
            pipeline.catalog.all(
                "SELECT object_id,object_key FROM normalized_objects WHERE dataset_id=%s",
                (dataset_id,),
            )
        )
        return commit(dataset_id, *args, **kwargs)

    monkeypatch.setattr(pipeline.catalog, "commit_dataset", capture)
    dataset = pipeline.import_records([record("stable-map", now(), "stable-map")], synthetic=True)[
        0
    ]
    after = pipeline.catalog.all(
        "SELECT object_id,object_key FROM normalized_objects WHERE dataset_id=%s", (dataset,)
    )
    assert before == after and len(after) == 1
    pipeline.catalog.invalidate_dataset(dataset, "controlled-retirement")
    assert pipeline.catalog.one("SELECT count(*) AS n FROM observation_deliveries")["n"] == 0
