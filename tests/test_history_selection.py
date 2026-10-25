"""Bounded evidence selection and maintenance at the native SQL/object boundary."""

import base64
import json
import os
import threading
from datetime import timedelta

import pytest
from psycopg.types.json import Jsonb

from sm_trending_products.contracts import Observation, now

pytestmark = pytest.mark.integration


def record(entity, timestamp, *, observation_id=None, value=1):
    result = {
        "source": "selection_fixture",
        "entity_type": "video",
        "entity_id": entity,
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "synthetic": True,
        "metrics": {"view_count": {"lower": value, "upper": value}},
        "attributes": {"title": "A reviewed product"},
    }
    if observation_id:
        result["observation_id"] = observation_id
    return result


def test_small_pages_keep_durable_raw_but_avoid_hash_bucket_fanout(pipeline):
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"history_partition_buckets": 64}
    )
    timestamp = now() - timedelta(hours=1)
    before = pipeline.history.metrics["put_requests"]
    dataset = pipeline.import_records(
        [record(f"video-{index}", timestamp) for index in range(20)], synthetic=True
    )[0]
    metadata = pipeline.catalog.one("SELECT manifest FROM datasets WHERE dataset_id=%s", (dataset,))
    manifest = metadata["manifest"]
    assert manifest["partition_buckets"] == 64
    assert manifest["physical_partition_buckets"] == 1
    assert len(manifest["normalized"]) == 1
    assert pipeline.history.metrics["put_requests"] - before == 3
    assert len(manifest["normalized"][0]["entity_keys"]) == 20
    assert pipeline.verify(dataset)["ok"]


def test_compaction_uses_larger_read_files_without_rewriting_source_history(pipeline):
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"history_partition_buckets": 64}
    )
    timestamp = now() - timedelta(hours=1)
    parents = [
        pipeline.import_records(
            [record(f"compact-{group}-{row}", timestamp) for row in range(1100)],
            synthetic=True,
        )[0]
        for group in range(2)
    ]
    original_manifests = {
        row["dataset_id"]: row["manifest"]
        for row in pipeline.catalog.all(
            "SELECT dataset_id,manifest FROM datasets WHERE dataset_id=ANY(%s)", (parents,)
        )
    }
    assert all(
        manifest["physical_partition_buckets"] == 2 for manifest in original_manifests.values()
    )
    before, _ = pipeline.history.observations(now(), True)
    result = pipeline.compact(synthetic=True)
    assert result["rows"] == 2200
    assert result["normalized_objects_before"] == 4
    assert result["normalized_objects_after"] == 1
    manifest = pipeline.catalog.one(
        "SELECT manifest FROM datasets WHERE dataset_id=%s", (result["dataset_id"],)
    )["manifest"]
    assert manifest["physical_partition_buckets"] == 1
    assert manifest["row_count"] == 2200
    after, _ = pipeline.history.observations(now(), True)
    assert after == before
    assert {row.raw_dataset_id for row in after} == set(parents)
    assert {
        row["dataset_id"]: row["manifest"]
        for row in pipeline.catalog.all(
            "SELECT dataset_id,manifest FROM datasets WHERE dataset_id=ANY(%s)", (parents,)
        )
    } == original_manifests
    assert pipeline.verify()["ok"]
    assert pipeline.replay(result["dataset_id"])["records"] == 2200
    replayed, _ = pipeline.history.observations(now(), True)
    assert replayed == before


def test_scoped_manifest_pages_prune_unrelated_objects_before_transfer(pipeline):
    timestamp = now() - timedelta(hours=1)
    for index in range(12):
        pipeline.import_records([record(f"other-{index}", timestamp)], synthetic=True)
    target = pipeline.import_records([record("target", timestamp)], synthetic=True)[0]
    scoped = list(
        pipeline.catalog.iter_manifests(
            synthetic=True,
            as_of=now(),
            entity_keys={"selection_fixture:video:target"},
            page_size=1,
        )
    )
    assert [row["dataset_id"] for row in scoped] == [target]
    assert set(scoped[0]["manifest"]) == {"dataset_id", "normalized"}
    assert scoped[0]["manifest"]["normalized"][0]["entity_keys"] == [
        "selection_fixture:video:target"
    ]
    assert list(pipeline.catalog.iter_manifests(synthetic=True, entity_keys=set())) == []
    # Metadata keyset paging neither repeats nor drops equal-time records.
    all_rows = list(pipeline.catalog.iter_manifests(synthetic=True, page_size=2))
    assert len(all_rows) == 13
    assert len({row["dataset_id"] for row in all_rows}) == 13


def test_object_index_upgrade_reads_legacy_descriptors_and_indexes_later_publications(pipeline):
    timestamp = now() - timedelta(hours=1)
    dataset = pipeline.import_records([record("legacy", timestamp)], synthetic=True)[0]
    metadata = pipeline.catalog.one("SELECT manifest FROM datasets WHERE dataset_id=%s", (dataset,))
    manifest = metadata["manifest"]
    for obj in manifest["normalized"]:
        for name in ("min_observed_at", "max_observed_at", "min_available_at", "max_available_at"):
            obj.pop(name)
    pipeline.catalog.execute(
        "UPDATE datasets SET manifest=%s WHERE dataset_id=%s", (Jsonb(manifest), dataset)
    )
    # Re-run the additive migration against populated historical manifests.
    with pipeline.catalog.transaction() as connection:
        connection.execute("DROP TRIGGER normalized_manifest_index ON datasets")
        connection.execute("DROP TABLE observation_deliveries")
        connection.execute("DROP TABLE normalized_object_entities")
        connection.execute("DROP TABLE normalized_objects")
        # Rebuild only the dropped object-reference schema and its collation/
        # compact-reference upgrades. Independent later schemas remain installed.
        connection.execute("DELETE FROM schema_migrations WHERE version IN (14,15,20)")
    pipeline.catalog.initialize()
    rows, manifests = pipeline.history.observations(
        now(),
        True,
        entity_keys={"selection_fixture:video:legacy"},
        since=timestamp,
    )
    assert len(rows) == len(manifests) == 1
    assert rows[0].entity_id == "legacy"
    later = pipeline.import_records([record("later", timestamp)], synthetic=True)[0]
    assert [
        row["dataset_id"]
        for row in pipeline.catalog.manifests(
            synthetic=True,
            entity_keys={"selection_fixture:video:later"},
        )
    ] == [later]


def test_evidence_pages_are_stable_beyond_analysis_budget_and_replay_copies(pipeline):
    timestamp = now() - timedelta(hours=2)
    dataset = pipeline.import_records(
        [
            record(
                "long-history",
                timestamp + timedelta(seconds=index // 3),
                observation_id=f"observation-{index:04d}",
                value=index,
            )
            for index in range(205)
        ],
        synthetic=True,
    )[0]
    pipeline.create_product("long-history-product", "History product", synthetic=True)
    pipeline.review_link("long-history-product", "selection_fixture:video:long-history")
    cutoff = now() + timedelta(seconds=1)
    pipeline.replay(dataset)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_rows_per_batch": 100}
    )
    cursor = None
    identifiers = []
    while True:
        page = pipeline.evidence_page("long-history-product", cutoff, True, limit=37, cursor=cursor)
        assert len(page["records"]) <= 37
        identifiers.extend(row["observation_id"] for row in page["records"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert identifiers == [f"observation-{index:04d}" for index in range(205)]
    offset_page = pipeline.evidence("long-history-product", cutoff, True, limit=40, offset=150)
    assert [row["observation_id"] for row in offset_page] == identifiers[150:190]
    first = pipeline.evidence_page("long-history-product", cutoff, True, limit=10)
    with pytest.raises(ValueError, match="cursor"):
        pipeline.evidence_page("another-product", cutoff, True, cursor=first["next_cursor"])


def test_evidence_page_detects_conflicts_even_beyond_its_candidate_boundary(pipeline):
    timestamp = now() - timedelta(hours=2)
    original = record("conflict", timestamp, observation_id="shared", value=1)
    pipeline.import_records([original], synthetic=True)
    pipeline.create_product("conflict-product", "Conflict product", synthetic=True)
    pipeline.review_link("conflict-product", "selection_fixture:video:conflict")
    changed = Observation.model_validate(
        {
            **record("conflict", timestamp + timedelta(hours=1), observation_id="shared", value=2),
        }
    )
    pipeline.history.publish(
        [changed], {"kind": "controlled-conflict"}, source="selection_fixture", synthetic=True
    )
    with pytest.raises(ValueError, match="Conflicting payloads"):
        pipeline.evidence_page("conflict-product", now(), True, limit=1)


@pytest.mark.parametrize("upgrade_from_v14", [False, True])
def test_evidence_keyset_uses_unicode_order_with_non_c_collation(pipeline, upgrade_from_v14):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Native PostgreSQL with ICU is required for the locale regression")
    pipeline.catalog.execute("CREATE COLLATION identifier_locale (provider=icu, locale='en-US')")
    locale_order = pipeline.catalog.all(
        "SELECT identifier FROM (VALUES ('a'),('B')) AS ids(identifier) "
        "ORDER BY identifier COLLATE identifier_locale"
    )
    assert [row["identifier"] for row in locale_order] == ["a", "B"]
    pipeline.catalog.execute(
        "ALTER TABLE observation_deliveries ALTER COLUMN observation_id "
        "TYPE text COLLATE identifier_locale"
    )
    if upgrade_from_v14:
        # Existing v14 databases have locale-dependent indexes and indexed history.
        with pipeline.catalog.transaction() as connection:
            connection.execute("DROP INDEX observation_page_lookup")
            connection.execute("DROP INDEX observation_conflict_lookup")
            connection.execute(
                "CREATE INDEX observation_page_lookup ON observation_deliveries "
                "(entity_key,observed_at,observation_id,available_at)"
            )
            connection.execute(
                "CREATE INDEX observation_conflict_lookup ON observation_deliveries "
                "(observation_id,entity_key,available_at)"
            )
            connection.execute("DELETE FROM schema_migrations WHERE version=15")
    timestamp = now() - timedelta(hours=1)
    identifiers = ["a", "B", "c", "Z", "é", "中"]
    pipeline.import_records(
        [record("locale", timestamp, observation_id=identifier) for identifier in identifiers],
        synthetic=True,
    )
    if upgrade_from_v14:
        pipeline.catalog.initialize()
    pipeline.create_product("locale-product", "Locale product", synthetic=True)
    pipeline.review_link("locale-product", "selection_fixture:video:locale")
    cutoff = now()
    cursor = None
    actual = []
    for _ in range(len(identifiers) + 1):
        page = pipeline.evidence_page("locale-product", cutoff, True, limit=1, cursor=cursor)
        actual.extend(row["observation_id"] for row in page["records"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert cursor is None
    assert actual == sorted(identifiers)
    index_query = (
        "SELECT c.oid,c.relname,pg_get_indexdef(c.oid) AS indexdef FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() "
        "AND c.relname IN ('observation_page_lookup','observation_conflict_lookup') "
        "ORDER BY c.relname"
    )
    indexes = pipeline.catalog.all(index_query)
    assert len(indexes) == 2
    assert all('COLLATE "C"' in row["indexdef"] for row in indexes)
    assert pipeline.catalog.one("SELECT version FROM schema_migrations WHERE version=15")
    pipeline.catalog.initialize()
    assert pipeline.catalog.all(index_query) == indexes


def test_current_page_reads_only_selected_objects_and_retirement_removes_lookup(pipeline):
    timestamp = now() - timedelta(hours=2)
    datasets = []
    for index in range(8):
        datasets.extend(
            pipeline.import_records(
                [
                    record(
                        "paged",
                        timestamp + timedelta(minutes=index),
                        observation_id=f"page-{index}",
                    )
                ],
                synthetic=True,
            )
        )
    pipeline.create_product("paged-product", "Paged product", synthetic=True)
    pipeline.review_link("paged-product", "selection_fixture:video:paged")
    before = pipeline.history.metrics["get_requests"]
    page = pipeline.evidence_page("paged-product", now(), True, limit=1)
    assert page["status"] == "ready"
    assert [row["observation_id"] for row in page["records"]] == ["page-0"]
    assert pipeline.history.metrics["get_requests"] - before == 1
    pipeline.catalog.invalidate_dataset(datasets[0], "controlled_retirement")
    indexed = pipeline.catalog.one(
        "SELECT count(*) AS count FROM observation_deliveries o "
        "JOIN normalized_objects n ON n.object_id=o.object_id WHERE n.dataset_id=%s",
        (datasets[0],),
    )
    assert indexed["count"] == 0


def test_legacy_indexing_is_a_bounded_explicit_progress_state(pipeline):
    timestamp = now() - timedelta(hours=2)
    for index in range(3):
        pipeline.import_records(
            [
                record(
                    "legacy-pages",
                    timestamp + timedelta(minutes=index),
                    observation_id=f"legacy-{index}",
                )
            ],
            synthetic=True,
        )
    pipeline.create_product("legacy-pages-product", "Legacy pages", synthetic=True)
    pipeline.review_link("legacy-pages-product", "selection_fixture:video:legacy-pages")
    pipeline.catalog.execute("DELETE FROM observation_deliveries")
    pipeline.catalog.execute("UPDATE normalized_objects SET observations_indexed=false")
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"history_index_objects_per_call": 1}
    )
    cutoff = now() + timedelta(seconds=1)
    for expected in (1, 2):
        page = pipeline.evidence_page("legacy-pages-product", cutoff, True, limit=10)
        assert page == {"records": [], "next_cursor": None, "status": "indexing"}
        count = pipeline.catalog.one(
            "SELECT count(*) AS count FROM normalized_objects WHERE observations_indexed"
        )
        assert count["count"] == expected
    complete = pipeline.evidence_page("legacy-pages-product", cutoff, True, limit=10)
    assert complete["status"] == "ready"
    assert [row["observation_id"] for row in complete["records"]] == [
        "legacy-0",
        "legacy-1",
        "legacy-2",
    ]


def test_full_compaction_output_does_not_starve_smaller_lots(pipeline):
    pipeline.settings = pipeline.settings.model_copy(update={"analysis_max_rows_per_batch": 100})
    timestamp = now() - timedelta(hours=2)
    full = pipeline.import_records(
        [record("full", timestamp + timedelta(seconds=index)) for index in range(100)],
        synthetic=True,
    )[0]
    small = [
        pipeline.import_records(
            [record(f"small-{index}", timestamp + timedelta(minutes=10))], synthetic=True
        )[0]
        for index in range(2)
    ]
    result = pipeline.compact(synthetic=True)
    assert result["status"] == "compacted"
    assert set(result["parent_dataset_ids"]) == set(small)
    assert full not in result["parent_dataset_ids"]
    assert result["rows"] == 2


def test_incompatible_early_groups_do_not_fill_compaction_candidate_window(pipeline):
    timestamp = now() - timedelta(hours=1)
    for index in range(3):
        row = {**record("singleton", timestamp), "source": f"singleton_{index}"}
        pipeline.import_records([row], synthetic=True)
    later = [
        pipeline.import_records([record(f"merge-{index}", timestamp)], synthetic=True)[0]
        for index in range(2)
    ]
    result = pipeline.compact(synthetic=True, max_datasets=2)
    assert result["status"] == "compacted"
    assert set(result["parent_dataset_ids"]) == set(later)


def test_compaction_does_not_publish_one_parent_after_frontier_race(pipeline, monkeypatch):
    timestamp = now() - timedelta(hours=1)
    first = pipeline.import_records([record("race-first", timestamp)], synthetic=True)[0]
    pipeline.import_records([record("race-second", timestamp)], synthetic=True)
    original_all = pipeline.catalog.all

    def changed_candidates(query, params=()):
        if "ORDER BY row_count,created_at,dataset_id LIMIT" in query:
            rows = original_all(query, params)
            return [row for row in rows if row["dataset_id"] == first]
        return original_all(query, params)

    monkeypatch.setattr(pipeline.catalog, "all", changed_candidates)
    before = pipeline.history.metrics["put_requests"]
    assert pipeline.compact(synthetic=True)["status"] == "no_work"
    assert pipeline.history.metrics["put_requests"] == before


def test_indexed_page_preserves_availability_cutoff_and_current_link_owner(pipeline):
    timestamp = now() - timedelta(hours=2)
    pipeline.import_records(
        [record("temporal", timestamp, observation_id="temporal")], synthetic=True
    )
    pipeline.create_product("old-owner", "Old owner", synthetic=True)
    pipeline.review_link("old-owner", "selection_fixture:video:temporal")
    before_transfer = now()
    pipeline.create_product("new-owner", "New owner", synthetic=True)
    pipeline.review_link("new-owner", "selection_fixture:video:temporal")
    late = Observation.model_validate(
        {
            **record("temporal", timestamp, observation_id="temporal", value=10),
            "available_at": (now() + timedelta(days=1)).isoformat(),
        }
    )
    pipeline.history.publish(
        [late], {"kind": "late-correction"}, source="selection_fixture", synthetic=True
    )
    old = pipeline.evidence_page("old-owner", before_transfer, True)
    assert len(old["records"]) == 1
    assert old["records"][0]["metrics"]["view_count"]["lower"] == 1
    assert pipeline.evidence_page("old-owner", now(), True)["records"] == []
    assert len(pipeline.evidence_page("new-owner", now(), True)["records"]) == 1
    with pytest.raises(ValueError, match="Conflicting payloads"):
        pipeline.evidence_page("new-owner", now() + timedelta(days=2), True)


def test_staged_legacy_publication_retries_after_layout_upgrade(pipeline, monkeypatch):
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"history_partition_buckets": 64}
    )
    timestamp = now() - timedelta(hours=1)
    observations = [
        Observation.model_validate(
            record(f"staged-{index}", timestamp, observation_id=f"staged-{index}")
        )
        for index in range(20)
    ]
    raw = {"kind": "controlled-stage", "observed_at": timestamp.isoformat()}
    normalize = pipeline.history.normalized_objects
    stage = pipeline.catalog.stage_dataset

    def old_objects(rows, prefix, partition_buckets=None, **kwargs):
        return normalize(rows, prefix, 64, physical_buckets=64)

    def old_manifest(dataset_id, source, synthetic, manifest, **kwargs):
        manifest.pop("physical_partition_buckets", None)
        for obj in manifest["normalized"]:
            for name in (
                "min_observed_at",
                "max_observed_at",
                "min_available_at",
                "max_available_at",
            ):
                obj.pop(name, None)
        return stage(dataset_id, source, synthetic, manifest, **kwargs)

    def crash_before_commit(*args, **kwargs):
        raise RuntimeError("controlled interruption before SQL commit")

    with monkeypatch.context() as failed:
        failed.setattr(pipeline.history, "normalized_objects", old_objects)
        failed.setattr(pipeline.catalog, "stage_dataset", old_manifest)
        failed.setattr(pipeline.catalog, "commit_dataset", crash_before_commit)
        with pytest.raises(RuntimeError, match="controlled interruption"):
            pipeline.history.publish(
                observations,
                raw,
                source="selection_fixture",
                synthetic=True,
                dataset_id="old-staged-layout",
            )
    staged = pipeline.catalog.one(
        "SELECT manifest,state FROM datasets WHERE dataset_id=%s", ("old-staged-layout",)
    )
    assert staged["state"] == "staged"
    assert "physical_partition_buckets" not in staged["manifest"]
    assert len(staged["manifest"]["normalized"]) > 1
    pipeline.history.settings = pipeline.settings
    pipeline.history.publish(
        observations,
        raw,
        source="selection_fixture",
        synthetic=True,
        dataset_id="old-staged-layout",
    )
    committed = pipeline.catalog.one(
        "SELECT manifest,state FROM datasets WHERE dataset_id=%s", ("old-staged-layout",)
    )
    assert committed == {"manifest": staged["manifest"], "state": "committed"}
    assert pipeline.verify("old-staged-layout")["ok"]
    records, _ = pipeline.history.observations(now(), True)
    assert len(records) == 20


@pytest.mark.parametrize("staged_target,resumed_target", [(1, 5000), (5000, 1)])
def test_staged_compaction_retries_after_sizing_change(
    pipeline, monkeypatch, staged_target, resumed_target
):
    timestamp = now() - timedelta(hours=1)
    for index in range(2):
        pipeline.import_records(
            [record(f"compact-{index}-{row}", timestamp) for row in range(10)], synthetic=True
        )
    pipeline.settings = pipeline.settings.model_copy(
        update={"compaction_target_rows_per_file": staged_target}
    )

    def interrupted_index(*args, **kwargs):
        raise RuntimeError("controlled compaction interruption")

    with monkeypatch.context() as failed:
        failed.setattr(pipeline.catalog, "index_observations", interrupted_index)
        with pytest.raises(RuntimeError, match="controlled compaction interruption"):
            pipeline.compact(synthetic=True)
    staged = pipeline.catalog.one("SELECT dataset_id,manifest FROM datasets WHERE state='staged'")
    assert len(staged["manifest"]["normalized"]) == (4 if staged_target == 1 else 1)
    pipeline.settings = pipeline.settings.model_copy(
        update={"compaction_target_rows_per_file": resumed_target}
    )
    result = pipeline.compact(synthetic=True)
    assert result["dataset_id"] == staged["dataset_id"]
    committed = pipeline.catalog.one(
        "SELECT manifest FROM datasets WHERE dataset_id=%s", (result["dataset_id"],)
    )
    assert committed["manifest"] == staged["manifest"]
    assert pipeline.verify()["ok"]
    records, _ = pipeline.history.observations(now(), True)
    assert len(records) == 20


@pytest.mark.parametrize("interrupted_legacy_compaction", [False, True])
def test_compaction_keeps_oldest_delivery_when_smaller_replay_is_selected_first(
    pipeline, monkeypatch, interrupted_legacy_compaction
):
    timestamp = now() - timedelta(hours=1)
    shared = record("shared", timestamp, observation_id="shared-delivery")
    original = pipeline.import_records(
        [shared, record("original-extra-a", timestamp), record("original-extra-b", timestamp)],
        synthetic=True,
    )[0]
    replay = pipeline.import_records([shared], synthetic=True)[0]
    before, _ = pipeline.history.observations(now(), True)
    assert len(before) == 3
    assert (
        next(row for row in before if row.observation_id == "shared-delivery").raw_dataset_id
        == original
    )
    staged = None
    if interrupted_legacy_compaction:
        read = pipeline.history.read

        def chronological_read(parents, **kwargs):
            # Earlier releases read compaction parents in catalog creation order.
            return read(
                sorted(parents, key=lambda row: (row["created_at"], row["dataset_id"])), **kwargs
            )

        def interrupted_index(*args, **kwargs):
            raise RuntimeError("controlled interruption after legacy manifest upload")

        with monkeypatch.context() as legacy:
            legacy.setattr(pipeline.history, "read", chronological_read)
            legacy.setattr(pipeline.catalog, "index_observations", interrupted_index)
            with pytest.raises(RuntimeError, match="controlled interruption"):
                pipeline.compact(synthetic=True)
        staged = pipeline.catalog.one(
            "SELECT dataset_id,manifest FROM datasets WHERE state='staged'"
        )
        assert staged is not None

    result = pipeline.compact(synthetic=True)
    assert result["status"] == "compacted"
    assert set(result["parent_dataset_ids"]) == {original, replay}
    assert result["rows"] == 3
    if staged:
        committed = pipeline.catalog.one(
            "SELECT dataset_id,manifest FROM datasets WHERE dataset_id=%s",
            (result["dataset_id"],),
        )
        assert committed == staged
    after, _ = pipeline.history.observations(now(), True)
    assert after == before
    assert pipeline.verify()["ok"]


@pytest.mark.parametrize("observed_at", [None, 123, []])
def test_malformed_cursor_fields_raise_controlled_validation_error(pipeline, observed_at):
    cursor = base64.urlsafe_b64encode(
        json.dumps(
            {"scope": "irrelevant", "observed_at": observed_at, "observation_id": "one"}
        ).encode()
    ).decode()
    with pytest.raises(ValueError, match="cursor"):
        pipeline.evidence_page("product", now(), True, cursor=cursor)


def test_exploration_selects_explicit_datasets_in_sql_and_caps_unscoped_metadata(
    pipeline, monkeypatch
):
    from sm_trending_products import exploration

    timestamp = now() - timedelta(hours=1)
    datasets = [
        pipeline.import_records([record(f"explore-{index}", timestamp)], synthetic=True)[0]
        for index in range(4)
    ]
    selected = pipeline.explore(
        "SELECT entity_id FROM observations", True, dataset_ids=[datasets[-1]]
    )
    assert selected["rows"] == [["explore-3"]]
    before = pipeline.history.metrics["get_requests"]
    monkeypatch.setattr(exploration, "MAX_INPUT_FILES", 2)
    with pytest.raises(ValueError, match="narrow"):
        pipeline.explore("SELECT * FROM observations", True)
    assert pipeline.history.metrics["get_requests"] == before


def test_period_scoped_reads_keep_delayed_reports_outside_observed_window(pipeline):
    start = now() - timedelta(days=3)
    end = start + timedelta(days=1)
    row = record("period", start - timedelta(days=1), observation_id="period-report")
    row["available_at"] = (end + timedelta(days=1)).isoformat()
    row["attributes"].update(period_start=start.isoformat(), period_end=end.isoformat())
    pipeline.import_records([row], synthetic=True)
    observations, manifests = pipeline.history.observations(
        now(),
        True,
        entity_keys={"selection_fixture:video:period"},
        since=start,
        until=end,
        period_window=(start, end),
    )
    assert len(manifests) == 1
    assert [row.observation_id for row in observations] == ["period-report"]


def test_page_retirement_keeps_a_complete_snapshot_and_a_valid_next_cursor(pipeline, monkeypatch):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Concurrent retention requires native PostgreSQL backends")
    timestamp = now() - timedelta(hours=2)
    datasets = [
        pipeline.import_records(
            [
                record(
                    "retirement",
                    timestamp + timedelta(minutes=index),
                    observation_id=f"retirement-{index}",
                )
            ],
            synthetic=True,
        )[0]
        for index in range(3)
    ]
    pipeline.create_product("retirement-product", "Retirement product", synthetic=True)
    pipeline.review_link("retirement-product", "selection_fixture:video:retirement")
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        ("selection_fixture",),
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (datasets[0],),
    )
    cutoff = now() + timedelta(seconds=1)
    reading = threading.Event()
    release = threading.Event()
    retiring = threading.Event()
    retired = threading.Event()
    pages, errors = [], []
    scan = pipeline.history._page_query

    def held_scan(*args):
        reading.set()
        assert release.wait(10)
        return scan(*args)

    monkeypatch.setattr(pipeline.history, "_page_query", held_scan)

    def read_page():
        try:
            pages.append(pipeline.evidence_page("retirement-product", cutoff, True, limit=1))
        except Exception as error:
            errors.append(error)

    def retire():
        retiring.set()
        try:
            pipeline.retention(apply=True)
        except Exception as error:
            errors.append(error)
        finally:
            retired.set()

    reader = threading.Thread(target=read_page)
    remover = threading.Thread(target=retire)
    reader.start()
    try:
        assert reading.wait(10)
        remover.start()
        assert retiring.wait(10)
        assert not retired.wait(0.2)
    finally:
        release.set()
        reader.join(timeout=15)
        if remover.ident is not None:
            remover.join(timeout=15)
    assert not reader.is_alive() and not remover.is_alive()
    assert not errors
    assert pages[0]["records"][0]["observation_id"] == "retirement-0"
    assert pages[0]["next_cursor"] is not None
    following = pipeline.evidence_page(
        "retirement-product", cutoff, True, limit=10, cursor=pages[0]["next_cursor"]
    )
    assert [row["observation_id"] for row in following["records"]] == [
        "retirement-1",
        "retirement-2",
    ]
