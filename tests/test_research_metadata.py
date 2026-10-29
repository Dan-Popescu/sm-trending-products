"""Research admits complete SQL metadata before fetching any source payload."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from time import monotonic, sleep

import pytest
from test_competition_insights import observation
from test_decision_changes import changes
from test_decision_changes import seed as changes_seed
from test_evaluation_integration import record

from sm_trending_products.analysis_batches import MetadataBudgetExceeded, WindowMetadataBudget
from sm_trending_products.catalog import RETENTION_GUARD_KEY
from sm_trending_products.competition_insights import CompetitionInsights
from sm_trending_products.contracts import canonical, now
from sm_trending_products.decision_brief import DecisionBrief
from sm_trending_products.trend_insights import TrendInsights

pytestmark = pytest.mark.integration


def seed(pipeline, kind, *, datasets=1, entities=20):
    cutoff = now() + timedelta(minutes=1)
    pipeline.create_product("p", "Research product", synthetic=True)
    pipeline.settings.history_target_rows_per_file = 1
    pipeline.settings.history_partition_buckets = 64
    keys = set()
    for sample in range(datasets):
        at = cutoff - timedelta(hours=sample)
        rows = [
            record(f"entity-{index}", at, 100 + sample)
            if kind == "trend"
            else observation(f"entity-{index}", at)
            for index in range(entities)
        ]
        pipeline.import_records(rows, synthetic=True)
        keys.update(f"{row['source']}:{row['entity_type']}:{row['entity_id']}" for row in rows)
    for key in sorted(keys):
        pipeline.review_link("p", key)
    service = TrendInsights if kind == "trend" else CompetitionInsights
    return service(pipeline.catalog, pipeline.history), cutoff, keys


def capture_admission(pipeline, monkeypatch):
    """Inspect the actual SQL response, including the refused scalar-only case."""
    results = []
    original = pipeline.catalog._select_objects

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def fetchone(self):
            row = self.cursor.fetchone()
            results.append(row)
            return row

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, *args):
            return Cursor(self.connection.execute(*args))

    def select(connection, *args, **kwargs):
        return original(Connection(connection), *args, **kwargs)

    monkeypatch.setattr(pipeline.catalog, "_select_objects", select)
    return results


@pytest.mark.parametrize("kind", ["trend", "competition"])
@pytest.mark.parametrize("datasets,entities", [(1, 20), (4, 1)])
def test_object_refusal_is_scalar_and_precedes_all_payload_traffic(
    pipeline, monkeypatch, kind, datasets, entities
):
    service, cutoff, _ = seed(pipeline, kind, datasets=datasets, entities=entities)
    count = pipeline.catalog.one("SELECT count(*) AS count FROM normalized_objects")["count"]
    assert count > 2
    assert len(pipeline.datasets()) == datasets
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    admissions = capture_admission(pipeline, monkeypatch)
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="object"):
        service.describe("p", cutoff, True)
    assert pipeline.history.metrics == before
    assert admissions[-1]["objects"] == 3
    assert admissions[-1]["records"] is None
    assert (
        pipeline.catalog.one(
            "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=%s "
            "AND mode='ShareLock' AND granted",
            (RETENTION_GUARD_KEY,),
        )
        is None
    )


@pytest.mark.parametrize("kind", ["trend", "competition"])
@pytest.mark.parametrize("oversized", ["descriptor", "aggregate"])
def test_byte_refusal_is_scalar_for_one_descriptor_and_aggregate_history(
    pipeline, monkeypatch, kind, oversized
):
    service, cutoff, _ = seed(
        pipeline, kind, datasets=1 if oversized == "descriptor" else 12, entities=1
    )
    if oversized == "descriptor":
        pipeline.catalog.execute(
            "UPDATE normalized_objects SET descriptor=descriptor || "
            "jsonb_build_object('legacy_note',repeat('x',32768))"
        )
    else:
        sizes = pipeline.catalog.all(
            "SELECT octet_length(descriptor::text) AS bytes FROM normalized_objects"
        )
        assert max(row["bytes"] for row in sizes) < 8192
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_metadata_bytes_per_batch": 8192}
    )
    admissions = capture_admission(pipeline, monkeypatch)
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="byte"):
        service.describe("p", cutoff, True)
    assert pipeline.history.metrics == before
    assert admissions[-1]["records"] is None
    assert admissions[-1]["bytes"] > 8192


@pytest.mark.parametrize("kind", ["trend", "competition"])
def test_exact_object_and_encoded_byte_boundaries_admit_complete_reports(pipeline, kind):
    service, cutoff, keys = seed(pipeline, kind, datasets=3, entities=1)
    oracle = service.describe("p", cutoff, True)
    budget = WindowMetadataBudget(100, 1000000)
    with pipeline.catalog.window_snapshot(True) as connection:
        pipeline.catalog.select_history_objects(
            connection, budget, synthetic=True, as_of=cutoff, entity_keys=keys
        )
    required_bytes = budget.bytes + sum(len(key.encode()) + 512 for key in keys)
    settings = pipeline.settings.model_copy(
        update={
            "analysis_max_objects_per_batch": budget.objects,
            "analysis_max_metadata_bytes_per_batch": required_bytes,
        }
    )
    pipeline.history.settings = settings
    assert canonical(service.describe("p", cutoff, True)) == canonical(oracle)
    for field, value, resource in (
        ("analysis_max_objects_per_batch", budget.objects - 1, "object"),
        ("analysis_max_metadata_bytes_per_batch", required_bytes - 1, "byte"),
    ):
        pipeline.history.settings = settings.model_copy(update={field: value})
        before = dict(pipeline.history.metrics)
        with pytest.raises(MetadataBudgetExceeded, match=resource):
            service.describe("p", cutoff, True)
        assert pipeline.history.metrics == before


@pytest.mark.parametrize("kind", ["trend", "competition"])
@pytest.mark.parametrize("legacy", [False, True])
def test_complete_report_bytes_ids_and_oldest_delivery_match_legacy_reader(
    pipeline, monkeypatch, kind, legacy
):
    service, cutoff, keys = seed(pipeline, kind, datasets=3, entities=3)
    # An additional delivery must keep the oldest active raw provenance.
    rows = pipeline.history.read(
        pipeline.catalog.iter_manifests(synthetic=True, as_of=cutoff, entity_keys=keys),
        as_of=cutoff,
        entity_keys=keys,
    )
    pipeline.import_records([row.model_dump(mode="json") for row in rows], synthetic=True)
    if legacy:
        pipeline.catalog.execute(
            "UPDATE normalized_objects SET min_observed_at=NULL,max_observed_at=NULL,"
            "min_available_at=NULL,max_available_at=NULL,periods_indexed=false,"
            "descriptor=descriptor-'min_observed_at'-'max_observed_at'"
            "-'min_available_at'-'max_available_at'-'periods_indexed'"
        )

    def legacy_read(**kwargs):
        selection = {key: value for key, value in kwargs.items() if key != "max_rows"}
        return pipeline.history.read(
            pipeline.catalog.iter_manifests(**selection),
            **{key: value for key, value in kwargs.items() if key != "synthetic"},
        )

    with monkeypatch.context() as old:
        old.setattr(pipeline.history, "read_scoped", legacy_read)
        oracle = service.describe("p", cutoff, True)
    assert canonical(service.describe("p", cutoff, True)) == canonical(oracle)


@pytest.mark.parametrize("legacy", [False, True])
def test_period_source_dataset_and_legacy_bounds_preserve_selected_history(pipeline, legacy):
    cutoff = now() + timedelta(minutes=1)
    start = cutoff - timedelta(days=3)
    end = start + timedelta(days=1)
    delayed = observation("period", start - timedelta(days=2))
    delayed["available_at"] = (start - timedelta(days=1)).isoformat()
    delayed["attributes"].update(period_start=start.isoformat(), period_end=end.isoformat())
    selected = pipeline.import_records([delayed], synthetic=True)[0]
    pipeline.import_records([observation("period", end)], synthetic=True)
    pipeline.import_records([record("other", end, 100)], synthetic=True)
    if legacy:
        pipeline.catalog.execute(
            "UPDATE normalized_objects SET min_observed_at=NULL,max_observed_at=NULL,"
            "min_available_at=NULL,max_available_at=NULL,periods_indexed=false"
        )
    kwargs = dict(
        synthetic=True,
        as_of=cutoff,
        since=start,
        until=end,
        period_window=(start, end),
        source="competition_fixture",
        dataset_ids={selected},
        entity_keys={"competition_fixture:offer:period", "fixture_import:video:other"},
    )
    manifests = list(pipeline.catalog.iter_manifests(**kwargs))
    assert [row["dataset_id"] for row in manifests] == [selected]
    read_args = {
        key: value
        for key, value in kwargs.items()
        if key not in {"synthetic", "source", "dataset_ids"}
    }
    oracle = pipeline.history.read(manifests, **read_args)
    assert len(oracle) == 1
    assert pipeline.history.read_scoped(**kwargs) == oracle


def test_research_snapshot_starts_after_waiting_retirement_commits(pipeline):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    _, cutoff, keys = seed(pipeline, "trend", entities=1)
    before = dict(pipeline.history.metrics)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pipeline.catalog.transaction() as retiring:
            pipeline.catalog.guard_retention(retiring, exclusive=True)
            future = executor.submit(
                pipeline.history.read_scoped, synthetic=True, as_of=cutoff, entity_keys=keys
            )
            deadline = monotonic() + 5
            while monotonic() < deadline:
                if pipeline.catalog.one(
                    "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=%s AND NOT granted",
                    (RETENTION_GUARD_KEY,),
                ):
                    break
                sleep(0.01)
            else:
                pytest.fail("Research reader did not wait for the native retirement fence")
            retiring.execute("UPDATE datasets SET state='invalidated'")
        assert future.result(timeout=10) == []
    assert pipeline.history.metrics == before


def test_retirement_waits_through_selected_research_payload_read(pipeline, monkeypatch):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    service, cutoff, _ = seed(pipeline, "trend", entities=1)
    original = pipeline.history.read

    def held_read(*args, **kwargs):
        # A separate native backend cannot retire the admitted input mid-read.
        with pipeline.catalog.transaction() as retiring:
            locked = retiring.execute(
                "SELECT pg_try_advisory_xact_lock(%s) AS locked", (RETENTION_GUARD_KEY,)
            ).fetchone()
            assert locked["locked"] is False
        return original(*args, **kwargs)

    with monkeypatch.context() as held:
        held.setattr(pipeline.history, "read", held_read)
        report = service.describe("p", cutoff, True)
    assert report["input_observation_count"] == 1
    with pipeline.catalog.transaction() as retiring:
        pipeline.catalog.guard_retention(retiring, exclusive=True)
        retiring.execute("UPDATE datasets SET state='invalidated'")
    assert service.describe("p", cutoff, True)["input_observation_count"] == 0


def test_composed_brief_and_changes_refuse_required_metadata_without_partial_reports(pipeline):
    previous, current, _ = changes_seed(pipeline)
    # A single dataset can exceed the research object limit.
    pipeline.settings.history_target_rows_per_file = 1
    pipeline.settings.history_partition_buckets = 64
    rows = [record(f"extra-{i}", previous - timedelta(hours=1), 100) for i in range(20)]
    pipeline.import_records(rows, synthetic=True)
    for row in rows:
        pipeline.review_link("p1", f"fixture_import:video:{row['entity_id']}")
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    before = dict(pipeline.history.metrics)
    for describe in (
        lambda: DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance).describe(
            "p1", "FR", current, True
        ),
        lambda: changes(pipeline, previous, current),
    ):
        with pytest.raises(MetadataBudgetExceeded):
            describe()
        assert pipeline.history.metrics == before
