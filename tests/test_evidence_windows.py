"""Published and frozen callers admit complete Evidence before reading payloads."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from time import monotonic, sleep

import pytest
from analytical_results import snapshot_semantics
from psycopg.types.json import Jsonb
from test_feature_projection import video
from test_feature_windows import rolling_case

from sm_trending_products.analysis_batches import MetadataBudgetExceeded
from sm_trending_products.analytics import AssessmentIdentity, assess
from sm_trending_products.assessment_service import AssessmentService
from sm_trending_products.cache import local_cache_descriptor, pin_local_cached, store_local_cached
from sm_trending_products.catalog import RETENTION_GUARD_KEY
from sm_trending_products.contracts import AssessmentPolicy, canonical, now


@pytest.mark.parametrize("published", [True, False], ids=["published", "frozen"])
def test_one_product_exceeding_object_budget_refuses_before_any_payload_read(pipeline, published):
    cutoff, *_ = rolling_case(pipeline, samples=3, videos=1)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="object"):
        if published:
            pipeline.assess(cutoff, True)
        else:
            pipeline.freeze(cutoff, True)
    assert pipeline.history.metrics == before
    assert (
        pipeline.catalog.one(
            "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=%s "
            "AND mode='ShareLock' AND granted",
            (RETENTION_GUARD_KEY,),
        )
        is None
    )


@pytest.mark.parametrize("oversized", ["coverage", "descriptor"])
def test_oversized_optional_legacy_window_declines_reuse_and_keeps_exact_result(
    pipeline, oversized
):
    cutoff, product, links, _scope, _cache, initial, _audit = rolling_case(
        pipeline, samples=3, videos=1
    )
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 5, "analysis_max_metadata_bytes_per_batch": 16384}
    )
    if oversized == "coverage":
        pipeline.catalog.execute(
            "UPDATE feature_product_windows w SET covered_object_ids=(SELECT array_agg(i) "
            "FROM unnest(w.covered_object_ids) AS i CROSS JOIN generate_series(1,100))"
        )
    else:
        original = pipeline.catalog.one("SELECT descriptor FROM feature_product_windows")[
            "descriptor"
        ]
        with pin_local_cached(pipeline.history, original) as path:
            assert path is not None
            body = path.read_bytes()
        legacy = {**local_cache_descriptor(body, "legacy-metadata"), "legacy_note": "x" * 32768}
        assert store_local_cached(pipeline.history, body, descriptor=legacy) is not None
        pipeline.catalog.execute(
            "UPDATE feature_product_windows SET descriptor=%s", (Jsonb(legacy),)
        )
    actual = pipeline.assess(cutoff, True)
    assert pipeline.history.analysis_metrics["full_windows"] == 1
    assert snapshot_semantics(pipeline, {"rows": actual["rows"]})["rows"] == assess(
        initial.observations, [product], links, cutoff, True
    )


def test_one_large_dataset_is_admitted_by_objects_not_dataset_count(pipeline):
    cutoff = now() + timedelta(minutes=1)
    pipeline.settings.history_target_rows_per_file = 1
    pipeline.settings.history_partition_buckets = 64
    pipeline.create_product("p", "P", synthetic=True)
    observations = [video(f"v{i}", cutoff, 100, cutoff - timedelta(days=40)) for i in range(20)]
    pipeline.import_records([row.model_dump(mode="json") for row in observations], synthetic=True)
    assert len(pipeline.datasets()) == 1
    for row in observations:
        pipeline.review_link("p", row.entity_key)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="object"):
        pipeline.assess(cutoff, True)
    assert pipeline.history.metrics == before


def test_one_oversized_descriptor_refuses_before_payload_transfer(pipeline):
    cutoff = now() + timedelta(minutes=1)
    pipeline.settings.history_partition_buckets = 1
    pipeline.create_product("p", "P", synthetic=True)
    observations = [video(f"v{i}", cutoff, 100, cutoff - timedelta(days=40)) for i in range(1000)]
    pipeline.import_records([row.model_dump(mode="json") for row in observations], synthetic=True)
    pipeline.review_link("p", observations[0].entity_key)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_metadata_bytes_per_batch": 16384}
    )
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="byte"):
        pipeline.assess(cutoff, True)
    assert pipeline.history.metrics == before


def test_many_small_descriptors_share_one_aggregate_byte_budget(pipeline):
    cutoff, *_ = rolling_case(pipeline, samples=40, videos=1)
    sizes = [
        len(canonical(row["descriptor"]).encode())
        for row in pipeline.catalog.all("SELECT descriptor FROM normalized_objects")
    ]
    assert max(sizes) < 16384 < sum(sizes)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={
            "analysis_max_objects_per_batch": 100,
            "analysis_max_metadata_bytes_per_batch": 16384,
        }
    )
    before = dict(pipeline.history.metrics)
    with pytest.raises(MetadataBudgetExceeded, match="byte"):
        pipeline.assess(cutoff, True)
    assert pipeline.history.metrics == before


def test_metadata_adaptive_split_preserves_global_rank_and_complete_publication(pipeline):
    cutoff = now() + timedelta(minutes=1)
    for entity, step in (("a", 10), ("b", 20)):
        pipeline.create_product(entity, entity.upper(), synthetic=True)
        for sample in range(2):
            at = cutoff - timedelta(hours=1 - sample)
            row = video(entity, at, 100 + sample * step, cutoff - timedelta(days=40))
            pipeline.import_records([row.model_dump(mode="json")], synthetic=True)
        pipeline.review_link(entity, row.entity_key)
    oracle = pipeline.assess(cutoff, True)
    # Recompute through the required input path without optional local state.
    pipeline.catalog.execute("DELETE FROM feature_product_projections")
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_max_objects_per_batch": 2}
    )
    actual = pipeline.assess(cutoff, True)
    assert snapshot_semantics(pipeline, {"rows": actual["rows"]}) == snapshot_semantics(
        pipeline, {"rows": oracle["rows"]}
    )
    assert actual["run_id"] == oracle["run_id"]


def test_cached_assessment_still_validates_its_captured_identity_context(pipeline):
    cutoff, product, links, *_ = rolling_case(pipeline)
    pipeline.assess(cutoff, True)
    policy = AssessmentPolicy()
    identity = AssessmentIdentity.resolve(
        [product], links, cutoff, True, policy.min_link_confidence
    )
    with pytest.raises(ValueError, match="identity"):
        AssessmentService(pipeline.history, pipeline.provenance).prepare(
            [product], links, identity, cutoff + timedelta(hours=1), True, policy
        )


def test_an_oversized_later_product_never_publishes_a_partial_assessment(pipeline):
    cutoff = now() + timedelta(minutes=1)
    for entity, samples in (("a", 1), ("b", 3)):
        pipeline.create_product(entity, entity.upper(), synthetic=True)
        for sample in range(samples):
            row = video(
                entity, cutoff - timedelta(minutes=sample), 100, cutoff - timedelta(days=40)
            )
            pipeline.import_records([row.model_dump(mode="json")], synthetic=True)
        pipeline.review_link(entity, row.entity_key)
    pipeline.history.settings = pipeline.settings.model_copy(
        update={"analysis_product_batch_size": 1, "analysis_max_objects_per_batch": 2}
    )
    with pytest.raises(MetadataBudgetExceeded):
        pipeline.assess(cutoff, True)
    assert pipeline.assessment("a", True) is None
    assert pipeline.assessment("b", True) is None
    assert pipeline.latest_assessments(True) == []


def test_window_snapshot_starts_after_a_waiting_retirement_fence(pipeline):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Requires independent native PostgreSQL backends")
    cutoff, _product, _links, scope, cache, *_ = rolling_case(pipeline)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pipeline.catalog.transaction() as retiring:
            pipeline.catalog.guard_retention(retiring, exclusive=True)
            future = executor.submit(
                cache.load_window, pipeline.history, scope, cutoff, AssessmentPolicy()
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
                pytest.fail("Window reader did not wait for the native retirement fence")
            # The authoritative mutation commits while the reader is waiting.
            retiring.execute("UPDATE datasets SET state='invalidated'")
        window = future.result(timeout=10)
    assert window.observations == []
    assert (
        pipeline.catalog.one(
            "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=%s "
            "AND mode='ShareLock' AND granted",
            (RETENTION_GUARD_KEY,),
        )
        is None
    )
