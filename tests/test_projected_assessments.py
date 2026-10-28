"""Real published/frozen callers reuse exact, fully audited product features."""

from datetime import timedelta
from time import perf_counter

import pytest
from analytical_results import assessment_key, snapshot_semantics
from test_feature_projection import video
from test_feature_windows import count_source_pins, rolling_case

import sm_trending_products.assessment_service as service_module
from sm_trending_products.analysis_batches import RowBudgetExceeded
from sm_trending_products.analytics import assess
from sm_trending_products.cache import _Cache
from sm_trending_products.contracts import AssessmentPolicy, Capability, now
from sm_trending_products.evaluation import EvaluationConfig, freeze_decision
from sm_trending_products.maintenance import Maintenance
from sm_trending_products.pipeline import Pipeline


def test_pipeline_future_hits_skip_history_and_full_assessor_and_survive_restart(
    pipeline, monkeypatch
):
    cutoff, product, links, _scope, _cache, initial, _audit = rolling_case(pipeline)
    first = pipeline.assess(cutoff, True)
    later = cutoff + timedelta(hours=1)
    oracle = assess(initial.observations, [product], links, later, True)
    original_root = first["input_provenance"]

    def forbidden(*_args, **_kwargs):
        pytest.fail("unchanged future features must not read or assess raw history")

    monkeypatch.setattr(service_module, "assess", forbidden)
    with Pipeline(pipeline.settings) as reopened:
        monkeypatch.setattr(reopened.history, "read", forbidden)
        second = reopened.assess(later, True)
    assert snapshot_semantics(pipeline, {"rows": second["rows"]})["rows"] == oracle
    assert second["input_provenance"] == original_root
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS count FROM derived_artifact_edges e "
            "JOIN derived_publications p ON p.object_id=e.parent_id WHERE p.object_key=%s",
            (assessment_key(pipeline, second["run_id"]),),
        )["count"]
        > 0
    )


def test_pipeline_counter_refresh_reads_only_new_object_and_matches_full_oracle(
    pipeline, monkeypatch
):
    cutoff, product, links, scope, _cache, _initial, _audit = rolling_case(pipeline)
    pipeline.assess(cutoff, True)
    later = cutoff + timedelta(hours=6)
    pipeline.import_records(
        [
            video(f"v{i}", later, 300 + i, later - timedelta(days=40)).model_dump(mode="json")
            for i in range(3)
        ],
        synthetic=True,
    )
    observations, _ = pipeline.history.observations(
        later, True, entity_keys=set(scope.entity_keys), since=later - timedelta(days=30)
    )
    oracle = assess(observations, [product], links, later, True)
    pins = count_source_pins(monkeypatch)
    updated = pipeline.assess(later, True)
    assert len(pins) == 1
    assert snapshot_semantics(pipeline, {"rows": updated["rows"]})["rows"] == oracle


def test_freeze_reuses_counts_and_roots_without_reading_or_assessing_history(pipeline, monkeypatch):
    cutoff, product, links, _scope, _cache, initial, _audit = rolling_case(pipeline)
    configuration = EvaluationConfig.for_creation({"horizon_days": [3], "launch_delay_days": [0]})
    pipeline.freeze(cutoff, True, configuration.model_dump(mode="json"))
    later = cutoff + timedelta(hours=1)
    oracle = freeze_decision(
        initial.observations, [product], links, later, configuration, synthetic=True
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("frozen cache hits must retain counts without loading raw history")

    monkeypatch.setattr(pipeline.history, "read", forbidden)
    monkeypatch.setattr(service_module, "assess", forbidden)
    frozen = pipeline.freeze(later, True, configuration.model_dump(mode="json"))
    assert snapshot_semantics(pipeline, frozen["result"]) == snapshot_semantics(pipeline, oracle)
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS count FROM evaluation_artifacts WHERE evaluation_id=%s",
            (frozen["evaluation_id"],),
        )["count"]
        > 0
    )


def test_pipeline_and_capability_filtered_decisions_cannot_reuse_incompatible_features(pipeline):
    cutoff, _product, _links, _scope, _cache, _initial, _audit = rolling_case(pipeline)
    pipeline.assess(cutoff, True)
    configuration = EvaluationConfig(horizon_days=(3,), launch_delay_days=(0,))
    service = pipeline._evaluation
    metadata = service._evaluation_inputs(cutoff, True)
    denied = Capability(
        source="projection_fixture",
        account_id="",
        capability="collect",
        state="denied",
        available_at=cutoff - timedelta(hours=1),
        details={},
    )
    metadata["capabilities"] = [denied]
    rejected, _ = service._prepare_decision(metadata, cutoff, True, configuration)
    assert rejected["selected"] == []
    assert rejected["status"] == "insufficient_history"
    # Same clock, same source frontier, different effective capability contract.
    metadata["capabilities"] = [denied.model_copy(update={"state": "available"})]
    granted, _ = service._prepare_decision(metadata, cutoff, True, configuration)
    assert granted["selected"] == ["p"]
    assert granted["coverage"]["observations"] == 15


def test_excluded_raw_inputs_invalidate_cached_hit_lineage_and_queue_physical_window(pipeline):
    cutoff, _product, _links, _scope, _cache, _initial, _audit = rolling_case(pipeline)
    excluded = video("v0", cutoff - timedelta(minutes=1), 999, cutoff - timedelta(days=40))
    excluded = excluded.model_copy(update={"market": "US"})
    datasets = pipeline.import_records([excluded.model_dump(mode="json")], synthetic=True)
    pipeline.catalog.execute("UPDATE products SET market='FR' WHERE product_id='p'")
    first = pipeline.assess(cutoff, True)
    assert first["rows"][0]["coverage"]["observations"] == 0
    saved = pipeline.catalog.one("SELECT * FROM feature_product_windows WHERE product_id='p'")
    path = _Cache(pipeline.history).paths(saved["descriptor"]["key"])[0]
    assert path.exists()
    # A feature hit still attaches the ALL-raw-input root, even if its row is empty.
    second = pipeline.assess(cutoff + timedelta(minutes=1), True)
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retired")
    run = pipeline.catalog.one(
        "SELECT invalidated FROM assessment_runs WHERE run_id=%s", (second["run_id"],)
    )
    assert run["invalidated"]
    tombstone = pipeline.catalog.one(
        "SELECT local_only FROM retention_objects WHERE object_key=%s",
        (saved["descriptor"]["key"],),
    )
    assert tombstone["local_only"]
    Maintenance(pipeline).retention_drain(max_batches=1, max_operations=100)
    assert not path.exists()


def test_clock_boundary_rebuilds_counts_before_freezing_insufficient_history(pipeline):
    policy = AssessmentPolicy(lookback_days=1, max_age_hours=1000)
    cutoff, _product, _links, _scope, _cache, _initial, _audit = rolling_case(
        pipeline, samples=2, policy=policy
    )
    config = {"horizon_days": [3], "launch_delay_days": [0]}
    assert pipeline.freeze(cutoff, True, config, policy=policy)["result"]["selected"] == ["p"]
    later = cutoff + timedelta(hours=18, microseconds=1)
    frozen = pipeline.freeze(later, True, config, policy=policy)["result"]
    assert frozen["selected"] == []
    assert frozen["status"] == "insufficient_history"


def test_one_hundred_products_share_cold_evicted_and_new_source_file_parses(pipeline, monkeypatch):
    pipeline.settings.history_partition_buckets = 1
    pipeline.settings.analysis_product_batch_size = 100
    cutoff = now() + timedelta(minutes=1)
    for index in range(100):
        pipeline.create_product(f"p{index}", f"P{index}", synthetic=True)
    observations = []
    for point in range(12):
        observed = cutoff - timedelta(hours=2 * (11 - point))
        batch = [
            video(f"v{i}", observed, 100 + point * 10 + i, cutoff - timedelta(days=3))
            for i in range(100)
        ]
        observations.extend(batch)
        pipeline.import_records([row.model_dump(mode="json") for row in batch], synthetic=True)
    for index in range(100):
        pipeline.review_link(f"p{index}", f"projection_fixture:video:v{index}")
    pins = count_source_pins(monkeypatch)
    started = perf_counter()
    cold = pipeline.assess(cutoff, True)
    cold_seconds = perf_counter() - started
    assert len(pins) == 12
    assert pipeline.history.metrics["get_requests"] == 12
    assert all(row["coverage"]["observations"] == 12 for row in cold["rows"])
    pins.clear()
    started = perf_counter()
    unchanged = pipeline.assess(cutoff + timedelta(minutes=1), True)
    warm_seconds = perf_counter() - started
    assert pins == []
    assert len(unchanged["rows"]) == 100
    later = cutoff + timedelta(hours=2)
    new = [video(f"v{i}", later, 300 + i, cutoff - timedelta(days=3)) for i in range(100)]
    observations.extend(new)
    pipeline.import_records([row.model_dump(mode="json") for row in new], synthetic=True)
    started = perf_counter()
    updated = pipeline.assess(later, True)
    delta_seconds = perf_counter() - started
    assert len(pins) == 1
    products = [
        service_module.Product.model_validate(row) for row in pipeline._all_products(True, later)
    ]
    links = [service_module.ProductLink.model_validate(row) for row in pipeline.links(later, True)]
    assert snapshot_semantics(pipeline, {"rows": updated["rows"]})["rows"] == assess(
        observations, products, links, later, True
    )
    # Optional local state can disappear across the whole working set. Cold
    # fallback still shares source parsing once per bounded product partition.
    for saved in pipeline.catalog.all("SELECT descriptor FROM feature_product_windows"):
        (_Cache(pipeline.history).paths(saved["descriptor"]["key"])[0]).unlink()
    pipeline.catalog.execute("DELETE FROM feature_product_projections")
    pins.clear()
    rebuilt = pipeline.assess(later + timedelta(minutes=1), True)
    assert len(pins) == 13 and len(rebuilt["rows"]) == 100
    print(
        {
            "probe": "shared_source_projected_callers",
            "products": 100,
            "cold_source_parses": 12,
            "unchanged_source_parses": 0,
            "delta_source_parses": 1,
            "evicted_source_parses": 13,
            "cold_seconds": cold_seconds,
            "unchanged_seconds": warm_seconds,
            "delta_seconds": delta_seconds,
        }
    )


def test_warm_local_row_budget_rejects_before_any_source_read(pipeline, monkeypatch):
    cutoff, _product, _links, scope, cache, _initial, _audit = rolling_case(
        pipeline, samples=40, videos=3, policy=AssessmentPolicy(max_age_hours=1000)
    )
    pipeline.settings.analysis_max_rows_per_batch = 100
    pins = count_source_pins(monkeypatch)
    with pytest.raises(RowBudgetExceeded):
        cache.load_window(pipeline.history, scope, cutoff, AssessmentPolicy(max_age_hours=1000))
    assert pins == []


def test_alternating_published_and_frozen_contexts_share_all_raw_window_inputs(
    pipeline, monkeypatch
):
    cutoff, _product, _links, _scope, _cache, _initial, _audit = rolling_case(pipeline)
    pipeline.assess(cutoff, True)
    pins = count_source_pins(monkeypatch)
    frozen = pipeline.freeze(
        cutoff + timedelta(minutes=1), True, {"horizon_days": [3], "launch_delay_days": [0]}
    )
    assert frozen["result"]["selected"] == ["p"]
    published = pipeline.assess(cutoff + timedelta(minutes=2), True)
    assert published["rows"][0]["coverage"]["observations"] == 15
    assert pins == []
