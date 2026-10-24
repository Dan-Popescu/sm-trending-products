"""New counter samples reuse bounded local Arrow state and preserve the full oracle."""

from contextlib import contextmanager
from datetime import timedelta
from hashlib import sha256

import pytest
from botocore.exceptions import ClientError
from test_feature_projection import video

import sm_trending_products.history as history_module
from sm_trending_products.analytics import assess
from sm_trending_products.cache import _Cache, pin_local_cached, store_local_cached
from sm_trending_products.contracts import AssessmentPolicy, Product, ProductLink, digest, now
from sm_trending_products.feature_projection import FeatureProjection, ProjectionScope
from sm_trending_products.maintenance import Maintenance


def rolling_case(pipeline, *, samples=5, videos=3, policy=None):
    cutoff = now() + timedelta(minutes=1)
    product = Product.model_validate(pipeline.create_product("p", "P", synthetic=True))
    observed = []
    for sample in range(samples):
        timestamp = cutoff - timedelta(hours=6 * (samples - sample - 1))
        batch = [
            video(f"v{index}", timestamp, 100 + sample * (index + 1), cutoff - timedelta(days=40))
            for index in range(videos)
        ]
        pipeline.import_records([row.model_dump(mode="json") for row in batch], synthetic=True)
        observed.extend(batch)
    for index in range(videos):
        pipeline.review_link("p", f"projection_fixture:video:v{index}")
    links = [ProductLink.model_validate(row) for row in pipeline.links(cutoff, True)]
    scope = ProjectionScope(product, frozenset(link.entity_key for link in links), digest(links))
    cache = FeatureProjection(pipeline.catalog)
    policy = policy or AssessmentPolicy()
    window = cache.load_window(pipeline.history, scope, cutoff, policy)
    audit = "window-tests/" + digest({"datasets": [row["dataset_id"] for row in window.manifests]})
    pipeline.catalog.stage_derivation(audit, [row["dataset_id"] for row in window.manifests])
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (audit,)
    )
    assert cache.store_window(pipeline.history, scope, cutoff, policy, window, audit_root=audit)
    return cutoff, product, links, scope, cache, window, audit


def rows(values):
    return [row.model_dump(mode="json") for row in values]


def count_source_pins(monkeypatch):
    pins = []
    original = history_module.pin_cached

    @contextmanager
    def counted(history, descriptor):
        pins.append(descriptor["key"])
        with original(history, descriptor) as path:
            yield path

    monkeypatch.setattr(history_module, "pin_cached", counted)
    return pins


def test_new_counter_and_backdated_import_read_only_new_objects_with_exact_features(
    pipeline, monkeypatch
):
    cutoff, product, links, scope, cache, initial, _audit = rolling_case(pipeline)
    later = cutoff + timedelta(hours=6)
    pipeline.import_records(
        [
            video(f"v{i}", later, 200 + i, cutoff - timedelta(days=40)).model_dump(mode="json")
            for i in range(3)
        ],
        synthetic=True,
    )
    # Newly published old availability cannot be found with available_at > old_cutoff.
    backdated = video("v0", cutoff - timedelta(hours=1), 150, cutoff - timedelta(days=40))
    pipeline.import_records([backdated.model_dump(mode="json")], synthetic=True)
    pins = count_source_pins(monkeypatch)
    delta = cache.load_window(pipeline.history, scope, later, AssessmentPolicy())
    assert delta.mode == "delta" and delta.source_objects_read == 2 and len(pins) == 2
    pins.clear()
    oracle, _ = pipeline.history.observations(
        later,
        True,
        entity_keys=set(scope.entity_keys),
        since=later - timedelta(days=30),
        until=later,
    )
    assert rows(delta.observations) == rows(oracle)
    assert len(pins) == initial.source_objects_read + 2
    assert assess(delta.observations, [product], links, later, True) == assess(
        oracle, [product], links, later, True
    )


def test_native_rolling_expiry_evicts_old_contributions_without_rereading_old_files(
    pipeline, monkeypatch
):
    policy = AssessmentPolicy(lookback_days=1, max_age_hours=1000)
    cutoff, product, links, scope, cache, _initial, _audit = rolling_case(
        pipeline, samples=5, policy=policy
    )
    later = cutoff + timedelta(hours=6, microseconds=1)
    pipeline.import_records(
        [
            video(f"v{i}", later, 300 + i, cutoff - timedelta(days=40)).model_dump(mode="json")
            for i in range(3)
        ],
        synthetic=True,
    )
    pins = count_source_pins(monkeypatch)
    delta = cache.load_window(pipeline.history, scope, later, policy)
    assert delta.mode == "delta" and len(pins) == 1
    oracle, _ = pipeline.history.observations(
        later,
        True,
        entity_keys=set(scope.entity_keys),
        since=later - timedelta(days=1),
        until=later,
    )
    assert rows(delta.observations) == rows(oracle)
    assert len(delta.observations) == 12
    assert assess(delta.observations, [product], links, later, True, policy) == assess(
        oracle, [product], links, later, True, policy
    )


@pytest.mark.parametrize("failure", ["evicted", "corrupt", "duplicate", "compacted"])
def test_eviction_corruption_ambiguous_delivery_and_compaction_rebuild_exactly(
    pipeline, monkeypatch, failure
):
    cutoff, _product, _links, scope, cache, _window, _audit = rolling_case(pipeline)
    saved = pipeline.catalog.one("SELECT * FROM feature_product_windows WHERE product_id='p'")
    path = _Cache(pipeline.history).paths(saved["descriptor"]["key"])[0]
    if failure == "evicted":
        path.unlink()
    elif failure == "corrupt":
        path.write_bytes(b"corrupt disposable state")
    elif failure == "duplicate":
        original, _ = pipeline.history.observations(
            cutoff, True, entity_keys=set(scope.entity_keys)
        )
        pipeline.import_records([original[0].model_dump(mode="json")], synthetic=True)
    else:
        pipeline.compact(synthetic=True)
    pins = count_source_pins(monkeypatch)
    rebuilt = cache.load_window(pipeline.history, scope, cutoff, AssessmentPolicy())
    assert rebuilt.mode != "delta" and len(pins) >= 1
    oracle, _ = pipeline.history.observations(
        cutoff,
        True,
        entity_keys=set(scope.entity_keys),
        since=cutoff - timedelta(days=30),
        until=cutoff,
    )
    assert rows(rebuilt.observations) == rows(oracle)
    assert all(not key.startswith("local-feature-state/") for key in pins)


def test_local_state_competes_for_the_same_budget_and_pins_survive_eviction(pipeline, monkeypatch):
    pipeline.history.settings.cache_max_bytes = 1_000_000
    cutoff, _product, _links, _scope, _cache, _window, _audit = rolling_case(pipeline)
    saved = pipeline.catalog.one(
        "SELECT descriptor FROM feature_product_windows WHERE product_id='p'"
    )
    descriptor = saved["descriptor"]
    # A provider call would be an implementation error for all-local admission/read.
    monkeypatch.setattr(
        pipeline.history.client, "get_object", lambda **_: pytest.fail("fake S3 GET")
    )
    with pin_local_cached(pipeline.history, descriptor) as pin:
        assert pin is not None
        assert store_local_cached(pipeline.history, b"x" * 995_000) is None
        assert pin.exists()
        assert sha256(pin.read_bytes()).hexdigest() == descriptor["sha256"]
    sizes = {}
    for path in pipeline.settings.cache_dir.rglob("*"):
        if path.is_file() and (
            path.parent == pipeline.settings.cache_dir or path.parent.name == ".pins"
        ):
            info = path.stat()
            sizes[(info.st_dev, info.st_ino)] = info.st_size
    assert sum(sizes.values()) <= pipeline.history.settings.cache_max_bytes


def test_retirement_physically_purges_local_state_without_provider_operations(
    pipeline, monkeypatch
):
    cutoff, _product, _links, scope, cache, window, audit = rolling_case(pipeline)
    saved = pipeline.catalog.one(
        "SELECT descriptor FROM feature_product_windows WHERE product_id='p'"
    )
    descriptor = saved["descriptor"]
    path = _Cache(pipeline.history).paths(descriptor["key"])[0]
    body = path.read_bytes()
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='invalidated' WHERE object_key=%s", (audit,)
    )
    retired = pipeline.catalog.one(
        "SELECT local_only FROM retention_objects WHERE object_key=%s", (descriptor["key"],)
    )
    assert retired["local_only"] is True
    monkeypatch.setattr(
        pipeline.history.client,
        "get_bucket_versioning",
        lambda **_: pytest.fail("local state is not S3"),
    )
    result = Maintenance(pipeline)._delete_pending_objects()
    assert result["deleted"] == 1 and result["failed"] == 0
    assert not path.exists()
    assert store_local_cached(pipeline.history, body, descriptor=descriptor) is None
    assert not cache.store_window(
        pipeline.history, scope, cutoff, AssessmentPolicy(), window, audit_root=audit
    )


def test_mixed_remote_failure_does_not_block_local_physical_cleanup(pipeline, monkeypatch):
    _cutoff, _product, _links, _scope, _cache, _window, audit = rolling_case(pipeline)
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='invalidated' WHERE object_key=%s", (audit,)
    )
    pipeline.catalog.execute(
        "INSERT INTO retention_objects(object_key,reason) VALUES('remote-pending','test')"
    )

    def unavailable(**_):
        raise ClientError({"Error": {"Code": "ServiceUnavailable"}}, "GetBucketVersioning")

    monkeypatch.setattr(pipeline.history.client, "get_bucket_versioning", unavailable)
    result = Maintenance(pipeline)._delete_pending_objects()
    assert result["deleted"] == 1 and result["failed"] == 1 and result["attempted"] == 2
    assert (
        pipeline.catalog.one(
            "SELECT state FROM retention_objects WHERE object_key='remote-pending'"
        )["state"]
        == "pending"
    )


def test_representative_counter_window_reports_state_bytes_and_new_refresh_io(
    pipeline, monkeypatch
):
    cutoff, product, links, scope, cache, initial, _audit = rolling_case(
        pipeline, samples=120, videos=20
    )
    saved = pipeline.catalog.one(
        "SELECT descriptor FROM feature_product_windows WHERE product_id='p'"
    )
    state_bytes = saved["descriptor"]["bytes"]
    later = cutoff + timedelta(hours=6, microseconds=1)
    pipeline.import_records(
        [
            video(f"v{i}", later, 1000 + i, cutoff - timedelta(days=40)).model_dump(mode="json")
            for i in range(20)
        ],
        synthetic=True,
    )

    # Keep the rolling state warm and start each comparison with no normalized
    # payload files. This measures HTTP object I/O, rather than only file pins.
    def clear_source_payloads(manifests):
        for manifest in manifests:
            for descriptor in manifest["manifest"]["normalized"]:
                name = sha256(descriptor["key"].encode()).hexdigest()
                (pipeline.settings.cache_dir / name).unlink(missing_ok=True)

    clear_source_payloads(initial.manifests)
    pins = count_source_pins(monkeypatch)
    before_gets = pipeline.history.metrics["get_requests"]
    before_bytes = pipeline.history.metrics["download_bytes"]
    delta = cache.load_window(pipeline.history, scope, later, AssessmentPolicy())
    delta_gets = pipeline.history.metrics["get_requests"] - before_gets
    delta_bytes = pipeline.history.metrics["download_bytes"] - before_bytes
    assert delta.mode == "delta" and delta.source_objects_read == len(pins) == delta_gets == 1
    clear_source_payloads(delta.manifests)
    pins.clear()
    before_gets = pipeline.history.metrics["get_requests"]
    before_bytes = pipeline.history.metrics["download_bytes"]
    oracle, _ = pipeline.history.observations(
        later,
        True,
        entity_keys=set(scope.entity_keys),
        since=later - timedelta(days=30),
        until=later,
    )
    oracle_gets = pipeline.history.metrics["get_requests"] - before_gets
    oracle_bytes = pipeline.history.metrics["download_bytes"] - before_bytes
    assert oracle_gets == 120
    assert rows(delta.observations) == rows(oracle)
    assert assess(delta.observations, [product], links, later, True) == assess(
        oracle, [product], links, later, True
    )
    assert len(pins) == 120
    print(
        {
            "probe": "representative_feature_window",
            "videos": 20,
            "initial_samples_per_video": 120,
            "initial_observations": len(initial.observations),
            "initial_source_objects": initial.source_objects_read,
            "state_bytes": state_bytes,
            "projected_10000_product_state_bytes": state_bytes * 10000,
            "configured_shared_cache_bytes": pipeline.settings.cache_max_bytes,
            "new_refresh_source_objects": delta.source_objects_read,
            "full_oracle_source_objects": len(pins),
            "current_observations": len(delta.observations),
            "cold_source_delta_gets": delta_gets,
            "cold_source_delta_download_bytes": delta_bytes,
            "cold_source_oracle_gets": oracle_gets,
            "cold_source_oracle_download_bytes": oracle_bytes,
        }
    )


def test_metadata_rebuild_clears_retired_future_bounds_without_reading_source_payloads(
    pipeline, monkeypatch
):
    cutoff, _product, _links, scope, cache, _window, _audit = rolling_case(pipeline)
    future = video("v0", cutoff + timedelta(days=10), 5000, cutoff - timedelta(days=40))
    dataset = pipeline.import_records([future.model_dump(mode="json")], synthetic=True)[0]
    poisoned = cache.read_many([scope], cutoff, AssessmentPolicy()).frontiers["p"]
    assert not poisoned.fully_visible(cutoff)
    pipeline.catalog.invalidate_dataset(dataset, "future input removed")
    assert (
        not cache.read_many([scope], cutoff, AssessmentPolicy())
        .frontiers["p"]
        .fully_visible(cutoff)
    )
    pins = count_source_pins(monkeypatch)
    rebuilt = cache.rebuild_frontiers([scope])
    assert rebuilt["p"].fully_visible(cutoff) and pins == []
    current = cache.load_window(pipeline.history, scope, cutoff, AssessmentPolicy())
    assert current.mode == "delta" and current.source_objects_read == 0 and pins == []
