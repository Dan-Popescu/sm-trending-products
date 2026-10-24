"""Local cache priority and physical retirement use the real lock/SQL seams."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from hashlib import sha256

import psycopg
import pytest
from test_cache import LARGE, Source, history_at, reference
from test_feature_windows import rolling_case

from sm_trending_products.cache import (
    _Cache,
    cache_object_lock,
    discard_retired_local,
    pin_local_cached,
    store_local_cached,
)
from sm_trending_products.catalog import RETENTION_GUARD_KEY
from sm_trending_products.contracts import AssessmentPolicy
from sm_trending_products.maintenance import Maintenance


def present(history, descriptor):
    return any(path.exists() for path in _Cache(history).paths(descriptor["key"]))


def test_sources_evict_before_local_windows_and_local_lru_remains_bounded(pipeline):
    history = pipeline.history
    history.settings.cache_max_bytes = 1_000_000
    first = store_local_cached(history, b"first" * 50_000)
    assert first is not None
    remote = history.put("normalized/synthetic/older", b"remote" * 40_000)
    history.cached(remote)
    latest = history.put("normalized/synthetic/newer", b"large" * 120_000)
    history.cached(latest)
    assert present(history, first)
    assert not _Cache(history).paths(remote["key"])[0].exists()
    second = store_local_cached(history, b"next" * 62_500)
    assert second is not None
    third = store_local_cached(history, b"last" * 150_000)
    assert third is not None
    assert not present(history, first)
    assert present(history, second) and present(history, third)
    with _Cache(history).global_lock():
        sizes, _pins, _names = _Cache(history)._snapshot()
    assert sum(sizes.values()) <= history.settings.cache_max_bytes


def test_legacy_window_is_verified_and_promoted_without_provider_io(pipeline, monkeypatch):
    history = pipeline.history
    body = b"legacy verified bytes" * 100
    descriptor = store_local_cached(history, body)
    assert descriptor is not None
    current, legacy = _Cache(history).paths(descriptor["key"])
    current.rename(legacy)
    monkeypatch.setattr(history.client, "get_object", lambda **_: pytest.fail("local S3 GET"))
    with pin_local_cached(history, descriptor) as pinned:
        assert pinned is not None and pinned.read_bytes() == body
        assert current.exists() and not legacy.exists()


def test_verified_legacy_hit_does_not_wait_for_a_distinct_cold_stripe(tmp_path):
    key, body = "local-feature-state/legacy.parquet", b"verified legacy"
    prefix = sha256(key.encode()).hexdigest()[:2]
    cold_key = next(
        value
        for index in range(10000)
        if sha256((value := f"cold-{index}").encode()).hexdigest().startswith(prefix)
    )
    source = Source({cold_key: b"cold" * (LARGE // 4)})
    source.block_key = cold_key
    history = history_at(tmp_path, source)
    descriptor = {**reference(key, body), "local_only": True}
    current, legacy = _Cache(history).paths(key)
    legacy.write_bytes(body)

    def warm():
        with pin_local_cached(history, descriptor) as pinned:
            assert pinned is not None
            return pinned.read_bytes()

    with ThreadPoolExecutor(max_workers=2) as workers:
        cold = workers.submit(history.cached, reference(cold_key, source.objects[cold_key]))
        assert source.waiting.wait(timeout=5)
        try:
            assert workers.submit(warm).result(timeout=1) == body
            assert current.exists() and not legacy.exists()
        finally:
            source.release.set()
        assert cold.result(timeout=5).read_bytes() == source.objects[cold_key]


def test_retirement_removes_both_aliases_and_preserves_active_inode(pipeline, monkeypatch):
    history = pipeline.history
    body = b"held reader" * 100
    descriptor = store_local_cached(history, body)
    assert descriptor is not None
    current, legacy = _Cache(history).paths(descriptor["key"])
    os.link(current, legacy)
    with pin_local_cached(history, descriptor) as pinned:
        pipeline.catalog.execute(
            "INSERT INTO retention_objects(object_key,reason,local_only) VALUES(%s,'test',true)",
            (descriptor["key"],),
        )
        monkeypatch.setattr(
            history.client, "get_bucket_versioning", lambda **_: pytest.fail("local provider call")
        )
        assert Maintenance(pipeline)._delete_pending_objects()["deleted"] == 1
        assert not current.exists() and not legacy.exists()
        assert pinned is not None and pinned.read_bytes() == body
        assert store_local_cached(history, body, descriptor=descriptor) is None
    assert not list((history.settings.cache_dir / ".pins").iterdir())


def test_waiting_admission_rechecks_tombstone_after_the_object_stripe(pipeline, monkeypatch):
    history = pipeline.history
    body = b"retirement won" * 100
    descriptor = store_local_cached(history, body)
    assert descriptor is not None
    attempted = threading.Event()
    owner = threading.get_ident()
    original = _Cache.object_lock

    @contextmanager
    def observed_lock(cache, key):
        if key == descriptor["key"] and threading.get_ident() != owner:
            attempted.set()
        with original(cache, key):
            yield

    monkeypatch.setattr(_Cache, "object_lock", observed_lock)
    with ThreadPoolExecutor(max_workers=1) as workers:
        with cache_object_lock(history, descriptor["key"]) as discard:
            waiting = workers.submit(store_local_cached, history, body, descriptor=descriptor)
            assert attempted.wait(timeout=5)
            pipeline.catalog.execute(
                "INSERT INTO retention_objects(object_key,reason,local_only) "
                "VALUES(%s,'test',true)",
                (descriptor["key"],),
            )
            discard()
        assert waiting.result(timeout=5) is None
    assert not present(history, descriptor)


def test_accepted_replacement_reclaims_old_name_after_commit_but_keeps_reader_pin(pipeline):
    cutoff, _product, _links, scope, cache, window, audit = rolling_case(pipeline)
    previous = pipeline.catalog.one("SELECT descriptor FROM feature_product_windows")["descriptor"]
    with pin_local_cached(pipeline.history, previous) as pinned:
        assert pinned is not None
        body = pinned.read_bytes()
        assert cache.store_window(
            pipeline.history,
            scope,
            cutoff + timedelta(minutes=1),
            AssessmentPolicy(),
            window,
            audit_root=audit,
        )
        assert not present(pipeline.history, previous)
        assert pinned.read_bytes() == body
        retired = pipeline.catalog.one(
            "SELECT state,local_only FROM retention_objects WHERE object_key=%s",
            (previous["key"],),
        )
        assert retired == {"state": "pending", "local_only": True}
    current = pipeline.catalog.one("SELECT descriptor FROM feature_product_windows")["descriptor"]
    assert present(pipeline.history, current)
    discard_retired_local(pipeline.history, previous["key"])
    assert Maintenance(pipeline)._delete_pending_objects()["failed"] == 0
    assert present(pipeline.history, current)


def test_failed_stage_and_rejected_older_writer_cannot_remove_current_window(pipeline):
    cutoff, _product, _links, scope, cache, window, audit = rolling_case(pipeline)
    previous = pipeline.catalog.one("SELECT descriptor FROM feature_product_windows")["descriptor"]
    pipeline.catalog.execute("""
        CREATE FUNCTION reject_cache_stage() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'Controlled cache rollback'; END $$;
        CREATE TRIGGER reject_cache_stage BEFORE UPDATE ON feature_product_windows
        FOR EACH ROW WHEN(NEW.state='staged') EXECUTE FUNCTION reject_cache_stage();
    """)
    with pytest.raises(psycopg.errors.RaiseException, match="Controlled cache rollback"):
        cache.store_window(
            pipeline.history,
            scope,
            cutoff + timedelta(minutes=1),
            AssessmentPolicy(),
            window,
            audit_root=audit,
        )
    assert present(pipeline.history, previous)
    assert (
        pipeline.catalog.one(
            "SELECT 1 FROM retention_objects WHERE object_key=%s", (previous["key"],)
        )
        is None
    )
    pipeline.catalog.execute("DROP TRIGGER reject_cache_stage ON feature_product_windows")
    pipeline.catalog.execute(
        "UPDATE feature_product_windows SET computed_at=%s", (cutoff + timedelta(hours=2),)
    )
    assert not cache.store_window(
        pipeline.history,
        scope,
        cutoff + timedelta(hours=1),
        AssessmentPolicy(),
        window,
        audit_root=audit,
    )
    assert present(pipeline.history, previous)


def test_optional_cleanup_contention_after_stage_cannot_abort_assessment(pipeline, monkeypatch):
    import sm_trending_products.feature_projection as feature_module

    cutoff, *_ = rolling_case(pipeline, samples=2, videos=1)
    previous = pipeline.catalog.one("SELECT descriptor FROM feature_product_windows")["descriptor"]
    original = feature_module.discard_retired_local
    original_guard = pipeline.catalog.guard_retention
    calls = []

    def contended_cleanup(history, key):
        calls.append(key)
        # This independent transaction obtains the exclusive fence only after
        # staging has committed. Only the optional cleanup sees this timeout.
        with psycopg.connect(history.settings.database_url.get_secret_value()) as holder:
            holder.execute("SELECT pg_advisory_xact_lock(%s)", (RETENTION_GUARD_KEY,))

            def short_guard(connection, *, exclusive=False):
                connection.execute("SET LOCAL lock_timeout='40ms'")
                return original_guard(connection, exclusive=exclusive)

            with monkeypatch.context() as local:
                local.setattr(history.catalog, "guard_retention", short_guard)
                return original(history, key)

    monkeypatch.setattr(feature_module, "discard_retired_local", contended_cleanup)
    result = pipeline.assess(cutoff + timedelta(microseconds=1), True)
    assert len(result["rows"]) == 1 and calls == [previous["key"]]
    current = pipeline.catalog.one("SELECT descriptor,state FROM feature_product_windows")
    assert current["descriptor"] != previous and current["state"] == "ready"
    assert present(pipeline.history, previous)
    assert pipeline.catalog.one(
        "SELECT local_only FROM retention_objects WHERE object_key=%s", (previous["key"],)
    ) == {"local_only": True}
