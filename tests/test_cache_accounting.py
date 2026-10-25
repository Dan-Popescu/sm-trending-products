"""Shared admission accounting across releases, processes and abrupt exits."""

from __future__ import annotations

import fcntl
import gc
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_cache import BUDGET, Source, history_at, reference, retained_bytes

from sm_trending_products import _cache_accounting
from sm_trending_products.cache import CacheCapacityError, _Cache, pin_cached


def shared_accounting(history):
    cache = _Cache(history)
    return _cache_accounting.accounting(history, (cache.root, cache.pins, cache.downloads))


def child_environment():
    # Pytest's test-module path is process-local; a real Python child needs it
    # explicitly even when the installed package already resolves without it.
    paths = [str(Path(__file__).resolve().parent)]
    if inherited := os.environ.get("PYTHONPATH"):
        paths.append(inherited)
    return {**os.environ, "PYTHONPATH": os.pathsep.join(paths)}


def count_snapshots(monkeypatch):
    calls = []
    original = _Cache._snapshot

    def snapshot(cache):
        calls.append(cache.root)
        return original(cache)

    monkeypatch.setattr(_Cache, "_snapshot", snapshot)
    return calls


def test_headroom_admissions_reuse_shared_charge_across_cache_instances(tmp_path, monkeypatch):
    objects = {f"file-{index}": bytes([index]) * 1024 for index in range(20)}
    history = history_at(tmp_path, Source(objects))
    calls = count_snapshots(monkeypatch)
    for key, body in objects.items():
        assert history.cached(reference(key, body)).read_bytes() == body
    assert len(calls) == 1
    assert retained_bytes(tmp_path) == 20 * 1024


def test_deleted_charge_rebuilds_only_when_conservative_estimate_reaches_budget(
    tmp_path, monkeypatch
):
    objects = {f"file-{index}": bytes([index]) * (BUDGET // 4) for index in range(6)}
    history = history_at(tmp_path, Source(objects))
    calls = count_snapshots(monkeypatch)
    for key, body in objects.items():
        local = history.cached(reference(key, body))
        assert local.read_bytes() == body
        _Cache(history).discard(key)
    # Deletions never undercount an inode still pinned by another reader.
    # Eventually an inventory refunds conservative charges without eviction.
    assert len(calls) == 2
    assert retained_bytes(tmp_path) == 0


def test_old_writer_without_accounting_invalidates_even_coalesced_directory_times(
    tmp_path, monkeypatch
):
    small, old_body, next_body = b"s" * 1024, b"o" * (3 * BUDGET // 4), b"n" * (BUDGET // 2)
    source = Source({"small": small, "next": next_body})
    history = history_at(tmp_path, source)
    history.cached(reference("small", small))
    calls = count_snapshots(monkeypatch)
    shared = shared_accounting(history)
    versions = shared._versions()
    # The unchanged versions model a filesystem whose timestamp resolution
    # coalesces directory mutations. Ordered kernel events must still detect it.
    monkeypatch.setattr(shared, "_versions", lambda: versions)
    with (tmp_path / ".cache.lock").open("r+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        old_path = _Cache(history).paths("old")[0]
        old_path.write_bytes(old_body)
        # The legacy protocol uses the same flock and payload names but knows
        # nothing about the new accounting file.
    assert history.cached(reference("next", next_body)).read_bytes() == next_body
    assert len(calls) == 1
    assert retained_bytes(tmp_path) <= BUDGET


def test_old_inplace_resize_cannot_hide_behind_unchanged_directory_versions(tmp_path, monkeypatch):
    first, grown, second = b"a" * 1024, b"x" * (3 * BUDGET // 4), b"b" * (BUDGET // 2)
    history = history_at(tmp_path, Source({"first": first, "second": second}))
    original = history.cached(reference("first", first))
    calls = count_snapshots(monkeypatch)
    with pin_cached(history, reference("first", first)) as pinned:
        with (tmp_path / ".cache.lock").open("r+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            original.write_bytes(grown)
        with pytest.raises(CacheCapacityError):
            history.cached(reference("second", second))
        assert pinned.read_bytes() == grown
        assert retained_bytes(tmp_path) == len(grown)
    assert calls
    assert history.cached(reference("second", second)).read_bytes() == second
    assert retained_bytes(tmp_path) <= BUDGET


@pytest.mark.parametrize("corrupt", ['{"version":1,"bytes":', "null", "[]", "42", '"text"'])
def test_corrupt_shared_record_rebuilds_before_admission(tmp_path, monkeypatch, corrupt):
    first, second = b"a" * (3 * BUDGET // 4), b"b" * (BUDGET // 2)
    history = history_at(tmp_path, Source({"first": first, "second": second}))
    history.cached(reference("first", first))
    (tmp_path / ".cache-accounting").write_text(corrupt)
    calls = count_snapshots(monkeypatch)
    history.cached(reference("second", second))
    assert len(calls) == 1
    assert retained_bytes(tmp_path) <= BUDGET


def test_valid_json_corruption_cannot_reduce_the_charge_of_a_pinned_payload(tmp_path):
    first, second = b"a" * (3 * BUDGET // 4), b"b" * (BUDGET // 2)
    history = history_at(tmp_path, Source({"first": first, "second": second}))
    with pin_cached(history, reference("first", first)) as pinned:
        record = tmp_path / ".cache-accounting"
        saved = json.loads(record.read_text())
        saved["bytes"] = 0
        record.write_text(json.dumps(saved))
        with pytest.raises(CacheCapacityError):
            history.cached(reference("second", second))
        assert pinned.read_bytes() == first
        assert retained_bytes(tmp_path) == len(first)
    assert history.cached(reference("second", second)).read_bytes() == second


def test_missing_filesystem_watch_uses_ordinary_inventory(tmp_path, monkeypatch):
    monkeypatch.setattr(_cache_accounting.ctypes, "CDLL", lambda *args, **kwargs: object())
    history = history_at(tmp_path, Source({"a": b"a", "b": b"b"}))
    calls = count_snapshots(monkeypatch)
    history.cached(reference("a", b"a"))
    history.cached(reference("b", b"b"))
    assert len(calls) >= 2
    assert retained_bytes(tmp_path) == 2


def test_lost_watch_reverts_to_inventory_and_gc_closes_watch_descriptor(tmp_path, monkeypatch):
    history = history_at(tmp_path, Source({"a": b"a", "b": b"b"}))
    history.cached(reference("a", b"a"))
    shared = shared_accounting(history)
    descriptor = shared.descriptor
    assert descriptor >= 0
    shared.close()
    calls = count_snapshots(monkeypatch)
    history.cached(reference("b", b"b"))
    assert calls
    with pytest.raises(OSError):
        os.fstat(descriptor)
    # A live History owns only one watcher despite short _Cache lifetimes.
    other = history_at(tmp_path / "other", Source({"a": b"a"}))
    other.cached(reference("a", b"a"))
    other_descriptor = shared_accounting(other).descriptor
    del other
    gc.collect()
    with pytest.raises(OSError):
        os.fstat(other_descriptor)


PEER = """
import json, os, sys
from pathlib import Path
from test_cache import history_at, Source, reference
from sm_trending_products.cache import _Cache
root, mode = Path(sys.argv[1]), sys.argv[2]
body = b'x' * (3*1024*1024//4)
h = history_at(root, Source({'peer': body}))
if mode == 'commit':
    h.cached(reference('peer', body))
else:
    cache = _Cache(h)
    with cache.global_lock():
        cache._make_room(len(body))
        cache.paths('peer')[0].write_bytes(body)
        os._exit(0)
"""


@pytest.mark.parametrize("mode", ["commit", "crash"])
def test_other_process_commit_or_crash_cannot_leave_an_undercharge(tmp_path, monkeypatch, mode):
    history = history_at(tmp_path, Source({"a": b"a", "b": b"b" * (BUDGET // 2)}))
    history.cached(reference("a", b"a"))
    subprocess.run(
        [sys.executable, "-c", PEER, str(tmp_path), mode],
        check=True,
        timeout=10,
        env=child_environment(),
    )
    calls = count_snapshots(monkeypatch)
    history.cached(reference("b", b"b" * (BUDGET // 2)))
    assert calls
    assert retained_bytes(tmp_path) <= BUDGET


def test_new_process_charge_can_be_reused_without_a_parent_inventory(tmp_path, monkeypatch):
    body = b"b" * 1024
    history = history_at(tmp_path, Source({"a": b"a", "b": body}))
    history.cached(reference("a", b"a"))
    subprocess.run(
        [sys.executable, "-c", PEER, str(tmp_path), "commit"],
        check=True,
        timeout=10,
        env=child_environment(),
    )
    calls = count_snapshots(monkeypatch)
    history.cached(reference("b", body))
    assert calls == []
    assert json.loads((tmp_path / ".cache-accounting").read_text())["bytes"] <= BUDGET


def test_queue_overflow_during_mutation_forces_next_inventory(tmp_path, monkeypatch):
    history = history_at(tmp_path, Source({"a": b"a", "b": b"b"}))
    history.cached(reference("a", b"a"))
    shared = shared_accounting(history)
    original = _cache_accounting.os.read
    pending = False

    def read(descriptor, size):
        nonlocal pending
        if descriptor == shared.descriptor and pending:
            pending = False
            return _cache_accounting._EVENT.pack(-1, _cache_accounting._OVERFLOW, 0, 0)
        return original(descriptor, size)

    monkeypatch.setattr(_cache_accounting.os, "read", read)
    with _Cache(history).global_lock():
        pending = True
    assert (tmp_path / ".cache-accounting").read_bytes() == b""
    calls = count_snapshots(monkeypatch)
    history.cached(reference("b", b"b"))
    assert len(calls) == 1


FORK = """
import json, os, sys
from pathlib import Path
from test_cache import history_at, Source, reference
from test_cache_accounting import shared_accounting
h = history_at(Path(sys.argv[1]), Source({'a': b'a', 'b': b'b'}))
h.cached(reference('a', b'a'))
parent = shared_accounting(h)
read, write = os.pipe()
child = os.fork()
if child == 0:
    os.close(read)
    h.cached(reference('b', b'b'))
    shared = shared_accounting(h)
    os.write(write, json.dumps([shared.process, os.getpid(), parent.process]).encode())
    os.close(write)
    os._exit(0)
os.close(write)
result = json.loads(os.read(read, 1024))
os.close(read)
assert os.waitpid(child, 0)[1] == 0
assert result[0] == result[1] != result[2]
assert parent.process == os.getpid() and parent.descriptor >= 0
assert h.cached(reference('a', b'a')).read_bytes() == b'a'
"""


def test_forked_history_gets_an_independent_watcher_and_initial_inventory(tmp_path):
    # Isolate fork from the test session's PostgreSQL/Moto background threads.
    subprocess.run(
        [sys.executable, "-c", FORK, str(tmp_path)],
        check=True,
        timeout=10,
        env=child_environment(),
    )


def test_pressure_reclaims_bounded_source_headroom_and_amortizes_inventories(tmp_path, monkeypatch):
    objects = {f"source/{index}": b"x" * 1024 for index in range(1024)}
    source = Source(objects)
    history = history_at(tmp_path, source)
    cache = _Cache(history)
    for key, body in objects.items():
        cache.paths(key)[0].write_bytes(body)
    cache._reconcile_budget(tmp_path / "initial")
    calls = count_snapshots(monkeypatch)
    assert cache.admit_local(b"window" * 4096 + b"w" * 8192)
    # The 32 KiB required eviction may reclaim at most another 1% (10 KiB
    # in whole files), which covers several later admissions.
    assert sum(cache.paths(key)[0].exists() for key in objects) == 1024 - 32 - 10
    assert len(calls) == 1
    for index in range(5):
        assert _Cache(history).admit_local(bytes([index]) * 1024)
    assert len(calls) == 1
    assert retained_bytes(tmp_path) <= BUDGET


def test_near_fit_hot_set_discloses_ten_additional_gets_for_optional_headroom(tmp_path):
    objects = {f"source/{index}": b"x" * 1024 for index in range(1024)}
    source = Source(objects)
    history = history_at(tmp_path, source)
    cache = _Cache(history)
    for key, body in objects.items():
        cache.paths(key)[0].write_bytes(body)
    window = cache.admit_local(b"w" * 32768)
    for index in range(32, 1024):
        key = f"source/{index}"
        assert history.cached(reference(key, objects[key])).read_bytes() == objects[key]
    assert len(source.requests) == 10
    assert history.metrics["download_bytes"] == 10240
    assert cache.paths(window["key"])[0].exists()
    assert retained_bytes(tmp_path) == BUDGET


def test_optional_trim_never_evicts_one_object_larger_than_extra_byte_bound(tmp_path):
    history = history_at(tmp_path, Source({}))
    cache = _Cache(history)
    required, large = cache.paths("old-required")[0], cache.paths("large-optional")[0]
    required.write_bytes(b"a" * 4096)
    large.write_bytes(b"b" * (3 * BUDGET // 4))
    window = cache.paths("local-feature-state/active")[0]
    window.write_bytes(b"w" * (BUDGET - required.stat().st_size - large.stat().st_size))
    os.utime(required, ns=(1, 1))
    os.utime(large, ns=(2, 2))
    assert cache.admit_local(b"n" * 4096)
    assert not required.exists()
    assert large.exists() and window.exists()
    assert retained_bytes(tmp_path) == BUDGET


def test_optional_trim_skips_an_oversized_object_but_can_reclaim_later_small_sources(tmp_path):
    history = history_at(tmp_path, Source({}))
    history.settings.cache_max_bytes = 100000
    cache = _Cache(history)
    required, large, small = (cache.paths(key)[0] for key in ("required", "large", "small"))
    for stamp, path, size in ((1, required, 64), (2, large, 2000), (3, small, 500)):
        path.write_bytes(b"x" * size)
        os.utime(path, ns=(stamp, stamp))
    window = cache.paths("local-feature-state/active")[0]
    window.write_bytes(b"w" * 97436)
    assert cache.admit_local(b"n" * 64)
    assert not required.exists() and not small.exists()
    assert large.exists() and window.exists()
    assert retained_bytes(tmp_path) == 99500


def test_optional_headroom_uses_the_32_mib_cap_for_larger_budgets(tmp_path):
    history = history_at(tmp_path, Source({}))
    history.settings.cache_max_bytes = 4 * 1024**3
    cache = _Cache(history)
    required, large, small = (cache.paths(key)[0] for key in ("required", "large", "small"))
    for stamp, path, size in (
        (1, required, 64),
        (2, large, 33 * 1024**2),
        (3, small, 1024**2),
    ):
        # Sparse files exercise logical byte accounting without allocating GiB.
        with path.open("wb") as handle:
            handle.truncate(size)
        os.utime(path, ns=(stamp, stamp))
    window = cache.paths("local-feature-state/active")[0]
    with window.open("wb") as handle:
        handle.truncate(cache.limit - 64 - 34 * 1024**2)
    assert cache.admit_local(b"n" * 64)
    assert not required.exists() and not small.exists()
    assert large.exists() and window.exists()
    assert retained_bytes(tmp_path) == cache.limit - 1024**2


def test_optional_trim_keeps_pinned_inodes_and_explicit_keep_path(tmp_path):
    history = history_at(tmp_path, Source({}))
    cache = _Cache(history)
    paths = [cache.paths(f"source/{index}")[0] for index in range(1024)]
    for index, path in enumerate(paths):
        path.write_bytes(b"x" * 1024)
        os.utime(path, ns=(index + 1, index + 1))
    cache._reconcile_budget(tmp_path / "initial")
    with cache.global_lock():
        target, handle = cache._pin(paths[0])
        cache._make_room(1024, keep=paths[1])
    try:
        assert target.read_bytes() == b"x" * 1024
        assert paths[0].exists() and paths[1].exists()
        assert sum(path.exists() for path in paths) == 1024 - 1 - 10
    finally:
        cache.release(target, handle)


def test_window_only_pressure_evicts_only_what_the_request_requires(tmp_path):
    history = history_at(tmp_path, Source({}))
    cache = _Cache(history)
    paths = [cache.paths(f"local-feature-state/seed-{index}")[0] for index in range(1024)]
    for path in paths:
        path.write_bytes(b"x" * 1024)
    assert cache.admit_local(b"new" * 341 + b"n")
    assert sum(path.exists() for path in paths) == 1023
    assert retained_bytes(tmp_path) == BUDGET


def test_optional_trim_does_not_remove_aliases_without_freeing_an_inode(tmp_path):
    history = history_at(tmp_path, Source({}))
    cache = _Cache(history)
    required, first, second = (cache.paths(key)[0] for key in ("required", "first", "second"))
    required.write_bytes(b"r" * 1024)
    first.write_bytes(b"a" * 1024)
    os.link(first, second)
    window = cache.paths("local-feature-state/active")[0]
    window.write_bytes(b"w" * (BUDGET - 2048))
    os.utime(required, ns=(1, 1))
    os.utime(first, ns=(2, 2))
    assert cache.admit_local(b"n" * 1024)
    assert not required.exists()
    assert first.exists() and second.exists() and window.exists()
    assert retained_bytes(tmp_path) == BUDGET
