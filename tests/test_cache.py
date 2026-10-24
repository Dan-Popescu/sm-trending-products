"""Real filesystem and process boundaries for the shared verified-cache budget."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from sm_trending_products.cache import CacheCapacityError, cache_object_lock, pin_cached
from sm_trending_products.history import History

BUDGET = 1024 * 1024
LARGE = 3 * BUDGET // 4


class Source:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.requests: list[str] = []
        self.waiting = threading.Event()
        self.release = threading.Event()
        self.block_key: str | None = None

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.requests.append(Key)
        body = io.BytesIO(self.objects[Key])
        if Key == self.block_key:
            source = self

            class SlowBody:
                def read(self, size: int) -> bytes:
                    source.waiting.set()
                    if not source.release.wait(timeout=5):
                        raise TimeoutError("Test did not release controlled download")
                    return body.read(size)

                def close(self) -> None:
                    body.close()

            return {"Body": SlowBody()}
        return {"Body": body}


def history_at(root: Path, source: Source) -> History:
    history = History.__new__(History)
    history.settings = SimpleNamespace(cache_dir=root, cache_max_bytes=BUDGET)
    history.bucket = "controlled"
    history._client = source
    history.metrics = {"get_requests": 0, "download_bytes": 0, "cache_hits": 0}
    return history


def reference(key: str, body: bytes) -> dict:
    return {"key": key, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}


def retained_bytes(root: Path) -> int:
    inodes = {}
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            if path.parent == root and not path.name.startswith(".") or path.parent.name == ".pins":
                info = path.stat()
                inodes[(info.st_dev, info.st_ino)] = info.st_size
    return sum(inodes.values())


def test_slow_fill_does_not_block_a_distinct_warm_read(tmp_path):
    warm, cold = b"w" * 4096, b"c" * LARGE
    prefix = hashlib.sha256(b"warm").hexdigest()[:2]
    cold_key = next(
        key
        for index in range(10000)
        if hashlib.sha256((key := f"cold-{index}").encode()).hexdigest().startswith(prefix)
    )
    source = Source({"warm": warm, cold_key: cold})
    source.block_key = cold_key
    history = history_at(tmp_path, source)
    history.cached(reference("warm", warm))
    with ThreadPoolExecutor(max_workers=2) as executor:
        filling = executor.submit(history.cached, reference(cold_key, cold))
        assert source.waiting.wait(timeout=2)
        try:
            hit = executor.submit(history.cached, reference("warm", warm))
            assert hit.result(timeout=1).read_bytes() == warm
            assert source.requests == ["warm", cold_key]
        finally:
            source.release.set()
        assert filling.result(timeout=2).read_bytes() == cold


def test_evicted_pins_count_toward_one_global_budget(tmp_path):
    first, second = b"a" * LARGE, b"b" * LARGE
    source = Source({"first": first, "second": second})
    history = history_at(tmp_path, source)
    with pin_cached(history, reference("first", first)) as pinned:
        history.cached(reference("first", first)).unlink()
        with pytest.raises(CacheCapacityError, match="active readers"):
            history.cached(reference("second", second))
        assert source.requests == ["first"]
        assert pinned.read_bytes() == first
        assert retained_bytes(tmp_path) == LARGE
    assert history.cached(reference("second", second)).read_bytes() == second
    assert retained_bytes(tmp_path) <= BUDGET


def test_duplicate_reader_pins_account_for_one_inode(tmp_path):
    first, second = b"a" * LARGE, b"b" * (BUDGET - LARGE)
    source = Source({"first": first, "second": second})
    history = history_at(tmp_path, source)
    with ExitStack() as stack:
        pins = [
            stack.enter_context(pin_cached(history, reference("first", first))) for _ in range(3)
        ]
        assert len({pin.stat().st_ino for pin in pins}) == 1
        history.cached(reference("second", second))
        assert retained_bytes(tmp_path) == BUDGET
        assert source.requests == ["first", "second"]


def test_inflight_reservations_prevent_concurrent_disk_overcommit(tmp_path):
    first, second = b"a" * LARGE, b"b" * LARGE
    source = Source({"first": first, "second": second})
    source.block_key = "first"
    history = history_at(tmp_path, source)
    with ThreadPoolExecutor(max_workers=1) as executor:
        filling = executor.submit(history.cached, reference("first", first))
        assert source.waiting.wait(timeout=2)
        try:
            with pytest.raises(CacheCapacityError, match="downloads"):
                history.cached(reference("second", second))
            assert source.requests == ["first"]
        finally:
            source.release.set()
        assert filling.result(timeout=2).read_bytes() == first
    assert retained_bytes(tmp_path) <= BUDGET


def test_reduced_budget_reconciles_warm_payloads_without_refetching_kept_input(tmp_path):
    first, second = b"a" * LARGE, b"b" * LARGE
    source = Source({"first": first, "second": second})
    previous = history_at(tmp_path, source)
    previous.settings.cache_max_bytes = 2 * BUDGET
    previous.cached(reference("first", first))
    previous.cached(reference("second", second))
    assert retained_bytes(tmp_path) > BUDGET
    restarted = history_at(tmp_path, source)
    with pin_cached(restarted, reference("second", second)) as pinned:
        assert pinned.read_bytes() == second
        assert retained_bytes(tmp_path) <= BUDGET
    assert source.requests == ["first", "second"]


def test_reduced_budget_waits_for_existing_readers_without_removing_their_pins(tmp_path):
    first, second = b"a" * LARGE, b"b" * LARGE
    source = Source({"first": first, "second": second})
    previous = history_at(tmp_path, source)
    previous.settings.cache_max_bytes = 2 * BUDGET
    restarted = history_at(tmp_path, source)
    with pin_cached(previous, reference("first", first)) as first_pin:
        with pin_cached(previous, reference("second", second)) as second_pin:
            with pytest.raises(CacheCapacityError):
                restarted.cached(reference("second", second))
            assert first_pin.read_bytes() == first
            assert second_pin.read_bytes() == second
    assert restarted.cached(reference("second", second)).read_bytes() == second
    assert retained_bytes(tmp_path) <= BUDGET


def test_legacy_unknown_size_cannot_keep_an_oversized_warm_payload_after_restart(tmp_path):
    body = b"x" * (BUDGET + 1)
    source = Source({"large": body})
    previous = history_at(tmp_path, source)
    previous.settings.cache_max_bytes = 2 * BUDGET
    previous.cached(reference("large", body))
    legacy_reference = reference("large", body)
    legacy_reference.pop("bytes")
    with pytest.raises(ValueError):
        history_at(tmp_path, source).cached(legacy_reference)
    assert retained_bytes(tmp_path) <= BUDGET
    assert not list((tmp_path / ".downloads").iterdir())


def test_reduced_budget_waits_for_a_larger_existing_download(tmp_path):
    warm, large = b"w" * 4096, b"x" * (BUDGET + LARGE)
    source = Source({"warm": warm, "large": large})
    previous = history_at(tmp_path, source)
    previous.settings.cache_max_bytes = 2 * BUDGET
    previous.cached(reference("warm", warm))
    source.block_key = "large"
    restarted = history_at(tmp_path, source)
    with ThreadPoolExecutor(max_workers=1) as executor:
        filling = executor.submit(previous.cached, reference("large", large))
        assert source.waiting.wait(timeout=2)
        try:
            with pytest.raises(CacheCapacityError, match="downloads"):
                restarted.cached(reference("warm", warm))
            assert source.requests == ["warm", "large"]
        finally:
            source.release.set()
        assert filling.result(timeout=2).read_bytes() == large
    assert restarted.cached(reference("warm", warm)).read_bytes() == warm
    assert retained_bytes(tmp_path) <= BUDGET
    assert source.requests == ["warm", "large"]


CRASH_READER = """
import io, json, os, sys
from pathlib import Path
from types import SimpleNamespace
from sm_trending_products.history import History
from sm_trending_products.cache import pin_cached
root, obj, mode = Path(sys.argv[1]), json.loads(sys.argv[2]), sys.argv[3]
data = b'a' * obj['bytes']
class Source:
    def get_object(self, **kwargs):
        class Body:
            reads = 0
            def read(self, size):
                self.reads += 1
                if self.reads == 1: return data
                if mode == 'download': os._exit(0)
                return b''
            def close(self): pass
        return {'Body': Body()}
h = History.__new__(History)
h.settings = SimpleNamespace(cache_dir=root, cache_max_bytes=1024*1024)
h.bucket, h._client = 'controlled', Source()
h.metrics = {'get_requests': 0, 'download_bytes': 0, 'cache_hits': 0}
with pin_cached(h, obj): os._exit(0)
"""


@pytest.mark.parametrize("mode", ["reader", "download"])
def test_hard_process_exit_reclaims_pin_or_download_reservation(tmp_path, mode):
    first, second = b"a" * LARGE, b"b" * LARGE
    subprocess.run(
        [
            sys.executable,
            "-c",
            CRASH_READER,
            str(tmp_path),
            json.dumps(reference("first", first)),
            mode,
        ],
        check=True,
        timeout=10,
    )
    assert list((tmp_path / (".pins" if mode == "reader" else ".downloads")).iterdir())
    history = history_at(tmp_path, Source({"second": second}))
    assert history.cached(reference("second", second)).read_bytes() == second
    assert retained_bytes(tmp_path) <= BUDGET
    assert not list((tmp_path / ".pins").iterdir())
    assert not list((tmp_path / ".downloads").iterdir())


def test_two_readers_fill_the_same_object_once(tmp_path):
    body = b"x" * LARGE
    source = Source({"same": body})
    source.block_key = "same"
    history = history_at(tmp_path, source)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(history.cached, reference("same", body))
        assert source.waiting.wait(timeout=2)
        second = executor.submit(history.cached, reference("same", body))
        source.release.set()
        assert first.result(timeout=2).read_bytes() == body
        assert second.result(timeout=2).read_bytes() == body
    assert source.requests == ["same"]


def test_corrupted_warm_object_is_verified_and_refetched(tmp_path):
    body = b"valid"
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    history.cached(reference("object", body)).write_bytes(b"wrong")
    assert history.cached(reference("object", body)).read_bytes() == body
    assert source.requests == ["object", "object"]


def test_download_cannot_write_beyond_its_reservation(tmp_path):
    body = b"x" * LARGE
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    with pytest.raises(ValueError, match="reserved cache budget"):
        history.cached({**reference("object", body), "bytes": 1})
    assert retained_bytes(tmp_path) == 0
    assert not list((tmp_path / ".downloads").iterdir())


def test_retirement_waits_for_fill_and_does_not_leave_a_late_cache_copy(tmp_path):
    body = b"x" * LARGE
    source = Source({"object": body})
    source.block_key = "object"
    history = history_at(tmp_path, source)
    deleted = threading.Event()

    def retire():
        with cache_object_lock(history, "object"):
            source.objects.pop("object")
            (tmp_path / hashlib.sha256(b"object").hexdigest()).unlink(missing_ok=True)
            deleted.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        filling = executor.submit(history.cached, reference("object", body))
        assert source.waiting.wait(timeout=2)
        retirement = executor.submit(retire)
        assert not deleted.wait(timeout=0.05)
        source.release.set()
        filling.result(timeout=2)
        retirement.result(timeout=2)
    assert retained_bytes(tmp_path) == 0
    with pytest.raises(KeyError):
        history.cached(reference("object", body))


def test_cache_control_symlink_is_rejected(tmp_path):
    outside = tmp_path.parent / "outside"
    outside.mkdir()
    (tmp_path / ".pins").symlink_to(outside, target_is_directory=True)
    history = history_at(tmp_path, Source({"object": b"data"}))
    with pytest.raises(ValueError, match="escaped"):
        history.cached(reference("object", b"data"))


def measured_hashes(monkeypatch):
    from sm_trending_products import cache

    original = cache.file_digest
    sizes = []

    def measure(path):
        sizes.append(path.stat().st_size)
        return original(path)

    monkeypatch.setattr(cache, "file_digest", measure)
    return sizes


def test_operation_session_hashes_twenty_warm_reads_once_then_verifies_again(tmp_path, monkeypatch):
    body = b"verified" * 4096
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    obj = reference("object", body)
    history.cached(obj)
    hashed = measured_hashes(monkeypatch)
    for _ in range(20):
        with pin_cached(history, obj) as path:
            assert path.read_bytes() == body
    assert sum(hashed) == 20 * len(body)
    hashed.clear()
    with history.read_session():
        for _ in range(20):
            with pin_cached(history, obj) as path:
                assert path.read_bytes() == body
        assert len(list((tmp_path / ".pins").iterdir())) == 1
    assert sum(hashed) == len(body)
    assert source.requests == ["object"]
    assert not list((tmp_path / ".pins").iterdir())
    with history.read_session():
        with pin_cached(history, obj):
            pass
    assert sum(hashed) == 2 * len(body)


@pytest.mark.parametrize("mutation", ["inplace", "replace", "different_expected"])
def test_session_rechecks_corruption_replacement_and_changed_descriptor(tmp_path, mutation):
    body = b"valid"
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    obj = reference("object", body)
    local = history.cached(obj)
    with history.read_session():
        with pin_cached(history, obj) as path:
            assert path.read_bytes() == body
        previous = local.stat()
        if mutation == "inplace":
            local.write_bytes(b"wrong")
            # Matching size and mtime do not conceal a changed ctime.
            os.utime(local, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        elif mutation == "replace":
            replacement = tmp_path / ".replacement"
            replacement.write_bytes(b"wrong")
            os.utime(replacement, ns=(previous.st_atime_ns, previous.st_mtime_ns))
            os.replace(replacement, local)
        else:
            body = b"newly published"
            source.objects["object"] = body
            obj = reference("object", body)
        with pin_cached(history, obj) as path:
            assert path.read_bytes() == body
    assert source.requests == ["object", "object"]
    assert not list((tmp_path / ".pins").iterdir())


def test_session_cannot_reopen_a_retired_object_through_its_retained_pin(tmp_path):
    body = b"valid"
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    obj = reference("object", body)
    local = history.cached(obj)
    with history.read_session():
        with pin_cached(history, obj) as active:
            with cache_object_lock(history, "object"):
                source.objects.pop("object")
                local.unlink()
            assert active.read_bytes() == body
            with pytest.raises(KeyError):
                with pin_cached(history, obj):
                    pytest.fail("Retained pin reopened a retired object")
            assert active.read_bytes() == body
    assert not list((tmp_path / ".pins").iterdir())
    assert retained_bytes(tmp_path) == 0


def test_session_lru_and_oversized_files_respect_bounded_pins(tmp_path, monkeypatch):
    bodies = {f"object-{index}": bytes([index]) * (70 * 1024) for index in range(5)}
    source = Source(bodies)
    history = history_at(tmp_path, source)
    for key, body in bodies.items():
        history.cached(reference(key, body))
    hashed = measured_hashes(monkeypatch)
    with history.read_session():
        for key, body in bodies.items():
            with pin_cached(history, reference(key, body)):
                pass
            pins = list((tmp_path / ".pins").iterdir())
            assert sum(pin.stat().st_size for pin in pins) <= BUDGET // 4
            assert retained_bytes(tmp_path) <= BUDGET
        first, body = next(iter(bodies.items()))
        before = sum(hashed)
        with pin_cached(history, reference(first, body)):
            pass
        assert sum(hashed) == before + len(body)
    large = b"large" * (LARGE // 5)
    source.objects["large"] = large
    history.cached(reference("large", large))
    hashed.clear()
    with history.read_session():
        for _ in range(3):
            with pin_cached(history, reference("large", large)):
                pass
            assert not list((tmp_path / ".pins").iterdir())
    assert sum(hashed) == 3 * len(large)


def test_session_pin_count_is_bounded_and_releases_on_exception(tmp_path):
    bodies = {f"object-{index}": bytes([index]) for index in range(70)}
    history = history_at(tmp_path, Source(bodies))
    for key, body in bodies.items():
        history.cached(reference(key, body))
    with pytest.raises(RuntimeError, match="controlled"):
        with history.read_session():
            for key, body in bodies.items():
                with pin_cached(history, reference(key, body)):
                    pass
                assert len(list((tmp_path / ".pins").iterdir())) <= 64
            raise RuntimeError("controlled")
    assert not list((tmp_path / ".pins").iterdir())


def test_session_relinquishes_idle_pins_before_capacity_retry(tmp_path):
    first, second = b"a" * (BUDGET // 4), b"b" * (7 * BUDGET // 8)
    source = Source({"first": first, "second": second})
    history = history_at(tmp_path, source)
    history.cached(reference("first", first))
    with history.read_session():
        with pin_cached(history, reference("first", first)):
            pass
        assert list((tmp_path / ".pins").iterdir())
        with pin_cached(history, reference("second", second)) as path:
            assert path.read_bytes() == second
            assert retained_bytes(tmp_path) <= BUDGET
    assert not list((tmp_path / ".pins").iterdir())
    assert source.requests == ["first", "second"]


def test_nested_session_reuses_outer_pins_and_rejects_invalid_size(tmp_path, monkeypatch):
    body = b"valid"
    history = history_at(tmp_path, Source({"object": body}))
    obj = reference("object", body)
    history.cached(obj)
    hashed = measured_hashes(monkeypatch)
    with history.read_session():
        with pin_cached(history, obj):
            pass
        with history.read_session():
            with pin_cached(history, obj):
                pass
        with pin_cached(history, obj):
            pass
        with pytest.raises(ValueError, match="Invalid object size"):
            with pin_cached(history, {**obj, "bytes": True}):
                pass
    assert sum(hashed) == len(body)


def test_simultaneous_sessions_keep_independent_reader_fences(tmp_path):
    body = b"valid"
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    obj = reference("object", body)
    history.cached(obj)
    barrier = threading.Barrier(2, timeout=3)

    def read():
        with history.read_session():
            for _ in range(3):
                with pin_cached(history, obj) as path:
                    barrier.wait()
                    assert path.read_bytes() == body
                    barrier.wait()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [executor.submit(read) for _ in range(2)]
        for result in results:
            result.result(timeout=5)
    assert source.requests == ["object"]
    assert not list((tmp_path / ".pins").iterdir())
    assert retained_bytes(tmp_path) <= BUDGET


def test_session_does_not_memoize_inode_changed_during_verification(tmp_path, monkeypatch):
    from sm_trending_products import cache

    body = b"valid"
    source = Source({"object": body})
    history = history_at(tmp_path, source)
    obj = reference("object", body)
    history.cached(obj)
    original = cache.file_digest

    def hash_then_change(path):
        checksum = original(path)
        path.write_bytes(b"wrong")
        return checksum

    with history.read_session():
        with monkeypatch.context() as changes:
            changes.setattr(cache, "file_digest", hash_then_change)
            with pin_cached(history, obj):
                pass
        assert not list((tmp_path / ".pins").iterdir())
        with pin_cached(history, obj) as path:
            assert path.read_bytes() == body
    assert source.requests == ["object", "object"]
