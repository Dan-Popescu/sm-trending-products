"""Real filesystem and process boundaries for the shared verified-cache budget."""

from __future__ import annotations

import hashlib
import io
import json
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
