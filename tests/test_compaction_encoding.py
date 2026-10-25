"""Compaction encoding preserves immutable legacy bytes with bounded row dictionaries."""

import hashlib
from datetime import UTC, datetime, timedelta, timezone

import polars as pl
import pytest

import sm_trending_products.compaction_encoding as encoding
from sm_trending_products.contracts import Observation, now
from sm_trending_products.history import History


def observation(index: int, *, mixed: bool = False) -> Observation:
    offset = timezone(timedelta(hours=5, minutes=30, seconds=3, microseconds=123000))
    instant = datetime(2026, 1, 1, 12, tzinfo=offset if mixed else UTC)
    return Observation.model_validate(
        {
            "observation_id": f"observation-{index}",
            "source": "encoding_fixture",
            "entity_type": "video",
            "entity_id": f"entity-{index % 23}",
            "observed_at": instant + timedelta(seconds=index),
            "available_at": instant + timedelta(seconds=index, microseconds=1),
            "published_at": instant - timedelta(days=2) if index >= 8192 else None,
            "synthetic": True,
            "account_id": "account" if index >= 8192 else None,
            "market": "FR" if index >= 8192 else None,
            "metrics": {
                "count": {
                    "lower": 2**120 + index,
                    "upper": 2**120 + index + 7,
                    "precision": "bounded",
                    "semantics": "interval",
                    "unit": "observed units",
                    "raw": "unknown source notation",
                }
            },
            "attributes": {
                "unknown": {"étiquette": [None, True, 2**105, "雪"]},
                "source_period_start": "2025-12-01T12:00:00+05:30",
                "source_period_end": "2025-12-02T12:00:00+05:30",
                "price": "0.00000000000000000001",
            },
            "missing_fields": {"audience": "not exposed"},
            "raw_dataset_id": "original-delivery" if mixed else None,
        }
    )


def legacy(rows):
    return History.encode(None, rows)


@pytest.mark.parametrize("count", [0, 1, 8192])
def test_small_files_keep_the_exact_existing_encoder(count):
    rows = [observation(index) for index in range(count)]
    calls = []

    def original(values):
        calls.append(values)
        return b"unchanged legacy encoder"

    assert encoding.encode_compaction(rows, original) == b"unchanged legacy encoder"
    assert calls == [rows]


@pytest.mark.parametrize("mixed", [False, True])
def test_large_file_bytes_match_legacy_including_offsets_and_schema_changes(mixed):
    rows = [observation(index, mixed=mixed) for index in range(9000)]
    expected = legacy(rows)
    assert encoding.encode_compaction(rows, legacy) == expected


def test_large_encoding_never_builds_the_complete_row_dictionary_list(monkeypatch):
    rows = [observation(index) for index in range(9000)]
    original = pl.DataFrame
    sizes = []

    def bounded_frame(values, **kwargs):
        sizes.append(len(values))
        return original(values, **kwargs)

    monkeypatch.setattr(encoding.pl, "DataFrame", bounded_frame)
    result = encoding.encode_compaction(rows, legacy)
    assert result
    assert max(sizes) <= 8192
    assert sum(sizes) == len(rows)


@pytest.mark.integration
def test_large_staged_compaction_survives_encoder_and_layout_changes(pipeline, monkeypatch):
    import sm_trending_products.history as history_module

    pipeline.history.settings = pipeline.settings.model_copy(
        update={"history_partition_buckets": 1}
    )
    observations = [observation(index, mixed=True) for index in range(9000)]
    parents = []
    for start in (0, 4500):
        parents.extend(
            pipeline.import_records(
                [row.model_dump(mode="json") for row in observations[start : start + 4500]],
                synthetic=True,
            )
        )
    before, _ = pipeline.history.observations(now(), True)
    original = pipeline.history.encode(before)

    def interrupted(*args, **kwargs):
        raise RuntimeError("controlled compaction commit interruption")

    with monkeypatch.context() as failed:
        failed.setattr(pipeline.catalog, "index_observations", interrupted)
        with pytest.raises(RuntimeError, match="controlled compaction"):
            pipeline.compact(synthetic=True)
    staged = pipeline.catalog.one("SELECT dataset_id,manifest FROM datasets WHERE state='staged'")
    descriptor = staged["manifest"]["normalized"][0]
    assert len(staged["manifest"]["normalized"]) == 1
    assert descriptor["sha256"] == hashlib.sha256(original).hexdigest()
    assert descriptor["bytes"] == len(original)
    # A rollback can use the old encoder: byte identity needs no new format gate.
    monkeypatch.setattr(history_module, "encode_compaction", lambda rows, old: old(rows))
    pipeline.settings = pipeline.settings.model_copy(update={"compaction_target_rows_per_file": 1})
    result = pipeline.compact(synthetic=True)
    assert result["dataset_id"] == staged["dataset_id"]
    assert (
        pipeline.catalog.one(
            "SELECT manifest FROM datasets WHERE dataset_id=%s", (result["dataset_id"],)
        )["manifest"]
        == staged["manifest"]
    )
    after, _ = pipeline.history.observations(now(), True)
    assert after == before
    assert {row.raw_dataset_id for row in after} == set(parents)
    page, _ = pipeline.history.page(now(), True, {before[0].entity_key}, limit=100)
    assert page == [row for row in before if row.entity_key == before[0].entity_key][:100]
    assert pipeline.replay(result["dataset_id"])["records"] == len(before)
    assert pipeline.history.observations(now(), True)[0] == before
    assert pipeline.verify()["ok"]
