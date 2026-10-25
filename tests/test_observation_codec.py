"""Source and local-window decoders preserve one complete evidence contract."""

import io
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from pydantic import ValidationError

from sm_trending_products.contracts import Observation, canonical
from sm_trending_products.feature_projection import _decode_window
from sm_trending_products.history import History


def columnar(row):
    encoded = row.model_dump(mode="json")
    encoded["entity_key"] = row.entity_key
    for name in ("metrics", "attributes", "missing_fields"):
        encoded[name + "_json"] = canonical(encoded.pop(name))
    return encoded


@pytest.mark.parametrize("entity_type", ["video", "ad", "commercial_content", "offer", "cost"])
def test_source_and_window_decode_preserve_all_fields_and_original_offsets(entity_type):
    offset = timezone(timedelta(hours=-4, minutes=-30, seconds=-5, microseconds=-123456))
    at = datetime(2026, 1, 2, 10, 30, 1, 123456, tzinfo=offset)
    original = Observation.model_validate(
        {
            "observation_id": 'id-été-"\\',
            "source": "codec_fixture",
            "entity_type": entity_type,
            "entity_id": "item",
            "observed_at": at,
            "available_at": at + timedelta(seconds=1),
            "published_at": None,
            "synthetic": True,
            "metrics": {
                "view_count": {"lower": 2**120, "upper": 2**120 + 17, "precision": "bounded"},
                "estimate": {
                    "lower": 1,
                    "upper": 9,
                    "precision": "estimated",
                    "semantics": "interval",
                    "model_version": "known-v1",
                    "raw": '1–9 "units"',
                    "unit": "orders",
                },
            },
            "attributes": {
                "unicode": "été ♜",
                "unknown": {"nested": [None, False, {}, [], 0.125, 2**130]},
                "escaped": '\\", "source": "other"',
            },
            "missing_fields": {"likes": "not supplied"},
            "raw_dataset_id": "original-raw",
        }
    )
    expected = original.model_dump(mode="json")
    for decoder in (History._decode, _decode_window):
        assert decoder(columnar(original)).model_dump(mode="json") == expected


@pytest.mark.parametrize(
    "field,value",
    [
        ("metrics_json", '{"views":{"lower":-1,"upper":5}}'),
        ("metrics_json", '{"views":{"lower":1.5,"upper":1.5}}'),
        ("attributes_json", "[]"),
        ("missing_fields_json", '{"views":null}'),
    ],
)
def test_source_and_window_decode_keep_model_validation(field, value):
    row = {
        "observation_id": "invalid",
        "source": "codec_fixture",
        "entity_type": "video",
        "entity_id": "item",
        "observed_at": "2026-01-01T00:00:00Z",
        "available_at": "2026-01-01T00:00:00Z",
        "metrics_json": "{}",
        "attributes_json": "{}",
        "missing_fields_json": "{}",
        field: value,
    }
    for decoder in (History._decode, _decode_window):
        with pytest.raises(ValidationError):
            decoder(dict(row))


def test_existing_deep_unknown_attributes_survive_source_parquet_roundtrip():
    nested = {"useful": "retained"}
    for _ in range(220):
        nested = {"key": nested}
    original = Observation.model_validate(
        {
            "observation_id": "deep-existing-evidence",
            "source": "codec_fixture",
            "entity_type": "video",
            "entity_id": "item",
            "observed_at": "2026-01-01T00:00:00Z",
            "available_at": "2026-01-01T00:00:00Z",
            "attributes": {"unknown": nested},
        }
    )
    body = History.__new__(History).encode([original])
    row = pl.read_parquet(io.BytesIO(body)).row(0, named=True)
    for decoder in (History._decode, _decode_window):
        assert decoder(dict(row)).model_dump(mode="json") == original.model_dump(mode="json")
