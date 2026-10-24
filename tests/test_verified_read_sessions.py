"""Measure checksum work through real SQL, HTTP storage and native Parquet scans."""

from contextlib import nullcontext
from datetime import timedelta

import pytest

from sm_trending_products import cache
from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def test_twenty_partition_assessment_freeze_and_maturation_reuse_verified_bytes(
    pipeline, monkeypatch, record_property
):
    cutoff = now() + timedelta(minutes=1)
    records = []
    for index in range(20):
        product, entity = f"product-{index:02d}", f"video-{index:02d}"
        pipeline.create_product(product, product, synthetic=True)
        for offset, value in ((-2 / 24, 100), (-1 / 24, 200), (3, 2200)):
            timestamp = cutoff + timedelta(days=offset)
            records.append(
                {
                    "source": "session_fixture",
                    "entity_type": "video",
                    "entity_id": entity,
                    "observed_at": timestamp.isoformat(),
                    "available_at": timestamp.isoformat(),
                    "synthetic": True,
                    "metrics": {"view_count": {"lower": value, "upper": value}},
                    "attributes": {"creator_id": entity},
                }
            )
    dataset = pipeline.import_records(records, synthetic=True)[0]
    for index in range(20):
        pipeline.review_link(f"product-{index:02d}", f"session_fixture:video:video-{index:02d}")
    manifest = pipeline.catalog.one(
        "SELECT manifest FROM datasets WHERE dataset_id=%s", (dataset,)
    )["manifest"]
    assert len(manifest["normalized"]) == 1
    obj = manifest["normalized"][0]
    pipeline.history.cached(obj)
    pipeline.settings.analysis_product_batch_size = 1
    hashed = []
    original = cache.file_digest

    def measure(path):
        hashed.append(path.stat().st_size)
        return original(path)

    monkeypatch.setattr(cache, "file_digest", measure)
    config = {"top_k": 20, "horizon_days": [3], "launch_delay_days": [0], "threshold": 1000}
    frozen = None

    def assessment():
        return pipeline.assess(cutoff, True)

    def freeze():
        return pipeline.freeze(cutoff, True, config)

    def mature():
        assert frozen is not None
        return pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(days=4))

    gets = pipeline.history.metrics["get_requests"]
    for name, operation in (("assessment", assessment), ("freeze", freeze), ("maturation", mature)):
        hashed.clear()
        with monkeypatch.context() as previous:
            previous.setattr(pipeline.history, "read_session", nullcontext)
            pipeline.catalog.execute("DELETE FROM feature_product_projections")
            pipeline.catalog.execute("DELETE FROM feature_product_windows")
            oracle = operation()
        baseline = sum(hashed)
        assert baseline == 20 * obj["bytes"]
        hashed.clear()
        pipeline.catalog.execute("DELETE FROM feature_product_projections")
        pipeline.catalog.execute("DELETE FROM feature_product_windows")
        actual = operation()
        assert actual == oracle
        assert sum(hashed) == obj["bytes"]
        assert pipeline.history.metrics["get_requests"] == gets
        assert not list((pipeline.settings.cache_dir / ".pins").iterdir())
        record_property(f"{name}_baseline_sha256_bytes", baseline)
        record_property(f"{name}_session_sha256_bytes", sum(hashed))
        if name == "freeze":
            frozen = actual
    assert frozen is not None
    assert len(frozen["result"]["selected"]) == 20
