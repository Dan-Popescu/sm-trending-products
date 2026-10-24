"""An actual fragmented source working set keeps exact reusable windows in 1 MB."""

import hashlib
import json
import subprocess
from datetime import timedelta

import pytest
from analytical_results import snapshot_semantics

from sm_trending_products.analytics import assess
from sm_trending_products.cache import _Cache
from sm_trending_products.contracts import Product, ProductLink, now


def fact(product, video, point, cutoff):
    timestamp = cutoff + timedelta(hours=6 * (point - 119))
    return {
        "observation_id": hashlib.sha256(
            f"pressure:{product}:{video}:{point}".encode()
        ).hexdigest(),
        "source": "cache_pressure_fixture",
        "entity_type": "video",
        "entity_id": f"p{product}-v{video}",
        "observed_at": timestamp.isoformat(),
        "available_at": timestamp.isoformat(),
        "published_at": (cutoff - timedelta(days=100)).isoformat(),
        "synthetic": True,
        "metrics": {"view_count": {"lower": 1000 + point * 100, "upper": 1000 + point * 100}},
        "attributes": {"creator_id": f"creator-{product}-{video % 7}"},
    }


def inventory(pipeline):
    cache = _Cache(pipeline.history)

    def paths(key):
        # The immutable baseline has the original opaque name only. This small
        # measurement adapter lets the same test observe both real revisions.
        if hasattr(cache, "paths"):
            return cache.paths(key)
        return (cache.root / hashlib.sha256(key.encode()).hexdigest(),)

    windows = pipeline.catalog.all(
        "SELECT product_id,descriptor FROM feature_product_windows WHERE state='ready' "
        "ORDER BY product_id"
    )
    current = []
    for row in windows:
        descriptor = row["descriptor"]
        actual = [path.stat().st_size for path in paths(descriptor["key"]) if path.exists()]
        current.append(
            {
                "product_id": row["product_id"],
                "declared_bytes": descriptor["bytes"],
                "present": bool(actual),
                "physical_alias_sizes": actual,
            }
        )
    obsolete = []
    for row in pipeline.catalog.all("SELECT object_key FROM retention_objects WHERE local_only"):
        obsolete.extend(path.stat().st_size for path in paths(row["object_key"]) if path.exists())
    inodes = {}
    for directory in (cache.root, cache.root / ".pins"):
        for path in directory.iterdir():
            if path.is_file() and (directory != cache.root or not path.name.startswith(".")):
                info = path.stat()
                inodes[info.st_dev, info.st_ino] = info.st_size
    return {
        "current": current,
        "current_total_bytes": sum(row["declared_bytes"] for row in current),
        "current_present": sum(row["present"] for row in current),
        "obsolete_file_bytes": sum(obsolete),
        "obsolete_files": len(obsolete),
        "payload_inode_bytes": sum(inodes.values()),
    }


@pytest.mark.integration
def test_fragmented_history_retains_exact_delta_windows_with_the_same_one_mb_budget(
    pipeline, tmp_path
):
    pipeline.history.settings.cache_max_bytes = 1_000_000
    pipeline.history.settings.analysis_product_batch_size = 1
    cutoff = now() + timedelta(hours=4)
    for product in range(6):
        pipeline.create_product(f"p{product}", f"Product {product}", synthetic=True)
        records = [
            fact(product, video, point, cutoff) for point in range(120) for video in range(20)
        ]
        for start in range(0, len(records), 100):
            pipeline.import_records(records[start : start + 100], synthetic=True)
        pipeline.identity.reassign_entities(
            [f"cache_pressure_fixture:video:p{product}-v{video}" for video in range(20)],
            f"p{product}",
            method="reviewed-cache-pressure-v1",
        )
    assert pipeline.catalog.one("SELECT count(*) AS n FROM observation_deliveries")["n"] == 14400
    assert pipeline.catalog.one("SELECT count(*) AS n FROM normalized_objects")["n"] == 144
    report = {
        "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "budget_bytes": 1_000_000,
        "products": 6,
        "videos_per_product": 20,
        "seed_samples_per_video": 120,
        "historical_imports": 144,
        "refresh_imports": 6,
        "lookback_days": 30,
        "scope": "single-counter native SQL/HTTP Moto fixture; no throughput or capacity claim",
    }

    def measure(label, at):
        metrics = dict(pipeline.history.metrics)
        work = dict(pipeline.history.analysis_metrics)
        payload = pipeline.assess(at, True)
        report[label] = {
            "metrics": {
                key: pipeline.history.metrics[key] - value for key, value in metrics.items()
            },
            "work": {
                key: pipeline.history.analysis_metrics[key] - value for key, value in work.items()
            },
            "selected_observations": sum(
                row["coverage"]["observations"] for row in payload["rows"]
            ),
            "cache": inventory(pipeline),
        }
        assert {row["product_id"] for row in payload["rows"]} == {f"p{i}" for i in range(6)}
        assert report[label]["cache"]["payload_inode_bytes"] <= report["budget_bytes"]
        return payload

    measure("cold", cutoff)
    for product in range(6):
        pipeline.import_records(
            [fact(product, video, 120, cutoff) for video in range(20)], synthetic=True
        )
    later = cutoff + timedelta(hours=6)
    result = measure("refresh", later)
    # Expand no oracle between the measured phases: its own source/provenance
    # cache traffic must not contaminate the comparison of retained working sets.
    observations, _ = pipeline.history.observations(
        later, True, since=later - timedelta(days=30), until=later
    )
    products = [Product.model_validate(row) for row in pipeline._all_products(True, later)]
    links = [ProductLink.model_validate(row) for row in pipeline.links(later, True)]
    oracle = assess(observations, products, links, later, True)
    assert snapshot_semantics(pipeline, {"rows": result["rows"]})["rows"] == oracle
    report["complete_scalar_rank_reference_oracle"] = True
    (tmp_path / "cache-pressure-observed.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    assert report["cold"]["selected_observations"] == 14400
    assert report["refresh"]["selected_observations"] == 14520
    assert report["cold"]["cache"]["current_present"] == 6
    assert report["refresh"]["cache"]["current_present"] == 6
    assert report["refresh"]["cache"]["obsolete_file_bytes"] == 0
    assert report["refresh"]["work"]["delta_windows"] == 6
    assert report["refresh"]["work"]["full_windows"] == 0
    assert report["refresh"]["metrics"]["get_requests"] == 6
