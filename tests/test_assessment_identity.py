"""Exact global ownership is reused across bounded analytical partitions."""

from datetime import timedelta

import pytest

import sm_trending_products.analytics as analytics
from sm_trending_products.contracts import (
    AssessmentPolicy,
    Measurement,
    Observation,
    Product,
    ProductLink,
    now,
    utc,
)

BASE = utc("2026-01-01T00:00:00Z")
CUTOFF = BASE + timedelta(days=2)


def case():
    products = [
        Product(product_id=f"p{i}", name=f"Product {i}", created_at=BASE, synthetic=True)
        for i in range(4)
    ]
    links = [
        ProductLink(
            product_id=product.product_id, entity_key=f"fixture:video:v{i}", available_at=BASE
        )
        for i, product in enumerate(products)
    ]
    # A newer owner outside the first partition must supersede the old owner.
    links.append(
        ProductLink(
            product_id="p3",
            entity_key="fixture:video:v0",
            available_at=BASE + timedelta(hours=1),
        )
    )
    observations = [
        Observation(
            observation_id=f"v{i}-{hour}",
            source="fixture",
            entity_type="video",
            entity_id=f"v{i}",
            observed_at=BASE + timedelta(hours=hour),
            available_at=BASE + timedelta(hours=hour),
            synthetic=True,
            metrics={"view_count": Measurement(lower=hour * 100, upper=hour * 100)},
        )
        for i in range(4)
        for hour in (1, 2)
    ]
    return products, links, observations


def test_partitioned_identity_matches_global_ownership_and_resolves_once(monkeypatch):
    products, links, observations = case()
    oracle = analytics.assess(observations, products, links, CUTOFF, True)
    calls = 0
    resolve = analytics.resolve_links

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return resolve(*args, **kwargs)

    monkeypatch.setattr(analytics, "resolve_links", counted)
    identity = analytics.AssessmentIdentity.resolve(products, links, CUTOFF, True, 0.9)
    partitions = [
        row
        for product in products
        for row in analytics.assess(
            observations,
            [product],
            links,
            CUTOFF,
            True,
            normalize=False,
            identity=identity,
        )
    ]
    assert calls == 1
    assert analytics.normalize_assessments(partitions) == oracle
    assert next(row for row in oracle if row["product_id"] == "p0")["evidence_refs"] == []
    assert len(next(row for row in oracle if row["product_id"] == "p3")["evidence_refs"]) == 4
    with pytest.raises(TypeError):
        identity.owners["another"] = links[0]


@pytest.mark.parametrize("change", ["cutoff", "namespace", "confidence"])
def test_reusing_identity_for_another_scope_is_rejected(change):
    products, links, observations = case()
    identity = analytics.AssessmentIdentity.resolve(products, links, CUTOFF, True, 0.9)
    cutoff = CUTOFF + timedelta(seconds=1) if change == "cutoff" else CUTOFF
    synthetic = False if change == "namespace" else True
    policy = AssessmentPolicy(min_link_confidence=0.8 if change == "confidence" else 0.9)
    with pytest.raises(ValueError, match="different scope"):
        analytics.assess(
            observations, products, links, cutoff, synthetic, policy, identity=identity
        )


@pytest.mark.integration
def test_published_assessment_resolves_once_for_multiple_native_partitions(pipeline, monkeypatch):
    cutoff = now() + timedelta(minutes=1)
    for index in range(7):
        product = f"product-{index}"
        entity = f"video-{index}"
        pipeline.create_product(product, product, synthetic=True)
        pipeline.import_records(
            [
                {
                    "observation_id": f"{entity}-{hour}",
                    "source": "fixture",
                    "entity_type": "video",
                    "entity_id": entity,
                    "observed_at": (cutoff - timedelta(hours=hour)).isoformat(),
                    "available_at": (cutoff - timedelta(hours=hour)).isoformat(),
                    "synthetic": True,
                    "metrics": {
                        "view_count": {"lower": 1000 - hour * 100, "upper": 1000 - hour * 100}
                    },
                }
                for hour in (2, 1)
            ],
            synthetic=True,
        )
        pipeline.review_link(product, f"fixture:video:{entity}")
    pipeline.settings.analysis_product_batch_size = 2
    calls = 0
    resolve = analytics.resolve_links

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return resolve(*args, **kwargs)

    monkeypatch.setattr(analytics, "resolve_links", counted)
    result = pipeline.assess(cutoff, True)
    assert calls == 1
    assert len(result["rows"]) == 7
    assert [row["features"]["growth_per_hour"] for row in result["rows"]] == pytest.approx(
        [100] * 7
    )


def test_projection_age_anchor_uses_only_the_same_usable_video_timelines():
    cutoff = BASE + timedelta(days=4)
    product = Product(product_id="p", name="Product", created_at=BASE, synthetic=True)
    links = [
        ProductLink(product_id="p", entity_key=f"fixture:video:v{i}", available_at=BASE)
        for i in range(4)
    ]
    rows = []
    for index in range(4):
        times = (
            [cutoff - timedelta(hours=1)]
            if index == 2
            else [BASE + timedelta(hours=1), BASE + timedelta(hours=2)]
            if index == 3
            else [cutoff - timedelta(hours=2), cutoff - timedelta(hours=1)]
        )
        for point, when in enumerate(times):
            rows.append(
                Observation(
                    observation_id=f"v{index}-{point}",
                    source="fixture",
                    entity_type="video",
                    entity_id=f"v{index}",
                    observed_at=when,
                    available_at=when,
                    published_at=BASE + timedelta(hours=index) if index != 3 else BASE,
                    synthetic=True,
                    metrics={"view_count": Measurement(lower=point * 100, upper=point * 100)},
                )
            )
    anchors = {}
    oracle = analytics.assess(rows, [product], links, cutoff, True)
    prepared = analytics.assess(rows, [product], links, cutoff, True, age_anchors=anchors)
    assert prepared == oracle
    assert anchors == {"p": BASE}
    assert prepared[0]["features"]["age_hours"] == 96
