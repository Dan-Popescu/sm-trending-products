"""Exact future-clock feature reuse against the complete analytical oracle."""

from dataclasses import replace
from datetime import timedelta

import pytest

from sm_trending_products.analytics import assess, normalize_assessments
from sm_trending_products.contracts import (
    AssessmentPolicy,
    Measurement,
    Observation,
    Product,
    ProductLink,
    digest,
    now,
)
from sm_trending_products.feature_projection import (
    FeatureProjection,
    ProjectionScope,
    projection_valid_through,
)
from sm_trending_products.pipeline import Pipeline


def video(entity, at, value, published):
    return Observation(
        observation_id=digest({"entity": entity, "at": at, "value": value}),
        source="projection_fixture",
        entity_type="video",
        entity_id=entity,
        observed_at=at,
        available_at=at,
        published_at=published,
        synthetic=True,
        metrics={"view_count": Measurement(lower=value, upper=value)},
        attributes={"creator_id": entity},
    )


def seeded(pipeline, *, age_hours=23, policy=None):
    cutoff = now() + timedelta(minutes=1)
    product = Product.model_validate(pipeline.create_product("p", "P", synthetic=True))
    published = cutoff - timedelta(hours=age_hours) if age_hours is not None else None
    evidence = [
        video("v", cutoff - timedelta(hours=12), 100, published),
        video("v", cutoff, 200, published),
    ]
    datasets = pipeline.import_records(
        [row.model_dump(mode="json") for row in evidence], synthetic=True
    )
    pipeline.review_link("p", evidence[0].entity_key)
    links = [ProductLink.model_validate(row) for row in pipeline.links(cutoff, True)]
    scope = ProjectionScope(product, frozenset(link.entity_key for link in links), digest(links))
    cache = FeatureProjection(pipeline.catalog)
    policy = policy or AssessmentPolicy()
    read = cache.read_many([scope], cutoff, policy)
    anchors = {}
    row = assess(
        evidence, [product], links, cutoff, True, policy, normalize=False, age_anchors=anchors
    )[0]
    audit_root = "projection-tests/" + digest({"evidence": datasets})
    pipeline.catalog.stage_derivation(audit_root, datasets)
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (audit_root,)
    )
    assert cache.store(
        scope,
        cutoff,
        policy,
        read.frontiers["p"],
        row,
        evidence,
        age_anchor=anchors["p"],
        audit_root=audit_root,
    )
    return cutoff, product, evidence, datasets, links, scope, cache, row, audit_root


@pytest.mark.parametrize("published_age", [23, 167, None])
def test_future_cutoff_reuses_exact_features_and_recomputes_global_cohort_ranks(
    pipeline, published_age
):
    cutoff, product, evidence, _datasets, links, scope, cache, row, audit_root = seeded(
        pipeline, age_hours=published_age
    )
    before = row.copy()
    for offset in (timedelta(minutes=30), timedelta(hours=1), timedelta(hours=2)):
        later = cutoff + offset
        reusable = cache.read_many([scope], later, AssessmentPolicy())
        assert reusable.audit_roots == {"p": audit_root}
        oracle = assess(evidence, [product], links, later, True, normalize=False)
        assert reusable.rows == {"p": oracle[0]}
        assert normalize_assessments(list(reusable.rows.values())) == normalize_assessments(oracle)
    assert row == before
    # Reopening the application/catalog does not replace its clock with startup time.
    with Pipeline(pipeline.settings) as restarted:
        reusable = FeatureProjection(restarted.catalog).read_many(
            [scope], cutoff + timedelta(hours=2), AssessmentPolicy()
        )
        assert (
            reusable.rows["p"]
            == assess(
                evidence, [product], links, cutoff + timedelta(hours=2), True, normalize=False
            )[0]
        )


def test_inclusive_staleness_and_rolling_expiry_boundaries_force_rebuild_after_equality(pipeline):
    policy = AssessmentPolicy(lookback_days=1, max_age_hours=1000)
    cutoff, _product, evidence, _datasets, _links, scope, cache, _row, _root = seeded(
        pipeline, policy=policy
    )
    boundary = evidence[0].observed_at + timedelta(days=1)
    assert projection_valid_through(evidence, cutoff, policy) == boundary
    assert "p" in cache.read_many([scope], boundary, policy).rows
    assert "p" not in cache.read_many([scope], boundary + timedelta(microseconds=1), policy).rows


def test_declared_interval_staleness_can_precede_observation_staleness(pipeline):
    cutoff, _product, evidence, _datasets, _links, _scope, _cache, _row, _root = seeded(pipeline)
    interval = evidence[-1].model_copy(
        update={
            "attributes": {
                "period_start": (cutoff - timedelta(hours=80)).isoformat(),
                "period_end": (cutoff - timedelta(hours=71)).isoformat(),
            }
        }
    )
    assert projection_valid_through([interval], cutoff, AssessmentPolicy()) == cutoff + timedelta(
        hours=1
    )


@pytest.mark.parametrize("hours", [72, 72.0000000001, 72.0000000002])
def test_fractional_microsecond_freshness_never_rounds_past_either_oracle(hours):
    cutoff = now()
    observed = cutoff - timedelta(hours=72)
    row = video("fractional", observed, 100, observed)
    policy = AssessmentPolicy(max_age_hours=hours)
    deadline = projection_valid_through([row], cutoff, policy)
    assert deadline == cutoff
    elapsed = (deadline - observed).total_seconds()
    assert elapsed <= hours * 3600
    assert elapsed / 3600 <= hours
    later = (deadline + timedelta(microseconds=1) - observed).total_seconds()
    assert later > hours * 3600 or later / 3600 > hours


def test_policy_identity_backward_time_and_audit_retirement_never_reuse_a_projection(pipeline):
    cutoff, _product, _evidence, _datasets, _links, scope, cache, _row, audit_root = seeded(
        pipeline
    )
    policy = AssessmentPolicy()
    assert not cache.read_many([scope], cutoff - timedelta(microseconds=1), policy).rows
    assert not cache.read_many(
        [scope], cutoff, policy.model_copy(update={"max_age_hours": 48})
    ).rows
    moved = replace(scope, identity_version="new ownership decision")
    assert not cache.read_many([moved], cutoff, policy).rows
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='invalidated' WHERE object_key=%s", (audit_root,)
    )
    read = cache.read_many([scope], cutoff, policy)
    assert read.reasons == {"p": "unavailable_audit_root"}


def test_new_late_correction_and_retention_change_only_affected_source_frontiers(pipeline):
    cutoff, _product, evidence, datasets, _links, scope, cache, _row, _audit_root = seeded(pipeline)
    other_product = Product.model_validate(
        pipeline.create_product("other", "Other", synthetic=True)
    )
    other = ProjectionScope(other_product, frozenset({"projection_fixture:video:other"}), "other")
    before = cache.read_many([scope, other], cutoff, AssessmentPolicy()).frontiers
    correction = evidence[-1].model_copy(
        update={
            "observation_id": "late-correction",
            "available_at": cutoff + timedelta(minutes=15),
            "metrics": {"view_count": Measurement(lower=300, upper=300)},
        }
    )
    pipeline.import_records([correction.model_dump(mode="json")], synthetic=True)
    changed = cache.read_many([scope, other], cutoff + timedelta(minutes=15), AssessmentPolicy())
    assert not changed.rows
    assert before["p"] != changed.frontiers["p"]
    assert before["other"] == changed.frontiers["other"]
    prior = changed.frontiers["p"]
    pipeline.catalog.invalidate_dataset(datasets[0], "projection-retirement-test")
    assert cache.read_many([scope], cutoff, AssessmentPolicy()).frontiers["p"] != prior


def test_cache_write_fences_concurrent_source_change_and_future_visible_inputs(pipeline):
    cutoff, _product, evidence, _datasets, _links, scope, cache, row, audit_root = seeded(pipeline)
    old = cache.read_many([scope], cutoff, AssessmentPolicy()).frontiers["p"]
    future = evidence[-1].model_copy(
        update={"observation_id": "future-availability", "available_at": cutoff + timedelta(days=1)}
    )
    pipeline.import_records([future.model_dump(mode="json")], synthetic=True)
    assert not cache.store(
        scope,
        cutoff,
        AssessmentPolicy(),
        old,
        row,
        evidence,
        age_anchor=evidence[-1].published_at,
        audit_root=audit_root,
    )
    later_frontier = cache.read_many([scope], cutoff, AssessmentPolicy()).frontiers["p"]
    assert not later_frontier.fully_visible(cutoff)
    assert later_frontier.fully_visible(cutoff + timedelta(days=1))


def test_bulk_future_clock_hits_preserve_changing_cross_product_cohort_scores(pipeline):
    cutoff = now() + timedelta(minutes=1)
    products, links, observations, scopes = [], [], [], []
    for index, age in enumerate((23, 25, 167)):
        product = Product.model_validate(
            pipeline.create_product(f"p{index}", f"P{index}", synthetic=True)
        )
        entity = f"v{index}"
        evidence = [
            video(entity, cutoff - timedelta(hours=12), 100, cutoff - timedelta(hours=age)),
            video(entity, cutoff, 200 * (index + 1), cutoff - timedelta(hours=age)),
        ]
        pipeline.import_records([row.model_dump(mode="json") for row in evidence], synthetic=True)
        pipeline.review_link(product.product_id, evidence[0].entity_key)
        products.append(product)
        observations.extend(evidence)
    links = [ProductLink.model_validate(row) for row in pipeline.links(cutoff, True)]
    for product in products:
        keys = frozenset(link.entity_key for link in links if link.product_id == product.product_id)
        scopes.append(ProjectionScope(product, keys, digest(links)))
    policy = AssessmentPolicy()
    cache = FeatureProjection(pipeline.catalog)
    read = cache.read_many(scopes, cutoff, policy)
    anchors = {}
    initial = assess(
        observations, products, links, cutoff, True, policy, normalize=False, age_anchors=anchors
    )
    audit_root = "projection-tests/bulk"
    datasets = [row["dataset_id"] for row in pipeline.datasets()]
    pipeline.catalog.stage_derivation(audit_root, datasets)
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' WHERE object_key=%s", (audit_root,)
    )
    by_product = {row["product_id"]: row for row in initial}
    for scope in scopes:
        assert cache.store(
            scope,
            cutoff,
            policy,
            read.frontiers[scope.product.product_id],
            by_product[scope.product.product_id],
            [row for row in observations if row.entity_key in scope.entity_keys],
            age_anchor=anchors[scope.product.product_id],
            audit_root=audit_root,
        )
    later = cutoff + timedelta(hours=2)
    hits = cache.read_many(scopes, later, policy)
    assert len(hits.rows) == 3
    oracle = assess(observations, products, links, later, True, policy)
    assert normalize_assessments(list(hits.rows.values())) == oracle
    assert {row["product_id"]: row["score"] for row in normalize_assessments(initial)} != {
        row["product_id"]: row["score"] for row in oracle
    }
