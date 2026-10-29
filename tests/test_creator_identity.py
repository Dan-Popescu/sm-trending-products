"""Creator grouping stays source-scoped across published and frozen callers."""

from datetime import timedelta

import pytest
from analytical_results import snapshot_semantics
from test_evaluation_integration import record
from test_feature_windows import count_source_pins

import sm_trending_products.analytics as analytics
import sm_trending_products.feature_projection as projection_module
from sm_trending_products.cache import _Cache
from sm_trending_products.contracts import (
    Observation,
    Product,
    ProductLink,
    canonical,
    digest,
    now,
    utc,
)
from sm_trending_products.creator_identity import creator_identity
from sm_trending_products.evaluation import EvaluationConfig, freeze_decision
from sm_trending_products.opportunity_trends import describe_diagnostics
from sm_trending_products.trend_insights import TrendInsights, _window


def complete_outputs(identities):
    cutoff = utc("2026-01-03T00:00:00Z")
    product = Product(
        product_id="p", name="P", synthetic=True, created_at=cutoff - timedelta(days=3)
    )
    rows = []
    for index, (source, creator, account) in enumerate(identities):
        for hour, count in ((-48, 100), (-24, 124), (0, 148)):
            row = record(str(index), cutoff + timedelta(hours=hour), count, account_id=account)
            row["source"] = source
            row["attributes"] = {"creator_id": creator}
            rows.append(Observation.model_validate(row))
    keys = {row.entity_key for row in rows}
    links = [
        ProductLink(product_id="p", entity_key=key, available_at=product.created_at)
        for key in sorted(keys)
    ]
    return {
        "assessment": analytics.assess(rows, [product], links, cutoff, True),
        "trend": _window(
            analytics._counter_windows(rows, "view_count", retain_rows=True),
            {row.observation_id: row for row in rows},
            keys,
            cutoff,
            24,
            timedelta(hours=24),
        ),
        "diagnostics": describe_diagnostics(rows, keys, cutoff, (24,), timedelta(hours=24)),
        "decision": freeze_decision(
            rows, [product], links, cutoff, EvaluationConfig(), synthetic=True
        ),
    }


@pytest.mark.parametrize(
    "creator", [None, "", " \t", {}, {"id": "x"}, [], ["x"], 0, 7, False, True]
)
def test_invalid_creator_metadata_is_unknown_or_uses_a_legitimate_account(creator):
    row = Observation.model_validate(record("v", utc("2026-01-03T00:00:00Z"), 1))
    row = row.model_copy(update={"attributes": {"creator_id": creator}})
    assert creator_identity(row) is None
    assert creator_identity(row.model_copy(update={"account_id": " account "})) == (
        "fixture_import",
        "account",
        "account",
    )
    for invalid in (" \t", {"id": "x"}, ["x"], 7, True):
        assert creator_identity(row.model_copy(update={"account_id": invalid})) is None


@pytest.mark.parametrize(
    ("identities", "expected"),
    [
        (
            [("fixture_import", "a", None), ("fixture_import", "b", None)],
            "44cf1de8b6add74fbe3f174c4b07e3676ea6d9a6f185b456f7bdcc31c9472f3c",
        ),
        (
            [("fixture_import", "a", None), ("fixture_import", "a", None)],
            "e7a2dd9ce5c3011234d2dc6e176ccc0db2ac382b76eeaa0f072ece4bf190f3c8",
        ),
        (
            [("fixture_import", None, "a"), ("fixture_import", None, "b")],
            "21eeef165f42fb7b7866a0c78e38ba8f5cbe7fb2bc5ded6c215c99343e7407bc",
        ),
    ],
)
def test_valid_single_source_complete_outputs_match_pre_fix_hashes(identities, expected):
    # Whole-output digests independently captured from baseline ad622c3 before this fix.
    assert digest(complete_outputs(identities)) == expected


def test_delimiters_cannot_merge_distinct_identity_parts_or_public_labels():
    outputs = complete_outputs([("s:creator", "id", None), ("s", "creator:id", None)])
    assert outputs["assessment"][0]["features"]["creator_count"] == 2
    assert outputs["trend"]["known_creator_count"] == 2
    robustness = outputs["diagnostics"]["comparisons"][0]["creator_robustness"]
    assert robustness["known_identity_count"] == 2
    assert robustness["identity_preview"] == ["s%3Acreator:creator:id", "s:creator:creator%3Aid"]


def seed(pipeline, product_id, identities, cutoff):
    pipeline.create_product(product_id, product_id, synthetic=True)
    rows = []
    for index, (source, creator, account) in enumerate(identities):
        entity = f"{product_id}-{index}"
        for hour, count in ((-48, 100), (-24, 124), (0, 148)):
            row = record(entity, cutoff + timedelta(hours=hour), count, account_id=account)
            row["source"] = source
            row["attributes"] = {"creator_id": creator}
            rows.append(row)
    datasets = pipeline.import_records(rows, synthetic=True)
    for index, (source, _creator, _account) in enumerate(identities):
        pipeline.review_link(product_id, f"{source}:video:{product_id}-{index}")
    return [Observation.model_validate(row) for row in rows], datasets


@pytest.mark.integration
@pytest.mark.parametrize(
    ("identities", "known", "unknown", "concentration"),
    [
        ([("tiktok_display", "same", None), ("explicit_public_import", "same", None)], 2, 0, 0.5),
        ([("fixture", "same", None), ("fixture", None, "same")], 2, 0, 0.5),
        ([("fixture", "same", None), ("fixture", "same", "other")], 1, 0, 1),
        ([("fixture", " same ", None), ("fixture", "same", None)], 1, 0, 1),
        ([("fixture", {"invalid": "id"}, None), ("fixture", ["invalid"], None)], 0, 2, None),
        ([("fixture", " \t", None), ("fixture", None, " \t")], 0, 2, None),
        ([("fixture", {"invalid": "id"}, "account"), ("fixture", "", "account")], 1, 0, 1),
        ([("first", None, "same"), ("second", None, "same")], 2, 0, 0.5),
    ],
    ids=["source", "kind", "dedup", "trim", "malformed", "blank", "fallback", "account-source"],
)
def test_native_assessment_trend_and_diagnostics_share_validated_creator_groups(
    pipeline, identities, known, unknown, concentration
):
    cutoff = now() + timedelta(days=3)
    seed(pipeline, "p", identities, cutoff)
    assessment = pipeline.assess(cutoff, True)["rows"][0]
    assert assessment["features"]["creator_count"] == known
    assert assessment["features"]["creator_concentration"] == concentration
    if concentration is None:
        assert "creator_spread" not in assessment["attention"]["components"]
    else:
        assert assessment["attention"]["components"]["creator_spread"]["value"] == 1 - concentration
    report = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "p", cutoff, True, windows_hours=(24,), diagnostics=True
    )
    window = report["windows"][0]
    assert window["known_creator_count"] == window["growing_creator_count"] == known
    assert window["unknown_creator_entities"] == unknown
    assert window["creator_growth_concentration"] == concentration
    robustness = report["diagnostics"]["comparisons"][0]["creator_robustness"]
    assert robustness["known_identity_count"] == known
    assert robustness["unknown_identity_entity_count"] == unknown


@pytest.mark.integration
def test_old_feature_projection_rebuilds_from_same_raw_window_and_preserves_publications(
    pipeline, monkeypatch
):
    cutoff = now() + timedelta(days=3)
    rows, datasets = seed(
        pipeline,
        "p",
        [("tiktok_display", "same", None), ("explicit_public_import", "same", None)],
        cutoff,
    )
    config = {"horizon_days": [3], "launch_delay_days": [0]}
    # Emulate the old computed feature grouping without modifying immutable history.
    with monkeypatch.context() as legacy:
        legacy.setattr(projection_module, "PROJECTION_VERSION", "exact-product-features-v3")
        legacy.setattr(
            analytics,
            "creator_identity",
            lambda row: ("legacy", "creator", row.attributes["creator_id"]),
            raising=False,
        )
        old_assessment = pipeline.assess(cutoff, True)
        old_frozen = pipeline.freeze(cutoff, True, config)
    assert old_assessment["rows"][0]["features"]["creator_count"] == 1
    assert old_frozen["result"]["universe"][0]["features"]["creator_count"] == 1
    old_archive = canonical(pipeline.assessment_snapshot(old_assessment["run_id"], True))
    old_decision = canonical(pipeline.evaluation(old_frozen["evaluation_id"], True))
    old_window = pipeline.catalog.one("SELECT * FROM feature_product_windows WHERE product_id='p'")
    assert (
        pipeline.catalog.one("SELECT projection_version FROM feature_product_projections")[
            "projection_version"
        ]
        == "exact-product-features-v3"
    )
    pins = count_source_pins(monkeypatch)
    corrected = pipeline.assess(cutoff, True)
    assert pins == []
    assert corrected["rows"][0]["features"]["creator_count"] == 2
    assert corrected["run_id"] != old_assessment["run_id"]
    current_window = pipeline.catalog.one(
        "SELECT * FROM feature_product_windows WHERE product_id='p'"
    )
    assert current_window["context_sha256"] == old_window["context_sha256"]
    assert current_window["descriptor"] == old_window["descriptor"]
    assert projection_module.PROJECTION_VERSION == "exact-product-features-v4"
    warm = pipeline.assess(cutoff, True)
    assert warm == corrected
    frozen = pipeline.freeze(cutoff, True, config)
    assert pins == []
    assert frozen["result"]["universe"][0]["features"] == corrected["rows"][0]["features"]
    products = [Product.model_validate(row) for row in pipeline._all_products(True, cutoff)]
    links = [ProductLink.model_validate(row) for row in pipeline.links(cutoff, True)]
    oracle = freeze_decision(
        rows, products, links, cutoff, EvaluationConfig.for_creation(config), synthetic=True
    )
    assert snapshot_semantics(pipeline, frozen["result"]) == snapshot_semantics(pipeline, oracle)
    # A full source rebuild and the corrected warm result produce the same publication.
    _Cache(pipeline.history).paths(current_window["descriptor"]["key"])[0].unlink()
    pipeline.catalog.execute("DELETE FROM feature_product_projections")
    cold = pipeline.assess(cutoff, True)
    assert pins
    assert cold == corrected
    assert canonical(pipeline.assessment_snapshot(old_assessment["run_id"], True)) == old_archive
    assert canonical(pipeline.evaluation(old_frozen["evaluation_id"], True)) == old_decision
    matured = pipeline.mature(old_frozen["evaluation_id"], cutoff + timedelta(days=3))
    assert matured["result"]["snapshot_id"] == old_frozen["result"]["snapshot_id"]
    assert canonical(pipeline.evaluation(old_frozen["evaluation_id"], True)) == old_decision
    # Current feature/window hits still honor source retirement at read time.
    pipeline.catalog.invalidate_dataset(datasets[0], "creator-identity-retirement")
    assert pipeline.latest_assessments(True) == []
    with pytest.raises(ValueError):
        pipeline.assessment_snapshot(old_assessment["run_id"], True)
    assert (
        pipeline.catalog.one("SELECT * FROM feature_product_projections WHERE product_id='p'")
        is None
    )
