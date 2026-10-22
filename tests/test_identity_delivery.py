"""Reviewed identity through stored evidence, historical queries and Streamlit."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_pipeline_integration import video_observation

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_video_offer_merge_and_split_preserve_reviewed_ownership_at_each_cutoff(
    pipeline, monkeypatch
):
    captured = now() - timedelta(hours=1)
    videos = [video_observation(identifier, captured, 100) for identifier in ("one", "two")]
    for video in videos:
        video["attributes"]["creative_fingerprint"] = "reviewed-same-creative"
    offer = {
        **video_observation("offer", captured, 100),
        "entity_type": "offer",
        "metrics": {"price_minor": {"lower": 1999, "upper": 1999, "unit": "EUR_minor"}},
        "attributes": {"currency": "EUR", "seller_id": "fixture-seller", "title": "Desk lamp"},
    }
    pipeline.import_records([*videos, offer], synthetic=True)
    pipeline.create_product("original", "Desk lamp", synthetic=True)
    pipeline.create_product("similar", "Similar lamp", synthetic=True)
    pipeline.set_family("original", "lighting")
    pipeline.set_family("similar", "lighting")
    before = now()
    assert all(not row["evidence_refs"] for row in pipeline.assess(before, True)["rows"])
    records, _ = pipeline.history.observations(now(), synthetic=True)
    refs = sorted(row.observation_id for row in records)
    keys = sorted(row.entity_key for row in records)
    decisions = [
        pipeline.review_link("original", key, confidence=0.98, evidence_refs=refs) for key in keys
    ]
    initial_cutoff = max(row["available_at"] for row in decisions)
    assert pipeline.evidence("original", before, True) == []
    initial = pipeline.evidence("original", initial_cutoff, True)
    assert {row["entity_type"] for row in initial} == {"video", "offer"}
    assert len(initial) == 3
    assert all(row["confidence"] == 0.98 and row["evidence_refs"] == refs for row in decisions)
    assert all(row["method"] == "manual" for row in decisions)

    rejected = pipeline.review_link("similar", keys[0], status="rejected", evidence_refs=refs)
    assert len(pipeline.evidence("original", rejected["available_at"], True)) == 3
    assert pipeline.evidence("similar", rejected["available_at"], True) == []
    links_before_suggestions = pipeline.links(now(), True)
    suggestions = pipeline.suggest_links("desk lamp", True)
    assert suggestions and all(row["requires_review"] for row in suggestions)
    assert suggestions == pipeline.suggest_links("desk lamp", True)
    assert pipeline.links(now(), True) == links_before_suggestions

    merged = pipeline.identity.reassign_entities(keys, "similar", evidence_refs=refs)
    assert len({row["available_at"] for row in merged}) == 1
    merge_cutoff = max(row["available_at"] for row in merged)
    assert len(pipeline.evidence("similar", merge_cutoff, True)) == 3
    assert pipeline.evidence("original", merge_cutoff, True) == []
    split_keys = [key for key in keys if key.endswith(":one") or ":offer:" in key]
    split = pipeline.identity.reassign_entities(split_keys, "original", evidence_refs=refs)
    split_cutoff = max(row["available_at"] for row in split)
    assert {row["entity_id"] for row in pipeline.evidence("original", split_cutoff, True)} == {
        "one",
        "offer",
    }
    assert {row["entity_id"] for row in pipeline.evidence("similar", split_cutoff, True)} == {"two"}
    assert pipeline.evidence("original", initial_cutoff, True) == initial
    assert len(pipeline.evidence("similar", merge_cutoff, True)) == 3
    assert {row["family_id"] for row in pipeline.identity.families(split_cutoff, True)} == {
        "lighting"
    }
    assert {row["product_id"] for row in pipeline.products(True)} == {"original", "similar"}
    assert all(
        row["attributes"]["creative_fingerprint"] == "reviewed-same-creative"
        for row in initial
        if row["entity_type"] == "video"
    )

    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    assert not app.exception
    assert app.selectbox(key="evidence_product").value == "original"
    shown = next(
        value
        for element in app.json
        if isinstance(value := json.loads(element.value), list)
        and value
        and "observation_id" in value[0]
    )
    assert {row["entity_id"] for row in shown} == {"one", "offer"}
    with Pipeline(pipeline.settings) as restarted:
        assert restarted.evidence("original", initial_cutoff, True) == initial
        assert len(restarted.evidence("similar", merge_cutoff, True)) == 3
