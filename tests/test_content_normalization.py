"""Full-text normalization reuse preserves public themes, offsets and source guards."""

import hashlib
from collections import Counter

import pytest
from test_transcript_insights import import_selected, selected

from sm_trending_products.content_insights import DEFAULT_TAXONOMY, ContentInsights
from sm_trending_products.content_insights import service as content_service
from sm_trending_products.contracts import digest, now


@pytest.mark.parametrize(
    ("text", "language", "expected"),
    [
        (
            "Straße : LÉGER, facile à utiliser. Peut gagner du temps ?",
            "fr",
            "24a0b71d11284db0e83a52f10b1edde03250ff43c33d919aa097f4623474ae00",
        ),
        (
            "ＳＡＶＥ time. It is not expensive. Could it be portable?",
            "en",
            "b0f5ba1c120ed7e2bb20ba88f1afeb7eec8a305321c9c680658e929714cc6841",
        ),
        (
            "Une plume bleue.",
            "fr",
            "386fc0d44b5fa4c5d0d69b140b0a13f0422b21b153e3757d2e778213915111aa",
        ),
        (
            "gagner du temps",
            "de",
            "ffd1b5ef2876102cd4ebca2688cf41424be777a1e5abf9f5bb6e8a61a52f3326",
        ),
        (
            "save time",
            None,
            "ffd1b5ef2876102cd4ebca2688cf41424be777a1e5abf9f5bb6e8a61a52f3326",
        ),
        (
            "save time. " * 25 + "tail one",
            "en",
            "85a3f407c23819683196139fed3b88dcf7273e9d1934da25cb241f17fe641a81",
        ),
    ],
)
def test_public_classifier_matches_pre_optimization_payload(text, language, expected):
    # Whole-payload goldens from immutable 3f43c9d, including coverage and offsets.
    result = content_service.classify(text, language, DEFAULT_TAXONOMY)
    assert digest(result) == expected
    assert all(
        mention["quote"] == text[mention["start"] : mention["end"]]
        for mention in result["mentions"]
    )


def test_aggregate_bounds_full_text_normalization_work(pipeline, monkeypatch):
    service, value = selected(pipeline)
    inputs = [
        ("Straße : LÉGER, facile à utiliser.", "fr"),
        ("save time. " * 25 + "different full tail", "en"),
        ("A small blue object without a theme.", "en"),
        ("Unbekannte Sprache: gagner du temps.", "de"),
        ("A text without language metadata.", None),
    ]
    for text, language in inputs:
        import_selected(service, value.model_copy(update={"text": text, "language": language}))
    folded = Counter()
    full_texts = {text for text, _ in inputs}
    original = content_service._fold_with_offsets

    def count_full_text(text):
        if text in full_texts:
            folded[text] += 1
        return original(text)

    monkeypatch.setattr(content_service, "_fold_with_offsets", count_full_text)
    result = ContentInsights(pipeline.catalog, pipeline.provenance).aggregate("p", synthetic=True)
    assert result["distinct_text_count"] == len(inputs)
    assert result["unsupported_text_count"] == 2
    assert result["truncated_text_count"] == 1
    # Work ceiling, not a requirement to retain a particular helper implementation.
    assert folded <= Counter({text: 1 for text, _ in inputs})
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_mention_limit_does_not_truncate_normalized_script_tail(pipeline):
    prefix = "save time. " * 25
    texts = [
        prefix + "x" * (16_000 - len(prefix) - len(tail)) + tail for tail in (" alpha", " beta")
    ]
    transcripts = []
    for index, text in enumerate((texts[0], texts[0], texts[1])):
        service, value = selected(pipeline, entity=f"tail-{index}")
        transcripts.append(
            import_selected(service, value.model_copy(update={"text": text, "language": "en"}))
        )
    report = ContentInsights(pipeline.catalog, pipeline.provenance).aggregate("p", synthetic=True)
    assert report["distinct_text_count"] == report["truncated_text_count"] == 3
    assert report["normalized_script_count"] == 2
    assert len(report["text_similarity_groups"]) == 1
    similar = report["text_similarity_groups"][0]
    expected_folded = " ".join(
        (prefix + "x" * (16_000 - len(prefix) - len(" alpha")) + " alpha").replace(".", "").split()
    )
    assert similar["normalized_text_sha256"] == hashlib.sha256(expected_folded.encode()).hexdigest()
    assert len(similar["content_keys"]) == 2
    assert {row["transcript_id"] for row in report["texts"]} == set(transcripts)
    assert all(len(row["mentions"]) == 24 for row in report["texts"])
    assert all(row["theme_coverage"]["price"] == "not_scanned" for row in report["texts"])


def test_unsupported_public_classification_still_skips_normalization(monkeypatch):
    def no_fold(text):
        raise AssertionError("Standalone unsupported classification needs no normalization")

    monkeypatch.setattr(content_service, "_fold_with_offsets", no_fold)
    result = content_service.classify("gagner du temps", "de", DEFAULT_TAXONOMY)
    assert result["status"] == "unsupported_language"


def test_aggregate_withdrawal_prevents_reused_fold_from_exposing_text(pipeline, monkeypatch):
    service, value = selected(pipeline)
    import_selected(service, value)
    cutoff = now()
    content = ContentInsights(pipeline.catalog, pipeline.provenance)
    before = content.aggregate("p", synthetic=True, as_of=cutoff)
    assert before["distinct_text_count"] == 1
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)

    def no_fold(text):
        raise AssertionError("Withdrawn text must not reach normalization")

    monkeypatch.setattr(content_service, "_fold_with_offsets", no_fold)
    after = content.aggregate("p", synthetic=True, as_of=cutoff)
    assert after["status"] == "not_available"
    assert after["texts"] == after["text_similarity_groups"] == []
    assert after["aggregate_id"] != before["aggregate_id"]
