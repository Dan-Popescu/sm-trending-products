"""Brief support distinguishes actual candidate quotes from other text availability."""

import pytest
from test_transcript_insights import import_selected, selected

from sm_trending_products.contracts import now


@pytest.mark.parametrize(
    ("text", "language", "candidate", "negated", "ambiguous"),
    [
        ("Un objet bleu.", "fr", 0, 0, 0),
        ("Un objet bleu.", "de", 0, 0, 0),
        ("It does not save time.", "en", 0, 1, 0),
        ("Could it save time?", "en", 0, 0, 1),
        ("It can save time.", "en", 1, 0, 0),
    ],
)
def test_native_brief_support_requires_actual_candidate_mentions(
    pipeline, text, language, candidate, negated, ambiguous
):
    texts, value = selected(pipeline)
    import_selected(texts, value.model_copy(update={"text": text, "language": language}))
    report = pipeline.decision_brief().describe("p", "FR", now(), True)
    quoted = report["sections"]["themes"]["report"]
    assert quoted["distinct_text_count"] == 1
    mentions = quoted["texts"][0]["mentions"]
    assert sum(m["status"] == "candidate" for m in mentions) == candidate
    assert sum(m["status"] == "negated" for m in mentions) == negated
    assert sum(m["status"] == "ambiguous" for m in mentions) == ambiguous
    support = [row for row in report["support"] if row["dimension"] == "themes"]
    assert bool(support) is bool(candidate)
    if candidate:
        assert support[0]["code"] == "supplied_text_mentions_available_not_buyer_demand"
        assert support[0]["candidate_mention_count"] == candidate
        assert support[0]["distinct_text_count"] == 1
    else:
        available = next(
            row
            for row in report["missing"]
            if row["code"] == "supplied_texts_without_candidate_theme_mentions"
        )
        assert available["distinct_text_count"] == 1
    for status, count in (("negated", negated), ("ambiguous", ambiguous)):
        notes = [
            row
            for row in report["missing"]
            if row["code"] == f"{status}_theme_mentions_require_context_review"
        ]
        assert bool(notes) is bool(count)
        if count:
            assert notes[0]["mention_count"] == count
    assert all(m["quote"] == text[m["start"] : m["end"]] for m in mentions)
    assert report["status"] == "watch"
    assert report["status_policy"]["launch_authorized"] is False
    assert report["status_policy"]["predictive_probability"] is None


def test_native_mixed_quotes_keep_each_meaning_and_french_review_context(pipeline):
    from sm_trending_products.decision_brief_presentation import present_brief, render_text

    texts, value = selected(pipeline)
    text = "It can save time. It does not save time. Could it save time?"
    value = value.model_copy(update={"text": text, "language": "en"})
    import_selected(texts, value)
    cutoff = now()
    report = pipeline.decision_brief().describe("p", "FR", cutoff, True)
    support = next(row for row in report["support"] if row["dimension"] == "themes")
    assert support["candidate_mention_count"] == support["distinct_text_count"] == 1
    for status in ("negated", "ambiguous"):
        note = next(
            row
            for row in report["missing"]
            if row["code"] == f"{status}_theme_mentions_require_context_review"
        )
        assert note["mention_count"] == note["distinct_text_count"] == 1
    french = render_text(present_brief(report)).split("Détails techniques", 1)[0]
    assert "mentions candidates citées : 1" in french
    assert "marquées comme niées" in french and "ambiguës" in french
    assert "ne prouvent ni le bénéfice décrit ni une demande" in french
    for _ in range(3):
        import_selected(texts, value)
    assert (
        pipeline.decision_brief().describe("p", "FR", cutoff, True)["report_id"]
        == report["report_id"]
    )
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    after = pipeline.decision_brief().describe("p", "FR", cutoff, True)
    assert not any(row["dimension"] == "themes" for row in after["support"])
    assert after["sections"]["themes"]["report"]["texts"] == []
