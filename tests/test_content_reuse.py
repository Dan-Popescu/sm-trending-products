"""Pure reuse preserves complete reports while source authority stays per read."""

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_transcript_insights import import_selected, selected

import sm_trending_products.content_insights.service as content
import sm_trending_products.decision_changes as changes_module
from sm_trending_products.content_insights import DEFAULT_TAXONOMY, ContentInsights
from sm_trending_products.content_insights._memo import _ClassificationMemo
from sm_trending_products.contracts import canonical, now
from sm_trending_products.decision_brief import DecisionBrief
from sm_trending_products.decision_changes import DecisionChanges
from sm_trending_products.enrichment import TranscriptContext


def supplied_page(text, language, cutoff):
    return {
        "items": [
            {
                "text": text,
                "language": language,
                "context": {},
                "transcript_id": "transcript",
                "content_key": "content",
                "entity_key": "fixture:video:one",
                "available_at": cutoff,
                "source_kind": "human_transcript",
            }
        ],
        "as_of": cutoff,
        "unavailable": [],
        "next_cursor": None,
        "scanned": 1,
    }


@pytest.mark.parametrize(
    ("text", "language"),
    [
        ("😀 LÉGER, compare\u0301 à. Gagner du temps ?", "fr"),
        ("Straße ﬁ. SAVE TIME. Not only expensive.", "en-US"),
        ("save time. " * 25 + "first full tail", "en"),
        ("save time. " * 25 + "different full tail", "en"),
        ("Unsupported LÉGER Straße", "de"),
        ("Unknown ﬁ language", None),
    ],
)
def test_complete_aggregate_bytes_offsets_and_mutable_results(text, language, monkeypatch):
    cutoff = now()
    page = supplied_page(text, language, cutoff)
    service = ContentInsights(None, None)
    service.texts = SimpleNamespace(read_text_page=lambda *args, **kwargs: page)
    expected = service.aggregate("p", as_of=cutoff)
    calls, original = [], content._classify_with_fold

    def classify(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(content, "_classify_with_fold", classify)
    with _ClassificationMemo() as memo:
        first = service.aggregate("p", as_of=cutoff, _memo=memo)
        assert canonical(first) == canonical(expected)
        first["texts"][0]["mentions"].clear()
        first["texts"][0]["theme_coverage"].clear()
        first["themes"][0]["mentions"].clear()
        repeated = service.aggregate("p", as_of=cutoff, _memo=memo)
        assert canonical(repeated) == canonical(expected)
        assert json.dumps(repeated, default=str) == json.dumps(expected, default=str)
        assert len(calls) == 1
        for mention in repeated["texts"][0]["mentions"]:
            assert mention["quote"] == text[mention["start"] : mention["end"]]
    assert memo.retained_bytes == 0 and not memo._entries


def test_exact_text_language_full_taxonomy_and_method_identities(monkeypatch):
    cutoff = now()
    page = supplied_page("save time", "en", cutoff)
    service = ContentInsights(None, None)
    service.texts = SimpleNamespace(read_text_page=lambda *args, **kwargs: page)
    calls, original = [], content._classify_with_fold

    def classify(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(content, "_classify_with_fold", classify)
    with _ClassificationMemo() as memo:

        def run(taxonomy=DEFAULT_TAXONOMY):
            actual = service.aggregate("p", as_of=cutoff, taxonomy=taxonomy, _memo=memo)
            assert actual == service.aggregate("p", as_of=cutoff, taxonomy=taxonomy)

        run()
        page["items"][0]["text"] = "SAVE TIME"
        run()
        page["items"][0]["language"] = "en-GB"
        run()
        # A same-version taxonomy with only a changed label/review is still a new identity.
        taxonomy = DEFAULT_TAXONOMY.model_copy(
            update={"review_reference": "another-review"}, deep=True
        )
        run(taxonomy)
        taxonomy.themes[0].__dict__["label"] = "Changed nested label"
        run(taxonomy)
        taxonomy.themes[0].phrases[0].__dict__["text"] = "different wording"
        run(taxonomy)
        monkeypatch.setattr(content, "METHOD", "another-classifier")
        run(taxonomy)
        monkeypatch.setattr(content, "_SCRIPT_METHOD", "another-script-method")
        run(taxonomy)
    assert len(calls) == 16  # Eight memo misses and eight independent controls.


def test_entry_and_byte_boundaries_decline_without_losing_facts_and_clear_on_failure():
    key = ("😀" * 100, None, "full-taxonomy", "method", "script-method")
    result = {"mentions": [{"quote": "😀"}], "theme_coverage": {"time": "complete"}}
    with _ClassificationMemo() as sizing:
        sizing.remember(key, result, "script-hash")
        exact = sizing.retained_bytes
    for maximum, admitted in ((exact - 1, False), (exact, True)):
        with _ClassificationMemo(max_entries=1, max_bytes=maximum) as memo:
            memo.remember(key, result, "script-hash")
            assert (memo.get(key) is not None) is admitted
            assert memo.retained_bytes <= maximum
            memo.remember(("new", *key[1:]), result, "other")
            assert (memo.get(("new", *key[1:])) is None) is admitted
    with pytest.raises(RuntimeError, match="release"):
        with _ClassificationMemo() as failed:
            failed.remember(key, result, "script-hash")
            assert failed.retained_bytes
            raise RuntimeError("release")
    assert failed.retained_bytes == 0 and not failed._entries


def test_native_whole_changes_parity_new_text_taxonomy_and_fresh_context(pipeline, monkeypatch):
    texts, value = selected(pipeline)
    import_selected(texts, value.model_copy(update={"text": "LÉGER. Gagner du temps."}))
    previous = now()
    current = previous + timedelta(hours=2)
    context = TranscriptContext(
        reference="later-context", known_at=previous + timedelta(hours=1), creator_id="later"
    )
    texts.import_transcript(
        value.model_copy(update={"text": "LÉGER. Gagner du temps.", "context": context}),
        as_of=current,
    )
    texts.import_transcript(
        value.model_copy(update={"text": "save time", "language": "en", "context": context}),
        as_of=current,
    )
    service = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance)
    real_memo, original_classify = _ClassificationMemo, content._classify_with_fold
    cases = [
        {},
        {
            "current_selection": {
                "content_taxonomy": DEFAULT_TAXONOMY.model_copy(
                    update={"review_reference": "same-version-reviewed-again"}
                )
            }
        },
    ]
    for options in cases:
        with monkeypatch.context() as control:
            control.setattr(changes_module, "_ClassificationMemo", lambda: real_memo(max_entries=0))
            expected = service.compare("p", "FR", previous, current, True, **options)
        calls = []

        def classify(*args, calls=calls):
            calls.append(args)
            return original_classify(*args)

        with monkeypatch.context() as treatment:
            treatment.setattr(content, "_classify_with_fold", classify)
            actual = service.compare("p", "FR", previous, current, True, **options)
        assert canonical(actual) == canonical(expected)
        assert len(calls) == (3 if options else 2)
    brief = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    before = brief.describe("p", "FR", previous, True)["sections"]["themes"]["report"]
    after = brief.describe("p", "FR", current, True)["sections"]["themes"]["report"]
    assert before["distinct_text_count"] == 1 and after["distinct_text_count"] == 2
    assert before["texts"][0]["creator_id"] is None
    assert all(row["creator_id"] == "later" for row in after["texts"])
    held = []
    original_describe = DecisionBrief.describe

    def fail_after_read(service, *args, **kwargs):
        original_describe(service, *args, **kwargs)
        held.append(service._content_memo)
        assert held[-1].retained_bytes
        raise RuntimeError("failed comparison")

    monkeypatch.setattr(DecisionBrief, "describe", fail_after_read)
    with pytest.raises(RuntimeError, match="failed comparison"):
        service.compare("p", "FR", previous, current, True)
    assert held[0].retained_bytes == 0 and not held[0]._entries


def test_native_current_owner_withdrawal_stops_hydration_and_reuse(pipeline, monkeypatch):
    texts, value = selected(pipeline)
    import_selected(texts, value)
    pipeline.create_product("other", "Other", synthetic=True)
    previous, current = now(), now() + timedelta(seconds=1)
    original_describe, original_classify = DecisionBrief.describe, content._classify_with_fold
    calls, memos = [], []

    def classify(*args):
        calls.append(args)
        return original_classify(*args)

    def describe(service, *args, **kwargs):
        if memos:
            pipeline.catalog.execute(
                "UPDATE product_links SET status='rejected' WHERE product_id='p'"
            )
        result = original_describe(service, *args, **kwargs)
        memos.append(service._content_memo)
        return result

    monkeypatch.setattr(DecisionBrief, "describe", describe)
    monkeypatch.setattr(content, "_classify_with_fold", classify)
    actual = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
        "p", "FR", previous, current, True
    )
    themes = next(row for row in actual["coverage"] if row["dimension"] == "themes")
    assert themes["quantities"]["previous"]["distinct_text_count"] == 1
    assert themes["quantities"]["current"]["distinct_text_count"] == 0
    assert len(calls) == 1 and memos[0] is memos[1]
    assert memos[0].retained_bytes == 0 and not memos[0]._entries
