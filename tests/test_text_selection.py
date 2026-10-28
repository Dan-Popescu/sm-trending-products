"""Native metadata plans, local application, budgets and forged-plan rejection."""

from copy import deepcopy
from datetime import timedelta

import pytest
from pydantic import ValidationError
from test_transcript_insights import analyze, import_selected, refresh, reservation, selected

from sm_trending_products.contracts import now
from sm_trending_products.enrichment import EnrichmentBudget, EnrichmentDenied, TranscriptContext
from sm_trending_products.enrichment.extractor import LocalTranscriptAdapter
from sm_trending_products.text_selection import (
    TextSelection,
    TextSelectionConfig,
    TextSelectionPlan,
)


def selector(pipeline):
    return TextSelection(pipeline.catalog, pipeline.provenance)


def test_preview_is_metadata_only_stable_bounded_and_diverse(pipeline, monkeypatch):
    service, first = selected(pipeline)
    for index, language in enumerate(("en", "fr", "en")):
        context = TranscriptContext(
            reference=f"review:{index}",
            known_at=now(),
            creator_id=f"creator-{index}",
            variant=f"variant-{index}",
            known_theme_ids=("time",) if index == 1 else (),
        )
        import_selected(
            service,
            first.model_copy(
                update={"text": f"Supplied {index}", "language": language, "context": context}
            ),
        )
    pipeline.create_product("q", "Second shortlisted product", synthetic=True)
    _, second = selected(pipeline, entity="q-video")
    pipeline.review_link("q", second.entity_key)
    import_selected(service, second.model_copy(update={"product_id": "q"}))

    def no_text(*args, **kwargs):
        raise AssertionError("Selection may inspect metadata only")

    monkeypatch.setattr(pipeline.provenance, "selected_records", no_text)
    config = TextSelectionConfig(max_selected=2, desired_theme_ids=("time",))
    cutoff = now()
    plan = selector(pipeline).preview(["p", "q"], config=config, synthetic=True, as_of=cutoff)
    assert {item["product_id"] for item in plan["items"]} == {"p", "q"}
    assert "new_known_theme" in plan["items"][0]["reasons"]
    assert (
        selector(pipeline).preview(["p", "q"], config=config, synthetic=True, as_of=cutoff) == plan
    )
    assert plan["selected_text_characters"] <= config.max_total_text_characters
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []


def test_explicit_local_application_and_repeated_plan_share_canonical_reservation(pipeline):
    service, value = selected(pipeline)
    import_selected(service, value)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    first = choice.execute(plan, request_prefix="batch")
    again = choice.execute(TextSelectionPlan.model_validate(plan), request_prefix="batch")
    alias = choice.execute(plan, request_prefix="other-batch")
    assert first == again
    assert alias["results"][0]["analysis_id"] == first["results"][0]["analysis_id"]
    assert first["provider"] == "local-rules" and first["state"] == "completed"
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    current = choice.preview(["p"], synthetic=True)
    assert current["items"] == []
    assert current["decisions"][0]["reason"] == "analysis_completed"


def test_selection_and_authoritative_daily_character_budgets_both_apply(pipeline, monkeypatch):
    service, value = selected(pipeline)
    service.configure_budget("local-rules", EnrichmentBudget(daily_requests=1), synthetic=True)
    for index in range(3):
        import_selected(service, value.model_copy(update={"text": f"Text {index}"}))
    calls = []
    local_analyze = LocalTranscriptAdapter.analyze

    def counted(adapter, document):
        calls.append(document["text"])
        return local_analyze(adapter, document)

    monkeypatch.setattr(LocalTranscriptAdapter, "analyze", counted)
    choice = selector(pipeline)
    small = choice.preview(
        ["p"], config=TextSelectionConfig(max_total_text_characters=6), synthetic=True
    )
    assert len(small["items"]) == 1 and small["selected_text_characters"] == 6
    assert {row["reason"] for row in small["decisions"]} == {"text_character_budget"}
    plan = choice.preview(["p"], synthetic=True)
    result = choice.execute(plan, request_prefix="bounded")
    assert [row["state"] for row in result["results"]].count("completed") == 1
    assert [row["state"] for row in result["results"]].count("denied") == 2
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    assert reservation(pipeline, "local-rules")["cost_reserved"] == 0
    assert len(calls) == 1


@pytest.mark.parametrize("change", ["namespace", "cutoff", "items", "budget", "identity"])
def test_forged_or_bypassed_plan_cannot_widen_scope_or_budget(pipeline, change):
    service, value = selected(pipeline)
    import_selected(service, value)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    forged = deepcopy(plan)
    if change == "namespace":
        forged["synthetic"] = False
    elif change == "cutoff":
        forged["as_of"] = (now() + timedelta(days=1)).isoformat()
    elif change == "items":
        forged["items"][0]["transcript_id"] = "a" * 64
    elif change == "budget":
        forged["config"]["max_selected"] = 21
    else:
        forged["plan_id"] = "a" * 64
    bypassed = TextSelectionPlan.model_construct(**forged)
    with pytest.raises(ValueError):
        choice.execute(bypassed, request_prefix="forged")
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_source_withdrawal_and_current_ownership_block_saved_plan_application(pipeline):
    service, value = selected(pipeline)
    import_selected(service, value)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    pipeline.create_product("q", "New owner", synthetic=True)
    pipeline.review_link("q", value.entity_key)
    result = choice.execute(plan, request_prefix="owner-changed")
    assert result["results"][0]["state"] == "denied"
    assert "not_owned" in result["results"][0]["reason"]
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    with pytest.raises(ValueError, match="no longer eligible"):
        choice.execute(plan, request_prefix="withdrawn")
    assert choice.preview(["q"], synthetic=True)["items"] == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_candidate_pages_and_unsupported_legacy_metadata_are_explained(pipeline):
    service, value = selected(pipeline)
    for index in range(4):
        import_selected(
            service, value.model_copy(update={"text": f"Text {index}", "language": "de"})
        )
    choice = selector(pipeline)
    config = TextSelectionConfig(max_candidates=2, per_product_candidates=2)
    first = choice.preview(["p"], config=config, synthetic=True)
    cursor = first["pages"][0]["next_cursor"]
    second = choice.preview(["p"], config=config, synthetic=True, after={"p": cursor})
    assert first["items"] == second["items"] == []
    assert first["pages"][0]["scanned"] == second["pages"][0]["scanned"] == 2
    assert {row["reason"] for row in first["decisions"]} == {"unsupported_or_unknown_language"}
    pipeline.catalog.execute("UPDATE enrichment_transcripts SET metadata='{}'")
    legacy = choice.preview(["p"], synthetic=True)
    assert {row["reason"] for row in legacy["decisions"]} == {"not_reviewed_metadata"}
    with pytest.raises(ValidationError):
        choice.preview(
            ["p"], config=config.model_copy(update={"max_candidates": 201}), synthetic=True
        )
    with pytest.raises(ValueError):
        choice.preview(["p"] * 11, synthetic=True)


@pytest.mark.parametrize("state", ["running", "uncertain"])
def test_unresolved_local_results_are_not_requeued_or_reset(pipeline, state):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    analyze(service, transcript)
    pipeline.catalog.execute("UPDATE enrichment_analyses SET state=%s", (state,))
    plan = selector(pipeline).preview(["p"], synthetic=True)
    assert plan["items"] == []
    assert plan["decisions"][0]["reason"] == "analysis_unresolved"
    assert selector(pipeline).execute(plan, request_prefix="blocked")["results"] == []
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_plan_replay_preserves_unresolved_state_until_actual_completion_cutoff(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    result = analyze(service, transcript)
    row = pipeline.catalog.one("SELECT created_at,finished_at FROM enrichment_analyses")
    cutoff = row["created_at"] + (row["finished_at"] - row["created_at"]) / 2
    choice = selector(pipeline)
    before = choice.preview(["p"], synthetic=True, as_of=cutoff)
    assert before["decisions"][0]["reason"] == "analysis_unresolved"
    pipeline.catalog.execute(
        "UPDATE enrichment_analyses SET state='uncertain' WHERE analysis_id=%s",
        (result["analysis_id"],),
    )
    assert choice.preview(["p"], synthetic=True, as_of=cutoff) == before


def test_free_selection_leaves_unknown_paid_outcome_and_its_barrier_untouched(pipeline):
    from test_transcript_insights import ControlledAdapter, external_budget

    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)

    def timeout():
        raise TimeoutError()

    adapter = ControlledAdapter(callback=timeout)
    with pytest.raises(EnrichmentDenied):
        analyze(service, transcript, adapter=adapter)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    assert choice.execute(plan, request_prefix="free-local")["state"] == "completed"
    assert adapter.calls == 1
    assert analyze(service, transcript, "paid-alias", adapter=adapter)["state"] == "uncertain"
    assert adapter.calls == 1
    assert reservation(pipeline)["requests_reserved"] == 1


def test_live_source_policy_and_future_cache_are_authoritative_for_metadata(pipeline):
    service, value = selected(pipeline, synthetic=False)
    with pytest.raises(EnrichmentDenied, match="policy_unverified"):
        import_selected(service, value)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        (value.entity_key.split(":", 1)[0],),
    )
    transcript = import_selected(service, value)
    cutoff = now()
    analyze(service, transcript, product="live-p", synthetic=False)
    choice = selector(pipeline)
    prior = choice.preview(["live-p"], synthetic=False, as_of=cutoff)
    assert len(prior["items"]) == 1
    assert choice.preview(["live-p"], synthetic=False)["items"] == []
    with pytest.raises(EnrichmentDenied, match="future_live_cutoff"):
        choice.preview(["live-p"], synthetic=False, as_of=now() + timedelta(hours=1))
    pipeline.catalog.execute(
        "UPDATE source_policies SET verified=false WHERE source=%s",
        (value.entity_key.split(":", 1)[0],),
    )
    assert choice.preview(["live-p"], synthetic=False)["items"] == []
    with pytest.raises(ValueError, match="no longer eligible"):
        choice.execute(prior, request_prefix="withdrawn-real-policy")


def test_completed_retired_generation_does_not_block_fresh_eligible_canonical_text(pipeline):
    service, value = selected(pipeline)
    original = import_selected(service, value)
    old_result = analyze(service, original)
    replacement = refresh(pipeline, value)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    later = import_selected(service, replacement)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    assert [item["transcript_id"] for item in plan["items"]] == [later]
    result = choice.execute(plan, request_prefix="fresh-generation")
    assert result["state"] == "completed"
    assert result["results"][0]["analysis_id"] != old_result["analysis_id"]
    assert choice.execute(plan, request_prefix="fresh-generation") == result
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 2


@pytest.mark.parametrize("state", ["running", "uncertain"])
def test_retirement_keeps_unresolved_generation_as_an_analysis_barrier(pipeline, state):
    service, value = selected(pipeline)
    original = import_selected(service, value)
    analyze(service, original)
    pipeline.catalog.execute("UPDATE enrichment_analyses SET state=%s", (state,))
    replacement = refresh(pipeline, value)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    later = import_selected(service, replacement)
    choice = selector(pipeline)
    plan = choice.preview(["p"], synthetic=True)
    assert plan["items"] == []
    assert any(
        row["transcript_id"] == later and row["reason"] == "analysis_unresolved"
        for row in plan["decisions"]
    )
    assert choice.execute(plan, request_prefix="still-blocked")["results"] == []
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
