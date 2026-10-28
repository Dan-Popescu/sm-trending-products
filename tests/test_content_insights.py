"""Native supplied-text aggregation, identity compatibility and evidence guards."""

import hashlib
from datetime import timedelta

import pytest
from pydantic import ValidationError
from test_transcript_insights import (
    ControlledAdapter,
    analyze,
    external_budget,
    import_selected,
    refresh,
    reservation,
    selected,
)

from sm_trending_products.content_insights import (
    DEFAULT_TAXONOMY,
    ContentInsights,
    ContentTaxonomy,
    ThemeRule,
)
from sm_trending_products.contracts import digest, now
from sm_trending_products.enrichment import EnrichmentDenied, TranscriptContext


def aggregate(pipeline, **kwargs):
    return ContentInsights(pipeline.catalog, pipeline.provenance).aggregate(
        "p", synthetic=True, **kwargs
    )


def theme(result, theme_id):
    return next(row for row in result["themes"] if row["theme_id"] == theme_id)


def test_distinct_texts_quotes_roles_and_similarity_are_not_independence_or_verified_claims(
    pipeline,
):
    service, value = selected(pipeline)
    context = TranscriptContext(
        reference="reviewed-source-metadata:creator-1",
        known_at=now(),
        creator_id="creator-1",
        creator_role="seller",
        variant="blue",
    )
    value = value.model_copy(update={"context": context})
    original = import_selected(service, value)
    for request in ("one", "two", "three"):
        analyze(service, original, request)
    assert import_selected(service, refresh(pipeline, value)) == original
    _, second = selected(pipeline, entity="second")
    import_selected(service, second)  # Same script; creator/role unknown.
    result = aggregate(pipeline)
    assert result["distinct_text_count"] == 2
    assert result["normalized_script_count"] == 1
    assert theme(result, "time")["distinct_text_count"] == 2
    assert theme(result, "time")["known_creator_count"] == 1
    assert theme(result, "time")["by_role"] == {"seller": 1, "unknown": 1}
    assert result["text_similarity_groups"][0]["meaning"] == "normalized_text_similarity_only"
    assert result["same_creative"] == "not_verified"
    assert result["independent_people"] == "not_established"
    assert result["buyer_intent"] == "not_established"
    assert result["claims"] == "unverified_text_mentions"
    for row in result["texts"]:
        assert all(
            mention["quote"] == value.text[mention["start"] : mention["end"]]
            for mention in row["mentions"]
        )
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_negation_ambiguity_accents_and_source_kind_are_explicit(pipeline):
    service, value = selected(pipeline)
    text = "This is not expensive. Could it save time? Easy to use."
    value = value.model_copy(update={"text": text, "language": "en", "source_kind": "description"})
    import_selected(service, value)
    result = aggregate(pipeline)
    assert theme(result, "price")["by_status"] == {"candidate": 0, "negated": 1, "ambiguous": 0}
    assert theme(result, "time")["by_status"]["ambiguous"] == 1
    assert theme(result, "ease")["by_source_kind"] == {"description": 1}
    assert all(m["quote"] == text[m["start"] : m["end"]] for m in result["texts"][0]["mentions"])
    accented = value.model_copy(update={"text": "LÉGER, comparé à un autre.", "language": "fr"})
    import_selected(service, accented)
    result = aggregate(pipeline)
    assert theme(result, "portability")["mentions"][0]["quote"] == "LÉGER"
    assert theme(result, "comparison")["mentions"][0]["quote"] == "comparé à"


def test_unavailable_unsupported_and_supported_no_mention_have_different_meanings(pipeline):
    service, value = selected(pipeline)
    assert aggregate(pipeline)["status"] == "not_available"
    unsupported = value.model_copy(update={"text": "expensive", "language": "de"})
    import_selected(service, unsupported)
    result = aggregate(pipeline)
    assert result["texts"][0]["status"] == "unsupported_language"
    assert theme(result, "price")["status"] == "unsupported_language"
    import_selected(
        service, value.model_copy(update={"text": "A small blue item.", "language": "en"})
    )
    result = aggregate(pipeline)
    assert "no_mention" in {row["status"] for row in result["texts"]}
    assert theme(result, "price")["status"] == "no_mention_in_supported_texts"


def test_taxonomy_merge_split_versions_cutoff_and_model_bypass(pipeline):
    service, value = selected(pipeline)
    import_selected(service, value)
    cutoff = now()
    merged = ThemeRule(
        theme_id="effort",
        label="Reviewed manual merge",
        phrases=DEFAULT_TAXONOMY.themes[0].phrases + DEFAULT_TAXONOMY.themes[1].phrases,
    )
    taxonomy = ContentTaxonomy(
        version="manual-merge-v2",
        reviewed_at=cutoff,
        review_reference="operator-review:2",
        themes=(merged,),
    )
    result = aggregate(pipeline, taxonomy=taxonomy, as_of=cutoff)
    assert result["taxonomy_version"] == "manual-merge-v2"
    assert {row["theme_id"] for row in result["themes"]} == {"effort"}
    assert (
        aggregate(pipeline, taxonomy=taxonomy, as_of=cutoff)["aggregate_id"]
        == result["aggregate_id"]
    )
    with pytest.raises(ValueError, match="not reviewed"):
        aggregate(pipeline, taxonomy=taxonomy, as_of=cutoff - timedelta(microseconds=1))
    unsafe = taxonomy.model_copy(update={"themes": (merged,) * 17})
    with pytest.raises(ValidationError):
        aggregate(pipeline, taxonomy=unsafe)


def test_metadata_page_is_bounded_advances_and_never_loads_text_or_reserves_work(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    ids = {
        import_selected(service, value.model_copy(update={"text": f"Text {index}"}))
        for index in range(3)
    }

    def no_text(*args, **kwargs):
        raise AssertionError("Metadata preview must not read transcript content")

    monkeypatch.setattr(pipeline.provenance, "selected_records", no_text)
    first = service.eligible_texts("p", synthetic=True, limit=2)
    second = service.eligible_texts("p", synthetic=True, limit=2, after=first["next_cursor"])
    assert {row["transcript_id"] for row in first["items"] + second["items"]} == ids
    assert first["scanned"] == 2 and second["scanned"] == 1
    assert second["next_cursor"] is None
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []
    for limit in (True, 0, 201):
        with pytest.raises(ValueError):
            service.eligible_texts("p", synthetic=True, limit=limit)


def test_bounded_reader_and_aggregation_recheck_observation_owner_namespace_and_retirement(
    pipeline,
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    cutoff = now()
    assert service.read_texts("p", [transcript], synthetic=True)["items"]
    assert service.read_texts("p", [transcript], synthetic=False)["items"] == []
    with pytest.raises(ValueError):
        service.read_texts("p", [transcript] * 201, synthetic=True)
    pipeline.catalog.execute(
        "UPDATE observation_deliveries SET available_at=clock_timestamp()+interval '1 day' "
        "WHERE observation_id=%s",
        (value.observation_id,),
    )
    assert service.read_texts("p", [transcript], synthetic=True)["items"] == []
    assert aggregate(pipeline)["status"] == "not_available"
    pipeline.catalog.execute(
        "UPDATE observation_deliveries SET available_at=%s WHERE observation_id=%s",
        (
            value.model_dump().get("available_at", cutoff - timedelta(minutes=1)),
            value.observation_id,
        ),
    )
    pipeline.create_product("other", "Other", synthetic=True)
    pipeline.identity.reassign_entities([value.entity_key], "other", confidence=0.5)
    assert aggregate(pipeline)["status"] == "not_available"
    assert service.read_texts("p", [transcript], synthetic=True)["items"] == []
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert aggregate(pipeline, as_of=cutoff)["status"] == "not_available"


def test_legacy_ids_descriptors_and_canonical_paid_identity_survive_optional_context(pipeline):
    service, value = selected(pipeline)
    assert "context" not in value.model_dump()
    original = import_selected(service, value)
    content_key = digest(
        {
            "entity_key": value.entity_key,
            "synthetic": True,
            "source_kind": value.source_kind,
            "language": value.language,
            "text_sha256": hashlib.sha256(value.text.encode()).hexdigest(),
        }
    )
    assert original == digest(
        {
            "content_key": content_key,
            "dataset": value.dataset_id,
            "observation": value.observation_id,
        }
    )
    external_budget(service)
    adapter = ControlledAdapter()
    first = analyze(service, original, adapter=adapter)
    context = TranscriptContext(
        reference="operator-attestation:creator",
        known_at=now(),
        creator_id="creator",
        creator_role="buyer",
    )
    later = import_selected(service, value.model_copy(update={"context": context}))
    assert later != original
    second = analyze(service, later, "context", adapter=adapter)
    assert first["analysis_id"] == second["analysis_id"]
    assert adapter.calls == 1
    assert aggregate(pipeline)["distinct_text_count"] == 1
    assert aggregate(pipeline)["texts"][0]["context_status"] == "operator_attested"


def test_future_context_and_future_transcripts_never_leak_into_earlier_page(pipeline):
    service, value = selected(pipeline)
    before = now()
    context = TranscriptContext(
        reference="later-review", known_at=before + timedelta(hours=1), creator_id="later"
    )
    future = value.model_copy(update={"context": context})
    with pytest.raises(EnrichmentDenied, match="context_not_available"):
        service.import_transcript(future)
    transcript = service.import_transcript(future, as_of=before + timedelta(hours=2))[
        "transcript_id"
    ]
    assert service.eligible_texts("p", synthetic=True, as_of=before)["items"] == []
    assert service.read_texts("p", [transcript], synthetic=True, as_of=before)["items"] == []
    assert aggregate(pipeline, as_of=before)["status"] == "not_available"


def test_additive_metadata_upgrade_preserves_legacy_identity_without_backfilling_on_read(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    before = pipeline.catalog.one(
        "SELECT transcript_id,content_key,descriptor,available_at FROM enrichment_transcripts"
    )
    # Reproduce the actual revision-1 table shape and checksum history around a
    # real imported graph, then exercise the packaged additive migration.
    pipeline.catalog.execute("DROP INDEX enrichment_transcript_entity_page")
    pipeline.catalog.execute("ALTER TABLE enrichment_transcripts DROP COLUMN metadata")
    pipeline.catalog.execute("ALTER TABLE enrichment_transcripts DROP COLUMN context_sha256")
    pipeline.catalog.execute(
        "DELETE FROM application_module_migrations WHERE module='transcript_insights' AND version=2"
    )
    assert service.initialize()["version"] == 2
    after = pipeline.catalog.one("SELECT * FROM enrichment_transcripts")
    assert {key: after[key] for key in before} == before
    assert after["metadata"] == {} and after["context_sha256"] == ""
    assert service.eligible_texts("p", synthetic=True)["items"][0]["metadata"] == {}
    assert aggregate(pipeline)["distinct_text_count"] == 1
    assert import_selected(service, value) == transcript
    assert pipeline.catalog.one("SELECT metadata FROM enrichment_transcripts")["metadata"] == {}


def test_mention_cap_preserves_unknown_coverage_for_later_themes(pipeline):
    service, value = selected(pipeline)
    import_selected(
        service,
        value.model_copy(
            update={
                "text": ("save time. " * 25) + "This is expensive.",
                "language": "en",
            }
        ),
    )
    result = aggregate(pipeline)
    assert len(result["texts"][0]["mentions"]) == 24
    assert result["texts"][0]["truncated"] is True
    assert result["truncated_text_count"] == 1
    assert "text_mention_limit_reached" in result["gaps"]
    assert theme(result, "time")["unscanned_or_partial_text_count"] == 1
    price = theme(result, "price")
    assert price["status"] == "not_fully_scanned"
    assert price["fully_scanned_text_count"] == 0
    assert price["unscanned_or_partial_text_count"] == 1
    assert result["texts"][0]["theme_coverage"]["price"] == "not_scanned"


def test_language_coverage_is_per_theme_for_reviewed_mixed_language_taxonomy(pipeline):
    service, value = selected(pipeline)
    import_selected(service, value)
    taxonomy = ContentTaxonomy(
        version="reviewed-mixed-v1",
        reviewed_at=now(),
        review_reference="operator-review:mixed",
        themes=(
            ThemeRule.model_validate(
                {
                    "theme_id": "time-en-only",
                    "label": "Time",
                    "phrases": [{"language": "en", "text": "save time", "category": "benefit"}],
                }
            ),
            ThemeRule.model_validate(
                {
                    "theme_id": "portability-fr",
                    "label": "Portability",
                    "phrases": [{"language": "fr", "text": "portable", "category": "benefit"}],
                }
            ),
        ),
    )
    result = aggregate(pipeline, taxonomy=taxonomy)
    time = theme(result, "time-en-only")
    assert time["status"] == "unsupported_language"
    assert time["supported_text_count"] == time["fully_scanned_text_count"] == 0
    assert time["unsupported_text_count"] == 1
    portability = theme(result, "portability-fr")
    assert portability["status"] == "no_mention_in_supported_texts"
    assert portability["supported_text_count"] == portability["fully_scanned_text_count"] == 1
    assert portability["unsupported_text_count"] == 0
    assert "theme_language_coverage_incomplete" in result["gaps"]
