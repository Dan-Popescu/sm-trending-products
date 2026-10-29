"""Native, guarded associations between supplied text and measured attention."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta

import pytest
from test_evaluation_integration import record
from test_text_hydration import count_reads, individual_page_guards

from sm_trending_products.catalog import InvalidationPending
from sm_trending_products.content_insights import DEFAULT_TAXONOMY
from sm_trending_products.content_progression import ContentProgression
from sm_trending_products.contracts import canonical, digest, now
from sm_trending_products.enrichment import TranscriptContext, TranscriptInput, TranscriptInsights
from sm_trending_products.trend_insights import TrendInsights

pytestmark = pytest.mark.integration


def seed(
    pipeline,
    *,
    texts=None,
    languages=None,
    kinds=None,
    creators=True,
    early=True,
    jitter=False,
    distribution="public",
    roles=None,
    contexts=None,
):
    service = TranscriptInsights(pipeline.catalog, pipeline.provenance)
    service.initialize()
    pipeline.create_product("patterns", "Patterns", synthetic=True)
    base = now() + timedelta(days=2)
    texts = texts or ["It can save time."] * 2 + ["A small blue item."] * 2
    values, datasets = [], []
    for index, text in enumerate(texts):
        rows = []
        for hour in (0, 6, 12):
            at = base + timedelta(hours=hour, seconds=10 if jitter and index and hour < 12 else 0)
            row = record(str(index), at, 100 + hour * (index + 1))
            row["published_at"] = (base - timedelta(hours=2)).isoformat()
            row["attributes"] = {"distribution_context": distribution}
            if creators:
                row["attributes"]["creator_id"] = f"c{index}"
            if roles is not None:
                row["attributes"]["creator_role"] = roles[index]
            rows.append(row)
        dataset = pipeline.import_records(rows, synthetic=True)[0]
        pipeline.review_link("patterns", f"fixture_import:video:{index}")
        value = TranscriptInput(
            product_id="patterns",
            entity_key=f"fixture_import:video:{index}",
            dataset_id=dataset,
            observation_id=rows[0]["observation_id"],
            synthetic=True,
            text=text,
            source_kind=kinds[index] if kinds else "human_transcript",
            language=languages[index] if languages else "en",
            permission_reference="synthetic:authorized",
            context=contexts[index]
            if contexts is not None
            else None
            if early
            else TranscriptContext(reference="later-import", known_at=base + timedelta(hours=13)),
        )
        service.import_transcript(
            value, as_of=rows[0]["observed_at"] if early else base + timedelta(hours=13)
        )
        values.append(value)
        datasets.append(dataset)
    return service, values, datasets, base + timedelta(hours=12)


def describe(pipeline, cutoff, **kwargs):
    return ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance).describe(
        "patterns", cutoff, True, window_hours=12, **kwargs
    )


def theme(report, theme_id="time"):
    return next(item for item in report["themes"] if item["theme_id"] == theme_id)


def role_contexts(role):
    return [
        TranscriptContext(
            reference=f"reviewed-creator-{index}",
            known_at=now(),
            creator_id=f"c{index}",
            creator_role=role,
        )
        for index in range(4)
    ]


def assert_complete_role_texts(report, values):
    assert report["version"] == "quoted-content-progression-v1"
    assert report["distinct_text_count"] == len(report["texts"]) == 4
    for text in report["texts"]:
        original = next(value.text for value in values if value.entity_key == text["entity_key"])
        assert all(m["quote"] == original[m["start"] : m["end"]] for m in text["mentions"])
    assert report["report_id"] == digest(
        {key: value for key, value in report.items() if key != "report_id"}
    )


def add_unknown_creator_video(pipeline, service, cutoff, text, *, corrected_short_interval=False):
    start = cutoff - timedelta(hours=12)
    points = (
        ((0, 100), (10, 1), (12, 160))
        if corrected_short_interval
        else ((0, 100), (6, 130), (12, 160))
    )
    rows = []
    for hour, count in points:
        row = record("unknown-fifth", start + timedelta(hours=hour), count)
        row["published_at"] = (start - timedelta(hours=2)).isoformat()
        row["attributes"] = {"distribution_context": "public"}
        rows.append(row)
    dataset = pipeline.import_records(rows, synthetic=True)[0]
    pipeline.review_link("patterns", "fixture_import:video:unknown-fifth")
    value = TranscriptInput(
        product_id="patterns",
        entity_key="fixture_import:video:unknown-fifth",
        dataset_id=dataset,
        observation_id=rows[0]["observation_id"],
        synthetic=True,
        text=text,
        source_kind="human_transcript",
        language="en",
        permission_reference="synthetic:authorized",
    )
    service.import_transcript(value, as_of=start)
    return value


@pytest.mark.parametrize(
    "text,classification,short",
    [
        ("Not save time.", "negated_or_ambiguous", False),
        ("Could it save time?", "negated_or_ambiguous", False),
        ("save time. " * 25, "incomplete_scan", False),
        ("Not save time.", "negated_or_ambiguous", True),
    ],
)
def test_excluded_unknown_creator_does_not_block_known_arms_or_contaminate_measurement_gates(
    pipeline, text, classification, short
):
    service, values, _, cutoff = seed(pipeline)
    before = describe(pipeline, cutoff)
    extra = add_unknown_creator_video(
        pipeline, service, cutoff, text, corrected_short_interval=short
    )
    result = describe(pipeline, cutoff)
    group = theme(result)["strata"][0]
    original = theme(before)["strata"][0]
    assert group["status"] == "observed_association" and group["reasons"] == []
    assert group["present"] == original["present"] and group["absent"] == original["absent"]
    assert group["present"]["video_count"] == group["absent"]["video_count"] == 2
    assert (
        group["present"]["observed_growth_lower"] == 36
        and group["absent"]["observed_growth_lower"] == 84
    )
    for key in (
        "difference_in_mean_growth_lower",
        "difference_in_mean_growth_upper",
        "difference_in_mean_rate_lower",
        "difference_in_mean_rate_upper",
    ):
        assert group[key] == original[key]
    assert group["excluded_classification_counts"] == {classification: 1}
    assert theme(result)["classification_counts"][classification] == 1
    assert (
        result["eligible_video_count"]
        == len(result["outcomes"])
        == result["distinct_text_count"]
        == 5
    )
    assert result["input_observation_count"] == 15
    fifth = next(row for row in result["outcomes"] if row["entity_key"] == extra.entity_key)
    assert fifth["creator_key"] is None
    assert fifth["observed_growth_lower"] == (159 if short else 60)
    if short:
        assert fifth["coverage_fraction"] < 0.5 and fifth["measured_hours"] == pytest.approx(2)
        assert fifth["counter_correction_count"] == 1
    assert [row for row in result["outcomes"] if row["entity_key"] != extra.entity_key] == before[
        "outcomes"
    ]
    assert [row for row in result["texts"] if row["entity_key"] != extra.entity_key] == before[
        "texts"
    ]
    texts = {value.entity_key: value.text for value in [*values, extra]}
    for row in result["texts"]:
        assert all(
            mention["quote"] == texts[row["entity_key"]][mention["start"] : mention["end"]]
            for mention in row["mentions"]
        )


@pytest.mark.parametrize(
    "text,arm", [("It can save time.", "present"), ("A small blue item.", "absent")]
)
def test_usable_unknown_creator_still_blocks_comparison(pipeline, text, arm):
    service, _, _, cutoff = seed(pipeline)
    add_unknown_creator_video(pipeline, service, cutoff, text)
    result = describe(pipeline, cutoff)
    group = theme(result)["strata"][0]
    assert group["status"] == "unavailable" and group["reasons"] == ["unknown_creator_identity"]
    assert group[arm]["video_count"] == 3 and group[arm]["known_creator_count"] == 2
    assert group[arm]["unknown_creator_video_count"] == 1
    assert group["excluded_classification_counts"] == {}
    assert group["difference_in_mean_rate_lower"] is group["difference_in_mean_rate_upper"] is None
    assert result["eligible_video_count"] == 5 and result["distinct_text_count"] == 5


def test_unknown_attested_role_does_not_pool_known_sellers_with_buyers(pipeline):
    _, values, _, cutoff = seed(
        pipeline, roles=["seller", "seller", "buyer", "buyer"], contexts=role_contexts("unknown")
    )
    result = describe(pipeline, cutoff)
    assert_complete_role_texts(result, values)
    strata = {group["creator_role"]: group for group in theme(result)["strata"]}
    assert set(strata) == {"seller", "buyer"}
    assert result["comparison_status"] == "unavailable" and result["eligible_video_count"] == 4
    assert strata["seller"]["present"]["video_count"] == 2
    assert strata["seller"]["absent"]["video_count"] == 0
    assert strata["seller"]["present"]["observed_growth_lower"] == 36
    assert strata["buyer"]["present"]["video_count"] == 0
    assert strata["buyer"]["absent"]["video_count"] == 2
    assert strata["buyer"]["absent"]["observed_growth_lower"] == 84
    assert all(
        group["status"] == "unavailable" and group["difference_in_mean_rate_lower"] is None
        for group in strata.values()
    )
    assert {outcome["creator_role"] for outcome in result["outcomes"]} == {"seller", "buyer"}
    assert describe(pipeline, cutoff)["report_id"] == result["report_id"]


@pytest.mark.parametrize(
    "video_role,text_role,expected",
    [
        ("seller", "unknown", "seller"),
        ("seller", "seller", "seller"),
        ("unknown", "buyer", "buyer"),
        ("unknown", "unknown", "unknown"),
    ],
)
def test_compatible_known_and_unknown_roles_keep_measured_facts(
    pipeline, video_role, text_role, expected
):
    _, values, _, cutoff = seed(pipeline, roles=[video_role] * 4, contexts=role_contexts(text_role))
    result = describe(pipeline, cutoff)
    assert_complete_role_texts(result, values)
    assert result["exclusion_counts"] == {}
    assert {outcome["creator_role"] for outcome in result["outcomes"]} == {expected}
    group = theme(result)["strata"][0]
    assert group["creator_role"] == expected and group["status"] == "observed_association"
    assert group["present"]["observed_growth_lower"] == 36
    assert group["absent"]["observed_growth_lower"] == 84
    assert group["difference_in_mean_growth_lower"] == -24


def test_conflicting_known_roles_are_explicit_exclusions_without_losing_quotes(pipeline):
    _, values, _, cutoff = seed(pipeline, roles=["seller"] * 4, contexts=role_contexts("buyer"))
    result = describe(pipeline, cutoff)
    assert_complete_role_texts(result, values)
    assert result["eligible_video_count"] == 0 and result["outcomes"] == []
    assert result["comparison_status"] == "unavailable"
    assert result["exclusion_counts"] == {"conflicting_creator_role": 4}
    assert all(
        item["reason"] == "conflicting_creator_role"
        and item["video_creator_role"] == "seller"
        and item["attested_creator_role"] == "buyer"
        for item in result["exclusions"]
    )
    assert {item["transcript_id"] for item in result["exclusions"]} == {
        text["transcript_id"] for text in result["texts"]
    }


def test_role_context_revision_changes_report_identity_without_changing_default_trend(pipeline):
    service, values, _, cutoff = seed(
        pipeline, roles=["seller"] * 4, contexts=role_contexts("unknown")
    )
    trend = TrendInsights(pipeline.catalog, pipeline.history).describe("patterns", cutoff, True)
    before = describe(pipeline, cutoff)
    revised = values[0].model_copy(
        update={
            "context": TranscriptContext(
                reference="corrected-creator-role",
                known_at=cutoff,
                creator_id="c0",
                creator_role="buyer",
            )
        }
    )
    imported = service.import_transcript(revised, as_of=cutoff)
    result = describe(pipeline, cutoff)
    assert_complete_role_texts(result, values)
    assert result["report_id"] != before["report_id"]
    assert result["eligible_video_count"] == 3
    assert result["exclusion_counts"] == {"conflicting_creator_role": 1}
    text = next(text for text in result["texts"] if text["entity_key"] == values[0].entity_key)
    previous = next(text for text in before["texts"] if text["entity_key"] == values[0].entity_key)
    assert text["transcript_id"] == imported["transcript_id"] != previous["transcript_id"]
    assert text["content_key"] == previous["content_key"] and text["creator_role"] == "buyer"
    assert (
        service.import_transcript(revised, as_of=cutoff)["transcript_id"]
        == imported["transcript_id"]
    )
    assert describe(pipeline, cutoff)["report_id"] == result["report_id"]
    assert (
        TrendInsights(pipeline.catalog, pipeline.history).describe("patterns", cutoff, True)
        == trend
    )


def test_comparable_groups_use_complete_kernel_population_once_and_keep_all_themes(
    pipeline, monkeypatch
):
    _, values, _, cutoff = seed(pipeline)
    reads = []
    read = pipeline.history.read

    def counted(*args, **kwargs):
        reads.append(kwargs)
        return read(*args, **kwargs)

    monkeypatch.setattr(pipeline.history, "read", counted)
    result = describe(pipeline, cutoff)
    assert len(reads) == 1
    assert len(result["themes"]) == len(DEFAULT_TAXONOMY.themes)
    assert result["eligible_video_count"] == 4
    group = theme(result)["strata"][0]
    assert group["status"] == "observed_association"
    assert group["timing_scope"] == "text_known_by_interval_start"
    assert group["present"]["video_count"] == group["absent"]["video_count"] == 2
    assert group["present"]["known_creator_count"] == group["absent"]["known_creator_count"] == 2
    assert group["present"]["observed_growth_lower"] == 36
    assert group["absent"]["observed_growth_lower"] == 84
    assert group["present"]["measured_video_hours"] == 24
    assert group["difference_in_mean_growth_lower"] == -24
    assert result["causal_effect"] is result["predictive_confidence"] is None
    assert result["independent_people"] == "not_established"
    assert not result.get("winner") and not group.get("p_value")
    for text in result["texts"]:
        original = next(v.text for v in values if v.entity_key == text["entity_key"])
        assert all(m["quote"] == original[m["start"] : m["end"]] for m in text["mentions"])
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []
    assert describe(pipeline, cutoff)["report_id"] == result["report_id"]


def test_absence_requires_complete_supported_scan_and_excludes_negated_ambiguous(pipeline):
    _, _, _, cutoff = seed(
        pipeline,
        texts=[
            "save time",
            "not save time",
            "Could it save time?",
            "expensive",
            "save time. " * 25,
        ],
        languages=["en", "en", "en", "de", "en"],
    )
    result = theme(describe(pipeline, cutoff))
    assert result["classification_counts"] == {
        "present": 1,
        "absent": 0,
        "negated_or_ambiguous": 2,
        "unsupported_language": 1,
        "incomplete_scan": 1,
    }
    assert all(s["status"] == "unavailable" for s in result["strata"])


@pytest.mark.parametrize(
    "distribution",
    [
        None,
        "unknown",
        "mixed",
        "unspecified",
        "not_available",
        " UNKNOWN ",
        "Not Available",
        "unknown_distribution",
        "unavailable",
        "not available",
        "n/a",
        " None ",
        "NULL",
        "not_applicable",
    ],
)
def test_unknown_or_mixed_distribution_keeps_facts_without_comparable_contrasts(
    pipeline, distribution
):
    _, _, _, cutoff = seed(pipeline, distribution=distribution)
    result = describe(pipeline, cutoff)
    assert result["eligible_video_count"] == 4
    assert result["comparison_status"] == "unavailable"
    for group in theme(result)["strata"]:
        assert group["status"] == "unavailable"
        assert "unknown_distribution_context" in group["reasons"]
        assert group["present"]["video_count"] == group["absent"]["video_count"] == 2
        assert group["present"]["observed_growth_lower"] == 36
        assert group["absent"]["observed_growth_lower"] == 84
        assert all(
            group[key] is None
            for key in (
                "difference_in_mean_growth_lower",
                "difference_in_mean_growth_upper",
                "difference_in_mean_rate_lower",
                "difference_in_mean_rate_upper",
            )
        )


def test_kinds_unknown_creators_and_age_are_never_pooled_as_controls(pipeline):
    _, _, _, cutoff = seed(
        pipeline, kinds=["human_transcript"] * 2 + ["description"] * 2, creators=False
    )
    result = theme(describe(pipeline, cutoff))
    assert len(result["strata"]) == 2
    assert {s["source_kind"] for s in result["strata"]} == {"description", "human_transcript"}
    assert all(s["difference_in_mean_growth_lower"] is None for s in result["strata"])
    assert all("unknown_creator_identity" in s["reasons"] for s in result["strata"])


def test_late_text_scope_is_descriptive_and_future_text_never_leaks(pipeline):
    _, _, _, cutoff = seed(pipeline, early=False)
    assert describe(pipeline, cutoff)["texts"] == []
    result = describe(pipeline, cutoff + timedelta(hours=1))
    strata = theme(result)["strata"]
    assert strata and all(s["timing_scope"] == "text_imported_after_interval_start" for s in strata)
    assert result["predictive_confidence"] is None


def test_context_changes_revisions_future_metrics_and_refresh_do_not_inflate_videos(pipeline):
    service, values, _, cutoff = seed(pipeline)
    before = describe(pipeline, cutoff)
    row = record("0", cutoff, 99999, available_at=cutoff + timedelta(hours=1))
    row["published_at"] = (cutoff - timedelta(hours=14)).isoformat()
    row["attributes"] = {"creator_id": "c0", "distribution_context": "paid"}
    pipeline.import_records([row], synthetic=True)
    assert describe(pipeline, cutoff)["report_id"] == before["report_id"]
    result = describe(pipeline, cutoff + timedelta(hours=1))
    assert result["exclusion_counts"]["changed_video_context"] == 1
    assert theme(result)["strata"][0]["status"] == "unavailable"
    for _ in range(3):
        service.import_transcript(values[1], as_of=cutoff)
    assert describe(pipeline, cutoff)["eligible_video_count"] == 4
    context = TranscriptContext(reference="review:context", known_at=cutoff, creator_id="other")
    service.import_transcript(values[1].model_copy(update={"context": context}), as_of=cutoff)
    result = describe(pipeline, cutoff)
    assert result["distinct_text_count"] == 4
    assert result["exclusion_counts"]["conflicting_creator_identity"] == 1


def test_measurement_units_period_duration_and_source_context_remain_separate(pipeline):
    _, _, _, cutoff = seed(pipeline)
    for index in (2, 3):
        for hour in (0, 6, 12):
            row = record(str(index), cutoff - timedelta(hours=12 - hour), 100 + hour)
            row["observation_id"] = f"zz-unit-{index}-{hour}"
            row["available_at"] = cutoff.isoformat()
            row["published_at"] = (cutoff - timedelta(hours=14)).isoformat()
            row["metrics"]["view_count"]["unit"] = "EUR"
            row["attributes"] = {"creator_id": f"c{index}", "distribution_context": "public"}
            pipeline.import_records([row], synthetic=True)
    result = describe(pipeline, cutoff)
    assert result["eligible_video_count"] == 2
    assert result["exclusion_counts"]["no_comparable_count_interval"] == 2
    assert theme(result)["strata"][0]["difference_in_mean_growth_lower"] is None


def test_namespace_ownership_retirement_and_missing_module_are_guarded_without_ddl(pipeline):
    baseline = pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version")
    with pytest.raises(ValueError):
        describe(pipeline, now())
    assert (
        pipeline.catalog.one("SELECT to_regclass('enrichment_transcripts') AS present")["present"]
        is None
    )
    _, values, datasets, cutoff = seed(pipeline)
    service = ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance)
    with pytest.raises(ValueError):
        service.describe("patterns", cutoff, False)
    pipeline.create_product("other", "Other", synthetic=True)
    pipeline.identity.reassign_entities([values[0].entity_key], "other", confidence=0.5)
    assert describe(pipeline, cutoff)["eligible_video_count"] == 3
    pipeline.catalog.invalidate_dataset(datasets[1], "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    result = describe(pipeline, cutoff)
    assert len(result["texts"]) == 2
    assert values[1].text not in str(
        [t for t in result["texts"] if t["entity_key"] == values[1].entity_key]
    )
    assert pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version") == baseline


def test_bounds_refuse_partial_candidate_population_and_default_trend_ids_stay_exact(pipeline):
    _, _, _, cutoff = seed(pipeline)
    old = TrendInsights(pipeline.catalog, pipeline.history).describe("patterns", cutoff, True)
    enriched = TrendInsights(pipeline.catalog, pipeline.history).describe(
        "patterns", cutoff, True, include_video_intervals=True
    )
    assert enriched["video_timelines"]
    assert {k: v for k, v in enriched.items() if k not in {"video_timelines", "report_id"}} == {
        k: v for k, v in old.items() if k != "report_id"
    }
    with pytest.raises(ValueError, match="population"):
        describe(pipeline, cutoff, max_texts=2)
    for kwargs in (
        {"max_rows": True},
        {"max_texts": 201},
        {"max_strata": 0},
        {"window_hours": True},
    ):
        with pytest.raises(ValueError):
            ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance).describe(
                "patterns", cutoff, True, **kwargs
            )


def test_small_capture_jitter_retains_comparable_groups_without_inventing_coverage(pipeline):
    _, _, _, cutoff = seed(pipeline, jitter=True)
    report = describe(pipeline, cutoff)
    comparison = theme(report)["strata"][0]
    assert comparison["status"] == "observed_association"
    assert report["comparability_policy"]["calibrated_confidence_thresholds"] is False
    assert len({o["measured_start"] for o in report["outcomes"]}) == 2
    assert min(o["coverage_fraction"] for o in report["outcomes"]) < 1


def test_conflicting_text_versions_are_excluded_without_choosing_a_statistical_winner(pipeline):
    service, values, _, cutoff = seed(pipeline)
    service.import_transcript(
        values[0].model_copy(update={"text": "A corrected blue item."}), as_of=cutoff
    )
    report = describe(pipeline, cutoff)
    assert report["distinct_text_count"] == 5
    assert report["eligible_video_count"] == 3
    assert report["exclusion_counts"]["multiple_text_versions_for_video"] == 1
    detail = next(
        e for e in report["exclusions"] if e["reason"] == "multiple_text_versions_for_video"
    )
    assert len(detail["transcript_ids"]) == 2
    assert theme(report)["strata"][0]["status"] == "unavailable"


def test_row_and_output_budgets_refuse_the_report(pipeline, monkeypatch):
    from sm_trending_products.history import RowBudgetExceeded

    _, _, _, cutoff = seed(pipeline)
    with pytest.raises(RowBudgetExceeded):
        describe(pipeline, cutoff, max_rows=2)
    monkeypatch.setattr("sm_trending_products.content_progression.service.MAX_REPORT_BYTES", 10)
    with pytest.raises(ValueError, match="output budget"):
        describe(pipeline, cutoff)


def test_source_retirement_between_composed_reads_never_returns_earlier_quotes(
    pipeline, monkeypatch
):
    _, _, datasets, cutoff = seed(pipeline)
    service = ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance)
    read = service.trends.describe

    def withdrawn(*args, **kwargs):
        result = read(*args, **kwargs)
        pipeline.catalog.invalidate_dataset(datasets[0], "withdrawn_during_composition")
        pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
        return result

    monkeypatch.setattr(service.trends, "describe", withdrawn)
    with pytest.raises(ValueError, match="changed during"):
        service.describe("patterns", cutoff, True, window_hours=12)


def test_final_eligibility_ignores_analysis_states_with_exact_report_bytes(pipeline, monkeypatch):
    texts, _, _, cutoff = seed(pipeline)
    service = ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance)
    expected = canonical(service.describe("patterns", cutoff, True, window_hours=12))
    rows = pipeline.catalog.all("SELECT transcript_id FROM enrichment_transcripts")
    for index, row in enumerate(rows):
        first = texts.analyze(
            f"initial-{index}", "patterns", row["transcript_id"], synthetic=True, as_of=cutoff
        )
        reused = texts.analyze(
            f"reused-{index}", "patterns", row["transcript_id"], synthetic=True, as_of=cutoff
        )
        assert first["analysis_id"] == reused["analysis_id"]
    for state in ("completed", "running", "uncertain"):
        pipeline.catalog.execute("UPDATE enrichment_analyses SET state=%s", (state,))
        with monkeypatch.context() as legacy:
            legacy.setattr(service.texts, "eligible_text_ids", service.texts.eligible_texts)
            with count_reads(monkeypatch, pipeline) as baseline:
                previous = canonical(service.describe("patterns", cutoff, True, window_hours=12))
        with count_reads(monkeypatch, pipeline) as treatment:
            actual = canonical(service.describe("patterns", cutoff, True, window_hours=12))
        assert actual == previous == expected
        assert baseline["analysis_sql"] == 8 and treatment["analysis_sql"] == 0
        assert treatment["sql"] < baseline["sql"]
        print({"state": state, "baseline": dict(baseline), "treatment": dict(treatment)})


@pytest.mark.parametrize("text_count", [1, 8])
def test_progression_batches_both_metadata_guards_with_exact_report_bytes(
    pipeline, monkeypatch, text_count
):
    _, _, _, cutoff = seed(pipeline, texts=["It can save time."] * text_count)
    service = ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance)
    with monkeypatch.context() as individual:
        individual.setattr(TranscriptInsights, "_guard_transcripts", individual_page_guards)
        with count_reads(monkeypatch, pipeline) as baseline:
            expected = canonical(service.describe("patterns", cutoff, True, window_hours=12))
    with count_reads(monkeypatch, pipeline) as treatment:
        actual = canonical(service.describe("patterns", cutoff, True, window_hours=12))
    assert actual == expected
    assert treatment["scope"] == text_count
    assert treatment["sql"] <= baseline["sql"]
    if text_count > 1:
        assert treatment["sql"] < baseline["sql"]
    print({"texts": text_count, "baseline": dict(baseline), "treatment": dict(treatment)})


def test_final_text_and_metric_checks_share_retention_fence_without_holding_history(
    pipeline, monkeypatch
):
    _, values, datasets, cutoff = seed(pipeline)
    replacement = []
    for hour in (0, 6, 12):
        row = record("0", cutoff - timedelta(hours=12 - hour), 100 + hour, available_at=cutoff)
        row["observation_id"] = f"zz-replacement-metric-{hour}"
        row["published_at"] = (cutoff - timedelta(hours=14)).isoformat()
        row["attributes"] = {"distribution_context": "public", "creator_id": "c0"}
        replacement.append(row)
    pipeline.import_records(replacement, synthetic=True)
    service = ContentProgression(pipeline.catalog, pipeline.history, pipeline.provenance)
    eligible, read = service.texts.eligible_text_ids, pipeline.history.read
    checked = False
    active = peak = 0
    pool_connection = pipeline.catalog.pool.connection

    @contextmanager
    def connection(*args, **kwargs):
        nonlocal active, peak
        with pool_connection(*args, **kwargs) as value:
            active += 1
            peak = max(peak, active)
            try:
                yield value
            finally:
                active -= 1

    def history(*args, **kwargs):
        # Scoped history protects admission through payload hydration on its own
        # connection. Progression still does not hold an extra report-wide fence.
        with pipeline.catalog.transaction() as other:
            acquired = other.execute(
                "SELECT pg_try_advisory_xact_lock(492371205) AS acquired"
            ).fetchone()["acquired"]
            assert acquired is (kwargs.get("connection") is None)
        return read(*args, **kwargs)

    with ThreadPoolExecutor(max_workers=1) as executor:

        def final_text_check(*args, **kwargs):
            nonlocal checked
            result = eligible(*args, **kwargs)
            checked = True
            removal = executor.submit(
                pipeline.catalog.invalidate_dataset, datasets[0], "final-check-race"
            )
            with pytest.raises(InvalidationPending):
                removal.result(timeout=5)
            assert (
                pipeline.catalog.one(
                    "SELECT state FROM datasets WHERE dataset_id=%s", (datasets[0],)
                )["state"]
                == "committed"
            )
            return result

        monkeypatch.setattr(pipeline.catalog.pool, "connection", connection)
        monkeypatch.setattr(pipeline.history, "read", history)
        monkeypatch.setattr(service.texts, "eligible_text_ids", final_text_check)
        result = service.describe("patterns", cutoff, True, window_hours=12)
    assert checked and peak <= 4
    refs = next(
        r["evidence_refs"]
        for r in result["video_evidence"]
        if r["entity_key"] == values[0].entity_key
    )
    assert refs and all(ref.startswith("zz-replacement-metric-") for ref in refs)
    assert any(t["entity_key"] == values[0].entity_key and t["mentions"] for t in result["texts"])
    monkeypatch.setattr(service.texts, "eligible_text_ids", eligible)
    pipeline.catalog.invalidate_dataset(datasets[0], "after-final-checks")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    refreshed = service.describe("patterns", cutoff, True, window_hours=12)
    assert all(t["entity_key"] != values[0].entity_key for t in refreshed["texts"])
