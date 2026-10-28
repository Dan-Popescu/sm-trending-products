"""Selected text evidence, native PostgreSQL cost fencing and source lifecycle."""

import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from threading import Barrier, Event, Lock

import pytest
from pydantic import ValidationError

from sm_trending_products.contracts import Observation, now
from sm_trending_products.enrichment import (
    EnrichmentBudget,
    EnrichmentDenied,
    InsightOutput,
    ProviderSpec,
    TextSegment,
    TranscriptInput,
    TranscriptInsights,
)

TEXT = "Marre de perdre du temps ? Cela permet de gagner du temps. Lien dans la bio !"


def selected(pipeline, *, entity="v", synthetic=True, source="transcript_fixture"):
    product = "p" if synthetic else "live-p"
    pipeline.create_product(product, "Selected product", synthetic=synthetic)
    at = now() - timedelta(minutes=2)
    observation = Observation(
        observation_id=f"{source}-{entity}-initial",
        source=source,
        entity_type="video",
        entity_id=entity,
        observed_at=at,
        available_at=at,
        synthetic=synthetic,
    )
    dataset = pipeline.import_records([observation.model_dump(mode="json")], synthetic=synthetic)[0]
    pipeline.review_link(product, observation.entity_key)
    service = TranscriptInsights(pipeline.catalog, pipeline.provenance)
    service.initialize()
    return service, TranscriptInput(
        product_id=product,
        entity_key=observation.entity_key,
        dataset_id=dataset,
        observation_id=observation.observation_id,
        synthetic=synthetic,
        text=TEXT,
        source_kind="human_transcript",
        language="fr",
        permission_reference="operator-supplied-transcript-with-permission",
    )


def import_selected(service, value):
    return service.import_transcript(value)["transcript_id"]


def analyze(service, transcript, request="request", *, product="p", adapter=None, synthetic=True):
    return service.analyze(request, product, transcript, synthetic=synthetic, adapter=adapter)


def refresh(pipeline, value, *, observation="metric-refresh"):
    at = now()
    source, _kind, entity = value.entity_key.split(":", 2)
    record = Observation(
        observation_id=observation,
        source=source,
        entity_type="video",
        entity_id=entity,
        observed_at=at,
        available_at=at,
        synthetic=value.synthetic,
    )
    dataset = pipeline.import_records([record.model_dump(mode="json")], synthetic=value.synthetic)[
        0
    ]
    return value.model_copy(update={"dataset_id": dataset, "observation_id": observation})


class ControlledAdapter:
    def __init__(self, *, version="test-v1", cost="0.10", callback=None, enabled=True):
        self.spec = ProviderSpec(
            provider_id="controlled-fixture",
            version=version,
            enabled=enabled,
            reserved_cost=cost,
        )
        self.calls = 0
        self.lock = Lock()
        self.callback = callback

    def analyze(self, transcript):
        with self.lock:
            self.calls += 1
        if self.callback:
            self.callback()
        return InsightOutput(
            method="model", segments=(TextSegment(start=0, end=5, label="problem"),)
        )


def external_budget(service, *, requests=10, daily="1.00", maximum="0.10", **kwargs):
    return service.configure_budget(
        "controlled-fixture",
        EnrichmentBudget(
            daily_requests=requests,
            daily_cost_limit=daily,
            maximum_request_cost=maximum,
            allow_external=True,
            **kwargs,
        ),
        synthetic=True,
    )


def reservation(pipeline, provider="controlled-fixture", synthetic=True):
    return pipeline.catalog.one(
        "SELECT * FROM enrichment_daily_budgets WHERE provider_id=%s AND synthetic=%s",
        (provider, synthetic),
    )


def test_supplied_text_is_quoted_distinct_from_description_and_not_a_sales_prediction(pipeline):
    service, value = selected(pipeline)
    baseline = pipeline.catalog.all("SELECT version FROM schema_migrations ORDER BY version")
    transcript = import_selected(service, value)
    result = analyze(service, transcript)
    assert result["state"] == "completed"
    assert result["source_kind"] == "human_transcript"
    assert result["speech_evidence"] is True
    assert result["classification_status"] == "unvalidated_text_interpretation"
    assert result["effectiveness"] == "not_measured"
    assert {row["label"] for row in result["segments"]} >= {
        "hook",
        "problem",
        "benefit",
        "call_to_action",
    }
    assert all(row["quote"] == value.text[row["start"] : row["end"]] for row in result["segments"])
    description = import_selected(service, value.model_copy(update={"source_kind": "description"}))
    assert description != transcript
    assert analyze(service, description, "description")["speech_evidence"] is False
    assert result["reserved_cost"] == 0
    assert result["reservation_is_actual_bill"] is False
    assert (
        pipeline.catalog.all("SELECT version FROM schema_migrations ORDER BY version") == baseline
    )
    service.initialize()  # Explicit initialization is idempotent.
    for table in ("enrichment_transcripts", "enrichment_analyses", "enrichment_requests"):
        assert TEXT not in str(pipeline.catalog.all(f"SELECT * FROM {table}"))


def test_metric_refresh_and_canonical_text_reuse_one_versioned_result_and_reservation(pipeline):
    service, value = selected(pipeline)
    first = import_selected(service, value)
    result = analyze(service, first)
    newer = refresh(pipeline, value)
    reused = service.import_transcript(newer)
    assert reused["reused"] is True
    assert reused["transcript_id"] == first
    again = analyze(service, first, "refresh")
    assert again["analysis_id"] == result["analysis_id"]
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    # Only NFC, line ending and boundary whitespace normalization is applied.
    normalized = TranscriptInput.model_validate(
        {**value.model_dump(), "text": "  Cafe\u0301\r\ntexte  "}
    )
    assert normalized.text == "Café\ntexte"
    assert import_selected(service, normalized) == import_selected(
        service, normalized.model_copy(update={"text": "Café\ntexte"})
    )


def test_extractor_version_changes_are_explicit_new_work(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    first, second = ControlledAdapter(), ControlledAdapter(version="test-v2")
    a = analyze(service, transcript, "a", adapter=first)
    b = analyze(service, transcript, "b", adapter=second)
    assert a["analysis_id"] != b["analysis_id"]
    assert first.calls == second.calls == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.20")


def test_concurrent_first_imports_from_distinct_metric_captures_share_one_analysis(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    newer = refresh(pipeline, value)
    waiting = Barrier(2)
    publish = pipeline.provenance.records

    def together(records, synthetic, **kwargs):
        if "transcript" in records:
            waiting.wait(timeout=15)
        return publish(records, synthetic, **kwargs)

    monkeypatch.setattr(pipeline.provenance, "records", together)
    with ThreadPoolExecutor(max_workers=2) as pool:
        transcripts = list(pool.map(lambda item: import_selected(service, item), (value, newer)))
    assert transcripts[0] != transcripts[1]
    external_budget(service)
    adapter = ControlledAdapter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda item: analyze(service, item[1], f"r{item[0]}", adapter=adapter),
                enumerate(transcripts),
            )
        )
    assert results[0]["analysis_id"] == results[1]["analysis_id"]
    assert adapter.calls == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")


def test_simultaneous_aliases_reserve_and_call_provider_once_without_holding_sql_transaction(
    pipeline,
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    entered, release = Event(), Event()

    def block():
        entered.set()
        assert release.wait(timeout=15)

    adapter = ControlledAdapter(callback=block)
    with ThreadPoolExecutor(max_workers=5) as pool:
        first = pool.submit(analyze, service, transcript, "first", adapter=adapter)
        assert entered.wait(timeout=15)
        aliases = [
            pool.submit(analyze, service, transcript, f"alias-{i}", adapter=adapter)
            for i in range(4)
        ]
        try:
            for item in aliases:
                assert item.result(timeout=15)["state"] == "running"
            assert reservation(pipeline)["requests_reserved"] == 1
            assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")
            assert adapter.calls == 1
        finally:
            release.set()
        assert first.result(timeout=15)["state"] == "completed"
    for index in range(4):
        assert service.result(f"alias-{index}", "p", synthetic=True)["state"] == "completed"
    # The same idempotency key cannot be redirected to different work.
    other = import_selected(service, value.model_copy(update={"text": "Different text"}))
    with pytest.raises(EnrichmentDenied, match="request_identity_conflict"):
        analyze(service, other, "first", adapter=adapter)
    assert adapter.calls == 1


@pytest.mark.parametrize("budget_kind", ["requests", "cost"])
def test_competing_distinct_videos_cannot_overrun_daily_requests_or_cost(pipeline, budget_kind):
    service, value = selected(pipeline)
    values = [value, selected(pipeline, entity="other")[1]]
    transcripts = [import_selected(service, item) for item in values]
    external_budget(
        service,
        requests=1 if budget_kind == "requests" else 10,
        daily="1.00" if budget_kind == "requests" else "0.10",
    )
    adapter = ControlledAdapter()

    def attempt(index):
        try:
            return analyze(service, transcripts[index], f"r{index}", adapter=adapter)["state"]
        except EnrichmentDenied as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (0, 1)))
    assert sorted(results) == ["completed", "enrichment_daily_budget_exhausted"]
    assert adapter.calls == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")
    assert reservation(pipeline)["requests_reserved"] == 1


@pytest.mark.parametrize("failure", ["timeout", "invalid_output", "process_death"])
def test_unknown_remote_outcomes_keep_reservations_and_do_not_retry_after_restart(
    pipeline, failure
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)

    class ProcessDeath(BaseException):
        pass

    def fail():
        if failure == "process_death":
            raise ProcessDeath()
        if failure == "timeout":
            raise TimeoutError("private provider response")

    adapter = ControlledAdapter(callback=fail)
    if failure == "invalid_output":
        adapter.analyze = lambda _: {"method": "model", "segments": [{"start": 0, "end": 999}]}
    expected = ProcessDeath if failure == "process_death" else EnrichmentDenied
    with pytest.raises(expected):
        analyze(service, transcript, adapter=adapter)
    resumed = TranscriptInsights(pipeline.catalog, pipeline.provenance)
    result = analyze(resumed, transcript, adapter=adapter)
    assert result["state"] == ("running" if failure == "process_death" else "uncertain")
    assert "private provider response" not in str(
        pipeline.catalog.all("SELECT * FROM enrichment_analyses")
    )
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")
    assert reservation(pipeline)["requests_reserved"] == 1
    if failure != "invalid_output":
        assert adapter.calls == 1


@pytest.mark.parametrize(
    "denial", ["disabled", "unconfigured", "not_allowed", "unit", "text", "currency"]
)
def test_external_provider_requires_explicit_enabled_bounded_configuration_before_any_call(
    pipeline, denial
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    adapter = ControlledAdapter(
        enabled=denial != "disabled", cost="0.20" if denial == "unit" else "0.10"
    )
    if denial not in {"disabled", "unconfigured"}:
        budget = EnrichmentBudget(
            daily_cost_limit="1.00",
            maximum_request_cost="0.10",
            allow_external=denial != "not_allowed",
            max_text_characters=1 if denial == "text" else 16000,
            currency="USD" if denial == "currency" else "EUR",
        )
        service.configure_budget("controlled-fixture", budget, synthetic=True)
    with pytest.raises(EnrichmentDenied):
        analyze(service, transcript, adapter=adapter)
    assert adapter.calls == 0
    row = reservation(pipeline)
    assert row is None or (row["requests_reserved"] == 0 and row["cost_reserved"] == 0)


def test_local_budget_is_configurable_and_cannot_be_reset_by_retry(pipeline):
    service, value = selected(pipeline)
    service.configure_budget("local-rules", EnrichmentBudget(daily_requests=1), synthetic=True)
    transcript = import_selected(service, value)
    analyze(service, transcript)
    other = import_selected(service, value.model_copy(update={"text": "Other supplied words"}))
    with pytest.raises(EnrichmentDenied, match="daily_budget_exhausted"):
        analyze(service, other, "second")
    with pytest.raises(EnrichmentDenied, match="daily_budget_already_defined"):
        service.configure_budget(
            "local-rules", EnrichmentBudget(daily_requests=100), synthetic=True
        )
    assert analyze(service, transcript)["state"] == "completed"
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_equivalent_exact_decimal_configuration_is_an_idempotent_retry(pipeline):
    service, _value = selected(pipeline)
    external_budget(service, daily="1.00", maximum="0.10")
    external_budget(service, daily="1.0", maximum="0.1")
    assert reservation(pipeline)["cost_reserved"] == 0


def test_namespace_owner_observation_membership_and_availability_remain_authoritative(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    result = analyze(service, transcript)
    with pytest.raises(EnrichmentDenied):
        service.result("request", "p", synthetic=False)
    with pytest.raises(EnrichmentDenied):
        service.import_transcript(value.model_copy(update={"synthetic": False}))
    with pytest.raises(EnrichmentDenied, match="observation_unavailable"):
        service.import_transcript(value.model_copy(update={"observation_id": "not-delivered"}))
    with pytest.raises(EnrichmentDenied):
        service.import_transcript(value, as_of=now() - timedelta(days=1))
    # An output produced later must not leak into a historical replay.
    finished = pipeline.catalog.one(
        "SELECT finished_at FROM enrichment_analyses WHERE analysis_id=%s", (result["analysis_id"],)
    )["finished_at"]
    with pytest.raises(EnrichmentDenied, match="not_available_at_cutoff"):
        service.result("request", "p", synthetic=True, as_of=finished - timedelta(microseconds=1))
    pipeline.create_product("new-owner", "New owner", synthetic=True)
    pipeline.identity.reassign_entities([value.entity_key], "new-owner", confidence=0.5)
    # Low-confidence current ownership must not resurrect the former high-confidence owner.
    with pytest.raises(EnrichmentDenied, match="not_owned_at_cutoff"):
        service.result("request", "p", synthetic=True)
    with pytest.raises(EnrichmentDenied, match="not_owned_at_cutoff"):
        analyze(service, transcript, "new", product="new-owner")
    pipeline.review_link("new-owner", value.entity_key, confidence=1)
    reused = analyze(service, transcript, "new", product="new-owner")
    assert reused["analysis_id"] == result["analysis_id"]
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_source_retirement_withdraws_text_and_insights_without_hiding_raw_collection(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    result = analyze(service, transcript)
    replacement = refresh(pipeline, value)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    with pytest.raises(EnrichmentDenied):
        service.result("request", "p", synthetic=True)
    assert service.summaries("p", synthetic=True)["items"] == []
    nodes = pipeline.catalog.all(
        "SELECT state FROM derived_publications WHERE object_key IN ("
        "SELECT descriptor#>>'{root,key}' FROM enrichment_transcripts UNION "
        "SELECT descriptor#>>'{root,key}' FROM enrichment_analyses)"
    )
    assert {row["state"] for row in nodes} == {"invalidated"}
    assert (
        pipeline.catalog.one(
            "SELECT state FROM datasets WHERE dataset_id=%s", (replacement.dataset_id,)
        )["state"]
        == "committed"
    )
    # Explicit later authorized evidence can form a fresh lifecycle generation.
    later = import_selected(service, replacement)
    assert later != transcript
    assert analyze(service, later, "replacement")["analysis_id"] != result["analysis_id"]


def test_retirement_never_turns_unknown_paid_outcome_into_an_automatic_retry(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)

    def timeout():
        raise TimeoutError()

    adapter = ControlledAdapter(callback=timeout)
    with pytest.raises(EnrichmentDenied, match="outcome_uncertain"):
        analyze(service, transcript, adapter=adapter)
    replacement = refresh(pipeline, value)
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    later = import_selected(service, replacement)
    with pytest.raises(EnrichmentDenied):
        analyze(service, later, "replacement", adapter=adapter)
    assert adapter.calls == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")


def test_source_withdrawal_during_external_call_prevents_publishing_returned_quotes(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    adapter = ControlledAdapter(
        callback=lambda: pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    )
    with pytest.raises(EnrichmentDenied, match="outcome_uncertain"):
        analyze(service, transcript, adapter=adapter)
    assert pipeline.catalog.one("SELECT state,descriptor FROM enrichment_analyses") == {
        "state": "uncertain",
        "descriptor": None,
    }
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")


def test_live_import_needs_verified_unexpired_policy_but_does_not_grant_remote_capability(pipeline):
    service, value = selected(pipeline, synthetic=False)
    with pytest.raises(EnrichmentDenied, match="policy_unverified"):
        import_selected(service, value)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('transcript_fixture',1,true)"
    )
    transcript = import_selected(service, value)
    assert analyze(service, transcript, product="live-p", synthetic=False)["state"] == "completed"
    assert pipeline.catalog.all("SELECT * FROM source_capabilities") == []
    with pytest.raises(EnrichmentDenied, match="future_live_cutoff"):
        service.import_transcript(value, as_of=now() + timedelta(hours=1))
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (value.dataset_id,),
    )
    with pytest.raises(EnrichmentDenied, match="retention_expired"):
        service.result("request", "live-p", synthetic=False)


def test_selected_summaries_have_bounded_cursor_pages_without_full_transcript_quotes(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    for request in ("a", "b", "c"):
        analyze(service, transcript, request)
    first = service.summaries("p", synthetic=True, limit=2)
    assert [item["request_id"] for item in first["items"]] == ["a", "b"]
    assert first["next_cursor"] == "b"
    assert "segments" not in first["items"][0]
    last = service.summaries("p", synthetic=True, limit=2, after=first["next_cursor"])
    assert [item["request_id"] for item in last["items"]] == ["c"]
    assert last["next_cursor"] is None
    for limit in (True, 0, 201):
        with pytest.raises(ValueError):
            service.summaries("p", synthetic=True, limit=limit)


@pytest.mark.parametrize("cost", [True, 0.1, "NaN", "Infinity", "-1"])
def test_cost_contract_rejects_inexact_or_unbounded_money(cost):
    with pytest.raises(ValidationError):
        EnrichmentBudget(daily_cost_limit=cost)


@pytest.mark.parametrize(
    "text", [" ", "bad\x00text", "a" * 16001], ids=["empty", "control", "oversize"]
)
def test_import_contract_rejects_empty_control_or_oversized_text(text):
    with pytest.raises(ValidationError):
        TranscriptInput(
            product_id="p",
            entity_key="source:video:v",
            dataset_id="d",
            observation_id="o",
            text=text,
            source_kind="subtitles",
            permission_reference="operator-permission",
        )


def test_import_contract_bounds_text_after_unicode_normalization():
    # NFC canonically expands this composition-excluded character to two code points.
    raw = "\u0958" * 16000
    assert len(raw) == 16000 and len(unicodedata.normalize("NFC", raw)) == 32000
    with pytest.raises(ValidationError, match="bounded printable text"):
        TranscriptInput(
            product_id="p",
            entity_key="source:video:v",
            dataset_id="d",
            observation_id="o",
            text=raw,
            source_kind="subtitles",
            permission_reference="operator-permission",
        )


def test_unicode_expansion_preserves_quoted_offsets_in_normalized_transcript(pipeline):
    service, value = selected(pipeline)
    raw = "\u0958. Cela permet de gagner du temps."
    normalized = unicodedata.normalize("NFC", raw)
    supplied = TranscriptInput.model_validate({**value.model_dump(), "text": raw})
    assert supplied.text == normalized and len(normalized) == len(raw) + 1
    result = analyze(service, import_selected(service, supplied))
    assert result["segments"][0]["quote"] == normalized[:3]
    assert result["segments"][0]["end"] == 3
    assert all(
        segment["quote"] == normalized[segment["start"] : segment["end"]]
        for segment in result["segments"]
    )


@pytest.mark.parametrize("bypass", ["copy", "construct"])
@pytest.mark.parametrize(
    "change",
    [
        {"text": "x" * 16001},
        {"text": "\u0958" * 16000},
        {"synthetic": "false"},
        {"permission_reference": " "},
    ],
    ids=["too_long", "nfc_expansion", "namespace_type", "permission"],
)
def test_import_revalidates_bypassed_models_before_any_publication(pipeline, bypass, change):
    service, value = selected(pipeline)
    unsafe = (
        value.model_copy(update=change)
        if bypass == "copy"
        else TranscriptInput.model_construct(**{**dict(value), **change})
    )
    before = pipeline.catalog.all("SELECT object_key FROM derived_publications")
    with pytest.raises(ValidationError):
        service.import_transcript(unsafe)
    assert pipeline.catalog.all("SELECT object_key FROM derived_publications") == before
    assert pipeline.catalog.all("SELECT * FROM enrichment_transcripts") == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


@pytest.mark.parametrize(
    "change",
    [
        {"daily_requests": True},
        {"daily_cost_limit": 0.1},
        {"maximum_request_cost": 0.1},
        {"max_text_characters": 16001},
        {"allow_external": "true"},
    ],
    ids=["request_type", "float_daily_cost", "float_request_cost", "text_bound", "external_type"],
)
def test_budget_revalidates_constructed_values_without_serializing_away_invalid_types(
    pipeline, change
):
    service, _value = selected(pipeline)
    budget = EnrichmentBudget.model_construct(**change)
    with pytest.raises(ValidationError):
        service.configure_budget("controlled-fixture", budget, synthetic=True)
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


@pytest.mark.parametrize(
    "change",
    [
        {"reserved_cost": 0.1},
        {"enabled": 1},
        {"currency": "euro"},
        {"provider_id": "bad provider"},
    ],
    ids=["float_cost", "enabled_type", "currency", "identifier"],
)
def test_provider_spec_is_revalidated_before_reserving_or_calling(pipeline, change):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    adapter = ControlledAdapter()
    adapter.spec = adapter.spec.model_copy(update=change)
    with pytest.raises(ValidationError):
        analyze(service, transcript, adapter=adapter)
    assert adapter.calls == 0
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_analyses") == []


@pytest.mark.parametrize(
    "failure",
    [
        "count_copy",
        "nested_construct_label",
        "nested_copy_range",
        "mutated_mapping",
        "strict_offset_type",
        "method_construct",
        "outside_source",
        "mutated_source_text",
    ],
)
def test_bypassed_or_mutated_adapter_outputs_remain_uncertain_without_paid_retry(pipeline, failure):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    adapter = ControlledAdapter()
    segment = TextSegment(start=0, end=5, label="hook")
    output = InsightOutput(method="model", segments=(segment,))

    def invalid(document):
        adapter.calls += 1
        if failure == "count_copy":
            return output.model_copy(update={"segments": output.segments * 25})
        if failure == "nested_construct_label":
            malformed = TextSegment.model_construct(start=0, end=5, label="winner_prediction")
            return output.model_copy(update={"segments": (malformed,)})
        if failure == "nested_copy_range":
            return output.model_copy(update={"segments": (segment.model_copy(update={"end": 0}),)})
        if failure == "mutated_mapping":
            mapping = {"method": "model", "segments": [{"start": 0, "end": 5, "label": "hook"}]}
            constructed = InsightOutput.model_construct(**mapping)
            mapping["segments"][0]["label"] = "invented"
            return constructed
        if failure == "strict_offset_type":
            return output.model_copy(
                update={"segments": (segment.model_copy(update={"start": False}),)}
            )
        if failure == "method_construct":
            return InsightOutput.model_construct(method="winner_prediction", segments=(segment,))
        if failure == "outside_source":
            return output.model_copy(
                update={
                    "segments": (
                        TextSegment(start=len(value.text), end=len(value.text) + 1, label="hook"),
                    )
                }
            )
        document["text"] = "x" * 500
        return InsightOutput(
            method="model", segments=(TextSegment(start=0, end=500, label="hook"),)
        )

    adapter.analyze = invalid
    with pytest.raises(EnrichmentDenied, match="outcome_uncertain"):
        analyze(service, transcript, adapter=adapter)
    assert pipeline.catalog.one("SELECT state,descriptor FROM enrichment_analyses") == {
        "state": "uncertain",
        "descriptor": None,
    }
    assert reservation(pipeline)["requests_reserved"] == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")
    assert analyze(service, transcript, "retry", adapter=adapter)["state"] == "uncertain"
    assert adapter.calls == 1


@pytest.mark.parametrize("container", [set, frozenset, iter], ids=["set", "frozenset", "iterator"])
@pytest.mark.parametrize(
    "change",
    [
        {"label": "winner_prediction"},
        {"start": False},
        {"end": 501},
    ],
    ids=["label", "offset_type", "offset_range"],
)
def test_nested_output_instances_are_revalidated_in_every_accepted_container(
    pipeline, container, change
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    adapter = ControlledAdapter()
    segment = TextSegment.model_construct(**{"start": 0, "end": 5, "label": "hook", **change})

    def malformed(_document):
        adapter.calls += 1
        return InsightOutput.model_construct(method="model", segments=container([segment]))

    adapter.analyze = malformed
    with pytest.raises(EnrichmentDenied, match="outcome_uncertain"):
        analyze(service, transcript, adapter=adapter)
    assert pipeline.catalog.one("SELECT state,descriptor FROM enrichment_analyses") == {
        "state": "uncertain",
        "descriptor": None,
    }
    assert reservation(pipeline)["requests_reserved"] == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")
    assert analyze(service, transcript, "retry", adapter=adapter)["state"] == "uncertain"
    assert adapter.calls == 1


def test_adapter_iterator_rejection_does_not_consume_unknown_iterables(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    adapter = ControlledAdapter()
    consumed = 0

    def segments():
        nonlocal consumed
        for _ in range(1000):
            consumed += 1
            yield TextSegment(start=0, end=5, label="hook")

    def excessive(_document):
        adapter.calls += 1
        return InsightOutput.model_construct(method="model", segments=segments())

    adapter.analyze = excessive
    with pytest.raises(EnrichmentDenied, match="outcome_uncertain"):
        analyze(service, transcript, adapter=adapter)
    assert consumed == 0
    assert analyze(service, transcript, "retry", adapter=adapter)["state"] == "uncertain"
    assert adapter.calls == 1 and consumed == 0
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")


@pytest.mark.parametrize(
    "contract", [TranscriptInput, EnrichmentBudget, ProviderSpec, TextSegment, InsightOutput]
)
def test_scoped_instance_revalidation_preserves_inherited_contract_guards(contract):
    assert contract.model_config["revalidate_instances"] == "always"
    assert contract.model_config["extra"] == "forbid"
    assert contract.model_config["frozen"] is True
    assert contract.model_config["allow_inf_nan"] is False


def future_source(pipeline, value):
    available = now() + timedelta(days=1)
    source, _kind, entity = value.entity_key.split(":", 2)
    observation = Observation(
        observation_id="future-transcript-source",
        source=source,
        entity_type="video",
        entity_id=entity,
        observed_at=available,
        available_at=available,
        synthetic=True,
    )
    dataset = pipeline.import_records([observation.model_dump(mode="json")], synthetic=True)[0]
    return value.model_copy(
        update={"dataset_id": dataset, "observation_id": observation.observation_id}
    ), available


def test_future_source_cannot_be_analyzed_or_read_before_its_availability(pipeline):
    service, value = selected(pipeline)
    future, available = future_source(pipeline, value)
    cutoff = available + timedelta(minutes=1)
    imported = service.import_transcript(future, as_of=cutoff)
    external_budget(service)
    adapter = ControlledAdapter()
    with pytest.raises(EnrichmentDenied):
        analyze(service, imported["transcript_id"], adapter=adapter)
    assert adapter.calls == 0
    assert reservation(pipeline)["requests_reserved"] == 0
    assert reservation(pipeline)["cost_reserved"] == 0
    assert imported["available_at"] == available
    for request in ("future-a", "future-b"):
        result = service.analyze(
            request, "p", imported["transcript_id"], synthetic=True, as_of=cutoff, adapter=adapter
        )
        assert result["state"] == "completed"
        with pytest.raises(EnrichmentDenied):
            service.result(request, "p", synthetic=True)
    assert adapter.calls == 1 and reservation(pipeline)["requests_reserved"] == 1
    assert service.summaries("p", synthetic=True, limit=1) == {"items": [], "next_cursor": None}


def test_stored_wall_clock_from_an_older_import_cannot_hide_future_source_dependency(pipeline):
    service, value = selected(pipeline)
    future, available = future_source(pipeline, value)
    cutoff = available + timedelta(minutes=1)
    transcript = service.import_transcript(future, as_of=cutoff)["transcript_id"]
    pipeline.catalog.execute(
        "UPDATE enrichment_transcripts SET available_at=clock_timestamp() WHERE transcript_id=%s",
        (transcript,),
    )
    external_budget(service)
    adapter = ControlledAdapter()
    with pytest.raises(EnrichmentDenied, match="source_observation_unavailable"):
        analyze(service, transcript, adapter=adapter)
    assert adapter.calls == 0 and reservation(pipeline)["requests_reserved"] == 0
    for request in ("future-a", "future-b"):
        service.analyze(request, "p", transcript, synthetic=True, as_of=cutoff, adapter=adapter)
        with pytest.raises(EnrichmentDenied, match="source_observation_unavailable"):
            service.result(request, "p", synthetic=True)
    assert service.summaries("p", synthetic=True, limit=1) == {"items": [], "next_cursor": None}
    assert adapter.calls == 1 and reservation(pipeline)["cost_reserved"] == Decimal("0.10")


def test_a_known_available_transcript_stays_reusable_after_future_metric_import(pipeline):
    service, value = selected(pipeline)
    known = service.import_transcript(value)
    future, available = future_source(pipeline, value)
    reused = service.import_transcript(future, as_of=available + timedelta(minutes=1))
    assert reused["reused"] is True and reused["transcript_id"] == known["transcript_id"]
    assert reused["available_at"] == known["available_at"]
    assert analyze(service, reused["transcript_id"])["state"] == "completed"


@pytest.mark.parametrize("future_first", [True, False], ids=["future_first", "current_first"])
def test_completed_cache_only_reuses_origins_available_at_the_requested_cutoff(
    pipeline, future_first
):
    service, value = selected(pipeline)
    future_value, available = future_source(pipeline, value)
    cutoff = available + timedelta(minutes=1)
    external_budget(service)
    adapter = ControlledAdapter()
    if future_first:
        future = service.import_transcript(future_value, as_of=cutoff)
        future_result = service.analyze(
            "future", "p", future["transcript_id"], synthetic=True, as_of=cutoff, adapter=adapter
        )
        current = service.import_transcript(value)
        assert current["transcript_id"] != future["transcript_id"]
        current_result = analyze(service, current["transcript_id"], "current", adapter=adapter)
        assert current_result["analysis_id"] != future_result["analysis_id"]
        with pytest.raises(EnrichmentDenied):
            service.result("future", "p", synthetic=True)
    else:
        current = service.import_transcript(value)
        current_result = analyze(service, current["transcript_id"], "current", adapter=adapter)
        future = service.import_transcript(future_value, as_of=cutoff)
        future_result = service.analyze(
            "future", "p", future["transcript_id"], synthetic=True, as_of=cutoff, adapter=adapter
        )
        assert current_result["analysis_id"] == future_result["analysis_id"]
    assert current_result["state"] == future_result["state"] == "completed"
    assert analyze(service, current["transcript_id"], "current", adapter=adapter) == current_result
    assert (
        analyze(service, current["transcript_id"], "alias", adapter=adapter)["analysis_id"]
        == current_result["analysis_id"]
    )
    assert adapter.calls == (2 if future_first else 1)
    assert reservation(pipeline)["requests_reserved"] == adapter.calls
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10") * adapter.calls


@pytest.mark.parametrize("field", ["created_at", "finished_at"])
def test_completed_cache_recorded_after_cutoff_cannot_poison_request_or_reserve_again(
    pipeline, field
):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    external_budget(service)
    adapter = ControlledAdapter()
    original = analyze(service, transcript, adapter=adapter)
    cutoff = now()
    pipeline.catalog.execute(
        f"UPDATE enrichment_analyses SET {field}=%s WHERE analysis_id=%s",
        (cutoff + timedelta(days=1), original["analysis_id"]),
    )
    with pytest.raises(EnrichmentDenied, match="cached_result_unavailable_at_cutoff"):
        service.analyze(
            "historical", "p", transcript, synthetic=True, as_of=cutoff, adapter=adapter
        )
    assert (
        pipeline.catalog.one("SELECT 1 FROM enrichment_requests WHERE request_id='historical'")
        is None
    )
    assert adapter.calls == 1 and reservation(pipeline)["requests_reserved"] == 1
    later = cutoff + timedelta(days=2)
    assert (
        service.analyze(
            "historical", "p", transcript, synthetic=True, as_of=later, adapter=adapter
        )["analysis_id"]
        == original["analysis_id"]
    )
    assert adapter.calls == 1


@pytest.mark.parametrize("outcome", ["running", "uncertain"])
def test_unresolved_future_cache_stays_a_barrier_against_a_new_paid_call(pipeline, outcome):
    service, value = selected(pipeline)
    future_value, available = future_source(pipeline, value)
    cutoff = available + timedelta(minutes=1)
    future = service.import_transcript(future_value, as_of=cutoff)
    external_budget(service)
    adapter = ControlledAdapter()

    class ProcessDeath(BaseException):
        pass

    def unresolved(_document):
        adapter.calls += 1
        if outcome == "running":
            raise ProcessDeath()
        raise TimeoutError()

    adapter.analyze = unresolved
    with pytest.raises(ProcessDeath if outcome == "running" else EnrichmentDenied):
        service.analyze(
            "future", "p", future["transcript_id"], synthetic=True, as_of=cutoff, adapter=adapter
        )
    current = service.import_transcript(value)
    with pytest.raises(EnrichmentDenied):
        analyze(service, current["transcript_id"], "current", adapter=adapter)
    assert adapter.calls == 1
    assert reservation(pipeline)["requests_reserved"] == 1
    assert reservation(pipeline)["cost_reserved"] == Decimal("0.10")


def test_exact_source_availability_remains_readable_after_native_compaction(pipeline):
    service, value = selected(pipeline)
    transcript = import_selected(service, value)
    original = analyze(service, transcript)
    refreshed = refresh(pipeline, value)
    compacted = pipeline.compact(synthetic=True)
    assert compacted["status"] == "compacted"
    assert value.dataset_id in compacted["parent_dataset_ids"]
    assert refreshed.dataset_id in compacted["parent_dataset_ids"]
    assert service.result("request", "p", synthetic=True) == original
    assert service.import_transcript(refreshed)["transcript_id"] == transcript
    assert (
        service.summaries("p", synthetic=True)["items"][0]["analysis_id"] == original["analysis_id"]
    )


@pytest.mark.parametrize("change", [{"label": "winner_prediction"}, {"start": False}, {"end": 501}])
def test_segment_and_output_models_revalidate_existing_nested_instances(change):
    segment = TextSegment.model_construct(**{"start": 0, "end": 5, "label": "hook", **change})
    with pytest.raises(ValidationError):
        TextSegment.model_validate(segment)
    output = InsightOutput.model_construct(method="model", segments=(segment,))
    with pytest.raises(ValidationError):
        InsightOutput.model_validate(output)
