"""Complete content reports with bounded, source-fenced text hydration work."""

from collections import Counter
from contextlib import contextmanager
from datetime import timedelta

import psycopg
import pytest
from test_transcript_insights import analyze, import_selected, selected

from sm_trending_products.content_insights import DEFAULT_TAXONOMY, ContentInsights
from sm_trending_products.contracts import canonical, now
from sm_trending_products.enrichment import EnrichmentDenied, TranscriptContext, TranscriptInsights


def legacy_text_page(service, product_id, **kwargs):
    """The previous content reader, kept independent of the optimized page reader."""
    page = service.eligible_texts(product_id, **kwargs)
    texts = {}
    for item in sorted(
        page["items"], key=lambda item: (item["available_at"], item["transcript_id"])
    ):
        texts[item["content_key"]] = item
    selected = service.read_texts(
        product_id,
        [item["transcript_id"] for item in texts.values()],
        synthetic=kwargs.get("synthetic", False),
        as_of=page["as_of"],
    )
    return page | {
        "items": selected["items"],
        "unavailable": page["unavailable"] + selected["unavailable"],
    }


@contextmanager
def count_reads(monkeypatch, pipeline):
    counts = Counter()
    execute = psycopg.Connection.execute
    scope = TranscriptInsights._scope
    guard = pipeline.provenance.guard

    def counted_execute(connection, query, *args, **kwargs):
        counts["sql"] += 1
        if isinstance(query, str) and "enrichment_analyses" in query:
            counts["analysis_sql"] += 1
        return execute(connection, query, *args, **kwargs)

    def counted_scope(service, *args, **kwargs):
        counts["scope"] += 1
        return scope(service, *args, **kwargs)

    def counted_guard(*args, **kwargs):
        counts["provenance"] += 1
        return guard(*args, **kwargs)

    with monkeypatch.context() as counted:
        counted.setattr(psycopg.Connection, "execute", counted_execute)
        counted.setattr(TranscriptInsights, "_scope", counted_scope)
        counted.setattr(pipeline.provenance, "guard", counted_guard)
        yield counts


@pytest.mark.parametrize("text_count", [1, 8])
def test_content_report_matches_legacy_bytes_with_less_guarded_work(
    pipeline, monkeypatch, text_count
):
    service, value = selected(pipeline)
    for index in range(text_count):
        transcript = import_selected(
            service, value.model_copy(update={"text": f"Texte {index} pour gagner du temps."})
        )
        analyze(service, transcript, f"local-{index}")
    cutoff = now()
    content = ContentInsights(pipeline.catalog, pipeline.provenance)
    with monkeypatch.context() as legacy:
        legacy.setattr(TranscriptInsights, "read_text_page", legacy_text_page, raising=False)
        with count_reads(monkeypatch, pipeline) as baseline:
            expected = canonical(content.aggregate("p", synthetic=True, as_of=cutoff))
    with count_reads(monkeypatch, pipeline) as treatment:
        actual = canonical(content.aggregate("p", synthetic=True, as_of=cutoff))
    print({"texts": text_count, "baseline": dict(baseline), "treatment": dict(treatment)})
    assert actual == expected
    assert treatment["analysis_sql"] == 0
    assert treatment["sql"] < baseline["sql"]
    assert treatment["scope"] < baseline["scope"]
    assert treatment["provenance"] < baseline["provenance"]


def test_hydrated_pages_match_legacy_context_selection_and_unavailable_scan_budget(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    import_selected(service, value)
    import_selected(
        service,
        value.model_copy(
            update={
                "context": TranscriptContext(
                    reference="later-context", known_at=now(), creator_id="reviewed-creator"
                )
            }
        ),
    )
    import_selected(service, value.model_copy(update={"text": "Another supplied text."}))
    _, withdrawn = selected(pipeline, entity="withdrawn")
    retired = import_selected(service, withdrawn)
    pipeline.catalog.invalidate_dataset(withdrawn.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    cutoff = now()
    cursor = None
    scanned = 0
    unavailable = []
    while True:
        inputs = {"synthetic": True, "as_of": cutoff, "limit": 2, "after": cursor}
        expected = legacy_text_page(service, "p", **inputs)
        actual = service.read_text_page("p", **inputs)
        # The hydrated public seam preserves cursor and scan semantics as well
        # as the texts, context versions and unavailable reasons.
        assert canonical(actual) == canonical({key: expected[key] for key in actual})
        content = ContentInsights(pipeline.catalog, pipeline.provenance)
        with monkeypatch.context() as legacy:
            legacy.setattr(TranscriptInsights, "read_text_page", legacy_text_page)
            old_report = canonical(content.aggregate("p", **inputs))
        assert canonical(content.aggregate("p", **inputs)) == old_report
        scanned += actual["scanned"]
        unavailable.extend(actual["unavailable"])
        cursor = actual["next_cursor"]
        if cursor is None:
            break
    assert scanned == 4
    assert [item["transcript_id"] for item in unavailable] == [retired]
    complete = service.read_text_page("p", synthetic=True, as_of=cutoff)
    assert len(complete["items"]) == 2
    reviewed = next(item for item in complete["items"] if item["text"] == value.text)
    assert reviewed["context"]["creator_id"] == "reviewed-creator"


def test_eligibility_only_pages_preserve_row_cursor_denials_without_analysis_or_payload(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    for index in range(3):
        import_selected(service, value.model_copy(update={"text": f"Text {index}"}))
    _, withdrawn = selected(pipeline, entity="withdrawn")
    retired = import_selected(service, withdrawn)
    pipeline.catalog.invalidate_dataset(withdrawn.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    cutoff, cursor = now(), None
    scanned, unavailable = 0, []

    def no_payload(*args, **kwargs):
        raise AssertionError("Eligibility does not hydrate transcripts")

    monkeypatch.setattr(pipeline.provenance, "selected_records", no_payload)
    while True:
        inputs = {"synthetic": True, "as_of": cutoff, "limit": 2, "after": cursor}
        expected = service.eligible_texts("p", **inputs)
        with count_reads(monkeypatch, pipeline) as work:
            actual = service.eligible_text_ids("p", **inputs)
        assert work["analysis_sql"] == 0
        assert actual == expected | {
            "items": [{"transcript_id": item["transcript_id"]} for item in expected["items"]]
        }
        scanned += actual["scanned"]
        unavailable.extend(actual["unavailable"])
        cursor = actual["next_cursor"]
        if cursor is None:
            break
    assert scanned == 4
    assert unavailable == [{"transcript_id": retired, "reason": "enrichment_source_unavailable"}]
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_hydration_excludes_future_text_context_and_wrong_workspace(pipeline):
    service, value = selected(pipeline)
    cutoff = now()
    later = value.model_copy(
        update={
            "context": TranscriptContext(
                reference="future-context",
                known_at=cutoff + timedelta(hours=1),
                creator_id="later-creator",
            )
        }
    )
    service.import_transcript(later, as_of=cutoff + timedelta(hours=2))
    assert service.read_text_page("p", synthetic=True, as_of=cutoff)["items"] == []
    assert service.read_text_page("p", synthetic=False)["items"] == []
    assert service.read_text_page("p", synthetic=True, as_of=cutoff + timedelta(hours=2))["items"]


@pytest.mark.parametrize("change", ["revocation", "ownership", "observation"])
def test_hydration_refreshes_authority_after_page_selection(pipeline, monkeypatch, change):
    live = change == "revocation"
    service, value = selected(pipeline, synthetic=not live)
    if live:
        pipeline.catalog.execute(
            "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
            (value.entity_key.split(":", 1)[0],),
        )
    transcript = import_selected(service, value)
    product = value.product_id
    cutoff = now() + timedelta(minutes=1) if not live else now()
    guard = service._guard_transcript
    checked = False

    def change_after_selection(connection, row, *args):
        nonlocal checked
        result = guard(connection, row, *args)
        if not checked:
            checked = True
            if change == "revocation":
                pipeline.catalog.execute(
                    "UPDATE source_policies SET verified=false WHERE source=%s",
                    (value.entity_key.split(":", 1)[0],),
                )
            elif change == "ownership":
                pipeline.create_product("other", "Other owner", synthetic=True)
                pipeline.review_link("other", value.entity_key)
            else:
                pipeline.catalog.execute(
                    "UPDATE observation_deliveries SET available_at=%s WHERE observation_id=%s",
                    (cutoff + timedelta(hours=1), value.observation_id),
                )
        return result

    monkeypatch.setattr(service, "_guard_transcript", change_after_selection)
    page = service.read_text_page(product, synthetic=not live, as_of=cutoff)
    assert checked and page["scanned"] == 1
    assert page["items"] == []
    assert [item["transcript_id"] for item in page["unavailable"]] == [transcript]


def test_hydration_holds_retirement_fence_through_actual_object_read(pipeline, monkeypatch):
    from sm_trending_products.catalog import RETENTION_GUARD_KEY

    service, value = selected(pipeline)
    import_selected(service, value)
    selected_records = pipeline.provenance.selected_records
    checked = False

    def check_fence(*args, **kwargs):
        nonlocal checked
        own_lock = (
            kwargs["connection"]
            .execute(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND locktype='advisory' AND classid=0 AND objid=%s "
                "AND mode='ShareLock' AND granted) AS held",
                (RETENTION_GUARD_KEY,),
            )
            .fetchone()
        )
        assert own_lock["held"] is True
        with pipeline.catalog.transaction() as connection:
            row = connection.execute(
                "SELECT pg_try_advisory_xact_lock(%s) AS acquired", (RETENTION_GUARD_KEY,)
            ).fetchone()
            assert row["acquired"] is False
        checked = True
        return selected_records(*args, **kwargs)

    monkeypatch.setattr(pipeline.provenance, "selected_records", check_fence)
    assert service.read_text_page("p", synthetic=True)["items"]
    assert checked
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert service.read_text_page("p", synthetic=True)["items"] == []


@pytest.mark.parametrize(
    "inputs", [{"limit": True}, {"limit": 201}, {"synthetic": 1}, {"after": "invalid"}]
)
def test_hydrated_page_rejects_invalid_budgets_without_reading_objects(
    pipeline, monkeypatch, inputs
):
    service, _value = selected(pipeline)

    def no_payload(*args, **kwargs):
        raise AssertionError("Invalid page inputs must fail before hydration")

    monkeypatch.setattr(pipeline.provenance, "selected_records", no_payload)
    with pytest.raises(ValueError):
        service.read_text_page("p", **({"synthetic": True} | inputs))


def test_live_hydrated_page_rejects_future_cutoff_and_expired_policy(pipeline):
    service, value = selected(pipeline, synthetic=False)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        (value.entity_key.split(":", 1)[0],),
    )
    import_selected(service, value)
    with pytest.raises(EnrichmentDenied, match="future_live_cutoff"):
        service.read_text_page("live-p", as_of=now() + timedelta(hours=1))
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (value.dataset_id,),
    )
    page = service.read_text_page("live-p")
    assert page["items"] == []
    assert page["unavailable"][0]["reason"] == "enrichment_source_retention_expired"


def test_hydrated_page_requires_explicit_module_initialization_without_ddl(pipeline):
    service = TranscriptInsights(pipeline.catalog, pipeline.provenance)

    def relations():
        return pipeline.catalog.all(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
        )

    baseline = relations()
    with pytest.raises(ValueError, match="not initialized"):
        service.read_text_page("p", synthetic=True)
    assert relations() == baseline


@pytest.mark.parametrize("explicit_cutoff", [True, False])
def test_content_rejects_future_taxonomy_before_object_hydration(
    pipeline, monkeypatch, explicit_cutoff
):
    service, value = selected(pipeline)
    import_selected(service, value)
    cutoff = now()
    taxonomy = DEFAULT_TAXONOMY.model_copy(update={"reviewed_at": cutoff + timedelta(hours=1)})

    selected_records = pipeline.provenance.selected_records
    reads = []

    def counted_payload(*args, **kwargs):
        reads.append(1)
        return selected_records(*args, **kwargs)

    monkeypatch.setattr(pipeline.provenance, "selected_records", counted_payload)
    metrics = dict(pipeline.history.metrics)
    with count_reads(monkeypatch, pipeline) as work:
        with pytest.raises(ValueError, match="not reviewed"):
            ContentInsights(pipeline.catalog, pipeline.provenance).aggregate(
                "p",
                synthetic=True,
                taxonomy=taxonomy,
                **({"as_of": cutoff} if explicit_cutoff else {}),
            )
    assert reads == []
    assert pipeline.history.metrics == metrics
    if explicit_cutoff:
        assert work["sql"] == 0


def test_content_default_database_cutoff_is_captured_once_and_matches_explicit_report(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    import_selected(service, value)
    calls = []
    clock = TranscriptInsights._clock

    def captured_clock(connection, as_of, synthetic):
        calls.append(as_of)
        return clock(connection, as_of, synthetic)

    monkeypatch.setattr(TranscriptInsights, "_clock", staticmethod(captured_clock))
    content = ContentInsights(pipeline.catalog, pipeline.provenance)
    actual = content.aggregate("p", synthetic=True)
    assert calls == [None, actual["as_of"]]
    assert content.aggregate("p", synthetic=True, as_of=actual["as_of"]) == actual
    assert calls == [None, actual["as_of"], actual["as_of"]]


def test_live_content_default_cutoff_ignores_application_clock_skew(pipeline, monkeypatch):
    service, value = selected(pipeline, synthetic=False)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) VALUES(%s,1,true)",
        (value.entity_key.split(":", 1)[0],),
    )
    import_selected(service, value)
    monkeypatch.setattr(
        "sm_trending_products.content_insights.service.now",
        lambda: now() + timedelta(seconds=1),
        raising=False,
    )
    report = ContentInsights(pipeline.catalog, pipeline.provenance).aggregate("live-p")
    assert report["distinct_text_count"] == 1
