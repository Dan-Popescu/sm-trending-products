"""Cross-page operator selections retain only bounded canonical IDs."""

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_transcript_insights import import_selected, reservation, selected
from test_transcript_operator import widget

from sm_trending_products.contracts import canonical, now
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.product_shortlist import shortlist_ids
from sm_trending_products.text_selection import TextSelection


def seed_catalog(pipeline, count=205):
    pipeline.catalog.execute(
        "INSERT INTO products(product_id,name,synthetic) "
        "SELECT 'p'||lpad(n::text,3,'0'),'Private source title '||n,true "
        "FROM generate_series(0,%s) AS n",
        (count - 1,),
    )


def shortlist_app(pipeline, monkeypatch):
    for key, value in {
        "S3_BUCKET": pipeline.settings.s3_bucket,
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
        "CACHE_DIR": str(pipeline.settings.cache_dir),
        "SPOOL_DIR": str(pipeline.settings.spool_dir),
    }.items():
        monkeypatch.setenv(key, value)
    return AppTest.from_string("""
import streamlit as st
from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.text_selection_dashboard import render_text_selection
synthetic = st.toggle('Synthetic workspace', value=True, key='shortlist-test-namespace')
page = st.number_input('Catalog page', min_value=1, value=1, step=1, key='shortlist-test-page')
with Pipeline.from_env() as pipeline:
    products = pipeline.products(synthetic, limit=200, offset=(page-1)*200)
    render_text_selection(pipeline, products, now(), synthetic)
""").run(timeout=30)


def shortlist(app):
    return widget(app, "multiselect", "Product shortlist for supplied text (maximum ten)")


@pytest.mark.integration
def test_shortlist_survives_pages_and_explicit_removal_without_caching_reports(
    pipeline, monkeypatch
):
    seed_catalog(pipeline)
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p000"]).run()
    widget(app, "number_input", "Catalog page").set_value(2).run()
    assert not app.exception
    assert shortlist(app).value == ["p000"]
    assert "p000" in shortlist(app).options and "p200" in shortlist(app).options
    shortlist(app).set_value(["p000", "p200"]).run()
    widget(app, "number_input", "Catalog page").set_value(1).run()
    assert shortlist(app).value == ["p000", "p200"]
    ten = ["p000", "p200", *[f"p{index:03}" for index in range(1, 9)]]
    shortlist(app).set_value(ten).run()
    widget(app, "number_input", "Catalog page").set_value(2).run()
    assert shortlist(app).value == ten and shortlist(app).proto.max_selections == 10
    shortlist(app).set_value(["p200"]).run()
    widget(app, "number_input", "Catalog page").set_value(3).run()
    assert not app.exception and shortlist(app).value == ["p200"]
    state = app.session_state.to_dict()
    assert next(
        value
        for key, value in state.items()
        if key.startswith("_research-text-shortlist:") and key.endswith(":True")
    ) == ["p200"]
    assert "Private source title" not in json.dumps(state)
    assert not app.json


@pytest.mark.integration
def test_shortlist_isolates_namespaces_and_drops_stale_product_ids(pipeline, monkeypatch):
    seed_catalog(pipeline)
    pipeline.create_product("live", "Real product", synthetic=False)
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p000"]).run()
    widget(app, "toggle", "Synthetic workspace").set_value(False).run()
    assert shortlist(app).value == [] and "p000" not in shortlist(app).options
    shortlist(app).set_value(["live"]).run()
    widget(app, "toggle", "Synthetic workspace").set_value(True).run()
    assert shortlist(app).value == ["p000"] and "live" not in shortlist(app).options
    widget(app, "number_input", "Catalog page").set_value(2).run()
    pipeline.catalog.execute("DELETE FROM products WHERE product_id='p000'")
    app.run()
    assert not app.exception and shortlist(app).value == []
    assert app.warning
    assert (
        next(
            value
            for key, value in app.session_state.to_dict().items()
            if key.startswith("_research-text-shortlist:") and key.endswith(":True")
        )
        == []
    )
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert app.error and not app.json


@pytest.mark.parametrize(
    "value", [[str(i) for i in range(11)], ["p", "p"], [" p"], ["p\n"], [1], "p"]
)
def test_invalid_shortlist_is_rejected_before_lookup(value):
    with pytest.raises(ValueError):
        shortlist_ids(value)


@pytest.mark.integration
def test_products_by_ids_uses_only_bounded_current_namespace_lookup(pipeline, monkeypatch):
    seed_catalog(pipeline, 500)
    pipeline.create_product("live", "Live", synthetic=False)
    calls = []
    original = pipeline.catalog.evidence_all

    def read(query, params=(), **kwargs):
        calls.append((query, params))
        return original(query, params, **kwargs)

    monkeypatch.setattr(pipeline.catalog, "evidence_all", read)
    rows = pipeline.products_by_ids(["p499", "p000", "missing", "live"], True)
    assert [row["product_id"] for row in rows] == ["p499", "p000"]
    assert len(calls) == 1 and "product_id=ANY" in calls[0][0]
    assert calls[0][1][0] == ["p499", "p000", "missing", "live"]
    assert pipeline.products_by_ids(["p000"], True, as_of=now() - timedelta(days=1)) == []
    assert pipeline.products_by_ids(["p000", "live"], False)[0]["product_id"] == "live"
    calls.clear()
    assert pipeline.products_by_ids([], True) == [] and calls == []
    with pytest.raises(ValueError):
        pipeline.products_by_ids([str(i) for i in range(11)], True)
    with pytest.raises(ValueError):
        pipeline.products_by_ids(["p000"], 1)
    assert calls == []


@pytest.mark.integration
def test_actual_schema_change_clears_shortlist_without_retaining_database_credentials(
    pipeline, monkeypatch
):
    from psycopg import sql

    seed_catalog(pipeline)
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p000"]).run()
    old_keys = {
        key for key in app.session_state.to_dict() if key.startswith("_research-text-shortlist:")
    }
    # A second real catalog schema in the same AppTest session changes the actual
    # connection context, even though the namespace flag and visible IDs match.
    with pipeline.catalog.transaction() as connection:
        schema = connection.execute("SELECT current_schema() AS schema").fetchone()["schema"]
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier("shortlist_other_" + schema))
        )
    from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

    from sm_trending_products.settings import Settings

    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    query = dict(parse_qsl(parsed.query))
    query["options"] = "-c search_path=shortlist_other_" + schema
    other_url = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, quote_via=quote),
            parsed.fragment,
        )
    )
    try:
        settings = pipeline.settings.model_copy(
            update={"database_url": Settings(database_url=other_url).database_url}
        )
        with Pipeline(settings) as other:
            other.initialize()
            other.create_product("p000", "Different workspace product", synthetic=True)
        monkeypatch.setenv("DATABASE_URL", other_url)
        app.run(timeout=30)
        assert not app.exception and shortlist(app).value == []
        state = app.session_state.to_dict()
        assert not old_keys.intersection(state)
        assert "search_path" not in canonical(state) and "postgresql://" not in canonical(state)
    finally:
        with pipeline.catalog.transaction() as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier("shortlist_other_" + schema)
                )
            )


@pytest.mark.integration
def test_cross_page_plan_apply_rechecks_exact_scope_and_current_source(pipeline, monkeypatch):
    service, value = selected(pipeline)
    import_selected(service, value)
    seed_catalog(pipeline)
    plan = TextSelection(pipeline.catalog, pipeline.provenance).preview(
        ["p", "p200"], synthetic=True
    )

    class Upload:
        size = len(json.dumps(plan).encode())

        def getvalue(self):
            return json.dumps(plan).encode()

    monkeypatch.setattr(
        "streamlit.file_uploader",
        lambda label, **kwargs: Upload() if "Reviewed complete selection" in label else None,
    )
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p"]).run()
    widget(app, "number_input", "Catalog page").set_value(2).run()
    shortlist(app).set_value(["p", "p200"]).run()
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert set(json.loads(app.json[0].value)["product_ids"]) == {"p", "p200"}
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    shortlist(app).set_value(["p200"]).run()
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert (
        app.error
        and not app.json
        and pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    )
    widget(app, "number_input", "Catalog page").set_value(1).run()
    shortlist(app).set_value(["p", "p200"]).run()
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert not app.exception and not app.error
    assert json.loads(app.json[0].value)["state"] == "completed"
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    app.run()
    assert not app.json
    assert "gagner du temps" not in canonical(app.session_state.to_dict())
    pipeline.catalog.invalidate_dataset(value.dataset_id, "shortlist-source-withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert app.error and not app.json
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


@pytest.mark.integration
def test_identifier_labels_scrub_current_api_keys_without_changing_session_ids(
    pipeline, monkeypatch
):
    seed_catalog(pipeline)
    monkeypatch.setenv("SHORTLIST_API_KEY", "p000")
    app = shortlist_app(pipeline, monkeypatch)
    assert not app.exception and "p000" not in shortlist(app).options
    assert "Private source title" not in str(shortlist(app).options)


@pytest.mark.integration
def test_preview_final_json_does_not_restore_encoded_secret_or_export_changed_plan(
    pipeline, monkeypatch
):
    service, value = selected(pipeline, entity=r"entity-\u2026")
    import_selected(service, value)
    original = TextSelection(pipeline.catalog, pipeline.provenance).preview(["p"], synthetic=True)
    original_bytes = canonical(original)
    downloads = []
    monkeypatch.setenv("REVIEW_API_KEY", r"\u2026")
    monkeypatch.setattr("streamlit.download_button", lambda *args, **kwargs: downloads.append(args))
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p"]).run()
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert r"\u2026" not in app.json[0].value
    assert json.loads(app.json[0].value)["items"]
    assert not downloads and any("cannot be exported" in row.value for row in app.warning)
    assert canonical(original) == original_bytes
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []


@pytest.mark.integration
def test_result_final_json_does_not_restore_secret_after_redacting_request_id(
    pipeline, monkeypatch
):
    service, value = selected(pipeline)
    import_selected(service, value)
    plan = TextSelection(pipeline.catalog, pipeline.provenance).preview(["p"], synthetic=True)

    class Upload:
        size = len(json.dumps(plan).encode())

        def getvalue(self):
            return json.dumps(plan).encode()

    monkeypatch.setattr(
        "streamlit.file_uploader",
        lambda label, **kwargs: Upload() if "Reviewed complete selection" in label else None,
    )
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p"]).run()
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert not app.exception and not app.error
    request = json.loads(app.json[0].value)["results"][0]["request_id"]
    monkeypatch.setenv("REVIEW_API_KEY", request)
    monkeypatch.setenv("REVIEW_CREDENTIAL", r"\u2026")
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert not app.exception and not app.error
    assert request not in app.json[0].value and r"\u2026" not in app.json[0].value
    assert json.loads(app.json[0].value)["state"] == "completed"
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


@pytest.mark.integration
def test_two_masked_product_ids_keep_exact_scope_across_pages_preview_and_apply(
    pipeline, monkeypatch
):
    seed_catalog(pipeline)
    service, first = selected(pipeline, entity="first-masked")
    pipeline.review_link("p000", first.entity_key)
    import_selected(service, first.model_copy(update={"product_id": "p000"}))
    service, second = selected(pipeline, entity="second-masked")
    pipeline.review_link("p200", second.entity_key)
    import_selected(service, second.model_copy(update={"product_id": "p200"}))
    plan = TextSelection(pipeline.catalog, pipeline.provenance).preview(
        ["p000", "p200"], synthetic=True
    )
    original_bytes = canonical(plan)

    class Upload:
        size = len(original_bytes.encode())

        def getvalue(self):
            return original_bytes.encode()

    monkeypatch.setattr(
        "streamlit.file_uploader",
        lambda label, **kwargs: Upload() if "Reviewed complete selection" in label else None,
    )
    downloads, preview_scopes, apply_scopes = [], [], []
    monkeypatch.setattr("streamlit.download_button", lambda *args, **kwargs: downloads.append(args))
    preview, execute = TextSelection.preview, TextSelection.execute

    def captured_preview(service, products, **kwargs):
        preview_scopes.append(tuple(products))
        return preview(service, products, **kwargs)

    def captured_execute(service, reviewed_plan, **kwargs):
        apply_scopes.append(tuple(reviewed_plan.product_ids))
        return execute(service, reviewed_plan, **kwargs)

    monkeypatch.setattr(TextSelection, "preview", captured_preview)
    monkeypatch.setattr(TextSelection, "execute", captured_execute)
    monkeypatch.setenv("FIRST_PRODUCT_API_KEY", "p000")
    monkeypatch.setenv("SECOND_PRODUCT_CREDENTIAL", "p200")
    app = shortlist_app(pipeline, monkeypatch)
    shortlist(app).set_value(["p000"]).run()
    widget(app, "number_input", "Catalog page").set_value(2).run()
    assert not app.exception and shortlist(app).value == ["p000"]
    assert len(shortlist(app).options) == len(set(shortlist(app).options))
    assert all(secret not in str(shortlist(app).options) for secret in ("p000", "p200"))
    shortlist(app).set_value(["p000", "p200"]).run()
    assert not app.exception and shortlist(app).value == ["p000", "p200"]
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert not app.exception and not app.error and preview_scopes == [("p000", "p200")]
    assert not downloads and app.warning
    assert all(secret not in app.json[0].value for secret in ("p000", "p200"))
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert not app.exception and not app.error and apply_scopes == [("p000", "p200")]
    assert json.loads(app.json[0].value)["state"] == "completed"
    assert canonical(plan) == original_bytes
    assert {
        row["product_id"]
        for row in pipeline.catalog.all("SELECT product_id FROM enrichment_requests")
    } == {"p000", "p200"}
