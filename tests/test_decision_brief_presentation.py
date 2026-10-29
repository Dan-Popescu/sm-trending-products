"""Readable briefs preserve the guarded report and its original analytical identity."""

import json

import pytest

from sm_trending_products.contracts import canonical, now


def test_partial_native_brief_has_french_unknowns_without_initializing_modules(pipeline):
    from sm_trending_products.decision_brief_presentation import present_brief, render_text

    pipeline.create_product("p", "Lampe", synthetic=True)
    before = pipeline.catalog.all(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
    )
    report = pipeline.decision_brief().describe("p", "FR", now(), True)
    original = canonical(report)
    text = render_text(present_brief(report))
    assert "Lampe" in text and "FR" in text
    assert "À surveiller" in text
    assert "inconnue" in text and "Achats réels" in text
    assert "non initialisé" in text
    assert "24 h" in text and "72 h" in text and "168 h" in text
    assert "Informations manquantes" in text and "Vérifications utiles" in text
    assert canonical(report) == original
    assert (
        pipeline.catalog.all(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
        )
        == before
    )


def test_human_cli_retains_known_downside_beside_unknown_scenario_and_exact_default(
    pipeline, monkeypatch, capsys, tmp_path
):
    from test_evaluation_integration import seed_product
    from test_launch_dossiers import draft

    from sm_trending_products.cli import main
    from sm_trending_products.collector import redact
    from sm_trending_products.commerce import LaunchService

    seed_product(pipeline, "p1")
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "mixed-presentation")
    profile = {"scenarios": [{"name": "adverse", "ad_cost": "100"}, {"name": "unknown"}]}
    path = tmp_path / "scenarios.json"
    path.write_text(json.dumps(profile))
    cutoff = now()
    report = pipeline.decision_brief().describe(
        "p1",
        "FR",
        cutoff,
        True,
        dossier_id="launch-p1-fr",
        expected_revision=1,
        scenario_profile=profile,
    )
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    # Current readiness has a current timestamp; bind the real pipeline boundary
    # to the already acquired native report when checking byte compatibility.
    from sm_trending_products.decision_brief import DecisionBrief

    monkeypatch.setattr(DecisionBrief, "describe", lambda *args, **kwargs: report)
    options = [
        "research",
        "brief",
        "--product-id",
        "p1",
        "--target-country",
        "FR",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
        "--dossier-id",
        "launch-p1-fr",
        "--expected-revision",
        "1",
        "--scenarios",
        str(path),
    ]
    assert main(options) == 0
    assert capsys.readouterr().out == canonical(redact(report)) + "\n"
    assert main([*options, "--format", "text"]) == 0
    text = capsys.readouterr().out
    assert "objectif manqué" in text and "résultat inconnu" in text
    assert "100 EUR/commande" in text and "révision exacte : 1" in text
    assert "Éléments défavorables connus" in text and "coûts inconnus" in text
    assert "État actuel des devis évalué à" in text
    assert "hypothèses" in text and "contribution après publicité" in text
    assert "marchandises : 8.25 EUR/commande" in text
    assert "taxe incluse : oui" in text
    monkeypatch.setenv("PRESENTATION_API_KEY", "Fixture supplier")
    assert main([*options, "--format", "html"]) == 0
    html = capsys.readouterr().out
    assert '<html lang="fr">' in html and "objectif manqué" in html
    assert "Fixture supplier" not in html


def test_presentation_escapes_source_text_and_preserves_unknown_future_codes():
    from sm_trending_products.decision_brief_presentation import (
        present_brief,
        render_html,
        render_text,
    )

    report = {
        "name": '<script>alert("source")</script>',
        "status": "future_state",
        "support": [{"dimension": "future_dimension", "code": "future_claim", "quantity": 8}],
        "missing": [{"dimension": "themes", "code": "unsupported_or_unknown_language"}],
        "sections": {},
        "reference": "https://user:private-pass@example.test/?token=private-token",
        "quote": "Voir https://user:private-pass@example.test/?api_key=private-key&x=1",
        "password": "private-password",
        "api_key": "private-metadata-key",
        "inline": "Base POSTGRESQL://user:private-db-pass@example.test/db puis "
        "HTTPS://u:private-url-pass@example.test/?api%5fkey=private-encoded-key",
    }
    original = canonical(report)
    view = present_brief(report)
    text, html = render_text(view), render_html(view)
    assert "État non interprété" in text and "Information non interprétée" in text
    assert "future_claim" in text and "future_dimension" in text
    assert "langue non prise en charge" in text
    assert "<script>" not in html and "&lt;script&gt;" in html
    for value in (
        "private-pass",
        "private-token",
        "private-key",
        "private-password",
        "private-metadata-key",
        "private-db-pass",
        "private-url-pass",
        "private-encoded-key",
    ):
        assert value not in text + html
    assert canonical(report) == original


def test_presentation_refuses_oversized_report_without_fabricated_summary():
    from sm_trending_products.decision_brief_presentation import present_brief

    with pytest.raises(ValueError, match="brief_presentation_budget_exceeded"):
        present_brief({"name": "x" * (2 * 1024 * 1024 + 1)})


@pytest.mark.parametrize(
    ("secret", "encoded"),
    [
        ("review-secret-42", "review%2Dsecret%2D42"),
        ("review+secret&42", "review%2Bsecret%2642"),
        ("redacted", "red%61cted"),
    ],
)
def test_url_normalization_and_html_generation_cannot_restore_bound_secrets(secret, encoded):
    from urllib.parse import unquote_plus

    from sm_trending_products.decision_brief_presentation import (
        present_brief,
        render_html,
        render_text,
    )

    report = {
        "name": f"Voir https://example.test/?q={encoded}",
        "status": "watch",
        "password": "private",
    }
    original = canonical(report)
    view = present_brief(report, secret_values=(secret,))
    assert secret not in unquote_plus(view.title)
    assert secret not in render_text(view)
    assert secret not in render_html(view)
    for output in (
        render_text(view, secret_values=(secret,)),
        render_html(view, secret_values=(secret,)),
    ):
        assert secret not in output
        assert secret not in unquote_plus(output)
    assert secret not in canonical(view.technical)
    assert secret not in unquote_plus(canonical(view.technical))
    assert canonical(report) == original
    quoted = present_brief({"name": '"'}, secret_values=("&quot;",))
    assert "&quot;" not in render_html(quoted, secret_values=("&quot;",))


@pytest.mark.parametrize(
    ("secret", "encoded"),
    [
        ("review-secret-42", "review%2Dsecret%2D42"),
        ("review+secret&42", "review%2Bsecret%2642"),
        ("redacted", "red%61cted"),
    ],
)
def test_native_human_exports_scrub_decoded_current_secret_and_preserve_default_json(
    pipeline, monkeypatch, capsys, secret, encoded
):
    from urllib.parse import unquote_plus

    from sm_trending_products.cli import main
    from sm_trending_products.collector import redact

    pipeline.create_product("encoded", f"Voir https://example.test/?q={encoded}", synthetic=True)
    cutoff = now()
    original = pipeline.decision_brief().describe("encoded", "FR", cutoff, True)
    monkeypatch.setenv("REVIEW_API_SECRET", secret)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    args = [
        "research",
        "brief",
        "--product-id",
        "encoded",
        "--target-country",
        "FR",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
    ]
    assert main(args) == 0
    assert capsys.readouterr().out == canonical(redact(original)) + "\n"
    for output_format in ("text", "html"):
        assert main([*args, "--format", output_format]) == 0
        output = capsys.readouterr().out
        assert secret not in output
        assert secret not in unquote_plus(output)
    assert pipeline.decision_brief().describe("encoded", "FR", cutoff, True) == original


def test_native_unknown_language_and_distribution_are_readable_gaps(pipeline):
    from test_transcript_insights import import_selected, selected

    from sm_trending_products.decision_brief_presentation import present_brief, render_text

    texts, value = selected(pipeline)
    import_selected(texts, value.model_copy(update={"language": None}))
    report = pipeline.decision_brief().describe("p", "FR", now(), True)
    text = render_text(present_brief(report))
    assert "Textes distincts : 1" in text
    assert "langue non prise en charge ou inconnue" in text
    assert "comparabilité non établie" in text
    assert "ne prouvent pas l’absence d’un thème" in text


def test_technical_display_scrubs_scalar_credentials_before_direct_ui_rendering():
    from sm_trending_products.decision_brief_presentation import present_brief

    report = {"name": "probe", "sections": {}, "metric": 123456, "ordinary": 17}
    view = present_brief(report, secret_values=("123456",))
    assert "123456" not in canonical(view.technical)
    assert view.technical["ordinary"] == report["ordinary"] == 17
    assert report["metric"] == 123456


def test_private_json_serialization_cannot_restore_current_unicode_escape_secret(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    pipeline.create_product("json-secret", r"Review \u2026", synthetic=True)
    cutoff = now()
    original = pipeline.decision_brief().describe("json-secret", "FR", cutoff, True)
    monkeypatch.setenv("REVIEW_API_KEY", r"\u2026")
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, [{"product_id":"json-secret"}], utc(CUTOFF), True)
""".replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=20)
    app.text_input[0].set_value("FR").run()
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert all(r"\u2026" not in block.value for block in app.json)
    assert json.loads(app.json[-1].value)["product_id"] == "json-secret"
    assert pipeline.decision_brief().describe("json-secret", "FR", cutoff, True) == original
    app.run()
    assert not app.json


def test_private_french_brief_clears_on_interaction_and_refreshes_retirement(pipeline, monkeypatch):
    from streamlit.testing.v1 import AppTest
    from test_transcript_insights import import_selected, selected

    texts, value = selected(pipeline)
    import_selected(texts, value)
    cutoff = now()
    monkeypatch.setenv("REVIEW_API_SECRET", "gagner du temps")
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, [{"product_id":"p"}], utc(CUTOFF), True)
""".replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.json and not app.text
    app.text_input[0].set_value("FR").run()
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert any("Dossier de décision" in block.value for block in app.text)
    assert any("Éléments défavorables" in block.value for block in app.subheader)
    assert app.json and "gagner du temps" not in app.json[-1].value
    assert (
        json.loads(app.json[-1].value)["sections"]["themes"]["report"]["distinct_text_count"] == 1
    )
    app.run()
    assert not app.json and not app.text
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert json.loads(app.json[-1].value)["sections"]["themes"]["report"]["texts"] == []
    assert all("gagner du temps" not in block.value for block in app.text)


def test_private_selector_masks_current_credentials_without_running_or_changing_analysis(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    from sm_trending_products.decision_brief import DecisionBrief

    identifier = "review-selector-42"
    pipeline.create_product(identifier, "Private selector fixture", synthetic=True)
    cutoff = now()
    monkeypatch.setenv("REVIEW_API_KEY", identifier)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    calls = []
    describe = DecisionBrief.describe

    def counted(service, product_id, *args, **kwargs):
        calls.append(product_id)
        report = describe(service, product_id, *args, **kwargs)
        monkeypatch.setenv("POST_READ_CREDENTIAL", "Private selector fixture")
        return report

    monkeypatch.setattr(DecisionBrief, "describe", counted)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, pipeline.products(True), utc(CUTOFF), True)
""".replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception and not calls
    assert identifier not in app.selectbox[0].options
    app.text_input[0].set_value("FR").run()
    assert not calls and not app.json
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert calls == [identifier]
    assert identifier not in app.json[-1].value
    assert "Private selector fixture" not in app.json[-1].value


def test_selector_keeps_the_exact_product_when_current_credential_labels_change(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    pipeline.create_product("first", "First", synthetic=True)
    pipeline.create_product("second", "Second", synthetic=True)
    monkeypatch.setenv("REVIEW_API_KEY", "second")
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, pipeline.products(True), now(), True)
""").run(timeout=20)
    app.selectbox[0].select_index(1).run()
    assert app.selectbox[0].value == "second" and not app.json
    monkeypatch.setenv("REVIEW_API_KEY", "first")
    app.run()
    assert not app.exception and app.selectbox[0].value == "second" and not app.json


def test_selector_distinguishes_two_masked_products_and_preserves_raw_action_after_rotation(
    pipeline, monkeypatch
):
    from streamlit.testing.v1 import AppTest

    from sm_trending_products.decision_brief import DecisionBrief

    for identifier in ("default-product", "masked-product-a", "masked-product-b"):
        pipeline.create_product(identifier, "Selector fixture", synthetic=True)
    monkeypatch.setenv("FIRST_API_KEY", "masked-product-a")
    monkeypatch.setenv("SECOND_CREDENTIAL", "masked-product-b")
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    calls = []
    describe = DecisionBrief.describe

    def counted(service, product_id, *args, **kwargs):
        calls.append(product_id)
        return describe(service, product_id, *args, **kwargs)

    monkeypatch.setattr(DecisionBrief, "describe", counted)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, pipeline.products(True), now(), True)
""").run(timeout=20)
    assert not app.exception and not calls
    assert all("masked-product" not in label for label in app.selectbox[0].options)
    app.selectbox[0].select_index(1).run()
    assert app.selectbox[0].value == "masked-product-a"
    assert len(set(app.selectbox[0].options)) == 3
    monkeypatch.setenv("FIRST_API_KEY", "default-product")
    app.run()
    assert not app.exception and app.selectbox[0].value == "masked-product-a"
    app.text_input[0].set_value("FR").run()
    assert not calls
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert calls == ["masked-product-a"]
    assert json.loads(app.json[-1].value)["product_id"] == "masked-product-a"


def test_selector_refuses_indistinguishable_safe_labels_before_analysis(pipeline, monkeypatch):
    from streamlit.testing.v1 import AppTest

    from sm_trending_products.decision_brief import DecisionBrief

    pipeline.create_product("first", "First", synthetic=True)
    pipeline.create_product("second", "Second", synthetic=True)
    for key, value in (
        ("LEFT_API_KEY", "first"),
        ("RIGHT_API_KEY", "second"),
        ("ONE_API_KEY", "1"),
        ("TWO_API_KEY", "2"),
    ):
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setattr(
        DecisionBrief,
        "describe",
        lambda *args, **kwargs: pytest.fail("Refused labels must not run analysis"),
    )
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, pipeline.products(True), now(), True)
""").run(timeout=20)
    assert not app.exception and app.error
    assert not app.selectbox and not app.button and not app.json
