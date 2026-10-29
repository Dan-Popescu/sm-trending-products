"""Real source-bound quoted panels through CLI and private UI."""

# ruff: noqa: F811 -- imported pytest fixtures are resolved by parameter name.

import json

import pytest
from streamlit.testing.v1 import AppTest
from test_transcript_insights import import_selected, selected
from test_transcript_operator import operator_environment, widget  # noqa: F401

from sm_trending_products.content_research_cli import main
from sm_trending_products.content_research_dashboard import _read_upload
from sm_trending_products.contracts import now


@pytest.mark.parametrize("arguments", [["--limit", "PRIVATE-SUBMITTED-VALUE"], ["--PRIVATE-FLAG"]])
def test_content_parse_errors_never_echo_arguments_before_settings(monkeypatch, capsys, arguments):
    def blocked(*args, **kwargs):
        raise AssertionError("Argument parsing reached service configuration")

    monkeypatch.setattr("sm_trending_products.settings.Settings.__init__", blocked)
    with pytest.raises(SystemExit) as stopped:
        main(["--product-id", "p", *arguments])
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    failure = json.loads(captured.err)
    assert failure["error"] == "invalid_research_arguments"
    assert "PRIVATE" not in captured.err and not captured.out


def test_content_help_requires_no_configuration(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        main(["--help"])
    assert stopped.value.code == 0
    assert "--taxonomy" in capsys.readouterr().out


def test_large_ui_upload_is_rejected_before_copying_its_body():
    class LargeUpload:
        size = 131_073

        def getvalue(self):
            raise AssertionError("An oversized upload must not be copied")

    with pytest.raises(ValueError, match="128 KiB"):
        _read_upload(LargeUpload())


@pytest.mark.parametrize(
    "arguments",
    [
        ["--limit", "201"],
        ["--after", "not-a-cursor"],
        ["--as-of", "2026-10-28T12:00:00"],
    ],
)
def test_invalid_content_scope_is_rejected_before_configuration(monkeypatch, capsys, arguments):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["--product-id", "p", *arguments]) == 2
    failure = json.loads(capsys.readouterr().err)
    assert failure["error"] == "invalid_research_input"
    assert "DATABASE_URL" not in str(failure)


def test_taxonomy_file_is_bounded_and_redacted_before_configuration(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    file = tmp_path / "private-token-do-not-print.json"
    for data in ('{"review_reference":"secret-input"}', "[]", "x" * 131_073):
        file.write_text(data)
        assert main(["--product-id", "p", "--taxonomy", str(file)]) == 2
        failure = capsys.readouterr().err
        assert "invalid_research_input" in failure
        assert "private-token" not in failure and "secret-input" not in failure


def test_content_cli_pages_cutoff_namespace_and_withdrawal(operator_environment, capsys):
    pipeline = operator_environment
    service, value = selected(pipeline)
    for text in ("It can save time.", "It is expensive."):
        import_selected(service, value.model_copy(update={"text": text, "language": "en"}))
    cutoff = now().isoformat()
    args = ["--product-id", "p", "--synthetic", "--as-of", cutoff, "--limit", "1"]
    assert main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["distinct_text_count"] == 1 and first["next_cursor"]
    assert main([*args, "--after", first["next_cursor"]]) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["distinct_text_count"] == 1 and second["next_cursor"] is None
    assert first["texts"][0]["content_key"] != second["texts"][0]["content_key"]
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    assert main(["--product-id", "p"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "not_available"
    pipeline.catalog.invalidate_dataset(value.dataset_id, "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert main(args) == 0
    withdrawn = json.loads(capsys.readouterr().out)
    assert withdrawn["status"] == "not_available" and not withdrawn["texts"]


def test_content_private_panel_requires_button_and_drops_withdrawn_quotes(operator_environment):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.content_research_dashboard import render_content
with Pipeline.from_env() as pipeline:
    render_content(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    assert not app.exception and not app.json
    widget(app, "button", "Inspect quoted themes").click().run(timeout=30)
    assert not app.exception
    result = json.loads(app.json[0].value)
    assert result["distinct_text_count"] == 1
    assert "gagner du temps" in str(result)
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    # Any interaction clears source-derived output; it is never stored in session state.
    widget(app, "number_input", "Maximum transcript rows in this panel").set_value(10).run()
    assert not app.json
    pipeline.catalog.invalidate_dataset(value.dataset_id, "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    widget(app, "button", "Inspect quoted themes").click().run(timeout=30)
    result = json.loads(app.json[0].value)
    assert result["status"] == "not_available" and "gagner du temps" not in str(result)


def test_content_private_panel_redacts_environment_secrets_in_quoted_evidence(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    # Evidence may predate a credential binding; output protection must use current secrets.
    monkeypatch.setenv("PRIVATE_CONTENT_SECRET", "gagner du temps")
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.content_research_dashboard import render_content
with Pipeline.from_env() as pipeline:
    render_content(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    widget(app, "button", "Inspect quoted themes").click().run(timeout=30)
    assert not app.exception and not app.error
    result = app.json[0].value
    assert "gagner du temps" not in result and "…" in result
