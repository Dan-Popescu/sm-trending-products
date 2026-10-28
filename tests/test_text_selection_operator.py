"""Read-only metadata queues and explicit free-local application through operator paths."""

# ruff: noqa: F811 -- imported pytest fixtures are resolved by parameter name.

import json

import pytest
from streamlit.testing.v1 import AppTest
from test_transcript_insights import import_selected, reservation, selected
from test_transcript_operator import operator_environment, widget  # noqa: F401

from sm_trending_products.contracts import canonical, now
from sm_trending_products.enrichment import EnrichmentBudget
from sm_trending_products.text_selection import TextSelection
from sm_trending_products.text_selection_cli import main


@pytest.mark.parametrize(
    "arguments",
    [
        ["preview", "--product-id", "p", "--PRIVATE-FLAG"],
        ["preview", "--product-id", "p", "--max-selected", "PRIVATE-SUBMITTED-VALUE"],
        ["apply", "--file", "private-file", "--request-prefix", "run", "--PRIVATE-FLAG"],
    ],
)
def test_selection_parse_errors_do_not_echo_values_or_flags(monkeypatch, capsys, arguments):
    def blocked(*args, **kwargs):
        raise AssertionError("Argument parsing reached service configuration")

    monkeypatch.setattr("sm_trending_products.settings.Settings.__init__", blocked)
    with pytest.raises(SystemExit) as stopped:
        main(arguments)
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert json.loads(captured.err)["error"] == "invalid_research_arguments"
    assert "PRIVATE" not in captured.err and "private-file" not in captured.err
    assert not captured.out


def test_selection_help_requires_no_configuration(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        main(["apply", "--help"])
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "--request-prefix" in help_text and "--adapter" not in help_text


@pytest.mark.parametrize(
    "arguments",
    [
        ["preview", "--product-id", "p", "--product-id", "p"],
        ["preview", "--product-id", "p", "--after", "q=" + "a" * 64],
        ["preview", "--product-id", "p", "--as-of", "2026-10-28T12:00:00"],
    ],
)
def test_selection_invalid_scope_fails_before_settings(monkeypatch, capsys, arguments):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(arguments) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "invalid_research_input"


def test_selection_config_and_plan_files_are_bounded_preflight(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    file = tmp_path / "secret-input-file.json"
    file.write_text('{"max_candidates":201,"secret":"do-not-print"}')
    assert main(["preview", "--product-id", "p", "--config", str(file)]) == 2
    failure = capsys.readouterr().err
    assert "invalid_research_input" in failure and "do-not-print" not in failure
    file.write_text("x" * 131_073)
    assert main(["apply", "--file", str(file), "--request-prefix", "run"]) == 2
    failure = capsys.readouterr().err
    assert "invalid_research_input" in failure and "secret-input-file" not in failure


def test_selection_cli_preview_apply_replay_and_namespace(operator_environment, tmp_path, capsys):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    assert main(["preview", "--product-id", "p", "--synthetic"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert len(plan["items"]) == 1
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    file = tmp_path / "plan.json"
    file.write_text(json.dumps(plan))
    args = ["apply", "--file", str(file), "--request-prefix", "stable-run", "--synthetic"]
    assert main(args[:-1]) == 2  # Real default cannot silently apply a synthetic plan.
    assert "invalid_research_input" in capsys.readouterr().err
    assert main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["provider"] == "local-rules" and first["state"] == "completed"
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out) == first
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    assert reservation(pipeline, "local-rules")["cost_reserved"] == 0
    pipeline.catalog.invalidate_dataset(value.dataset_id, "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert main(args) == 2
    failure = capsys.readouterr().err
    assert "gagner du temps" not in failure
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_selection_cli_exports_exact_nonsecret_executable_plan(
    operator_environment, tmp_path, capsys, monkeypatch
):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    cutoff = now()
    expected = TextSelection(pipeline.catalog, pipeline.provenance).preview(
        ["p"], synthetic=True, as_of=cutoff
    )
    assert main(["preview", "--product-id", "p", "--synthetic", "--as-of", cutoff.isoformat()]) == 0
    captured = capsys.readouterr()
    assert captured.out == canonical(expected) + "\n" and not captured.err
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    file = tmp_path / "exact-plan.json"
    file.write_text(captured.out)
    assert (
        main(["apply", "--file", str(file), "--request-prefix", "exact-export", "--synthetic"]) == 0
    )
    applied = json.loads(capsys.readouterr().out)
    assert applied["state"] == "completed"
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    secret_request = applied["results"][0]["request_id"]
    monkeypatch.setenv("CURRENT_RESULT_SECRET", secret_request)
    assert (
        main(["apply", "--file", str(file), "--request-prefix", "exact-export", "--synthetic"]) == 0
    )
    captured = capsys.readouterr()
    assert secret_request not in captured.out and "REDACTED" in captured.out
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_selection_cli_refuses_secret_altered_plan_without_stdout_or_reservations(
    operator_environment, monkeypatch, capsys
):
    pipeline = operator_environment
    secret = "current-private-entity"
    service, value = selected(pipeline, entity=secret)
    import_selected(service, value)
    monkeypatch.setenv("CURRENT_EXPORT_SECRET", secret)
    assert main(["preview", "--product-id", "p", "--synthetic"]) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and secret not in captured.err
    assert json.loads(captured.err)["error"] == "selection_plan_not_exportable"
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_daily_budgets") == []


def test_selection_cli_partial_budget_result_is_nonzero(operator_environment, tmp_path, capsys):
    pipeline = operator_environment
    service, value = selected(pipeline)
    service.configure_budget("local-rules", EnrichmentBudget(daily_requests=1), synthetic=True)
    import_selected(service, value)
    import_selected(service, value.model_copy(update={"text": "Another supplied text"}))
    assert main(["preview", "--product-id", "p", "--synthetic"]) == 0
    file = tmp_path / "plan.json"
    file.write_text(capsys.readouterr().out)
    assert main(["apply", "--file", str(file), "--request-prefix", "budget", "--synthetic"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "partial"
    assert {row["state"] for row in result["results"]} == {"completed", "denied"}
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_selection_private_preview_is_explicit_metadata_only_and_not_cached(operator_environment):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.text_selection_dashboard import render_text_selection
with Pipeline.from_env() as pipeline:
    render_text_selection(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    assert not app.exception and not app.json
    widget(app, "multiselect", "Product shortlist for supplied text (maximum ten)").set_value(["p"])
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert not app.exception
    plan = json.loads(app.json[0].value)
    assert len(plan["items"]) == 1 and "gagner du temps" not in str(plan)
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    widget(app, "text_input", "Stable local selection request prefix").set_value("different").run()
    assert not app.json
    pipeline.catalog.invalidate_dataset(value.dataset_id, "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert json.loads(app.json[0].value)["items"] == []
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []


def test_selection_private_saved_plan_apply_replays_and_rechecks_withdrawal(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    service, value = selected(pipeline)
    import_selected(service, value)
    plan = TextSelection(pipeline.catalog, pipeline.provenance).preview(["p"], synthetic=True)

    class PlanUpload:
        data = json.dumps(plan).encode()
        size = len(data)

        def getvalue(self):
            return self.data

    monkeypatch.setattr(
        "streamlit.file_uploader",
        lambda label, **kwargs: PlanUpload() if "Reviewed complete selection" in label else None,
    )
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.text_selection_dashboard import render_text_selection
with Pipeline.from_env() as pipeline:
    render_text_selection(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    assert not app.exception and not app.json
    assert pipeline.catalog.all("SELECT * FROM enrichment_requests") == []
    widget(app, "multiselect", "Product shortlist for supplied text (maximum ten)").set_value(["p"])
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert not app.exception and not app.error
    first = json.loads(app.json[0].value)
    assert first["state"] == "completed" and first["provider"] == "local-rules"
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert json.loads(app.json[0].value) == first
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    secret_request = first["results"][0]["request_id"]
    monkeypatch.setenv("PRIVATE_RESULT_SECRET", secret_request)
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert secret_request not in app.json[0].value and "REDACTED" in app.json[0].value
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1
    pipeline.catalog.invalidate_dataset(value.dataset_id, "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    widget(app, "button", "Apply reviewed plan with free local rules").click().run(timeout=30)
    assert app.error and not app.json
    assert reservation(pipeline, "local-rules")["requests_reserved"] == 1


def test_selection_download_keeps_exact_plan_and_refuses_secret_bearing_metadata(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    secret = "sensitive-entity-label"
    service, value = selected(pipeline, entity=secret)
    import_selected(service, value)
    downloads = []

    def download(label, data, *args, **kwargs):
        downloads.append(data)
        return False

    monkeypatch.setattr("streamlit.download_button", download)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import now
from sm_trending_products.text_selection_dashboard import render_text_selection
with Pipeline.from_env() as pipeline:
    render_text_selection(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    widget(app, "multiselect", "Product shortlist for supplied text (maximum ten)").set_value(["p"])
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert not app.exception and not app.error
    plan = json.loads(app.json[0].value)
    assert json.loads(downloads[-1]) == plan
    # The exact downloaded plan remains executable; the UI never silently modifies its identity.
    result = TextSelection(pipeline.catalog, pipeline.provenance).execute(
        json.loads(downloads[-1]), request_prefix="download-proof"
    )
    assert result["state"] == "completed"
    # A new text gives another selected item after the first plan was completed.
    import_selected(service, value.model_copy(update={"text": "Another supplied description"}))
    downloads.clear()
    monkeypatch.setenv("PRIVATE_METADATA_SECRET", secret)
    widget(app, "button", "Preview useful supplied texts").click().run(timeout=30)
    assert not app.exception and not app.error
    assert secret not in app.json[0].value and "REDACTED" in app.json[0].value
    assert not downloads
    assert any("cannot be exported" in warning.value for warning in app.warning)
