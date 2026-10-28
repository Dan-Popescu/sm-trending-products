"""Native operator paths for selected supplied text, local reuse and source withdrawal."""

from __future__ import annotations

import json

import pytest
from streamlit.testing.v1 import AppTest
from test_transcript_insights import selected

from sm_trending_products.cli import main

pytestmark = pytest.mark.integration


@pytest.fixture
def operator_environment(pipeline, monkeypatch):
    # New CLI/UI Pipeline instances must share only this test's SQL and object-store scope.
    for key, value in {
        "S3_BUCKET": pipeline.settings.s3_bucket,
        "S3_ENDPOINT_URL": pipeline.settings.s3_endpoint_url,
        "CACHE_DIR": str(pipeline.settings.cache_dir),
        "SPOOL_DIR": str(pipeline.settings.spool_dir),
        "COLLECTOR_BINARY": pipeline.settings.collector_binary,
        "HISTORY_PARTITION_BUCKETS": str(pipeline.settings.history_partition_buckets),
    }.items():
        monkeypatch.setenv(key, value)
    return pipeline


def command(capsys, *arguments):
    code = main(["transcripts", *arguments])
    captured = capsys.readouterr()
    return code, json.loads(captured.out or captured.err)


def widget(app, kind, label):
    return next(item for item in getattr(app, kind) if item.label == label)


def review_app() -> AppTest:
    return AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.transcript_dashboard import render_transcripts
with Pipeline.from_env() as pipeline:
    render_transcripts(pipeline.transcript_insights(), pipeline.products(True), True)
""").run(timeout=30)


def test_transcript_help_is_available_without_configuration(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        main(["transcripts", "--help"])
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "init" in help_text and "No media download or paid provider" in help_text
    with pytest.raises(SystemExit) as stopped:
        main(["transcripts", "analyze", "--help"])
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "--request-id" in help_text and "--transcript-id" in help_text
    assert "--adapter" not in help_text


def test_optional_transcript_tables_require_explicit_operator_initialization(
    operator_environment, capsys
):
    pipeline = operator_environment
    baseline = pipeline.catalog.all("SELECT version FROM schema_migrations ORDER BY version")
    code, failure = command(capsys, "list", "--product-id", "unknown", "--synthetic")
    assert code == 2 and failure["error"] == "transcript_operation_failed"
    assert (
        pipeline.catalog.one("SELECT to_regclass('enrichment_transcripts') AS table")["table"]
        is None
    )
    pipeline.create_product("unselected", "Unselected product", synthetic=True)
    app = review_app()
    assert not app.exception and not app.json
    assert (
        pipeline.catalog.one("SELECT to_regclass('enrichment_transcripts') AS table")["table"]
        is None
    )
    code, initialized = command(capsys, "init")
    assert code == 0 and initialized["status"] == "initialized"
    assert command(capsys, "init") == (0, initialized)
    assert (
        pipeline.catalog.all("SELECT version FROM schema_migrations ORDER BY version") == baseline
    )


def test_cli_import_local_analysis_replay_list_and_source_withdrawal(
    operator_environment, tmp_path, capsys
):
    pipeline = operator_environment
    _service, value = selected(pipeline)
    file = tmp_path / "supplied-text.json"
    file.write_text(value.model_dump_json())
    code, imported = command(capsys, "import", "--file", str(file))
    assert code == 0
    transcript = imported["transcript_id"]
    arguments = (
        "analyze",
        "--product-id",
        "p",
        "--transcript-id",
        transcript,
        "--request-id",
        "operator-request",
        "--synthetic",
    )
    code, result = command(capsys, *arguments)
    assert code == 0 and result["state"] == "completed"
    assert result["provider_id"] == "local-rules" and result["reserved_cost"] == "0"
    assert result["classification_status"] == "unvalidated_text_interpretation"
    assert result["effectiveness"] == "not_measured"
    assert all(
        segment["quote"] == value.text[segment["start"] : segment["end"]]
        for segment in result["segments"]
    )
    assert command(capsys, *arguments) == (0, result)
    assert command(
        capsys, "show", "--product-id", "p", "--request-id", "operator-request", "--synthetic"
    ) == (0, result)
    code, page = command(capsys, "list", "--product-id", "p", "--synthetic", "--limit", "1")
    assert code == 0 and len(page["items"]) == 1 and "segments" not in page["items"][0]
    assert pipeline.catalog.one(
        "SELECT requests_reserved,cost_reserved FROM enrichment_daily_budgets"
    ) == {
        "requests_reserved": 1,
        "cost_reserved": 0,
    }
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    code, failure = command(
        capsys, "show", "--product-id", "p", "--request-id", "operator-request", "--synthetic"
    )
    assert code == 2 and failure["error"].startswith("enrichment_")
    assert "quote" not in failure
    assert command(capsys, "list", "--product-id", "p", "--synthetic")[1]["items"] == []


def test_private_ui_import_analyze_read_and_withdrawal_without_cached_output(operator_environment):
    pipeline = operator_environment
    _service, value = selected(pipeline)
    app = review_app()
    assert not app.exception and not app.json
    inputs = {
        "Confirmed video entity key": value.entity_key,
        "Committed source dataset ID": value.dataset_id,
        "Source observation ID": value.observation_id,
        "Permission reference": value.permission_reference,
        "Language (optional, for example fr)": "fr",
    }
    for label, text in inputs.items():
        widget(app, "text_input", label).set_value(text)
    widget(app, "text_area", "Supplied text").set_value(value.text)
    widget(app, "button", "Import selected text").click().run()
    assert not app.exception and not app.error
    imported = json.loads(app.json[0].value)
    widget(app, "text_input", "Imported transcript ID").set_value(imported["transcript_id"])
    request = widget(
        app, "text_input", "Analysis request ID (reuse to read the same request)"
    ).value
    widget(app, "button", "Analyze selected text with local rules").click().run()
    assert not app.exception and not app.error
    result = json.loads(app.json[0].value)
    assert result["request_id"] == request and result["provider_id"] == "local-rules"
    app.run()
    assert not app.json
    assert (
        widget(app, "text_input", "Analysis request ID (reuse to read the same request)").value
        == request
    )
    widget(app, "button", "Read existing analysis request").click().run()
    assert json.loads(app.json[0].value) == result
    pipeline.catalog.invalidate_dataset(value.dataset_id, "source_removed")
    app.run()
    assert not app.json
    widget(app, "button", "Read existing analysis request").click().run()
    assert not app.exception and app.error and not app.json


def test_private_ui_keeps_request_identity_when_switching_products(operator_environment):
    pipeline = operator_environment
    selected(pipeline)
    pipeline.create_product("q", "Another selected product", synthetic=True)
    app = review_app()
    assert not app.exception
    label = "Analysis request ID (reuse to read the same request)"
    request = widget(app, "text_input", label).value
    widget(app, "selectbox", "Product for transcript insights").select("q").run()
    assert widget(app, "text_input", label).value != request
    widget(app, "selectbox", "Product for transcript insights").select("p").run()
    assert widget(app, "text_input", label).value == request
    widget(app, "text_input", label).set_value("manual-request")
    widget(app, "button", "Read existing analysis request").click().run()
    assert app.error and not app.json
    widget(app, "selectbox", "Product for transcript insights").select("q").run()
    widget(app, "selectbox", "Product for transcript insights").select("p").run()
    assert widget(app, "text_input", label).value == "manual-request"


def test_cli_explicit_budget_reuses_free_work_and_refuses_more_new_work(
    operator_environment, tmp_path, capsys, monkeypatch
):
    pipeline = operator_environment
    _service, value = selected(pipeline)
    from sm_trending_products.collector import Collector

    def unexpected_collection(*args, **kwargs):
        pytest.fail("Transcript operator commands must not collect or download source data")

    monkeypatch.setattr(Collector, "run", unexpected_collection)
    monkeypatch.setattr(Collector, "capture", unexpected_collection)
    budget = tmp_path / "budget.json"
    budget.write_text(json.dumps({"daily_requests": 1}))
    code, configured = command(capsys, "budget", "--file", str(budget), "--synthetic")
    assert code == 0 and configured["provider"] == "local-rules"
    assert configured["daily_requests"] == 1 and configured["allow_external"] is False
    assert command(capsys, "budget", "--file", str(budget), "--synthetic") == (0, configured)
    supplied = tmp_path / "supplied.json"
    supplied.write_text(value.model_dump_json())
    transcript = command(capsys, "import", "--file", str(supplied))[1]["transcript_id"]
    arguments = ("--product-id", "p", "--transcript-id", transcript, "--synthetic")
    code, first = command(capsys, "analyze", *arguments, "--request-id", "first")
    assert code == 0
    code, reused = command(capsys, "analyze", *arguments, "--request-id", "alias")
    assert code == 0 and reused["analysis_id"] == first["analysis_id"]
    supplied.write_text(
        value.model_copy(update={"text": "Another text helps save time."}).model_dump_json()
    )
    transcript = command(capsys, "import", "--file", str(supplied))[1]["transcript_id"]
    code, denied = command(
        capsys,
        "analyze",
        "--product-id",
        "p",
        "--transcript-id",
        transcript,
        "--synthetic",
        "--request-id",
        "more-work",
    )
    assert code == 2 and denied["error"] == "enrichment_daily_budget_exhausted"
    reserved = pipeline.catalog.one(
        "SELECT provider_id,requests_reserved,cost_reserved FROM enrichment_daily_budgets"
    )
    assert reserved == {"provider_id": "local-rules", "requests_reserved": 1, "cost_reserved": 0}
    assert pipeline.catalog.one("SELECT count(*) AS count FROM enrichment_analyses")["count"] == 1


def test_cli_keeps_source_kind_and_authoritative_source_namespace_checks(
    operator_environment, tmp_path, capsys
):
    pipeline = operator_environment
    _service, value = selected(pipeline)
    supplied = tmp_path / "description.json"
    supplied.write_text(
        value.model_copy(
            update={
                "source_kind": "description",
                "observation_id": "not-delivered",
            }
        ).model_dump_json()
    )
    code, rejected = command(capsys, "import", "--file", str(supplied))
    assert code == 2 and rejected["error"] == "enrichment_source_observation_unavailable"
    assert (
        pipeline.catalog.one("SELECT count(*) AS count FROM enrichment_transcripts")["count"] == 0
    )
    supplied.write_text(value.model_copy(update={"source_kind": "description"}).model_dump_json())
    transcript = command(capsys, "import", "--file", str(supplied))[1]["transcript_id"]
    code, rejected = command(
        capsys,
        "analyze",
        "--product-id",
        "p",
        "--transcript-id",
        transcript,
        "--request-id",
        "wrong-workspace",
    )
    assert code == 2 and rejected["error"] == "enrichment_transcript_unavailable"
    code, result = command(
        capsys,
        "analyze",
        "--product-id",
        "p",
        "--transcript-id",
        transcript,
        "--request-id",
        "description",
        "--synthetic",
    )
    assert code == 0 and result["source_kind"] == "description"
    assert result["speech_evidence"] is False


def test_cli_bounds_import_bytes_and_text_without_echoing_rejected_content(
    operator_environment, tmp_path, capsys, monkeypatch
):
    pipeline = operator_environment
    _service, value = selected(pipeline)
    supplied = tmp_path / "too-long.json"
    supplied.write_text(value.model_copy(update={"text": "x" * 16001}).model_dump_json())
    code, failure = command(capsys, "import", "--file", str(supplied))
    assert code == 2 and failure["error"] == "invalid_transcript_record"
    assert "xxx" not in json.dumps(failure)
    supplied.write_bytes(b" " * 131073)
    monkeypatch.delenv("DATABASE_URL")
    # The byte cap applies before attempting runtime configuration or database access.
    code, failure = command(capsys, "import", "--file", str(supplied))
    assert code == 2 and failure["error"] == "transcript_operation_failed"
    assert (
        pipeline.catalog.one("SELECT count(*) AS count FROM enrichment_transcripts")["count"] == 0
    )


def test_cli_and_private_ui_redact_secrets_while_rendering_quoted_html_as_text(
    operator_environment, tmp_path, capsys, monkeypatch
):
    pipeline = operator_environment
    service, value = selected(pipeline)
    secret = "operator-test-secret"
    monkeypatch.setenv("TIKTOK_DISPLAY_ACCESS_TOKEN", secret)
    text = f"Helps save time <script>fixture()</script> Bearer {secret}! Link in bio."
    supplied = tmp_path / "text.json"
    supplied.write_text(value.model_copy(update={"text": text}).model_dump_json())
    transcript = command(capsys, "import", "--file", str(supplied))[1]["transcript_id"]
    code, cli_result = command(
        capsys,
        "analyze",
        "--product-id",
        "p",
        "--transcript-id",
        transcript,
        "--request-id",
        "quoted-html",
        "--synthetic",
    )
    assert code == 0 and secret not in json.dumps(cli_result)
    assert "<script>fixture()</script>" in json.dumps(cli_result)
    # Source fidelity remains intact in the guarded evidence interface; presentation alone redacts.
    original = service.result("quoted-html", "p", synthetic=True)
    assert all(
        segment["quote"] == text[segment["start"] : segment["end"]]
        for segment in original["segments"]
    )
    app = review_app()
    assert not app.exception and not app.json
    widget(app, "text_input", "Analysis request ID (reuse to read the same request)").set_value(
        "quoted-html"
    )
    widget(app, "button", "Read existing analysis request").click().run()
    assert not app.exception and not app.error
    assert secret not in app.json[0].value
    assert "<script>fixture()</script>" in app.json[0].value
    assert not app.get("html") and not app.get("iframe")
    assert (
        pipeline.catalog.one("SELECT requests_reserved FROM enrichment_daily_budgets")[
            "requests_reserved"
        ]
        == 1
    )
