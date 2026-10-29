"""Safe standalone CLI and private-view comparison behavior."""

import json
import os
import subprocess
import sys
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_decision_changes import seed

from sm_trending_products.contracts import now
from sm_trending_products.decision_changes import DecisionChanges
from sm_trending_products.decision_changes_cli import main


@pytest.mark.parametrize("option", ["--help", "--examples"])
def test_standalone_help_and_examples_are_environment_free(option, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    result = subprocess.run(
        [sys.executable, "-m", "sm_trending_products.decision_changes_cli", option],
        capture_output=True,
        text=True,
        timeout=20,
        env=os.environ.copy(),
    )
    assert result.returncode == 0
    assert "--product-id" in result.stdout and not result.stderr


@pytest.mark.parametrize(
    "options",
    [
        ["--as-of", "PRIVATE-CLOCK"],
        ["--max-rows", "100001"],
        ["--max-products", "0"],
        ["--text-limit", "201"],
        ["--previous-as-of", "2026-10-29T00:00:00Z", "--as-of", "2026-10-28T00:00:00Z"],
    ],
)
def test_invalid_input_precedes_settings_and_never_echoes_content(options, monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setattr(
        Pipeline, "from_env", lambda: pytest.fail("Invalid input needs no services")
    )
    assert main(["--product-id", "p", "--target-country", "FR", *options]) == 2
    result = capsys.readouterr()
    assert json.loads(result.err)["error"] == "invalid_changes_input"
    assert "PRIVATE" not in result.err + result.out


@pytest.mark.parametrize(
    "arguments",
    [
        ["--PRIVATE-TOKEN"],
        ["--product-id", "PRIVATE\nVALUE"],
        ["--target-country", "PRIVATE"],
        ["--max-rows", "PRIVATE"],
    ],
)
def test_parser_errors_never_echo_arguments(arguments, capsys):
    with pytest.raises(SystemExit) as stopped:
        main(arguments)
    assert stopped.value.code == 2
    result = capsys.readouterr()
    assert json.loads(result.err)["error"] == "invalid_changes_arguments"
    assert "PRIVATE" not in result.err + result.out


def test_selection_file_bounds_and_semantics_precede_configuration(tmp_path, monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setattr(
        Pipeline, "from_env", lambda: pytest.fail("Invalid input needs no services")
    )
    path = tmp_path / "PRIVATE.json"
    for content in (
        b"x" * (128 * 1024 + 1),
        b"[]",
        b'{"PRIVATE":"TOKEN"}',
        b'{"dossier_id":"alone"}',
        b'{"expected_revision":true}',
    ):
        path.write_bytes(content)
        assert (
            main(["--product-id", "p", "--target-country", "FR", "--previous-selection", str(path)])
            == 2
        )
        result = capsys.readouterr()
        assert json.loads(result.err)["error"] == "invalid_changes_input"
        assert "PRIVATE" not in result.err + result.out


@pytest.mark.integration
def test_standalone_cli_reaches_public_comparison_with_explicit_default_cutoff(
    pipeline, monkeypatch, capsys
):
    previous, current, _ = seed(pipeline)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    expected = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
        "p1",
        "FR",
        previous,
        current,
        True,
    )
    assert (
        main(
            [
                "--product-id",
                "p1",
                "--target-country",
                "FR",
                "--synthetic",
                "--as-of",
                current.isoformat(),
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result == expected
    assert result["previous_as_of"] == (current - timedelta(hours=24)).isoformat()
    assert (
        main(
            [
                "--product-id",
                "p1",
                "--target-country",
                "FR",
                "--synthetic",
                "--as-of",
                current.isoformat(),
                "--format",
                "text",
            ]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "multiplié par 2" in text
    assert "PRIVATE SOURCE TITLE" not in text


@pytest.mark.integration
def test_private_comparison_runs_on_action_and_clears_after_every_interaction(
    pipeline, monkeypatch
):
    _, current, _ = seed(pipeline)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_changes_dashboard import render_changes
with Pipeline.from_env() as pipeline:
    render_changes(pipeline, [{"product_id":"p1"}], utc(CUTOFF), True)
""".replace("CUTOFF", repr(current.isoformat()))
    app = AppTest.from_string(source).run(timeout=30)
    assert not app.exception and not app.json
    next(widget for widget in app.text_input if widget.key == "changes_country").set_value(
        "FR"
    ).run()
    assert not app.json
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert app.json
    report = json.loads(app.json[-1].value)
    assert report["product_id"] == "p1"
    assert "PRIVATE SOURCE TITLE" not in app.json[-1].value
    app.run()
    assert not app.json
    next(widget for widget in app.text_input if widget.key == "changes_country").set_value(
        "PRIVATE"
    ).run()
    app.button[0].click().run(timeout=30)
    assert app.error and not app.json


def test_equal_or_reversed_cutoffs_reject_before_database():
    class Unavailable:
        def __getattr__(self, name):
            raise AssertionError("Invalid chronology reached infrastructure")

    infrastructure, current = Unavailable(), now()
    for previous in (current, current + timedelta(seconds=1)):
        with pytest.raises(ValueError, match="chronological"):
            DecisionChanges(infrastructure, infrastructure, infrastructure).compare(
                "p",
                "FR",
                previous,
                current,
            )


@pytest.mark.integration
@pytest.mark.parametrize("secret_key", ["API_KEY", "ACCESS_KEY", "CREDENTIAL", "DSN"])
def test_cli_redacts_current_secret_identifiers_in_json_and_text(
    pipeline, monkeypatch, capsys, secret_key
):
    marker = "review-api-key-42"
    pipeline.create_product(marker, "Reviewed item", synthetic=True)
    cutoff = now() + timedelta(days=3)
    previous = cutoff - timedelta(hours=24)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv(f"RESEARCH_{secret_key}", marker)
    raw = DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
        marker,
        "FR",
        previous,
        cutoff,
        True,
    )
    for format_name in ("json", "text"):
        assert (
            main(
                [
                    "--product-id",
                    marker,
                    "--target-country",
                    "FR",
                    "--synthetic",
                    "--previous-as-of",
                    previous.isoformat(),
                    "--as-of",
                    cutoff.isoformat(),
                    "--format",
                    format_name,
                ]
            )
            == 0
        )
        output = capsys.readouterr().out
        assert marker not in output
        if format_name == "json":
            assert json.loads(output)["product_id"] != marker
    assert raw["product_id"] == marker
    assert (
        DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
            marker,
            "FR",
            previous,
            cutoff,
            True,
        )
        == raw
    )


@pytest.mark.integration
def test_numeric_secret_does_not_break_json_or_measured_text_output(pipeline, monkeypatch, capsys):
    previous, current, _ = seed(pipeline)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("RESEARCH_API_KEY", "2")
    for format_name in ("json", "text"):
        assert (
            main(
                [
                    "--product-id",
                    "p1",
                    "--target-country",
                    "FR",
                    "--synthetic",
                    "--previous-as-of",
                    previous.isoformat(),
                    "--as-of",
                    current.isoformat(),
                    "--format",
                    format_name,
                ]
            )
            == 0
        )
        output = capsys.readouterr().out
        assert "2" not in output
        if format_name == "json":
            assert json.loads(output)["product_id"] == "p1"


@pytest.mark.integration
def test_private_view_refreshes_api_key_redaction_on_action(pipeline, monkeypatch):
    marker = "review-api-key-42"
    pipeline.create_product(marker, "Reviewed item", synthetic=True)
    cutoff = now() + timedelta(days=3)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_changes_dashboard import render_changes
with Pipeline.from_env() as pipeline:
    render_changes(pipeline, [{"product_id":PRODUCT}], utc(CUTOFF), True)
""".replace("PRODUCT", repr(marker)).replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=30)
    assert not app.exception and not app.json
    monkeypatch.setenv("RESEARCH_API_KEY", marker)
    next(widget for widget in app.text_input if widget.key == "changes_country").set_value(
        "FR"
    ).run()
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert app.json and all(marker not in block.value for block in app.json)
    assert all(marker not in block.value for block in app.markdown)
    app.run()
    assert not app.json


@pytest.mark.integration
def test_private_product_labels_are_safe_before_action_but_keep_the_original_selection(
    pipeline, monkeypatch
):
    marker, rotated = "private-selector-key-417", "rotated-selector-key-418"
    pipeline.create_product(marker, "Selected item", synthetic=True)
    pipeline.create_product(rotated, "Other item", synthetic=True)
    cutoff = now() + timedelta(days=3)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("RESEARCH_API_KEY", marker)
    calls, original = [], DecisionChanges.compare

    def compare(service, product_id, *args, **kwargs):
        calls.append(product_id)
        return original(service, product_id, *args, **kwargs)

    monkeypatch.setattr(DecisionChanges, "compare", compare)
    source = (
        """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_changes_dashboard import render_changes
with Pipeline.from_env() as pipeline:
    render_changes(pipeline, [{"product_id":OTHER}, {"product_id":PRODUCT}], utc(CUTOFF), True)
""".replace("PRODUCT", repr(marker))
        .replace("OTHER", repr(rotated))
        .replace("CUTOFF", repr(cutoff.isoformat()))
    )
    app = AppTest.from_string(source).run(timeout=30)
    assert not app.exception and not app.json and calls == []
    selector = next(widget for widget in app.selectbox if widget.key == "changes_product")
    selector.set_value(marker).run()
    selector = next(widget for widget in app.selectbox if widget.key == "changes_product")
    assert selector.value == marker
    assert all(marker not in option for option in selector.options)
    monkeypatch.setenv("RESEARCH_API_KEY", rotated)
    monkeypatch.setenv("RESEARCH_ACCESS_KEY", marker)
    app.run()
    selector = next(widget for widget in app.selectbox if widget.key == "changes_product")
    assert selector.value == marker and calls == []
    assert len(set(selector.options)) == 2
    assert all(marker not in option and rotated not in option for option in selector.options)
    next(widget for widget in app.text_input if widget.key == "changes_country").set_value(
        "FR"
    ).run()
    assert calls == []
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error and calls == [marker]
    assert app.json and all(marker not in block.value for block in app.json)
    app.run()
    assert not app.json and calls == [marker]
