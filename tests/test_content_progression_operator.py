"""Bounded CLI preflight and private report lifecycle through native services."""

# ruff: noqa: F811 -- imported pytest fixtures are resolved by parameter name.

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_content_progression import seed
from test_transcript_operator import operator_environment, widget  # noqa: F401

from sm_trending_products.content_progression import ContentProgression
from sm_trending_products.content_progression_cli import main
from sm_trending_products.contracts import digest
from sm_trending_products.enrichment import TranscriptContext


@pytest.mark.parametrize("args", [["--max-texts", "PRIVATE"], ["--PRIVATE"]])
def test_parser_never_echoes_inputs_or_loads_configuration(monkeypatch, capsys, args):
    def forbidden(*args, **kwargs):
        raise AssertionError("Configuration reached")

    monkeypatch.setattr("sm_trending_products.settings.Settings.__init__", forbidden)
    with pytest.raises(SystemExit) as stopped:
        main(["--product-id", "p", *args])
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert "PRIVATE" not in captured.err and not captured.out
    assert json.loads(captured.err)["error"] == "invalid_research_arguments"


def test_help_and_bounded_invalid_config_are_local(monkeypatch, capsys, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        main(["--help"])
    assert stopped.value.code == 0
    assert "--max-texts" in capsys.readouterr().out
    file = tmp_path / "PRIVATE-secret.json"
    file.write_bytes(b"x" * 131_073)
    for args in (
        ["--max-rows", "100001"],
        ["--window-hours", "0"],
        ["--max-strata", "129"],
        ["--as-of", "2026-10-28"],
        ["--taxonomy", str(file)],
    ):
        assert main(["--product-id", "p", *args]) == 2
        failure = capsys.readouterr().err
        assert "invalid_research_input" in failure
        assert "PRIVATE" not in failure and "DATABASE_URL" not in failure


def test_native_cli_quotes_are_redacted_and_private_view_clears_each_interaction(
    operator_environment, capsys
):
    pipeline = operator_environment
    _, _, datasets, cutoff = seed(
        pipeline,
        texts=["save time fixture-token <script>alert(1)</script>"] * 2 + ["A blue item"] * 2,
    )
    args = [
        "--product-id",
        "patterns",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
        "--window-hours",
        "12",
    ]
    assert main(args) == 0
    raw = capsys.readouterr().out
    assert "fixture-token" not in raw
    assert json.loads(raw)["eligible_video_count"] == 4
    app = AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import utc
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), utc({cutoff.isoformat()!r}), True)
""").run(timeout=30)
    assert not app.exception and not app.json
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(12)
    widget(app, "button", "Examiner les thèmes et la progression").click().run(timeout=30)
    assert not app.exception and app.json
    assert "fixture-token" not in str(app.json[0].value)
    assert not any(
        isinstance(value, dict) and "themes" in value
        for value in app.session_state._state.filtered_state.values()
    )
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(24).run(timeout=30)
    assert not app.exception and not app.json
    pipeline.catalog.invalidate_dataset(datasets[0], "withdrawn")
    pipeline.catalog.resume_invalidations(max_batches=100, max_seconds=10, synthetic=True)
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["eligible_video_count"] == 3


def test_private_url_normalization_and_cli_do_not_restore_current_secret(
    operator_environment, monkeypatch, capsys
):
    pipeline = operator_environment
    service, values, _, cutoff = seed(pipeline)
    encoded = "review%2Dsecret%2D42"
    secret = "review-secret-42"
    context = TranscriptContext(
        reference=f"https://example.test/?q={encoded}",
        known_at=cutoff - timedelta(hours=12) + timedelta(seconds=1),
    )
    service.import_transcript(values[0].model_copy(update={"context": context}), as_of=cutoff)
    monkeypatch.setenv("REVIEW_API_SECRET", secret)
    app = AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import utc
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), utc({cutoff.isoformat()!r}), True)
""").run(timeout=30)
    assert not app.exception and not app.json
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(12)
    widget(app, "button", "Examiner les thèmes et la progression").click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert all(secret not in block.value and encoded not in block.value for block in app.json)
    assert (
        main(
            [
                "--product-id",
                "patterns",
                "--synthetic",
                "--as-of",
                cutoff.isoformat(),
                "--window-hours",
                "12",
            ]
        )
        == 0
    )
    stdout = capsys.readouterr().out
    assert secret not in stdout and encoded not in stdout


def test_product_selector_is_safe_before_analysis_and_actions_keep_raw_id(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    pipeline.create_product("unselected", "Unselected", synthetic=True)
    _, _, _, cutoff = seed(pipeline)
    monkeypatch.setenv("REVIEW_API_KEY", "patterns")
    monkeypatch.setenv("SECOND_NAV_API_KEY", "unselected")
    describe = ContentProgression.describe
    calls = []

    def capture(service, product_id, *args, **kwargs):
        calls.append(product_id)
        return describe(service, product_id, *args, **kwargs)

    monkeypatch.setattr(ContentProgression, "describe", capture)
    app = AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import utc
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), utc({cutoff.isoformat()!r}), True)
""").run(timeout=30)
    assert not app.exception and not app.json and calls == []
    assert all("patterns" not in label for label in app.selectbox[0].options)
    assert all("unselected" not in label for label in app.selectbox[0].options)
    assert len(app.selectbox[0].options) == len(set(app.selectbox[0].options)) == 2
    app.selectbox[0].set_value("patterns").run(timeout=30)
    assert not app.exception and not app.json and calls == []
    assert app.selectbox[0].value == "patterns"
    # A later action refreshes current credentials independently of navigation.
    monkeypatch.setenv("REVIEW_API_KEY", "save time")
    monkeypatch.setenv("SECOND_NAV_API_KEY", "rotated-nav-key")
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(12)
    widget(app, "button", "Examiner les thèmes et la progression").click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert calls == ["patterns"]
    assert all("save time" not in block.value for block in app.json)


def test_product_selector_refuses_if_safe_labels_cannot_be_distinguished(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    pipeline.create_product("first", "First", synthetic=True)
    pipeline.create_product("second", "Second", synthetic=True)
    for index, value in enumerate(("first", "second", "1", "2")):
        monkeypatch.setenv(f"NAV_{index}_API_KEY", value)
    calls = []

    def forbidden(*args, **kwargs):
        calls.append(1)
        raise AssertionError("Unavailable selector must not run analysis")

    monkeypatch.setattr(ContentProgression, "describe", forbidden)
    app = AppTest.from_string("""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import now
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), now(), True)
""").run(timeout=30)
    assert not app.exception and app.error and not app.selectbox and not app.json
    assert calls == []
    assert all("first" not in error.value and "second" not in error.value for error in app.error)


@pytest.mark.parametrize("credential", ["themes", "distinct_text_count", "null", "2"])
def test_display_does_not_interpret_redacted_report_structure(
    operator_environment, monkeypatch, credential
):
    pipeline = operator_environment
    _, _, _, cutoff = seed(pipeline)
    monkeypatch.setenv("STRUCTURE_API_KEY", credential)
    describe = ContentProgression.describe
    reports = []

    def capture(*args, **kwargs):
        report = describe(*args, **kwargs)
        reports.append(report)
        return report

    monkeypatch.setattr(ContentProgression, "describe", capture)
    app = AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import utc
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), utc({cutoff.isoformat()!r}), True)
""").run(timeout=30)
    assert not app.exception and not app.json and not reports
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(12)
    widget(app, "button", "Examiner les thèmes et la progression").click().run(timeout=30)
    assert not app.exception and not app.error and app.json
    assert len(reports) == 1 and len(app.dataframe) == 1
    table = app.dataframe[0].value
    assert len(table) == len(reports[0]["themes"]) == 7
    assert table.loc[table["Thème"] == "Temps et effort", "Comparaison"].tolist() == [
        "Association observée"
    ]
    assert all(credential not in block.value for block in app.json)
    assert reports[0]["report_id"] == digest(
        {key: value for key, value in reports[0].items() if key != "report_id"}
    )


def test_render_failure_clears_partial_derived_output_with_a_fixed_safe_error(
    operator_environment, monkeypatch
):
    pipeline = operator_environment
    _, _, _, cutoff = seed(pipeline)
    calls = []

    def failure(*args, **kwargs):
        calls.append(1)
        raise ValueError("private-sink-render-failure")

    monkeypatch.setattr("streamlit.dataframe", failure)
    app = AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.content_progression_dashboard import render_patterns
from sm_trending_products.contracts import utc
with Pipeline.from_env() as pipeline:
    render_patterns(pipeline, pipeline.products(True), utc({cutoff.isoformat()!r}), True)
""").run(timeout=30)
    widget(app, "number_input", "Fenêtre observée, en heures").set_value(12)
    widget(app, "button", "Examiner les thèmes et la progression").click().run(timeout=30)
    assert calls == [1] and not app.exception and app.error
    assert not app.json and not app.dataframe
    assert all("private-sink-render-failure" not in error.value for error in app.error)
    assert all("4 textes" not in block.value for block in app.markdown)
