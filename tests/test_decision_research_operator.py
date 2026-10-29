"""The installed command and private view reach the guarded research modules."""

import json

import pytest
from streamlit.testing.v1 import AppTest
from test_trend_insights import seed

from sm_trending_products.cli import main


@pytest.mark.parametrize(
    "command",
    [
        ["research"],
        ["research", "brief"],
        ["research", "validation"],
        ["research", "cohorts"],
        ["research", "economics"],
        ["research", "competition"],
        ["research", "content"],
        ["research", "select-texts"],
    ],
)
def test_research_help_needs_no_credentials_or_application(command, monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setattr(
        Pipeline, "from_env", lambda: pytest.fail("Help must not construct services")
    )
    monkeypatch.delenv("DATABASE_URL", raising=False)
    try:
        code = main([*command, "--help"])
    except SystemExit as stopped:
        code = stopped.code
    assert code == 0
    assert "Example" in capsys.readouterr().out


@pytest.mark.parametrize(
    "options",
    [
        ["--max-rows", "100001"],
        ["--max-products", "10001"],
        ["--text-limit", "0"],
        ["--as-of", "invalid-private-clock"],
        ["--category-id", "category-without-taxonomy"],
        ["--dossier-id", "dossier-without-revision"],
        ["--query-scope", "private-query\n"],
        ["--query-scope", " private-query"],
        ["--query-scope", "private-query\u0000scope"],
    ],
)
def test_brief_invalid_scope_fails_before_application(options, monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setattr(
        Pipeline, "from_env", lambda: pytest.fail("Invalid inputs need no services")
    )
    assert (
        main(
            [
                "research",
                "brief",
                "--product-id",
                "p",
                "--target-country",
                "FR",
                *options,
            ]
        )
        == 2
    )
    assert json.loads(capsys.readouterr().err)["error"] == "invalid_brief_input"


@pytest.mark.parametrize(
    "arguments",
    [
        ["research", "--Bearer-private-review-value"],
        [
            "research",
            "brief",
            "--product-id",
            "p",
            "--target-country",
            "FR",
            "--Bearer-private-review-value",
        ],
        ["research", "brief", "--product-id", "p", "--target-country", "private-review-value"],
    ],
)
def test_shared_parser_errors_never_echo_private_arguments(arguments, capsys):
    with pytest.raises(SystemExit) as stopped:
        main(arguments)
    assert stopped.value.code == 2
    output = capsys.readouterr()
    assert "private-review-value" not in output.err + output.out
    assert json.loads(output.err)["error"].startswith("invalid_")


def test_brief_file_limits_precede_configuration(tmp_path, monkeypatch, capsys):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setattr(
        Pipeline, "from_env", lambda: pytest.fail("Rejected input needs no services")
    )
    options = ["research", "brief", "--product-id", "p", "--target-country", "FR"]
    profile = tmp_path / "private.json"
    for content in (b"x" * (128 * 1024 + 1), b"[]", b'{"private-input":"invalid-profile"}'):
        profile.write_bytes(content)
        assert main([*options, "--matching-profile", str(profile)]) == 2
        error = capsys.readouterr().err
        assert "private-input" not in error and "invalid-profile" not in error
        assert json.loads(error)["error"] == "invalid_brief_input"


def test_installed_brief_command_matches_public_module_and_never_initializes(
    pipeline, monkeypatch, capsys
):
    _, cutoff = seed(pipeline, gaps=1)
    modules = pipeline.catalog.all(
        "SELECT * FROM application_module_migrations ORDER BY module,version"
    )
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    tables_before = pipeline.catalog.all(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
    )
    expected = pipeline.decision_brief().describe("trend", "FR", cutoff, True)
    assert (
        main(
            [
                "research",
                "brief",
                "--product-id",
                "trend",
                "--target-country",
                "FR",
                "--synthetic",
                "--as-of",
                cutoff.isoformat(),
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report == expected
    assert report["sections"]["themes"]["status"] == "not_initialized"
    assert (
        pipeline.catalog.all("SELECT * FROM application_module_migrations ORDER BY module,version")
        == modules
    )
    assert (
        pipeline.catalog.all(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
        )
        == tables_before
    )


def test_private_research_navigation_runs_only_requested_brief_and_clears_it(pipeline, monkeypatch):
    _, cutoff = seed(pipeline, gaps=1)
    modules = pipeline.catalog.all(
        "SELECT * FROM application_module_migrations ORDER BY module,version"
    )
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    tables_before = pipeline.catalog.all(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
    )
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.dashboard import render_dashboard
with Pipeline.from_env() as pipeline:
    render_dashboard(pipeline)
"""
    app = AppTest.from_string(source).run(timeout=30)
    app.sidebar.toggle[0].set_value(True).run()
    app.sidebar.text_input[0].set_value(cutoff.isoformat()).run()
    app.sidebar.radio[0].set_value("Decision research").run()
    assert not app.exception
    assert not app.json
    next(widget for widget in app.text_input if widget.label.startswith("Pays cible")).set_value(
        "FR"
    ).run()
    next(
        widget for widget in app.button if widget.label == "Construire le dossier de décision"
    ).click().run(timeout=30)
    assert not app.exception and not app.error
    assert app.json
    report = json.loads(app.json[-1].value)
    assert report["product_id"] == "trend" and report["target_country"] == "FR"
    assert report["synthetic"] is True
    app.run()
    assert not app.json
    # Other reports render controls only. Optional modules are initialized solely
    # by their explicit action buttons, never by navigating to a view.
    report_selector = next(widget for widget in app.selectbox if widget.label == "Research report")
    for view in report_selector.options:
        next(widget for widget in app.selectbox if widget.label == "Research report").select(
            view
        ).run(timeout=30)
        assert not app.exception, view
    assert (
        pipeline.catalog.all("SELECT * FROM application_module_migrations ORDER BY module,version")
        == modules
    )
    assert (
        pipeline.catalog.all(
            "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
        )
        == tables_before
    )


def test_private_brief_hides_newly_configured_secrets_in_source_quotes(pipeline, monkeypatch):
    from test_transcript_insights import import_selected, selected

    from sm_trending_products.contracts import canonical, now

    texts, value = selected(pipeline)
    import_selected(texts, value)
    cutoff = now()
    expected = pipeline.decision_brief().describe("p", "FR", cutoff, True)
    assert "gagner du temps" in canonical(expected)
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
    app.text_input[0].set_value("FR").run()
    app.button[0].click().run(timeout=30)
    assert not app.exception and not app.error
    assert app.json
    assert all("gagner du temps" not in block.value for block in app.json)
    # Presentation redaction never rewrites the evidence-owning report.
    assert pipeline.decision_brief().describe("p", "FR", cutoff, True) == expected
