"""Optional diagnostics cross the same guarded operator interface as old trends."""

import json

from streamlit.testing.v1 import AppTest
from test_trend_insights import seed

from sm_trending_products.cli import main


def test_cli_opt_in_exposes_disjoint_diagnostics_without_changing_default(
    pipeline, monkeypatch, capsys
):
    _, cutoff = seed(pipeline, gaps=1)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    arguments = [
        "trend-insights",
        "--product-id",
        "trend",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
    ]
    assert main(arguments) == 0
    original = json.loads(capsys.readouterr().out)
    assert "diagnostics" not in original
    assert main([*arguments, "--diagnostics"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["version"] == "descriptive-trends-v2"
    assert result["diagnostics"]["comparisons"]
    assert result["windows"] == original["windows"]


def test_private_opt_in_needs_a_button_and_does_not_cache_source_reports(pipeline, monkeypatch):
    _, cutoff = seed(pipeline, gaps=1)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.trend_dashboard import render_trends
with Pipeline.from_env() as pipeline:
    render_trends(pipeline.trend_insights(), [{"product_id":"trend"}], utc(CUTOFF), True)
""".replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=20)
    app.checkbox[0].check().run()
    assert not app.exception
    assert not app.json
    app.button[0].click().run()
    assert not app.exception
    assert not app.error
    assert app.json
    # Another interaction recomputes/guards rather than keeping a source-derived
    # result inside session state after the original button action.
    app.run()
    assert not app.json
