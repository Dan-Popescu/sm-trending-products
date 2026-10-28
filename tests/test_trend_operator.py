"""Operators explore bounded descriptive windows through CLI and private review."""

import json

from streamlit.testing.v1 import AppTest
from test_trend_insights import seed

from sm_trending_products.cli import main


def test_trend_command_keeps_windows_and_known_limits(pipeline, monkeypatch, capsys):
    _, cutoff = seed(pipeline, gaps=1)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    assert (
        main(
            [
                "trend-insights",
                "--product-id",
                "trend",
                "--synthetic",
                "--as-of",
                cutoff.isoformat(),
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert [window["window_hours"] for window in result["windows"]] == [24, 72, 168]
    assert result["windows"][0]["observed_growth_lower"] == 24
    assert result["windows"][0]["predictive_confidence"] is None


def test_private_trend_view_runs_only_after_explicit_request(pipeline, monkeypatch):
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
    assert not app.exception
    assert not app.json
    app.button[0].click().run()
    assert not app.exception
    assert not app.error
    windows = app.dataframe[0].value
    assert list(windows["Hours"]) == [24, 72, 168]
    assert list(windows["Measured growth (lower)"]) == [24, 72, 168]
