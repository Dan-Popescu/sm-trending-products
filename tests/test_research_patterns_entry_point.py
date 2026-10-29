"""The installed route reaches a measured multi-creator progression comparison."""

import json

from test_content_progression import seed

from sm_trending_products.cli import main
from sm_trending_products.contracts import canonical


def test_installed_patterns_matches_public_service(pipeline, monkeypatch, capsys):
    _, _, _, cutoff = seed(pipeline)
    expected = pipeline.content_progression().describe("patterns", cutoff, True, window_hours=12)
    assert expected["eligible_video_count"] == 4
    assert expected["comparison_status"] == "available"
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    assert (
        main(
            [
                "research",
                "patterns",
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
    assert json.loads(capsys.readouterr().out) == json.loads(canonical(expected))
