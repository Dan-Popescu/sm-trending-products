"""The installed route reaches a real guarded comparison with actual progression."""

import json

from test_decision_changes import seed

from sm_trending_products.cli import main
from sm_trending_products.contracts import canonical


def test_installed_changes_matches_public_service(pipeline, monkeypatch, capsys):
    previous, cutoff, _ = seed(pipeline)
    expected = pipeline.decision_changes().compare("p1", "FR", previous, cutoff, True)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    assert (
        main(
            [
                "research",
                "changes",
                "--product-id",
                "p1",
                "--target-country",
                "FR",
                "--synthetic",
                "--as-of",
                cutoff.isoformat(),
                "--previous-as-of",
                previous.isoformat(),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == json.loads(canonical(expected))
