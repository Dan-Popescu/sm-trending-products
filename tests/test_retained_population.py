"""The measurement refuses incomplete populations even when aggregate counts match."""

import hashlib
import json
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest
from test_retained_history_runner import RUNNER

from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "fault", ["duplicate_unsampled", "missing_product", "wrong_observation_count"]
)
def test_retained_worker_never_certifies_an_incorrect_requested_population(
    pipeline, tmp_path, monkeypatch, fault
):
    # The sampled oracle covers p0/p3/p5. Replacing p1 with p2 therefore requires
    # the population guard, even though row count, observation sum and samples fit.
    options = RUNNER.arguments(
        [
            "--products",
            "6",
            "--videos-per-product",
            "2",
            "--refresh-waves",
            "0",
            "--output",
            str(tmp_path / "result.json"),
        ]
    )
    directory = tmp_path / "worker-owned"
    directory.mkdir()
    config = {
        **vars(options),
        "directory": str(directory),
        "output": str(options.output),
        "database_url": pipeline.settings.database_url.get_secret_value(),
        "report": {
            "ok": False,
            "status": "starting",
            "phases": {},
            "checks": {},
            "harness_sha256": hashlib.sha256(Path(RUNNER.__file__).read_bytes()).hexdigest(),
        },
    }
    path = tmp_path / "configuration.json"
    path.write_text(json.dumps(config, default=str))
    path.chmod(0o600)
    original = Pipeline.assess

    def inaccurate(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        if fault == "wrong_observation_count":
            result["rows"][0]["coverage"]["observations"] -= 1
        elif fault == "missing_product":
            result["rows"] = [row for row in result["rows"] if row["product_id"] != "p1"]
        else:
            replacement = next(row for row in result["rows"] if row["product_id"] == "p2")
            result["rows"] = [
                deepcopy(replacement) if row["product_id"] == "p1" else row
                for row in result["rows"]
            ]
        return result

    monkeypatch.setattr(Pipeline, "assess", inaccurate)
    monkeypatch.setenv("COLLECTOR_BINARY", pipeline.settings.collector_binary)
    previous = tempfile.tempdir
    message = "observation count differs" if fault == "wrong_observation_count" else "omitted"
    try:
        with pytest.raises(ValueError, match=message):
            RUNNER.worker(path)
    finally:
        tempfile.tempdir = previous
    report = json.loads(options.output.read_text())
    assert report["ok"] is False
    assert report["status"] == "worker_error"
    assert report["seeded_products"] == 6
