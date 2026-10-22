"""The actual operator view distinguishes frozen, matured and replayed results."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_evaluation_integration import outcome_config, record, seed_product

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_operator_inspects_original_forward_outcome_and_backtest_versions(pipeline, monkeypatch):
    entity, _, _ = seed_product(pipeline, "reviewed-opportunity")
    cutoff = now() + timedelta(minutes=1)
    pipeline.import_records(
        [record(entity, cutoff, 300), record(entity, cutoff + timedelta(days=3), 1800)],
        synthetic=True,
    )
    config = outcome_config()
    forward = pipeline.freeze(cutoff, True, config)
    matured = pipeline.mature(forward["evaluation_id"], cutoff + timedelta(days=3))
    backtest = pipeline.backtest([cutoff], cutoff + timedelta(days=3), True, config)
    assert backtest["result"]["snapshots"] == [forward["result"]]
    assert backtest["result"]["reports"] == [matured["result"]]
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(pipeline.settings)))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.toggle[0].set_value(True).run(timeout=20)
    app.radio(key="review_view").set_value("Datasets and evaluations").run(timeout=20)
    assert not app.exception
    summaries = next(frame.value for frame in app.dataframe if "evaluation_id" in frame.value)
    assert set(summaries["kind"]) == {"forward", "outcome", "backtest"}
    assert all(summaries["synthetic"])
    for kind, result in (("forward", forward), ("outcome", matured), ("backtest", backtest)):
        selected = app.selectbox[0].options.index(result["evaluation_id"])
        app.selectbox[0].select_index(selected).run(timeout=20)
        assert not app.exception
        displayed = next(
            json.loads(value.value) for value in app.json if "evaluation_id" in value.value
        )
        assert displayed["evaluation_id"] == result["evaluation_id"]
        assert displayed["kind"] == kind and displayed["synthetic"] is True
        assert displayed["payload"] == result["result"]
    app.toggle[0].set_value(False).run(timeout=20)
    assert not app.exception
    assert not any("evaluation_id" in frame.value for frame in app.dataframe)
    assert pipeline.evaluations(False) == []
