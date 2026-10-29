"""Report adapters protect final serialized values without interpreting masked structure."""

import copy
import importlib
import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest


@pytest.mark.parametrize(
    ("module_name", "helper"),
    [
        ("cohort_research_dashboard", "_safe_details"),
        ("research_validation_dashboard", "_show"),
        ("commerce_operations_dashboard", "_show"),
    ],
)
def test_report_widgets_receive_final_json_and_current_rotated_credentials(
    module_name, helper, monkeypatch
):
    module = importlib.import_module(f"sm_trending_products.{module_name}")
    shown, errors = [], []
    monkeypatch.setattr(
        module,
        "st",
        SimpleNamespace(
            json=shown.append, error=errors.append, expander=lambda *args: nullcontext()
        ),
    )
    source = {
        "first": "first-current-key",
        "second": "second-current-key",
        "newline": "\n",
        "quote": '"',
        "ordinary": "Français",
    }
    original = copy.deepcopy(source)
    monkeypatch.setenv("RESEARCH_CREDENTIAL", r"\n")
    monkeypatch.setenv("RESEARCH_DSN", r"\"")
    for secret, other in (
        ("first-current-key", "second-current-key"),
        ("second-current-key", "first-current-key"),
    ):
        monkeypatch.setenv("RESEARCH_API_KEY", secret)
        getattr(module, helper)(source)
        assert not errors and isinstance(shown[-1], str)
        assert secret not in shown[-1] and other in shown[-1]
        assert r"\n" not in shown[-1] and r"\"" not in shown[-1]
        assert json.loads(shown[-1])["ordinary"] == "Français"
    assert source == original
    shown.clear()
    monkeypatch.setenv("RESEARCH_API_KEY", '":')
    getattr(module, helper)(source)
    assert errors and not shown  # An unsafe final encoding has no partial JSON output.


CONTENT_APP = """
from types import SimpleNamespace
from sm_trending_products.contracts import now
from sm_trending_products.content_research_dashboard import render_content
render_content(SimpleNamespace(catalog=None, provenance=None), [{"product_id":"p"}], now(), True)
"""


@pytest.mark.parametrize("secret", ["gaps", "title", "contrary", "null"])
def test_content_panel_interprets_raw_gaps_before_masking_typed_keys(secret, monkeypatch):
    from sm_trending_products.content_insights import ContentInsights

    source = {"gaps": ["missing supplied text"], "title": "quoted evidence", "contrary": None}
    monkeypatch.setattr(ContentInsights, "aggregate", lambda *args, **kwargs: source)
    monkeypatch.setenv("RESEARCH_API_KEY", secret)
    app = AppTest.from_string(CONTENT_APP).run()
    app.button[0].click().run()
    assert not app.exception and not app.error and app.json
    assert any("missing supplied text" in row.value for row in app.warning)
    assert secret not in app.json[0].value
    assert source["gaps"] == ["missing supplied text"] and source["contrary"] is None


def test_content_panel_uses_final_serialization_and_clears_after_display_failure(monkeypatch):
    from sm_trending_products.content_insights import ContentInsights

    source = {"gaps": [], "quote": "\n", "evidence": "retained only until the next interaction"}
    monkeypatch.setattr(ContentInsights, "aggregate", lambda *args, **kwargs: source)
    monkeypatch.setenv("RESEARCH_API_KEY", r"\n")
    app = AppTest.from_string(CONTENT_APP).run()
    app.button[0].click().run()
    assert not app.exception and not app.error and app.json
    assert r"\n" not in app.json[0].value
    assert json.loads(app.json[0].value)["quote"] != "\n"
    monkeypatch.setenv("RESEARCH_API_KEY", '":')
    app.button[0].click().run()
    assert not app.exception and app.error and not app.json
    app.run()
    assert not app.json and not app.error
