"""Operator-visible review reads only the chosen view and bounded summaries."""

import json

import pytest
from streamlit.testing.v1 import AppTest

CONTROLLED_APP = """
import streamlit as st
from sm_trending_products.dashboard import render_dashboard


def record(name):
    calls = st.session_state.setdefault("calls", {})
    calls[name] = calls.get(name, 0) + 1


class ControlledPipeline:
    def status(self):
        record("status")
        return {
            "queues": [
                {"state": "pending", "synthetic": False, "count": 7},
                {"state": "quarantined", "synthetic": False, "count": 2},
                {"state": "pending", "synthetic": True, "count": 30}
            ],
            "sources": [
                {"synthetic": False, "last_observed_at": "2026-10-22T10:00:00Z"},
                {"synthetic": True, "last_observed_at": "2026-10-22T11:00:00Z"}
            ],
            "capabilities": [{"source": "tiktok_display", "state": "unverified"}]
        }

    def products(self, synthetic=False, *, limit=200, offset=0):
        record("products")
        assert limit == 200
        return ([{
            "product_id": "test-product" if synthetic else "live-product",
            "name": "Desk lamp", "created_at": "2026-10-01T00:00:00Z",
            "synthetic": synthetic
        }] + [row for row in st.session_state.get("created_products", [])
              if row["synthetic"] is synthetic])[offset:offset + limit]

    def product_links(self, product_id, as_of, synthetic=False, *, limit=200, offset=0):
        record("product_links")
        assert limit == 200
        return []

    def assessment_summaries(self, synthetic=False, *, limit=200, offset=0):
        record("assessment_summaries")
        assert limit == 200
        return [{
            "product_id": "test-product" if synthetic else "live-product",
            "name": "Desk lamp", "score": 82.0, "as_of": "2026-10-22T10:00:00Z",
            "synthetic": synthetic, "policy_version": "attention-v1",
            "attention": {"score": 82.0, "status": "observed"},
            "demand": {"score": None, "status": "unknown"},
            "competition": {"score": None, "status": "unknown"},
            "feasibility": {"score": None, "status": "unknown"}
        }]

    def assessment(self, product_id, synthetic=False):
        record("assessment")
        return {"product_id": product_id, "evidence_refs": ["obs-1", "obs-2"]}

    def alert_summaries(self, synthetic=False, *, limit=200, offset=0):
        record("alert_summaries")
        assert limit == 200
        return [{"alert_id": "alert-1", "product_id": "live-product"}]

    def alert(self, alert_id, synthetic=False):
        record("alert")
        return {"alert_id": alert_id, "reasons": ["Frozen at decision time"]}

    def datasets(self, limit=100):
        record("datasets")
        return [{"dataset_id": "dataset-1", "status": "committed", "record_count": 2}]

    def evaluation_summaries(self, synthetic=False, *, limit=200, offset=0):
        record("evaluation_summaries")
        assert limit == 200
        return [{"evaluation_id": "evaluation-1", "kind": "forward", "status": "pending"}]

    def evaluation(self, evaluation_id, synthetic=False):
        record("evaluation")
        return {"evaluation_id": evaluation_id, "payload": {"evidence_refs": ["obs-1"]}}

    def evidence_page(self, product_id, as_of, synthetic=False, *, limit=100, cursor=None):
        record("evidence")
        assert limit == 100
        st.session_state.setdefault("evidence_reads", []).append({
            "product": product_id, "synthetic": synthetic, "cutoff": as_of.isoformat(),
            "cursor": cursor
        })
        return {
            "records": [{"observation_id": "obs-1" if cursor is None else "obs-2",
                         "source": "tiktok_display", "synthetic": synthetic,
                         "missing_fields": {"retention": "not_available"}}],
            "next_cursor": "second-page" if cursor is None else None,
            "status": "ready"
        }

    def create_product(self, product_id, name, synthetic=False, market=None):
        record("create_product")
        row = {"product_id": product_id, "name": name, "synthetic": synthetic, "market": market}
        st.session_state.setdefault("created_products", []).append(row)
        return row

    def review_link(self, product_id, entity_key, **options):
        record("review_link")
        return {"product_id": product_id, "entity_key": entity_key, **options}

    def set_family(self, product_id, family_id):
        return {"product_id": product_id, "family_id": family_id}

    def suggest_links(self, query, synthetic=False, limit=20):
        return [{"entity_key": "tiktok_display:video:video-1", "score": 0.4}]


render_dashboard(ControlledPipeline())
"""


def test_operator_sees_live_evidence_and_explicit_unknown_dimensions() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    assert not app.exception
    assert app.toggle[0].value is False
    opportunities = app.dataframe[0].value
    assert opportunities.iloc[0]["Product"] == "Desk lamp"
    for dimension in ("Demand", "Competition", "Feasibility"):
        assert opportunities.iloc[0][dimension] == "Unknown"
    assert any("not establish purchasing" in warning.value for warning in app.warning)
    assert set(app.session_state["calls"]) == {
        "products",
        "assessment_summaries",
        "assessment",
        "evidence",
        "alert_summaries",
        "alert",
    }


def test_operator_must_explicitly_enter_synthetic_workspace() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    app.toggle[0].set_value(True).run()
    assert not app.exception
    assert any("Synthetic workspace" in warning.value for warning in app.warning)
    assert app.selectbox(key="evidence_product").value == "test-product"


def test_inactive_views_are_not_loaded_and_health_uses_one_status_read() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    before = dict(app.session_state["calls"])
    app.radio(key="review_view").set_value("Health and coverage").run()
    assert not app.exception
    assert app.session_state["calls"] == {**before, "status": 1}
    assert any("No verified live source" in warning.value for warning in app.warning)
    assert app.metric[0].value == "7"
    assert app.metric[1].value == "2"
    assert "10:00:00" in app.metric[2].value
    app.toggle[0].set_value(True).run()
    assert app.metric[0].value == "30"
    assert app.metric[1].value == "0"
    assert "11:00:00" in app.metric[2].value


def test_quota_units_are_visible_without_exposing_authentication_tokens() -> None:
    source = CONTROLLED_APP.replace(
        '"queues": [',
        """"budgets": [
            {"tokens": 7.5, "access_token": "fixture-authentication-secret"},
            {"tokens": "act.invalid-counter-credential"}
        ], "queues": [""",
        1,
    )
    app = AppTest.from_string(source).run(timeout=20)
    app.radio(key="review_view").set_value("Health and coverage").run()
    assert not app.exception
    shown = next(
        value
        for element in app.json
        if isinstance(value := json.loads(element.value), dict) and "budgets" in value
    )
    assert shown["budgets"][0]["remaining_request_units"] == 7.5
    assert shown["budgets"][0]["access_token"] == "[redacted]"
    assert shown["budgets"][1]["remaining_request_units"] is None
    rendered = "\n".join(element.value for element in app.json)
    assert "fixture-authentication-secret" not in rendered
    assert "act.invalid-counter-credential" not in rendered


def test_history_loads_summary_and_only_the_selected_evaluation_detail() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    before = dict(app.session_state["calls"])
    app.radio(key="review_view").set_value("Datasets and evaluations").run()
    assert not app.exception
    assert app.session_state["calls"] == {
        **before,
        "datasets": 1,
        "evaluation_summaries": 1,
        "evaluation": 1,
    }
    assert "payload" not in app.dataframe[1].value.columns
    assert "evidence_refs" in app.json[0].value


def test_missing_pipeline_configuration_is_safe_and_actionable(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    assert not app.exception
    assert any("Configure DATABASE_URL" in message.value for message in app.info)


def test_operator_mutation_refreshes_lists_and_cutoff_immediately() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    app.radio(key="review_view").set_value("Catalog").run()
    initial_cutoff = app.text_input(key="review_cutoff").value
    next(button for button in app.button if button.label == "Create product").click().run()
    assert any("Product ID and name are required" in error.value for error in app.error)
    next(widget for widget in app.text_input if widget.label == "Product ID").set_value(
        "travel-cup"
    )
    next(widget for widget in app.text_input if widget.label == "Product name").set_value(
        "Travel cup"
    )
    next(widget for widget in app.text_input if widget.label == "Target market").set_value("FR")
    next(button for button in app.button if button.label == "Create product").click().run()
    assert not app.exception
    assert any("Product created" in message.value for message in app.success)
    catalog = next(frame.value for frame in app.dataframe if "product_id" in frame.value)
    created = catalog.loc[catalog["product_id"] == "travel-cup"].iloc[0]
    assert created["name"] == "Travel cup" and created["market"] == "FR"
    assert not bool(created["synthetic"])
    assert app.text_input(key="review_cutoff").value > initial_cutoff
    assert "status" not in app.session_state["calls"]


def test_connection_details_and_access_tokens_are_not_rendered() -> None:
    source = CONTROLLED_APP.replace(
        '"capabilities": [{"source": "tiktok_display", "state": "unverified"}]',
        '"access_token": "act.fixture-secret", '
        '"connection": "postgresql://operator:fixture-password@db/trends", '
        '"source_url": "https://example.test/path?access_token=fixture-query-secret", '
        '"capabilities": []',
    )
    app = AppTest.from_string(source).run(timeout=20)
    app.radio(key="review_view").set_value("Health and coverage").run()
    assert not app.exception
    rendered = "\n".join(element.value for element in app.json)
    for secret in ("fixture-secret", "fixture-password", "fixture-query-secret"):
        assert secret not in rendered
    assert "[redacted]" in rendered


def test_session_resource_reuses_pool_and_closes_on_configuration_change(monkeypatch) -> None:
    from sm_trending_products.pipeline import Pipeline

    created = []

    class Resource:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def create(cls):
        resource = Resource()
        created.append(resource)
        return resource

    monkeypatch.setattr(Pipeline, "from_env", classmethod(create))
    source = """
import streamlit as st
from sm_trending_products.dashboard import _review_pipeline
fingerprint = st.text_input("Configuration", value="fingerprint-one")
resource = _review_pipeline(fingerprint)
st.write(id(resource))
if st.button("Release resource"):
    _review_pipeline.clear()
"""
    app = AppTest.from_string(source).run(timeout=20)
    app.run()
    assert not app.exception and len(created) == 1 and not created[0].closed
    app.text_input[0].set_value("fingerprint-two").run()
    assert not app.exception and len(created) == 2
    assert created[0].closed and not created[1].closed
    app.button[0].click().run()
    assert created[1].closed


def test_evidence_navigation_scopes_cursors_to_product_workspace_and_cutoff():
    source = CONTROLLED_APP.replace(
        "render_dashboard(ControlledPipeline())",
        'st.session_state.setdefault("created_products", [{"product_id": "second-product", '
        '"name": "Second", "synthetic": False}])\nrender_dashboard(ControlledPipeline())',
    )
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception
    assert app.button(key="evidence_previous").disabled
    app.button(key="evidence_next").click().run()
    assert app.session_state["evidence_reads"][-1]["cursor"] == "second-page"
    assert app.button(key="evidence_next").disabled
    app.button(key="evidence_previous").click().run()
    assert app.session_state["evidence_reads"][-1]["cursor"] is None
    app.button(key="evidence_next").click().run()
    app.selectbox(key="evidence_product").set_value("second-product").run()
    assert app.session_state["evidence_reads"][-1]["product"] == "second-product"
    assert app.session_state["evidence_reads"][-1]["cursor"] is None
    app.button(key="evidence_next").click().run()
    app.text_input(key="review_cutoff").set_value("2026-10-21T00:00:00+00:00").run()
    assert app.session_state["evidence_reads"][-1]["cutoff"] == "2026-10-21T00:00:00+00:00"
    assert app.session_state["evidence_reads"][-1]["cursor"] is None
    app.button(key="evidence_next").click().run()
    app.toggle[0].set_value(True).run()
    assert app.session_state["evidence_reads"][-1]["synthetic"] is True
    assert app.session_state["evidence_reads"][-1]["cursor"] is None
    assert not app.exception


def test_pending_history_is_not_presented_as_an_empty_evidence_page():
    source = CONTROLLED_APP.replace(
        '        return {\n            "records":',
        '        if len(st.session_state["evidence_reads"]) == 1:\n'
        '            return {"records": [], "next_cursor": None, "status": "indexing"}\n'
        '        return {\n            "records":',
    )
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception
    assert any("Historical evidence is being prepared" in message.value for message in app.info)
    assert not any("No observations" in message.value for message in app.info)
    assert not app.error
    app.button(key="evidence_continue").click().run()
    assert not app.exception
    assert app.session_state["evidence_reads"][-1]["cursor"] is None
    assert len([frame for frame in app.dataframe if "observation_id" in frame.value]) == 1


@pytest.mark.parametrize("selector", ["AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"])
def test_provider_selector_change_replaces_and_closes_the_session_resource(
    monkeypatch, tmp_path, selector
):
    from sm_trending_products.pipeline import Pipeline

    monkeypatch.setenv("DATABASE_URL", "postgresql://testing:testing@localhost/testing")
    first = tmp_path / "first-provider"
    second = tmp_path / "second-provider"
    first.write_text("[default]\nregion = us-east-1\n")
    second.write_text("[default]\nregion = us-east-2\n")
    monkeypatch.setenv(selector, str(first))
    created = []

    class Resource:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def create(cls):
        resource = Resource()
        created.append(resource)
        return resource

    monkeypatch.setattr(Pipeline, "from_env", classmethod(create))
    source = """
import streamlit as st
from sm_trending_products.dashboard import _configuration_fingerprint, _review_pipeline
resource = _review_pipeline(_configuration_fingerprint())
st.write(id(resource))
if st.button("Release resource"):
    _review_pipeline.clear()
"""
    app = AppTest.from_string(source).run(timeout=20)
    app.run()
    assert not app.exception and len(created) == 1
    monkeypatch.setenv(selector, str(second))
    app.run()
    assert not app.exception and len(created) == 2
    assert created[0].closed and not created[1].closed
    app.button[0].click().run()
    assert created[1].closed


@pytest.mark.parametrize("filename", ["config", "credentials"])
def test_default_provider_file_rotation_invalidates_fingerprint_without_reading_contents(
    monkeypatch, tmp_path, filename
):
    import os
    from pathlib import Path

    from sm_trending_products.dashboard import _configuration_fingerprint

    monkeypatch.setenv("DATABASE_URL", "postgresql://testing:testing@localhost/testing")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("AWS_CONFIG_FILE", raising=False)
    monkeypatch.delenv("AWS_SHARED_CREDENTIALS_FILE", raising=False)
    directory = tmp_path / ".aws"
    directory.mkdir()
    provider = directory / filename
    provider.write_text("controlled-profile-a")
    original = _configuration_fingerprint()
    metadata = provider.stat()
    provider.write_text("controlled-profile-b")
    os.utime(provider, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1))

    def forbid_contents(*args, **kwargs):
        raise AssertionError("Resource identity must not read credential file contents")

    monkeypatch.setattr(Path, "read_text", forbid_contents)
    monkeypatch.setattr(Path, "read_bytes", forbid_contents)
    changed = _configuration_fingerprint()
    assert changed != original
    assert len(changed) == 64 and "controlled-profile" not in changed
