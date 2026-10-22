"""Operator-visible checks through Streamlit and the public pipeline facade."""

import pytest
from streamlit.testing.v1 import AppTest

CONTROLLED_APP = """
from datetime import datetime, timezone
import streamlit as st
from sm_trending_products.dashboard import render_dashboard


class ControlledPipeline:
    def status(self):
        return {"queued_tasks": 2, "failed_tasks": 1, "last_observed_at": "2026-10-22T10:00:00Z"}

    def capabilities(self):
        return [
            {
                "source": "tiktok_display",
                "account_id": "account-1",
                "capability": "video.list",
                "state": "unverified",
            }
        ]

    def products(self, synthetic=False):
        return [
            {
                "product_id": "test-product" if synthetic else "live-product",
                "name": "Desk lamp",
                "created_at": "2026-10-01T00:00:00Z",
                "synthetic": synthetic,
            }
        ] + [
            row for row in st.session_state.get("created_products", [])
            if row["synthetic"] is synthetic
        ]

    def links(self, as_of, synthetic=False):
        return []

    def latest_assessments(self, synthetic=False):
        return [
            {
                "product_id": "test-product" if synthetic else "live-product",
                "name": "Desk lamp",
                "score": 82.0,
                "as_of": "2026-10-22T10:00:00Z",
                "synthetic": synthetic,
                "policy_version": "attention-v1",
                "attention": {"score": 82.0, "status": "observed"},
                "demand": {"score": None, "status": "unknown"},
                "competition": {"score": None, "status": "unknown"},
                "feasibility": {"score": None, "status": "unknown"},
                "coverage": {"videos": 2},
                "evidence_refs": ["obs-1", "obs-2"],
                "reasons": ["Attention growth across two observations"],
            }
        ]

    def alerts(self, synthetic=False):
        return [
            {
                "alert_id": "alert-1",
                "product_id": "live-product",
                "policy_version": "attention-v1",
                "reasons": ["Frozen at decision time"],
            }
        ]

    def datasets(self, limit=100):
        return [{"dataset_id": "dataset-1", "status": "committed", "record_count": 2}]

    def evaluations(self, synthetic=False):
        return [
            {
                "evaluation_id": "evaluation-1",
                "mode": "forward",
                "status": "pending",
                "horizon_days": 7,
                "synthetic": synthetic,
            }
        ]

    def evidence(self, product_id, as_of, synthetic=False, limit=100, offset=0):
        return (
            [
                {
                    "observation_id": "obs-1",
                    "source": "tiktok_display",
                    "available_at": "2026-10-22T10:00:00Z",
                    "synthetic": synthetic,
                    "metrics": {"view_count": {"lower": 150, "upper": 150, "precision": "exact"}},
                    "missing_fields": {"retention": "not_available"},
                }
            ]
            if offset == 0
            else []
        )

    def create_product(self, product_id, name, synthetic=False, market=None):
        row = {
            "product_id": product_id, "name": name, "synthetic": synthetic, "market": market
        }
        st.session_state.setdefault("created_products", []).append(row)
        return row

    def review_link(
        self,
        product_id,
        entity_key,
        status="confirmed",
        confidence=1.0,
        method="manual",
        evidence_refs=None,
    ):
        return {"product_id": product_id, "entity_key": entity_key, "status": status}

    def set_family(self, product_id, family_id):
        return {"product_id": product_id, "family_id": family_id}

    def suggest_links(self, query, synthetic=False, limit=20):
        return [
            {
                "entity_key": "tiktok_display:video:video-1",
                "score": 0.4,
                "method": "text_candidate",
                "query": query,
            }
        ]


render_dashboard(ControlledPipeline())
"""


def test_operator_sees_live_evidence_and_explicit_unknown_dimensions() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    assert not app.exception
    assert app.toggle[0].value is False
    opportunities = app.dataframe[0].value
    assert opportunities.iloc[0]["Product"] == "Desk lamp"
    assert opportunities.iloc[0]["Demand"] == "Unknown"
    assert opportunities.iloc[0]["Competition"] == "Unknown"
    assert opportunities.iloc[0]["Feasibility"] == "Unknown"
    assert any("not establish purchasing" in warning.value for warning in app.warning)
    assert any("No verified live source" in warning.value for warning in app.warning)


def test_operator_must_explicitly_enter_synthetic_workspace() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
    app.toggle[0].set_value(True).run()
    assert not app.exception
    assert any("Synthetic workspace" in warning.value for warning in app.warning)
    assert app.selectbox(key="evidence_product").value == "test-product"


def test_health_cards_read_the_actual_status_contract_and_selected_workspace() -> None:
    source = CONTROLLED_APP.replace(
        'return {"queued_tasks": 2, "failed_tasks": 1, "last_observed_at": "2026-10-22T10:00:00Z"}',
        """return {
            "queues": [
                {"state": "pending", "synthetic": False, "count": 7},
                {"state": "quarantined", "synthetic": False, "count": 2},
                {"state": "pending", "synthetic": True, "count": 30}
            ],
            "sources": [
                {"synthetic": False, "last_observed_at": "2026-10-22T10:00:00Z"},
                {"synthetic": True, "last_observed_at": "2026-10-22T11:00:00Z"}
            ]
        }""",
    )
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception
    assert app.metric[0].value == "7"
    assert app.metric[1].value == "2"
    assert "10:00:00" in app.metric[2].value
    app.toggle[0].set_value(True).run()
    assert app.metric[0].value == "30"
    assert app.metric[1].value == "0"
    assert "11:00:00" in app.metric[2].value


def test_missing_pipeline_configuration_is_safe_and_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    assert not app.exception
    assert any("Configure DATABASE_URL" in message.value for message in app.info)


def test_operator_can_create_a_product_without_silent_empty_identifiers() -> None:
    app = AppTest.from_string(CONTROLLED_APP).run(timeout=20)
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
    next(
        button for button in app.button if button.label == "Refresh published results"
    ).click().run()
    catalog = next(
        frame.value
        for frame in app.dataframe
        if {"product_id", "name"}.issubset(frame.value.columns)
    )
    created = catalog.loc[catalog["product_id"] == "travel-cup"].iloc[0]
    assert created["name"] == "Travel cup"
    assert created["market"] == "FR"
    assert not bool(created["synthetic"])


def test_connection_details_and_access_tokens_are_not_rendered() -> None:
    app_source = CONTROLLED_APP.replace(
        '"queued_tasks": 2,',
        '"access_token": "act.fixture-secret", '
        '"connection": "postgresql://operator:fixture-password@db/trends", '
        '"source_url": "https://example.test/path?access_token=fixture-query-secret", '
        '"queued_tasks": 2,',
    )
    app = AppTest.from_string(app_source).run(timeout=20)
    assert not app.exception
    rendered_json = "\n".join(element.value for element in app.json)
    assert "fixture-secret" not in rendered_json
    assert "fixture-password" not in rendered_json
    assert "fixture-query-secret" not in rendered_json
    assert "[redacted]" in rendered_json
