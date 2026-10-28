"""Inactive connections and durable draft actions through the operator boundary."""

import json

from streamlit.testing.v1 import AppTest
from test_commerce_delivery import context as context

from sm_trending_products.cli import main
from sm_trending_products.commerce_delivery import CommerceDelivery


def test_delivery_intent_survives_hidden_widget_cleanup(context, monkeypatch):
    launches, _, provider, original = context
    launches.save(original.model_copy(update={"dossier_id": "second-fr"}), 0, "second")
    monkeypatch.setattr(
        "sm_trending_products.commerce_connection.load_connection", lambda _: provider
    )
    source = """
import streamlit as st
from sm_trending_products.catalog import Catalog
from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery
from sm_trending_products.commerce_delivery_dashboard import render_delivery
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    selected = st.selectbox("Selected dossier", [FIRST, "second-fr"])
    render_delivery(CommerceDelivery(catalog), LaunchService(catalog).dossier(selected, True), True)
finally:
    catalog.close()
""".replace("FIRST", repr(original.dossier_id))
    app = AppTest.from_string(source).run(timeout=20)
    request = next(
        item.value for item in app.text_input if item.label.startswith("Draft request ID")
    )
    app.selectbox[0].set_value("second-fr").run()
    launches.save(original.model_copy(update={"title": "Concurrent revision"}), 1, "newer")
    app.selectbox[0].set_value(original.dossier_id).run()
    assert not app.exception
    assert (
        next(item.value for item in app.text_input if item.label.startswith("Draft request ID"))
        == request
    )
    assert (
        next(item.value for item in app.text_input if item.label == "Dossier revision to prepare")
        == "1"
    )


def test_connection_template_and_inspection_do_not_need_database(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["commerce", "connection-template", "--provider", "shopify"]) == 0
    config = json.loads(capsys.readouterr().out)
    assert config["enabled"] is False
    path = tmp_path / "connection.json"
    path.write_text(json.dumps(config))
    assert main(["commerce", "connection-inspect", "--connection", str(path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["network_contacted"] is False
    assert report["enabled"] is False


def test_draft_command_defaults_to_preview_and_reconciles_lost_response(
    context, monkeypatch, capsys
):
    launches, delivery, provider, original = context
    monkeypatch.setattr(
        "sm_trending_products.commerce_connection.load_connection", lambda _: provider
    )
    command = [
        "commerce",
        "prepare-draft",
        "--connection",
        "private.json",
        "--id",
        original.dossier_id,
        "--expected-revision",
        "1",
        "--request-id",
        "command-1",
        "--synthetic",
    ]
    assert main(command) == 0
    assert json.loads(capsys.readouterr().out)["network_contacted"] is False
    assert provider.calls == []
    assert launches.catalog.one("SELECT count(*) AS n FROM commerce_delivery_operations")["n"] == 0
    provider.mode = "lost_response"
    assert main([*command, "--apply"]) == 1
    uncertain = json.loads(capsys.readouterr().out)
    assert uncertain["state"] == "uncertain"
    assert "PRIVATE" not in str(uncertain)
    assert main([*command, "--apply"]) == 1
    capsys.readouterr()
    assert [kind for kind, _ in provider.calls].count("prepare") == 1
    provider.mode = "normal"
    assert (
        main(
            [
                "commerce",
                "recover-draft",
                "--operation-id",
                uncertain["operation_id"],
                "--connection",
                "private.json",
                "--synthetic",
                "--apply",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["state"] == "succeeded"
    assert (
        main(
            [
                "commerce",
                "draft-operation",
                "--operation-id",
                uncertain["operation_id"],
                "--synthetic",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["product_id"] == "51"


def test_connection_files_cannot_embed_credentials(tmp_path, capsys):
    path = tmp_path / "private.json"
    path.write_text(
        json.dumps(
            {
                "provider": "woocommerce",
                "connection_id": "store",
                "base_url": "https://store.example",
                "access_token": "PRIVATE-secret",
            }
        )
    )
    assert main(["commerce", "connection-inspect", "--connection", str(path)]) == 2
    error = capsys.readouterr().err
    assert "PRIVATE-secret" not in error
    assert "access_token" in error


def test_private_delivery_control_uses_the_durable_service(context, monkeypatch):
    _, delivery, provider, original = context
    monkeypatch.setattr(
        "sm_trending_products.commerce_connection.load_connection", lambda _: provider
    )
    source = """
from sm_trending_products.catalog import Catalog
from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery
from sm_trending_products.commerce_delivery_dashboard import render_delivery
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    dossier = LaunchService(catalog).dossier(DOSSIER, synthetic=True)
    render_delivery(CommerceDelivery(catalog), dossier, True)
finally:
    catalog.close()
""".replace("DOSSIER", repr(original.dossier_id))
    app = AppTest.from_string(source).run(timeout=20)
    assert not app.exception
    assert provider.calls == []
    app.text_input[0].set_value("private.json")
    next(button for button in app.button if button.label == "Prepare product draft").click().run()
    assert not app.exception
    assert not app.error
    result = json.loads(app.json[0].value)
    assert result["state"] == "succeeded"
    assert delivery.operation(result["operation_id"], synthetic=True)["state"] == "succeeded"


def delivery_app(original):
    return AppTest.from_string(
        """
from sm_trending_products.catalog import Catalog
from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_delivery import CommerceDelivery
from sm_trending_products.commerce_delivery_dashboard import render_delivery
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    render_delivery(CommerceDelivery(catalog), LaunchService(catalog).dossier(DOSSIER, True), True)
finally:
    catalog.close()
""".replace("DOSSIER", repr(original.dossier_id))
    ).run(timeout=20)


def test_ui_keeps_request_after_post_enqueue_failure_and_finds_it_after_restart(
    context, monkeypatch, capsys
):
    launches, delivery, provider, original = context
    monkeypatch.setattr(
        "sm_trending_products.commerce_connection.load_connection", lambda _: provider
    )
    app = delivery_app(original)
    app.text_input[0].set_value("private.json")
    request = next(
        item.value for item in app.text_input if item.label.startswith("Draft request ID")
    )

    def fail_after_enqueue(*args, **kwargs):
        raise RuntimeError("Controlled failure after durable enqueue")

    with monkeypatch.context() as failure:
        failure.setattr(CommerceDelivery, "_execute", fail_after_enqueue)
        next(
            button for button in app.button if button.label == "Prepare product draft"
        ).click().run()
    assert app.error
    assert (
        next(item.value for item in app.text_input if item.label.startswith("Draft request ID"))
        == request
    )
    next(button for button in app.button if button.label == "Prepare product draft").click().run()
    pending = json.loads(app.json[0].value)
    assert pending["state"] == "pending" and pending["request_id"] == request
    assert provider.calls == []
    assert (
        main(
            [
                "commerce",
                "prepare-draft",
                "--connection",
                "private.json",
                "--id",
                original.dossier_id,
                "--expected-revision",
                "1",
                "--request-id",
                request,
                "--synthetic",
                "--apply",
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["state"] == "pending"
    assert (
        main(
            [
                "commerce",
                "draft-operations",
                "--connection-id",
                provider.config.connection_id,
                "--id",
                original.dossier_id,
                "--synthetic",
            ]
        )
        == 0
    )
    assert (
        json.loads(capsys.readouterr().out)["operations"][0]["operation_id"]
        == pending["operation_id"]
    )
    next(
        item for item in app.text_input if item.label.startswith("Existing draft operation ID")
    ).set_value(pending["operation_id"])
    next(
        button for button in app.button if button.label == "Recover uncertain draft operation"
    ).click().run()
    assert not app.exception and not app.error
    assert json.loads(app.json[0].value)["state"] == "succeeded"
    assert delivery.operations(provider.config.connection_id, original.dossier_id) == []


def test_ui_preserves_displayed_delivery_revision_until_explicit_new_request(context, monkeypatch):
    launches, _, provider, original = context
    monkeypatch.setattr(
        "sm_trending_products.commerce_connection.load_connection", lambda _: provider
    )
    app = delivery_app(original)
    app.text_input[0].set_value("private.json")
    request = next(
        item.value for item in app.text_input if item.label.startswith("Draft request ID")
    )
    launches.save(original.model_copy(update={"title": "Concurrent revision"}), 1, "concurrent")
    next(button for button in app.button if button.label == "Prepare product draft").click().run()
    assert not app.exception and app.error
    assert app.text_input[0].value == "private.json"
    assert (
        next(item.value for item in app.text_input if item.label == "Dossier revision to prepare")
        == "1"
    )
    assert provider.calls == []
    next(
        button
        for button in app.button
        if button.label == "Use latest dossier revision for a new request"
    ).click().run()
    assert (
        next(item.value for item in app.text_input if item.label == "Dossier revision to prepare")
        == "2"
    )
    assert (
        next(item.value for item in app.text_input if item.label.startswith("Draft request ID"))
        != request
    )
