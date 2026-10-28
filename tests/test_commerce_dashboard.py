"""The private launch desk saves typed revisions without starting commerce effects."""

from streamlit.testing.v1 import AppTest

from sm_trending_products.commerce import LaunchDraft
from sm_trending_products.commerce_cli import template

APP = """
from sm_trending_products.catalog import Catalog
from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_dashboard import render_launches
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    render_launches(LaunchService(catalog), synthetic=True)
finally:
    catalog.close()
"""


def test_launch_view_never_initializes_tables_on_read(pipeline):
    app = AppTest.from_string(APP).run(timeout=20)
    assert not app.exception
    assert "commerce init" in app.error[0].value
    row = pipeline.catalog.one("SELECT to_regclass('commerce_dossiers') AS name")
    assert row["name"] is None


def test_operator_creates_and_revises_a_launch_dossier(pipeline):
    service = pipeline.launch_service()
    service.initialize()
    pipeline.create_product("lamp", "Lamp", synthetic=True)
    app = AppTest.from_string(APP).run(timeout=20)
    assert not app.exception
    values = {
        "Dossier ID": "lamp-fr",
        "Existing product ID": "lamp",
        "Country (ISO code, for example FR)": "FR",
        "Currency (ISO code, for example EUR)": "EUR",
        "Product title": "Lamp",
        "Minimum delivery days (blank = unknown)": "0",
        "Maximum delivery days (blank = unknown)": "0",
    }
    for field in app.text_input:
        if field.label in values:
            field.set_value(values[field.label])
    next(button for button in app.button if button.label == "Save new revision").click().run()
    assert not app.exception
    assert not app.error
    assert service.dossier("lamp-fr", synthetic=True)["revision"] == 1
    next(box for box in app.selectbox if box.label == "Launch dossier").set_value("lamp-fr").run()
    title = next(field for field in app.text_input if field.label == "Product title")
    title.set_value("Revised lamp")
    next(button for button in app.button if button.label == "Save new revision").click().run()
    assert not app.exception
    saved = service.dossier("lamp-fr", synthetic=True)
    assert saved["revision"] == 2
    assert saved["draft"]["title"] == "Revised lamp"
    assert saved["draft"]["delivery_min_days"] == 0
    assert saved["draft"]["delivery_max_days"] == 0
    assert saved["readiness"]["status"] == "incomplete"


def test_concurrent_edit_keeps_the_displayed_revision_and_reports_conflict(pipeline):
    service = pipeline.launch_service()
    service.initialize()
    pipeline.create_product("lamp", "Lamp", synthetic=True)
    original = LaunchDraft.model_validate(
        {
            **template(),
            "dossier_id": "lamp-fr",
            "product_id": "lamp",
            "country": "FR",
            "currency": "EUR",
            "synthetic": True,
            "title": "Original lamp",
        }
    )
    service.save(original, 0, "initial")
    app = AppTest.from_string(APP).run(timeout=20)
    next(box for box in app.selectbox if box.label == "Launch dossier").set_value("lamp-fr").run()
    next(field for field in app.text_input if field.label == "Product title").set_value(
        "My edited lamp"
    )
    service.save(original.model_copy(update={"title": "Another session's edit"}), 1, "concurrent")
    next(button for button in app.button if button.label == "Save new revision").click().run()
    assert not app.exception
    assert app.error
    assert "reload" in app.error[0].value.lower()
    assert service.dossier("lamp-fr", synthetic=True)["revision"] == 2
    assert (
        next(field for field in app.text_input if field.label == "Product title").value
        == "My edited lamp"
    )
    next(button for button in app.button if button.label == "Load latest revision").click().run()
    assert (
        next(field for field in app.text_input if field.label == "Product title").value
        == "Another session's edit"
    )
