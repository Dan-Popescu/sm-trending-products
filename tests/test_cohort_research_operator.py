"""Public SQL-only cohort commands and on-demand private category review."""

import json
import os
from datetime import timedelta

from streamlit.testing.v1 import AppTest
from test_cohort_insights import seed

from sm_trending_products.cohort_research_cli import MAX_JSON_BYTES, main
from sm_trending_products.contracts import now
from sm_trending_products.product_cohorts import CohortInsights


def _field(app, kind, label):
    return next(field for field in getattr(app, kind) if field.label == label)


def _app(cutoff, products=("product-0",)):
    return AppTest.from_string(f"""
from types import SimpleNamespace
from sm_trending_products.catalog import Catalog
from sm_trending_products.contracts import utc
from sm_trending_products.cohort_research_dashboard import render_cohorts
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    render_cohorts(SimpleNamespace(catalog=catalog),
        {[{"product_id": product} for product in products]!r}, utc({cutoff.isoformat()!r}), True)
finally:
    catalog.close()
""").run(timeout=20)


def test_cohort_cli_mutations_replay_cas_and_comparison_are_sql_only(
    pipeline, tmp_path, capsys, monkeypatch
):
    pipeline.create_product("panel", "Panel product", synthetic=True)
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://unavailable.invalid")

    def no_history(*args, **kwargs):
        raise AssertionError("Cohort commands must not construct History")

    monkeypatch.setattr("sm_trending_products.history.History.__init__", no_history)
    assert main(["init"]) == 0
    capsys.readouterr()
    category_file = tmp_path / "categories.json"
    category_file.write_text('[{"category_id":"tools","name":"Tools"}]')
    save = [
        "taxonomy-save",
        "--taxonomy-id",
        "physical",
        "--file",
        str(category_file),
        "--expected-revision",
        "0",
        "--request-id",
        "taxonomy-1",
        "--reviewer",
        "operator",
        "--rationale",
        "Reviewed definitions",
    ]
    assert main(save) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["revision"] == 1
    assert main(save) == 0
    assert json.loads(capsys.readouterr().out) == first
    assert main(["show-taxonomy", "--taxonomy-id", "physical"]) == 0
    assert json.loads(capsys.readouterr().out)["revision"] == 1
    assign = [
        "assign",
        "--product-id",
        "panel",
        "--taxonomy-id",
        "physical",
        "--category-id",
        "tools",
        "--taxonomy-revision",
        "1",
        "--expected-revision",
        "0",
        "--request-id",
        "assignment-1",
        "--reviewer",
        "operator",
        "--rationale",
        "Reviewed item",
        "--synthetic",
    ]
    assert main(assign) == 0
    assert json.loads(capsys.readouterr().out)["revision"] == 1
    assert main(assign) == 0
    capsys.readouterr()
    assert main(["compare", "--taxonomy-id", "physical", "--synthetic"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["metric"] == "attention_score"
    assert report["rows"][0]["assignment_revision"] == 1
    assert report["rows"][0]["observed_audience_country"] is None
    withdraw = [
        "assign",
        "--product-id",
        "panel",
        "--taxonomy-id",
        "physical",
        "--withdraw",
        "--taxonomy-revision",
        "1",
        "--expected-revision",
        "1",
        "--request-id",
        "withdraw-1",
        "--reviewer",
        "operator",
        "--rationale",
        "Withdraw category",
        "--synthetic",
    ]
    assert main(withdraw) == 0
    assert json.loads(capsys.readouterr().out)["category_id"] is None
    assert main(assign[:-1]) == 2  # The live workspace cannot replay a synthetic assignment.
    assert json.loads(capsys.readouterr().err)["error"] == "cohort_operation_failed"


def test_cohort_input_and_help_need_no_settings_and_never_echo_secrets(
    tmp_path, capsys, monkeypatch
):
    def no_settings(*args, **kwargs):
        raise AssertionError("Validate input before constructing Settings")

    monkeypatch.setattr("sm_trending_products.settings.Settings", no_settings)
    assert main(["--help"]) == 0
    assert "attention" in capsys.readouterr().out
    for action in ("init", "taxonomy-save", "assign", "show-taxonomy", "compare"):
        assert main([action, "--help"]) == 0
        assert "Example:" in capsys.readouterr().out
    secret = "do-not-display-this-secret"
    category_file = tmp_path / "invalid.json"
    args = [
        "taxonomy-save",
        "--taxonomy-id",
        "physical",
        "--file",
        str(category_file),
        "--expected-revision",
        "0",
        "--request-id",
        "request",
        "--reviewer",
        "operator",
        "--rationale",
        "Review",
    ]
    for content in (
        b" " * (MAX_JSON_BYTES + 1),
        b'{"' + secret.encode() + b'": []}',
        b'[{"category_id":1,"name":"' + secret.encode() + b'"}]',
    ):
        category_file.write_bytes(content)
        assert main(args) == 2
        captured = capsys.readouterr()
        assert json.loads(captured.err)["error"] == "invalid_cohort_input"
        assert secret not in captured.err + captured.out
    assert main(["compare", "--taxonomy-id", "physical", "--max-products", secret]) == 2
    assert secret not in capsys.readouterr().err
    assert main(["compare", "--taxonomy-id", "physical", "--max-assessment-age-hours", "nan"]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "invalid_cohort_input"
    category_file.unlink()
    os.mkfifo(category_file)
    assert main(args) == 2  # Never open an unbounded pipe while waiting for authored JSON.
    assert json.loads(capsys.readouterr().err)["error"] == "invalid_cohort_input"


def test_cohort_dashboard_explicit_initialization_reviews_and_assignment_cas(pipeline):
    pipeline.create_product("product-0", "A product", synthetic=True)
    app = _app(now() + timedelta(days=1))
    assert not app.exception and not app.json and not app.dataframe
    assert (
        pipeline.catalog.one("SELECT to_regclass('cohort_taxonomies') AS relation")["relation"]
        is None
    )
    _field(app, "button", "Compare observed panel").click().run()
    assert app.error
    assert (
        pipeline.catalog.one("SELECT to_regclass('cohort_taxonomies') AS relation")["relation"]
        is None
    )
    _field(app, "radio", "Cohort task").set_value("Initialize cohorts").run()
    _field(app, "button", "Initialize cohort module").click().run()
    assert app.success and not app.exception
    _field(app, "radio", "Cohort task").set_value("Review taxonomy").run()
    _field(app, "text_input", "Taxonomy reviewer").set_value("operator")
    _field(app, "text_area", "Taxonomy review rationale").set_value("Reviewed definitions")
    _field(app, "button", "Save reviewed taxonomy").click().run()
    assert app.success and not app.error and not app.exception
    _field(app, "button", "Save reviewed taxonomy").click().run()
    assert pipeline.catalog.one("SELECT revision FROM cohort_taxonomies")["revision"] == 1
    _field(app, "button", "Show taxonomy revision").click().run()
    assert any("Current taxonomy revision: 1" in text.value for text in app.markdown)
    _field(app, "radio", "Cohort task").set_value("Assign category").run()
    _field(app, "text_input", "Reviewed category ID").set_value("tools")
    _field(app, "text_input", "Assignment reviewer").set_value("operator")
    _field(app, "text_area", "Assignment review rationale").set_value("Reviewed item")
    _field(app, "button", "Save category assignment").click().run()
    assert app.success and not app.exception
    _field(app, "text_input", "Assignment request ID").set_value("new-request")
    _field(app, "button", "Save category assignment").click().run()
    assert app.error  # Expected revision stays visible and does not silently advance.
    assert _field(app, "number_input", "Expected assignment revision").value == 0
    assert pipeline.catalog.one("SELECT revision FROM cohort_assignments")["revision"] == 1


def test_cohort_dashboard_reports_unknowns_uncertified_units_and_rechecks_retirement(pipeline):
    _, cutoff, datasets = seed(pipeline, markets=["FR", "FR", "FR", None])
    app = _app(cutoff, ("product-0", "product-1", "product-2", "product-3"))
    _field(app, "button", "Compare observed panel").click().run()
    assert not app.exception and app.warning
    panel = app.dataframe[0].value
    assert set(panel["Audience country"]) == {"Unknown"}
    assert "unknown_target_country" in set(panel["Unavailable reason"])
    _field(app, "selectbox", "Cohort metric").set_value("view_count").run()
    _field(app, "button", "Compare observed panel").click().run()
    assert any("uncertified" in warning.value for warning in app.warning)
    assert set(app.dataframe[0].value["Unit"]) == {"uncertified"}
    assert app.dataframe[0].value["Observed-panel percentile"].isna().all()
    app.run()
    assert not app.json and not app.dataframe  # Source-derived reports are never cached in session.
    pipeline.catalog.invalidate_dataset(datasets[0], "source_removed")
    _field(app, "button", "Compare observed panel").click().run()
    assert not app.exception
    assert app.dataframe[0].value["Value"].isna().all()


def test_cohort_dashboard_sanitizes_taxonomy_and_panel_without_mutating_records(
    pipeline, monkeypatch
):
    secret = "FAKE-REVIEWED-CATEGORY-SECRET"
    url = "https://reader:pass@example.test/x?access_token=shape-value&signature=signed-value"
    service = CohortInsights(pipeline.catalog)
    service.initialize()
    original = service.save_taxonomy(
        "physical",
        [{"category_id": secret, "name": secret}, {"category_id": "url", "name": url}],
        0,
        "taxonomy-first",
        reviewer="operator",
        rationale="Reviewed definitions",
    )
    pipeline.create_product(secret, "Product", synthetic=True, market="FR")
    service.assign(
        secret,
        "physical",
        secret,
        0,
        "assignment-first",
        taxonomy_revision=1,
        synthetic=True,
        reviewer="operator",
        rationale="Reviewed product",
    )
    cutoff = now() + timedelta(days=1)
    monkeypatch.setenv("REVIEW_API_SECRET", secret)
    app = _app(cutoff, (secret,))
    _field(app, "radio", "Cohort task").set_value("Review taxonomy").run()
    _field(app, "button", "Show taxonomy revision").click().run()
    assert not app.exception
    visible = str(app.dataframe[0].value.to_dict()) + app.json[0].value
    for value in (secret, "reader:pass", "shape-value", "signed-value"):
        assert value not in visible
    _field(app, "radio", "Cohort task").set_value("Compare products").run()
    _field(app, "button", "Compare observed panel").click().run()
    assert not app.exception
    assert secret not in str(app.dataframe[0].value.to_dict()) + app.json[0].value
    assert set(app.dataframe[0].value["Target country"]) == {"FR"}
    assert set(app.dataframe[0].value["Unavailable reason"]) == {"no_current_assessment_at_cutoff"}
    _field(app, "radio", "Cohort task").set_value("Assign category").run()
    assert secret not in str(_field(app, "selectbox", "Product for category review").options)
    assert service.taxonomy("physical", cutoff) == original  # Display uses a safe copy only.
