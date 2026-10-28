"""Bounded economic scenarios through native SQL and private operator controls."""

import io
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest
from test_commercial_sensitivity import cash_plan
from test_launch_dossiers import draft

from sm_trending_products.commerce import LaunchService
from sm_trending_products.economic_research_cli import PROFILE_MAX_BYTES, main


@pytest.fixture
def economic_dossier(pipeline):
    pipeline.create_product("p1", "Fixture product", synthetic=True)
    launches = LaunchService(pipeline.catalog)
    launches.initialize()
    launches.save(draft(), 0, "create")
    return launches


def command(path, revision="1", synthetic=True):
    return [
        "--dossier-id",
        "launch-p1-fr",
        "--expected-revision",
        revision,
        "--file",
        str(path),
        *(["--synthetic"] if synthetic else []),
    ]


def write_profile(path, **changes):
    values = {"scenarios": [{"name": "declared", "ad_cost": "5"}]}
    values.update(changes)
    path.write_text(json.dumps(values))
    return path


def test_cli_uses_sql_only_and_preserves_cash_and_revision(
    economic_dossier, tmp_path, monkeypatch, capsys
):
    from sm_trending_products.pipeline import Pipeline

    def reject_pipeline(*args, **kwargs):
        raise AssertionError("Economic inspection must not construct history or S3 clients")

    monkeypatch.setattr(Pipeline, "__init__", reject_pipeline)
    path = write_profile(tmp_path / "profile.json", cash=cash_plan())
    assert main(command(path)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["revision"] == 1 and result["synthetic"] is True
    assert result["source_facts"]["readiness"]["status"] == "ready"
    point = result["scenarios"][0]
    assert Decimal(point["economics"]["post_ad_contribution"]) == 10
    assert Decimal(point["cash"]["additional_cash_required"]) == 200
    assert result["scenario_only"] is True
    assert main(command(path, synthetic=False)) == 2
    assert json.loads(capsys.readouterr().err) == {"error": "not_found"}
    economic_dossier.save(draft(note="Concurrent edit"), 1, "edit")
    assert main(command(path)) == 2
    assert json.loads(capsys.readouterr().err) == {"error": "revision_conflict"}
    assert main(command(path, revision="2")) == 0
    assert json.loads(capsys.readouterr().out)["revision"] == 2
    assert (
        economic_dossier.catalog.one("SELECT count(*) AS n FROM commerce_dossier_revisions")["n"]
        == 2
    )


@pytest.mark.parametrize(
    "content,expected",
    [
        (b"[1]", "invalid_economic_profile"),
        (b'{"scenarios":[{"name":"private","ad_cost":0.1}]}', "invalid_economic_profile"),
        (b'{"PRIVATE-SECRET-KEY":"PRIVATE-SECRET-VALUE"}', "invalid_economic_profile"),
        (b"\xff", "invalid_economic_profile"),
        (b" " * (PROFILE_MAX_BYTES + 1), "economic_profile_too_large"),
    ],
)
def test_cli_rejects_private_or_oversized_input_before_configuration(
    tmp_path, monkeypatch, capsys, content, expected
):
    from sm_trending_products import settings

    def reject_settings(*args, **kwargs):
        pytest.fail("Invalid input must be rejected before configuration")

    monkeypatch.setattr(settings, "Settings", reject_settings)
    path = tmp_path / "PRIVATE-PATH.json"
    path.write_bytes(content)
    assert main(command(path)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"error": expected}
    assert "PRIVATE" not in captured.err


def test_cli_help_and_invalid_flags_need_no_configuration(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        main(["--help"])
    assert stopped.value.code == 0
    assert "sm-trends research economics --dossier-id" in capsys.readouterr().out
    with pytest.raises(SystemExit) as stopped:
        main(command(tmp_path / "missing.json", revision="0"))
    assert stopped.value.code == 2
    capsys.readouterr()
    assert main(command(tmp_path)) == 2
    assert json.loads(capsys.readouterr().err) == {"error": "economic_profile_unreadable"}


@pytest.mark.parametrize(
    "arguments",
    [
        ["--Bearer-do-not-display-secret"],
        ["--dossier-id", "p", "--expected-revision", "PRIVATE-SECRET", "--file", "p.json"],
        ["--dossier-id", "PRIVATE SECRET", "--expected-revision", "1", "--file", "p.json"],
        ["--dossier-id", "p", "--expected-revision", "1", "--file", "p.json", "PRIVATE-SECRET"],
    ],
)
def test_parser_errors_never_echo_private_arguments(arguments, monkeypatch, capsys):
    from sm_trending_products import settings

    def forbidden(*args, **kwargs):
        pytest.fail("Parser failures must not construct configuration")

    monkeypatch.setattr(settings, "Settings", forbidden)
    with pytest.raises(SystemExit) as stopped:
        main(arguments)
    assert stopped.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": "invalid_arguments",
        "hint": "Run sm-trends research economics --help.",
    }
    assert "PRIVATE" not in captured.err and "Bearer" not in captured.err


def test_uninitialized_commerce_stays_uninitialized(pipeline, tmp_path, monkeypatch, capsys):
    path = write_profile(tmp_path / "profile.json")
    assert main(command(path)) == 2
    assert json.loads(capsys.readouterr().err) == {"error": "not_initialized"}
    app = economic_app(monkeypatch)
    assert not app.exception
    assert any("commerce init explicitly" in error.value for error in app.error)
    assert pipeline.catalog.one("SELECT to_regclass('commerce_dossiers') AS name")["name"] is None


def test_configuration_failure_does_not_echo_secrets_or_blame_profile(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setenv("DATABASE_URL", "PRIVATE-CONFIG-SECRET")
    path = write_profile(tmp_path / "profile.json")
    assert main(command(path)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"error": "economic_insights_unavailable"}


def test_success_redacts_accidental_secret_echoes(economic_dossier, tmp_path, monkeypatch, capsys):
    secret = "private-fixture-api-secret"
    monkeypatch.setenv("TEST_API_SECRET", secret)
    path = write_profile(
        tmp_path / "profile.json",
        scenarios=[{"name": "declared", "ad_cost": "5", "basis": secret}],
    )
    assert main(command(path)) == 0
    output = capsys.readouterr().out
    assert secret not in output
    assert json.loads(output)["profile"]["scenarios"][0]["basis"] == "[REDACTED]"


def economic_app(monkeypatch, profile=None):
    if profile is not None:
        payload = json.dumps(profile).encode()
        monkeypatch.setattr(
            "sm_trending_products.economic_research_dashboard.st.file_uploader",
            lambda *args, **kwargs: io.BytesIO(payload),
        )
    return AppTest.from_string("""
from types import SimpleNamespace
from sm_trending_products.catalog import Catalog
from sm_trending_products.economic_research_dashboard import render_economics
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    render_economics(SimpleNamespace(catalog=catalog), [{"product_id":"p1"}], True)
finally:
    catalog.close()
""").run(timeout=20)


def inspect(app):
    return (
        next(button for button in app.button if button.label == "Inspect economics").click().run()
    )


def test_private_economics_runs_on_request_and_clears_results_after_edit(
    economic_dossier, monkeypatch
):
    app = economic_app(
        monkeypatch, {"scenarios": [{"name": "declared", "ad_cost": "5"}], "cash": cash_plan()}
    )
    assert not app.exception and not app.error
    assert not app.dataframe and not app.json
    inspect(app)
    assert not app.exception and not app.error
    values = app.dataframe[0].value
    assert list(values["Post-ad contribution"]) == ["10.00"]
    assert [Decimal(value) for value in values["Additional cash required"]] == [Decimal(200)]
    assert len(app.dataframe[1].value) == 2
    # A normal rerun does not retain a prior report in the current display.
    app.run()
    assert not app.dataframe and not app.json
    economic_dossier.save(draft(note="Concurrent edit"), 1, "edit")
    inspect(app)
    assert not app.exception
    assert any("dossier changed" in error.value for error in app.error)
    assert not app.dataframe and not app.json
    next(item for item in app.number_input if item.label == "Expected dossier revision").set_value(
        2
    )
    inspect(app)
    assert not app.exception and not app.error and app.dataframe


def test_private_economics_keeps_missing_cash_facts_visible(economic_dossier, monkeypatch):
    app = economic_app(monkeypatch, {"scenarios": [{"name": "declared", "ad_cost": "5"}]})
    inspect(app)
    assert not app.exception and not app.error
    assert list(app.dataframe[0].value["Cash status"]) == ["unknown"]
    assert any("cash_plan" in warning.value for warning in app.warning)
    assert not any("Closing cash" in str(frame.value) for frame in app.dataframe)


def test_private_economics_does_not_infer_absence_from_first_page(economic_dossier, monkeypatch):
    original = LaunchService.dossiers
    calls = []

    def paged(self, synthetic=False, limit=100, offset=0):
        calls.append((synthetic, limit, offset))
        return original(self, synthetic, limit, 0) if offset == 100 else []

    monkeypatch.setattr(LaunchService, "dossiers", paged)
    app = economic_app(monkeypatch)
    assert not app.exception
    assert any("on this page" in info.value for info in app.info)
    next(item for item in app.number_input if item.label == "Economic dossier page").set_value(
        2
    ).run()
    assert not app.exception
    assert any(item.label == "Economic launch dossier" for item in app.selectbox)
    assert calls == [(True, 100, 0), (True, 100, 100)]


def test_private_economics_validates_upload_before_report(economic_dossier, monkeypatch):
    monkeypatch.setattr(
        "sm_trending_products.economic_research_dashboard.st.file_uploader",
        lambda *args, **kwargs: SimpleNamespace(size=PROFILE_MAX_BYTES + 1),
    )
    app = economic_app(monkeypatch)
    inspect(app)
    assert not app.exception
    assert any("Invalid scenario profile" in error.value for error in app.error)
    assert not app.dataframe and not app.json
