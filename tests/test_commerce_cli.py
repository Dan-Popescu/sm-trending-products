"""Launch preparation works through the operator command, independently from S3."""

import json

from sm_trending_products.cli import main
from sm_trending_products.commerce_cli import template


def test_launch_help_and_template_need_no_database(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["commerce", "template"]) == 0
    assert json.loads(capsys.readouterr().out)["prices_include_tax"] is None
    try:
        main(["commerce", "--help"])
    except SystemExit as error:
        assert error.code == 0
    assert "expected-revision" in capsys.readouterr().out


def test_launch_save_replay_and_private_preview(pipeline, tmp_path, capsys, monkeypatch):
    # Dossier commands use only PostgreSQL even if object-store configuration is unusable.
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://unreachable.invalid")
    pipeline.create_product("lamp", "Lamp", synthetic=True)
    assert main(["commerce", "init"]) == 0
    capsys.readouterr()
    record = {
        **template(),
        "dossier_id": "lamp-fr",
        "product_id": "lamp",
        "country": "FR",
        "currency": "EUR",
        "synthetic": True,
        "title": "<script>not executed</script>",
    }
    file = tmp_path / "dossier.json"
    file.write_text(json.dumps(record))
    command = [
        "commerce",
        "save",
        "--file",
        str(file),
        "--expected-revision",
        "0",
        "--request-id",
        "create-lamp",
    ]
    assert main(command) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["revision"] == 1
    assert first["readiness"]["status"] == "incomplete"
    assert main(command) == 0
    assert json.loads(capsys.readouterr().out)["revision"] == 1
    assert main(["commerce", "show", "--id", "lamp-fr"]) == 2
    assert "lamp" not in capsys.readouterr().err
    page = tmp_path / "draft.html"
    args = ["commerce", "preview", "--id", "lamp-fr", "--synthetic", "--output", str(page)]
    assert main(args) == 0
    capsys.readouterr()
    assert "<script>" not in page.read_text()
    assert "&lt;script&gt;" in page.read_text()
    assert page.stat().st_mode & 0o777 == 0o600
    assert main(args) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "launch_operation_failed"
