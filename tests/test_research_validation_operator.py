"""Private operator paths execute the same governed, source-guarded research."""

import json
import os
from contextlib import contextmanager
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_research_validation import protocol_at, seed

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.research_validation_cli import MAX_PROFILE_BYTES, main


def connect_fixture(monkeypatch, pipeline):
    @contextmanager
    def fixture_pipeline(*args, **kwargs):
        yield pipeline

    monkeypatch.setattr(Pipeline, "from_env", fixture_pipeline)


def run(capsys, argv, expected=0):
    assert main(argv) == expected
    output = capsys.readouterr()
    return json.loads(output.err if expected == 2 else output.out)


def test_help_example_and_invalid_inputs_need_no_configuration(tmp_path, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid input must not construct the application")

    monkeypatch.setattr(Pipeline, "from_env", forbidden)
    with pytest.raises(SystemExit) as help_exit:
        main(["run", "--help"])
    assert help_exit.value.code == 0
    assert "--request-id" in capsys.readouterr().out
    profile = run(capsys, ["example"])
    assert set(profile["partitions"]) == {"development", "test", "holdout"}
    with pytest.raises(SystemExit) as bad_exit:
        main(["--Bearer-super-secret"])
    assert bad_exit.value.code == 2
    assert "super-secret" not in capsys.readouterr().err

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (MAX_PROFILE_BYTES + 1))
    assert (
        run(capsys, ["register", "--file", str(oversized), "--request-id", "r"], 2)["error"]
        == "protocol_file_too_large"
    )
    pipe = tmp_path / "input-pipe"
    os.mkfifo(pipe)
    assert (
        run(capsys, ["register", "--file", str(pipe), "--request-id", "r"], 2)["error"]
        == "protocol_file_not_regular"
    )
    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps({"Bearer super-secret": "never echo this value"}))
    body = run(capsys, ["register", "--file", str(invalid), "--request-id", "r"], 2)
    assert body["error"] == "invalid_validation_protocol"
    assert "super-secret" not in str(body) and "never echo" not in str(body)
    assert (
        run(
            capsys,
            [
                "lock",
                "--protocol-id",
                "p",
                "--policy-id",
                "a",
                "--request-id",
                "r",
                "--as-of",
                now().isoformat(),
            ],
            2,
        )["error"]
        == "real_clock_cannot_be_supplied"
    )
    assert (
        run(capsys, ["list-attempts", "--protocol-id", "p", "--limit", "129"], 2)["error"]
        == "invalid_page"
    )


@pytest.mark.integration
def test_cli_initialization_registration_replay_and_namespace(
    pipeline, tmp_path, monkeypatch, capsys
):
    connect_fixture(monkeypatch, pipeline)
    assert (
        run(capsys, ["show", "--protocol-id", "unknown"], 2)["error"]
        == "research_validation_failed"
    )
    assert (
        pipeline.catalog.one("SELECT to_regclass('research_protocols') AS value")["value"] is None
    )
    run(capsys, ["init"])
    base = now() + timedelta(hours=1)
    profile = protocol_at(base)
    for partition in profile.partitions.values():
        for identifier in partition.product_ids:
            pipeline.create_product(identifier, identifier, synthetic=True)
    file = tmp_path / "protocol.json"
    file.write_text(profile.model_dump_json())
    command = [
        "register",
        "--file",
        str(file),
        "--request-id",
        "register",
        "--synthetic",
        "--as-of",
        now().isoformat(),
    ]
    registered = run(capsys, command)
    assert run(capsys, command) == registered
    identifier = registered["protocol_id"]
    assert run(capsys, ["show", "--protocol-id", identifier, "--synthetic"]) == registered
    assert run(capsys, ["show", "--protocol-id", identifier], 2)["error"] == "protocol_not_found"
    assert (
        run(capsys, ["list-attempts", "--protocol-id", identifier, "--synthetic"])["attempts"] == []
    )


@pytest.mark.integration
def test_cli_retains_failed_runs_and_honors_lock_and_source_withdrawal(
    pipeline, monkeypatch, capsys
):
    connect_fixture(monkeypatch, pipeline)
    service, _, identifier, base, datasets = seed(pipeline)
    failed = run(
        capsys,
        [
            "run",
            "--protocol-id",
            identifier,
            "--partition",
            "test",
            "--policy-id",
            "attention",
            "--request-id",
            "unlocked",
            "--synthetic",
            "--as-of",
            (base + timedelta(days=3)).isoformat(),
        ],
        1,
    )
    assert failed["error_code"] == "heldout_requires_prior_selected_policy_lock"
    assert (
        run(capsys, ["attempt", "--attempt-id", failed["attempt_id"], "--synthetic"])["state"]
        == "failed"
    )
    locked = run(
        capsys,
        [
            "lock",
            "--protocol-id",
            identifier,
            "--policy-id",
            "attention",
            "--request-id",
            "lock",
            "--synthetic",
            "--as-of",
            (base + timedelta(days=1)).isoformat(),
        ],
    )
    assert locked["locked_policy_id"] == "attention"
    command = [
        "run",
        "--protocol-id",
        identifier,
        "--partition",
        "development",
        "--policy-id",
        "attention",
        "--request-id",
        "dev",
        "--synthetic",
        "--as-of",
        (base + timedelta(days=1)).isoformat(),
    ]
    complete = run(capsys, command)
    assert complete["state"] == "completed" and complete["evaluation_id"]
    assert run(capsys, command) == complete
    rows = run(
        capsys,
        [
            "list-attempts",
            "--protocol-id",
            identifier,
            "--synthetic",
            "--limit",
            "1",
            "--offset",
            "1",
        ],
    )["attempts"]
    assert len(rows) == 1 and rows[0]["attempt_id"] == complete["attempt_id"]
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    withdrawn = run(capsys, command, 1)
    assert withdrawn["result_unavailable"] == "evidence_invalidated" and "result" not in withdrawn
    assert len(service.attempts(identifier, True)) == 2


@pytest.mark.integration
def test_private_desk_reads_on_request_and_refreshes_withdrawn_results(pipeline, monkeypatch):
    service, _, identifier, base, datasets = seed(pipeline)
    attempt = service.run(
        identifier,
        "development",
        "attention",
        "view",
        synthetic=True,
        as_of=base + timedelta(days=1),
    )
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.research_validation_dashboard import render_validation
with Pipeline.from_env() as pipeline:
    render_validation(pipeline, True)
"""
    app = AppTest.from_string(source).run(timeout=30)
    assert not app.exception and not app.json
    app.text_input(key="research-protocol-widget:True").set_value(identifier).run()
    assert not app.exception and not app.json
    app.button(key="research-inspect:True").click().run()
    assert not app.exception and not app.error
    assert any(json.loads(item.value).get("protocol_id") == identifier for item in app.json)
    app.text_input(key="research-attempt-widget:True").set_value(attempt["attempt_id"])
    app.button(key=f"research-inspect-attempt:True:{identifier}").click().run()
    assert not app.exception and not app.error
    displayed = json.loads(app.json[-1].value)
    assert displayed["state"] == "completed" and "result" in displayed
    app.text_input(key="research-clock:True").set_value(
        (base + timedelta(days=1)).isoformat()
    ).run()
    next(button for button in app.button if button.label == "Lock selected policy").click().run()
    assert not app.exception and not app.error
    assert service.protocol(identifier, True)["locked_policy_id"] == "attention"
    next(
        button for button in app.button if button.label == "Execute declared research subset"
    ).click().run()
    assert not app.exception and not app.error
    assert len(service.attempts(identifier, True)) == 2
    next(
        button for button in app.button if button.label == "Execute declared research subset"
    ).click().run()
    assert not app.exception and not app.error
    assert len(service.attempts(identifier, True)) == 2
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    app.run()
    assert not app.exception
    displayed = json.loads(app.json[-1].value)
    assert displayed["result_unavailable"] == "evidence_invalidated"
    assert "result" not in displayed


@pytest.mark.integration
def test_private_initialization_is_an_explicit_action(pipeline):
    app = AppTest.from_string("""
from sm_trending_products.catalog import Catalog
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings
from sm_trending_products.research_validation_dashboard import render_validation
with Pipeline.from_env() as pipeline:
    render_validation(pipeline, False)
""").run(timeout=30)
    assert not app.exception
    assert (
        pipeline.catalog.one("SELECT to_regclass('research_protocols') AS value")["value"] is None
    )
    app.button(key="research-init:False").click().run()
    assert not app.exception and not app.error
    assert pipeline.catalog.one("SELECT to_regclass('research_protocols') AS value")["value"]
