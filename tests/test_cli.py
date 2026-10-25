"""The command boundary must be usable without credentials or a database."""

import os
import subprocess
import sys

import pytest


def test_help_is_available_without_credentials():
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"DATABASE_URL", "TIKTOK_DISPLAY_ACCESS_TOKEN", "TIKTOK_CCL_ACCESS_TOKEN"}
    }
    result = subprocess.run(
        [sys.executable, "-m", "sm_trending_products", "--help"],
        capture_output=True,
        text=True,
        env=environment,
    )
    assert result.returncode == 0
    assert "backtest" in result.stdout
    assert "verify" in result.stdout


def test_source_defaults_are_canonical_at_command_dispatch(tmp_path):
    from sm_trending_products.cli import dispatch, parser

    payload = tmp_path / "payload.json"
    payload.write_text('{"filters":{}}')

    class Commands:
        def enqueue(self, source, account, operation, payload, **kwargs):
            self.operation = operation
            return "task"

        def verify_live(self, source, account, operation, payload, **kwargs):
            self.operation = operation
            return {"operation": operation}

    for source, expected in [
        ("tiktok_display", "list"),
        ("tiktok_ads", "query"),
        ("tiktok_commercial", "query"),
    ]:
        for command in ("enqueue", "verify-live"):
            arguments = [
                command,
                "--source",
                source,
                "--account-id",
                "account",
                "--payload-file",
                str(payload),
            ]
            arguments.extend(
                ["--idempotency-key", "key"]
                if command == "enqueue"
                else ["--approve-use", "--evidence", "approved-scope", "--retention-days", "7"]
            )
            application = Commands()
            dispatch(application, parser().parse_args(arguments))
            assert application.operation == expected


def test_verify_live_retention_options_are_exclusive_and_keep_source_evidence_separate(tmp_path):
    from sm_trending_products.cli import dispatch, parser

    payload = tmp_path / "payload.json"
    payload.write_text("{}")
    arguments = [
        "verify-live",
        "--source",
        "tiktok_display",
        "--account-id",
        "account",
        "--payload-file",
        str(payload),
        "--approve-use",
        "--evidence",
        "account-approval",
    ]

    class Commands:
        def verify_live(self, *args, **kwargs):
            return kwargs

    with pytest.raises(SystemExit):
        parser().parse_args(arguments)
    with pytest.raises(SystemExit):
        parser().parse_args(arguments + ["--retention-days", "7", "--source-retention-unlimited"])
    result = dispatch(
        Commands(),
        parser().parse_args(
            arguments
            + [
                "--source-retention-unlimited",
                "--source-retention-evidence",
                "source-contract",
            ]
        ),
    )
    assert result == {
        "evidence": "account-approval",
        "retention_days": None,
        "source_retention_unlimited": True,
        "source_retention_evidence": "source-contract",
    }
