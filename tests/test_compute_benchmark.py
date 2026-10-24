"""The comparison harness rejects unbounded work and counterfeit oracle modules."""

import importlib.util
import sys
from pathlib import Path

import pytest


def harness():
    path = Path(__file__).resolve().parents[1] / "scripts/benchmark-analytical-compute.py"
    spec = importlib.util.spec_from_file_location("compute_benchmark", path)
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


@pytest.mark.parametrize(
    "arguments",
    [
        ["--products", "0"],
        ["--samples", "361"],
        ["--repeats", "0"],
        ["--max-seconds", "nan"],
        ["--max-rss-mib", "0"],
    ],
)
def test_invalid_workload_is_rejected_before_oracle_execution_or_output(
    tmp_path, monkeypatch, arguments
):
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark",
            "--baseline-analytics",
            "absent",
            "--baseline-windows",
            "absent",
            "--output",
            str(output),
            *arguments,
        ],
    )
    with pytest.raises(SystemExit) as result:
        harness().main()
    assert result.value.code == 2
    assert not output.exists()


def test_counterfeit_baseline_is_not_executed_or_reported_as_old_code(
    tmp_path, monkeypatch, capsys
):
    wrong = tmp_path / "wrong.py"
    wrong.write_text("raise AssertionError('must not execute an unverified baseline')\n")
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark",
            "--baseline-analytics",
            str(wrong),
            "--baseline-windows",
            str(wrong),
            "--output",
            str(output),
        ],
    )
    with pytest.raises(SystemExit) as result:
        harness().main()
    assert result.value.code == 2
    assert "immutable declared commit" in capsys.readouterr().err
    assert not output.exists()
