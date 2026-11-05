"""CI must cover nested/future tests without duplicates or silent collection loss."""

import runpy
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/ci-native-tests.py"
PARTITION = runpy.run_path(str(SCRIPT))


def test_partition_covers_each_collected_test_once_including_nested_files():
    nodes = [
        "tests/test_alpha.py::test_one",
        "tests/test_alpha.py::test_two",
        "tests/nested/test_feature.py::test_one",
        "tests/new/sub_test.py::test_one",
        *[path + "::test_one" for paths in PARTITION["SEPARATE"].values() for path in paths],
    ]
    groups = PARTITION["partition"](nodes)
    assert groups["a-d"] == nodes[:2]
    assert groups["e-m"] == [nodes[2]] and groups["n-z"] == [nodes[3]]
    flattened = [node for values in groups.values() for node in values]
    assert set(flattened) == set(nodes) and len(flattened) == len(nodes)


def test_partition_refuses_duplicate_collection_instead_of_running_partial_coverage():
    with pytest.raises(ValueError, match="unique"):
        PARTITION["partition"](["tests/test_one.py::test_one"] * 2)


def test_collection_parser_refuses_unrecognized_nodes_instead_of_omitting_them():
    with pytest.raises(ValueError, match="omit"):
        PARTITION["parse_collection"]("tests/test_one.py::test_one\n2 tests collected in 1s")
    assert PARTITION["parse_collection"]("tests/test_one.py::test_one\n1 test collected in 1s") == [
        "tests/test_one.py::test_one"
    ]


def test_batch_help_and_missing_flags_are_noninteractive_without_database(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "invalid-never-connect")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0 and "Examples:" in result.stdout
    result = subprocess.run(
        [sys.executable, str(SCRIPT)], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 2 and "--batch" in result.stderr
    assert "Example:" in result.stderr
