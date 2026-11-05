"""Partition the complete collected suite into bounded sequential CI stages."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

BATCHES = ("a-d", "e-m", "n-z")
SEPARATE = {
    "lifecycle": {"tests/test_resource_cpu.py", "tests/test_worker_draining.py"},
    "video": {
        "tests/test_video_research.py",
        "tests/test_video_research_providers.py",
        "tests/test_video_research_sdks.py",
    },
}


def partition(node_ids: list[str]) -> dict[str, list[str]]:
    if not node_ids or len(node_ids) != len(set(node_ids)):
        raise ValueError("Collection must contain unique tests; inspect pytest --collect-only -q")
    groups: dict[str, list[str]] = {name: [] for name in (*BATCHES, *SEPARATE)}
    for node in node_ids:
        filename = node.split("::", 1)[0]
        if not filename.startswith("tests/") or ".." in Path(filename).parts:
            raise ValueError("Collected test lies outside tests/: " + filename)
        group = next((name for name, files in SEPARATE.items() if filename in files), None)
        if group is None:
            stem = Path(filename).stem.removeprefix("test_").casefold()
            group = "a-d" if stem < "e" else "e-m" if stem < "n" else "n-z"
        groups[group].append(node)
    return groups


def parse_collection(output: str) -> list[str]:
    nodes = [line for line in output.splitlines() if line.startswith("tests/") and "::" in line]
    summary = re.search(r"(?m)^(\d+) tests? collected(?: in|,|$)", output)
    if summary is None or int(summary[1]) != len(nodes):
        raise ValueError("Unsupported collection output; refuse to silently omit tests")
    return nodes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        usage="%(prog)s (--batch {a-d,e-m,n-z} | --verify-partition)\n"
        "Example: uv run --locked python scripts/ci-native-tests.py --verify-partition",
        epilog="Examples: uv run --locked python scripts/ci-native-tests.py --verify-partition; "
        "uv run --locked python scripts/ci-native-tests.py --batch a-d",
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--batch", choices=BATCHES)
    action.add_argument("--verify-partition", action="store_true", help="Collect and print only")
    args = parser.parse_args(argv)
    if not Path("pyproject.toml").is_file() or not Path("tests").is_dir():
        parser.error("Run from the repository root; see --help for examples")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode:
            print((result.stdout + result.stderr)[-8000:], file=sys.stderr)
            return result.returncode
        nodes = parse_collection(result.stdout)
        groups = partition(nodes)
        if any(not values for values in groups.values()):
            raise ValueError("A required stage is empty; inspect collection and stage assignments")
        print(
            json.dumps({"total": len(nodes), "groups": {k: len(v) for k, v in groups.items()}}),
            flush=True,
        )
        if args.verify_partition:
            return 0
        files = sorted({node.split("::", 1)[0] for node in groups[args.batch]})
        # Replace this process so the existing external watchdog reaches pytest itself.
        os.execv(
            sys.executable,
            [
                sys.executable,
                "-m",
                "pytest",
                "-x",
                "-vv",
                "-ra",
                "--durations=15",
                "-o",
                "faulthandler_timeout=90",
                f"--junitxml=output/native-batch-{args.batch}.xml",
                *files,
            ],
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(
            f"Cannot run native CI batch: {error}. Use --verify-partition or --help.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
