"""Lazy entry points for bounded, optional decision research."""

from __future__ import annotations

import argparse
import sys
from importlib import import_module
from typing import NoReturn

from .contracts import canonical

COMMANDS = {
    "validation": ("research_validation_cli", "Register and run governed validation"),
    "cohorts": ("cohort_research_cli", "Review categories and comparable product panels"),
    "economics": ("economic_research_cli", "Inspect explicit profit and cash scenarios"),
    "competition": ("competition_research_cli", "Inspect observed competition and prices"),
    "content": ("content_research_cli", "Inspect quoted themes in eligible supplied text"),
    "select-texts": (
        "text_selection_cli",
        "Preview useful texts or explicitly apply local analysis",
    ),
    "brief": ("decision_brief_cli", "Explain support, contrary evidence and missing facts"),
    "changes": ("decision_changes_cli", "Explain changes between two evidence cutoffs"),
}


class _SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        print(
            canonical({"error": "invalid_research_arguments", "hint": "See research --help."}),
            file=sys.stderr,
        )
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    root = _SafeParser(
        prog="sm-trends research",
        description="Bounded decision research; no collection or commerce is launched.",
        epilog="Examples: sm-trends research brief --help; "
        "sm-trends research economics --help; sm-trends research validation --help. "
        "Each command documents its inputs and limits in --help.",
    )
    commands = root.add_subparsers(dest="command", required=True)
    for name, (_, description) in COMMANDS.items():
        commands.add_parser(name, help=description)
    return root


def main(argv: list[str] | None = None) -> int:
    effective = sys.argv[1:] if argv is None else argv
    if effective and effective[0] in COMMANDS:
        module, _ = COMMANDS[effective[0]]
        return import_module(f".{module}", __package__).main(effective[1:])
    parser().parse_args(effective)
    return 2
