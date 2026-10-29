"""Noninteractive, bounded comparison inputs validated before environment setup."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import NoReturn

from .contracts import canonical, now, utc
from .decision_brief_cli import _country, _identifier, _object_file


class _SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        print(
            canonical(
                {"error": "invalid_changes_arguments", "hint": "See research changes --help."}
            ),
            file=sys.stderr,
        )
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    root = _SafeParser(
        prog="sm-trends research changes",
        description="Compare one selected product and target country at two evidence cutoffs.",
        epilog="Example: sm-trends research changes --product-id LAMP --target-country FR "
        "--previous-as-of 2026-10-27T12:00:00Z --as-of 2026-10-28T12:00:00Z. "
        "No collection, paid analysis or commerce is executed.",
    )
    root.add_argument(
        "--examples", action="store_true", help="Print examples without environment setup"
    )
    root.add_argument("--product-id", type=_identifier)
    root.add_argument("--target-country", type=_country)
    root.add_argument("--as-of", help="Current evidence cutoff, ISO timestamp; default current UTC")
    root.add_argument(
        "--previous-as-of", help="Earlier evidence cutoff; default 24 hours before --as-of"
    )
    root.add_argument("--synthetic", action="store_true")
    root.add_argument(
        "--previous-selection", type=Path, help="Explicit selection JSON, at most 128 KiB"
    )
    root.add_argument(
        "--current-selection",
        type=Path,
        help="Explicit selection JSON; omitted means reuse previous selection",
    )
    root.add_argument("--max-rows", type=int, default=50_000)
    root.add_argument("--max-products", type=int, default=1000)
    root.add_argument("--text-limit", type=int, default=100)
    root.add_argument("--format", choices=("json", "text"), default="json")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.examples:
        print(
            "sm-trends research changes --product-id LAMP --target-country FR "
            "--previous-as-of 2026-10-27T12:00:00Z --as-of 2026-10-28T12:00:00Z\n"
            "sm-trends research changes --product-id LAMP --target-country FR "
            "--previous-selection /private/prior.json --current-selection /private/current.json "
            "--format text"
        )
        return 0
    try:
        from .decision_changes import ChangesSelection

        if args.product_id is None or args.target_country is None:
            raise ValueError("Explicit product and country required")
        cutoff = utc(args.as_of) if args.as_of else now()
        previous = utc(args.previous_as_of) if args.previous_as_of else cutoff - timedelta(hours=24)
        if previous >= cutoff or any(
            not 1 <= value <= maximum
            for value, maximum in (
                (args.max_rows, 100_000),
                (args.max_products, 10_000),
                (args.text_limit, 200),
            )
        ):
            raise ValueError("Invalid chronology or budget")
        prior_selection = ChangesSelection.model_validate(
            _object_file(args.previous_selection) if args.previous_selection else {}
        )
        current_selection = (
            ChangesSelection.model_validate(_object_file(args.current_selection))
            if args.current_selection
            else None
        )
    except (OSError, ValueError, TypeError, RecursionError):
        print(
            canonical(
                {
                    "error": "invalid_changes_input",
                    "hint": "Check chronological cutoffs, bounded selection JSON and budgets; "
                    "see research changes --help.",
                }
            ),
            file=sys.stderr,
        )
        return 2
    try:
        from .decision_changes import MAX_OUTPUT_BYTES, DecisionChanges, explain_changes
        from .display_safety import current_display_secrets, sanitize_display, scrub_display_text
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            report = DecisionChanges(
                pipeline.catalog, pipeline.history, pipeline.provenance
            ).compare(
                args.product_id,
                args.target_country,
                previous,
                cutoff,
                args.synthetic,
                previous_selection=prior_selection,
                current_selection=current_selection,
                max_rows=args.max_rows,
                max_products=args.max_products,
                text_limit=args.text_limit,
            )
        secrets = current_display_secrets()
        safe = sanitize_display(report, secret_values=secrets, max_bytes=MAX_OUTPUT_BYTES)
        output = (
            canonical(safe)
            if args.format == "json"
            else sanitize_display(
                "\n".join(explain_changes(report)),
                secret_values=secrets,
                max_bytes=MAX_OUTPUT_BYTES,
            )
        )
        output = scrub_display_text(output, secret_values=secrets)
        if len(output.encode()) > MAX_OUTPUT_BYTES:
            raise ValueError("Decision changes display exceeds budget")
        if args.format == "json":
            json.loads(output)
        print(output)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        print(
            canonical(
                {
                    "error": "decision_changes_unavailable",
                    "hint": "Check product workspace, exact selection, source availability "
                    "and bounded reads.",
                }
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
