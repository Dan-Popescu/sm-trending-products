"""Explicit bounded competition inspection over already captured evidence."""

from __future__ import annotations

import argparse
import math
import re
import sys
from typing import NoReturn

from .contracts import canonical, now, utc


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(
            2,
            canonical(
                {
                    "error": "invalid_competition_arguments",
                    "hint": "Use sm-trends research competition --help.",
                }
            )
            + "\n",
        )


def _text(value: str, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum
        and value == value.strip()
        and value.isprintable()
    )


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(
        prog="sm-trends research competition",
        description="Inspect observed sellers, advertisers and compatible retail-price intervals.",
        epilog="Example: sm-trends research competition --product-id LAMP --country FR "
        "--query-scope reviewed-query-v1 --window-days 30 --max-rows 50000. "
        "Filters never fill missing country, query, coverage or distribution declarations.",
    )
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--family", action="store_true", help="Use the known family at the cutoff")
    parser.add_argument("--country", help="Explicit observed country filter, for example FR")
    parser.add_argument("--source")
    parser.add_argument("--query-scope")
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--max-age-hours", type=float, default=72)
    parser.add_argument("--max-rows", type=int, default=50000)
    parser.add_argument("--min-link-confidence", type=float, default=0.9)
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code or 0)
    try:
        cutoff = utc(args.as_of) if args.as_of else now()
        if (
            not _text(args.product_id, 256)
            or not 1 <= args.window_days <= 366
            or not 1 <= args.max_rows <= 100000
            or not math.isfinite(args.max_age_hours)
            or not 0 < args.max_age_hours <= 8760
            or not math.isfinite(args.min_link_confidence)
            or not 0 <= args.min_link_confidence <= 1
            or (args.country is not None and not re.fullmatch(r"[A-Z]{2}", args.country))
            or (args.source is not None and not _text(args.source, 128))
            or (args.query_scope is not None and not _text(args.query_scope, 1000))
        ):
            raise ValueError("Invalid bounded scope")
    except Exception:
        print(
            canonical(
                {
                    "error": "invalid_competition_input",
                    "hint": "Check product, explicit cutoff, filters and bounded limits.",
                }
            ),
            file=sys.stderr,
        )
        return 2
    try:
        from .collector import redact
        from .competition_insights import CompetitionInsights
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            result = CompetitionInsights(pipeline.catalog, pipeline.history).describe(
                args.product_id,
                cutoff,
                args.synthetic,
                family=args.family,
                country=args.country,
                source=args.source,
                query_scope=args.query_scope,
                window_days=args.window_days,
                max_age_hours=args.max_age_hours,
                max_rows=args.max_rows,
                min_link_confidence=args.min_link_confidence,
            )
        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        print(
            canonical(
                {
                    "error": "competition_inspection_failed",
                    "hint": "Check product, source evidence availability and history budget.",
                }
            ),
            file=sys.stderr,
        )
        return 2
