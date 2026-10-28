"""Explicit bounded trend exploration over already collected product evidence."""

from __future__ import annotations

import argparse
import sys

from .contracts import canonical, now, utc


def main(argv: list[str] | None = None) -> int:
    root = argparse.ArgumentParser(
        prog="sm-trends trend-insights",
        description="Explain measured product growth over multiple durations "
        "without collecting data.",
        epilog="Example: sm-trends trend-insights --product-id LAMP --windows-hours 24 72 168",
    )
    root.add_argument("--product-id", required=True)
    root.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    root.add_argument("--synthetic", action="store_true")
    root.add_argument("--windows-hours", nargs="+", type=int, default=[24, 72, 168])
    root.add_argument("--max-boundary-gap-hours", type=float, default=24)
    root.add_argument("--max-rows", type=int, default=50000)
    args = root.parse_args(argv)
    try:
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            result = pipeline.trend_insights().describe(
                args.product_id,
                utc(args.as_of) if args.as_of else now(),
                args.synthetic,
                windows_hours=args.windows_hours,
                max_boundary_gap_hours=args.max_boundary_gap_hours,
                max_rows=args.max_rows,
            )
        from .collector import redact

        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        print(
            canonical(
                {
                    "error": "trend_insights_failed",
                    "hint": "Check product, cutoff, history availability and row budget.",
                }
            ),
            file=sys.stderr,
        )
        return 2
