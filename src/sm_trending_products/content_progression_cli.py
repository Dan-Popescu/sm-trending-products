"""Explicit bounded, secret-safe inspection of quoted progression associations."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .content_research_cli import _failure, _read_object, _SafeParser
from .contracts import canonical, now, utc


def main(argv: list[str] | None = None) -> int:
    parser = _SafeParser(
        prog="sm-trends research patterns",
        description="Relate supplied text themes to measured video progression; "
        "descriptive associations only, without source fetch or provider execution.",
        epilog="Example: sm-trends research patterns --product-id PRODUCT --window-hours 168",
    )
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--as-of", help="Timezone-aware evidence cutoff; default current UTC")
    parser.add_argument("--window-hours", type=int, default=168)
    parser.add_argument(
        "--max-texts", type=int, default=200, help="Complete candidate population cap, at most 200"
    )
    parser.add_argument(
        "--max-rows", type=int, default=50000, help="History row cap, at most 100000"
    )
    parser.add_argument("--max-strata", type=int, default=128)
    parser.add_argument("--taxonomy", type=Path, help="Reviewed taxonomy JSON, at most 128 KiB")
    args = parser.parse_args(argv)
    try:
        from .content_insights import DEFAULT_TAXONOMY, ContentTaxonomy
        from .content_progression import ContentProgression, validate_scope
        from .display_safety import (
            current_display_secrets,
            sanitize_display,
            scrub_display_text,
        )

        validate_scope(
            args.product_id,
            args.synthetic,
            args.window_hours,
            args.max_rows,
            args.max_texts,
            args.max_strata,
        )
        cutoff = utc(args.as_of) if args.as_of else now()
        taxonomy = (
            ContentTaxonomy.model_validate(_read_object(args.taxonomy))
            if args.taxonomy
            else DEFAULT_TAXONOMY
        )
        if taxonomy.reviewed_at > cutoff:
            raise ValueError("The taxonomy was not reviewed at this cutoff")
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            report = ContentProgression(
                pipeline.catalog, pipeline.history, pipeline.provenance
            ).describe(
                args.product_id,
                cutoff,
                args.synthetic,
                window_hours=args.window_hours,
                max_rows=args.max_rows,
                max_texts=args.max_texts,
                max_strata=args.max_strata,
                taxonomy=taxonomy,
            )
        secrets = current_display_secrets()
        output = scrub_display_text(
            canonical(sanitize_display(report, secret_values=secrets)), secret_values=secrets
        )
        json.loads(output)
        print(output)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        return _failure(error, "patterns")


if __name__ == "__main__":
    sys.exit(main())
