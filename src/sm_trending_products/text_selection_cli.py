"""Preview supplied-text queues and explicitly apply only the free local analyzer."""

from __future__ import annotations

import re
from pathlib import Path

from .content_research_cli import _cursor, _failure, _identifier, _read_object, _SafeParser
from .contracts import canonical, utc


def main(argv: list[str] | None = None) -> int:
    parser = _SafeParser(
        prog="sm-trends research select-texts",
        description="Metadata-only preview; explicit apply uses only free local rules.",
        epilog="Examples: sm-trends research select-texts preview --product-id PRODUCT; "
        "sm-trends research select-texts apply --file plan.json --request-prefix RUN-1",
    )
    commands = parser.add_subparsers(dest="action", required=True)
    preview = commands.add_parser("preview", help="Read a bounded metadata-only selection plan")
    preview.add_argument("--product-id", action="append", required=True)
    preview.add_argument("--as-of", help="Timezone-aware evidence cutoff")
    preview.add_argument(
        "--config", type=Path, help="Selection configuration JSON, at most 128 KiB"
    )
    preview.add_argument("--after", action="append", default=[], help="PRODUCT=TRANSCRIPT_CURSOR")
    preview.add_argument("--synthetic", action="store_true")
    apply = commands.add_parser("apply", help="Revalidate a saved plan and run free local rules")
    apply.add_argument(
        "--file", type=Path, required=True, help="Complete plan JSON, at most 128 KiB"
    )
    apply.add_argument("--request-prefix", required=True, help="Stable operation identity")
    apply.add_argument(
        "--synthetic", action="store_true", help="Must match the saved plan namespace"
    )
    args = parser.parse_args(argv)
    try:
        from .collector import redact
        from .text_selection import TextSelection, TextSelectionConfig, TextSelectionPlan

        if args.action == "preview":
            config = (
                TextSelectionConfig.model_validate(_read_object(args.config))
                if args.config
                else TextSelectionConfig()
            )
            products = [_identifier(product) for product in args.product_id]
            if not 1 <= len(products) <= config.max_products or len(set(products)) != len(products):
                raise ValueError("Provide a bounded distinct shortlist")
            after: dict[str, str] = {}
            if len(args.after) > len(products):
                raise ValueError("Too many selection cursors")
            for entry in args.after:
                product, separator, cursor = entry.partition("=")
                if not separator or product not in products or product in after:
                    raise ValueError("Cursors require a selected product exactly once")
                checked_cursor = _cursor(cursor)
                assert checked_cursor is not None
                after[product] = checked_cursor
            cutoff = utc(args.as_of) if args.as_of else None
        else:
            plan = TextSelectionPlan.model_validate(_read_object(args.file))
            if plan.synthetic != args.synthetic:
                raise ValueError("The selected namespace must match the plan")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}", args.request_prefix):
                raise ValueError("A stable bounded request prefix is required")
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            service = TextSelection(pipeline.catalog, pipeline.provenance)
            result = (
                service.preview(
                    products,
                    config=config,
                    synthetic=args.synthetic,
                    as_of=cutoff,
                    after=after,
                )
                if args.action == "preview"
                else service.execute(plan, request_prefix=args.request_prefix)
            )
        print(canonical(redact(result)))
        return 1 if args.action == "apply" and result["state"] != "completed" else 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        return _failure(error, "select_texts")
