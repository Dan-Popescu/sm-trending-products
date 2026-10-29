"""Bounded, read-only inspection of quoted themes in supplied text."""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
from pathlib import Path
from typing import Never

from .contracts import canonical, utc
from .json_input import MAX_INPUT_BYTES as MAX_INPUT_BYTES
from .json_input import InputTooLarge, ObjectRequired
from .json_input import object_from_bytes as _decode_object


class _SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        print(
            canonical(
                {
                    "error": "invalid_research_arguments",
                    "hint": "See the research command's --help for its supported arguments.",
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(2)


def _read_object(path: Path) -> dict:
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("A regular JSON file is required")
    with path.open("rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("A regular JSON file is required")
        value = handle.read(MAX_INPUT_BYTES + 1)
    return _object_bytes(value)


def _object_bytes(value: bytes) -> dict:
    try:
        return _decode_object(value)
    except InputTooLarge:
        raise ValueError("Input exceeds the bounded file size") from None
    except ObjectRequired:
        raise ValueError("A JSON object is required") from None


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value):
        raise ValueError("A bounded identifier is required")
    return value


def _cursor(value: str | None) -> str | None:
    if value is not None and not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("A bounded transcript cursor is required")
    return value


def _failure(error: Exception, operation: str) -> int:
    from pydantic import ValidationError

    from .enrichment import EnrichmentDenied

    code = "research_operation_failed"
    if isinstance(error, ValueError | OSError | ValidationError):
        code = "invalid_research_input"
    if isinstance(error, EnrichmentDenied):
        code = "source_or_module_unavailable"
    print(
        canonical(
            {
                "error": code,
                "operation": operation,
                "hint": "Check bounded input, transcript initialization, namespace, "
                "current source access and the local daily budget.",
            }
        ),
        file=sys.stderr,
    )
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = _SafeParser(
        prog="sm-trends research content",
        description="Read one bounded supplied-text theme panel; no source fetch or provider call.",
        epilog="Example: sm-trends research content --product-id PRODUCT --limit 100",
    )
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--as-of", help="Timezone-aware evidence cutoff")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--after", help="Transcript cursor from the previous panel")
    parser.add_argument("--taxonomy", type=Path, help="Reviewed taxonomy JSON, at most 128 KiB")
    args = parser.parse_args(argv)
    try:
        from .collector import redact
        from .content_insights import DEFAULT_TAXONOMY, ContentInsights, ContentTaxonomy

        product = _identifier(args.product_id)
        cursor = _cursor(args.after)
        if not 1 <= args.limit <= 200:
            raise ValueError("Panel limit must be between one and 200")
        cutoff = utc(args.as_of) if args.as_of else None
        taxonomy = (
            ContentTaxonomy.model_validate(_read_object(args.taxonomy))
            if args.taxonomy
            else DEFAULT_TAXONOMY
        )
        if cutoff is not None and taxonomy.reviewed_at > cutoff:
            raise ValueError("The taxonomy was not reviewed at the cutoff")
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            result = ContentInsights(pipeline.catalog, pipeline.provenance).aggregate(
                product,
                synthetic=args.synthetic,
                as_of=cutoff,
                limit=args.limit,
                after=cursor,
                taxonomy=taxonomy,
            )
        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        return _failure(error, "content")
