"""Import permissioned text and run reusable local insights, never paid analysis."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from .contracts import canonical


def main(argv: list[str] | None = None) -> int:
    root = argparse.ArgumentParser(
        prog="sm-trends transcripts",
        description="Analyze selected, permissioned text. "
        "No media download or paid provider is enabled.",
        epilog="Examples: sm-trends transcripts init; "
        "sm-trends transcripts import --file text.json; "
        "sm-trends transcripts analyze --product-id PRODUCT "
        "--transcript-id TRANSCRIPT --request-id RUN-1",
    )
    commands = root.add_subparsers(dest="action", required=True)
    commands.add_parser("init", help="Explicitly initialize optional transcript tables")
    import_text = commands.add_parser("import", help="Import source-bound permissioned text JSON")
    import_text.add_argument("--file", type=Path, required=True)
    budget = commands.add_parser("budget", help="Configure today's explicit provider budget")
    budget.add_argument("--provider", default="local-rules")
    budget.add_argument("--file", type=Path, required=True)
    budget.add_argument("--synthetic", action="store_true")
    for action in ("analyze", "show", "list"):
        child = commands.add_parser(action, help="Run or read selected-video transcript insights")
        child.add_argument("--product-id", required=True)
        child.add_argument("--synthetic", action="store_true")
        if action != "list":
            child.add_argument("--request-id", required=True)
        else:
            child.add_argument("--limit", type=int, default=20)
            child.add_argument("--after")
        if action == "analyze":
            child.add_argument("--transcript-id", required=True)
    args = root.parse_args(argv)
    try:
        from .collector import redact
        from .enrichment import EnrichmentBudget, TranscriptInput
        from .pipeline import Pipeline

        content = None
        if args.action in {"import", "budget"}:
            with args.file.open("rb") as file:
                content = file.read(131_073)
            if len(content) > 131_072:
                raise ValueError("Text configuration exceeds its file limit")
        with Pipeline.from_env() as pipeline:
            service = pipeline.transcript_insights()
            if args.action == "init":
                result = service.initialize()
            elif args.action == "budget":
                assert content is not None
                result = service.configure_budget(
                    args.provider,
                    EnrichmentBudget.model_validate_json(content),
                    synthetic=args.synthetic,
                )
            elif args.action == "import":
                assert content is not None
                result = service.import_transcript(TranscriptInput.model_validate_json(content))
            elif args.action == "analyze":
                result = service.analyze(
                    args.request_id, args.product_id, args.transcript_id, synthetic=args.synthetic
                )
            elif args.action == "show":
                result = service.result(args.request_id, args.product_id, synthetic=args.synthetic)
            else:
                result = service.summaries(
                    args.product_id, synthetic=args.synthetic, limit=args.limit, after=args.after
                )
        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from pydantic import ValidationError

        from .enrichment import EnrichmentDenied

        code = "transcript_operation_failed"
        if isinstance(error, ValidationError):
            code = "invalid_transcript_record"
        elif isinstance(error, EnrichmentDenied) and re.fullmatch(r"[a-z_]{1,128}", str(error)):
            code = str(error)
        print(
            canonical(
                {
                    "error": code,
                    "hint": "Check transcript init, source ownership, "
                    "permission reference and today's budget.",
                }
            ),
            file=sys.stderr,
        )
        return 2
