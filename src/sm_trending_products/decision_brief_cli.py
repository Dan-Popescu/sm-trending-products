"""Explicit selected-product decision briefs, with bounded local configuration."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import NoReturn

from .contracts import canonical, now, utc

MAX_INPUT_BYTES = 128 * 1024


class _SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        print(
            canonical({"error": "invalid_brief_arguments", "hint": "See research brief --help."}),
            file=sys.stderr,
        )
        raise SystemExit(2)


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value):
        raise argparse.ArgumentTypeError("Use a bounded product or research identifier")
    return value


def _country(value: str) -> str:
    if not re.fullmatch(r"[A-Z]{2}", value):
        raise argparse.ArgumentTypeError("Use an explicit uppercase country code, for example FR")
    return value


def object_from_bytes(content: bytes) -> dict:
    if len(content) > MAX_INPUT_BYTES:
        raise ValueError("brief_input_too_large")
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("brief_input_requires_object")
    return payload


def _object_file(path: Path) -> dict:
    if not path.is_file():
        raise ValueError("brief_input_requires_regular_file")
    with path.open("rb") as source:
        return object_from_bytes(source.read(MAX_INPUT_BYTES + 1))


def parser() -> argparse.ArgumentParser:
    root = _SafeParser(
        prog="sm-trends research brief",
        description="Explain the evidence and gaps for one product and target country.",
        epilog="Examples: sm-trends research brief --product-id LAMP --target-country FR; "
        "sm-trends research brief --product-id LAMP --target-country FR "
        "--dossier-id LAMP-FR --expected-revision 3 --scenarios /private/scenarios.json. "
        "No launch, purchase, collection or paid analysis is performed.",
    )
    root.add_argument("--product-id", type=_identifier, required=True)
    root.add_argument("--target-country", type=_country, required=True)
    root.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    root.add_argument("--synthetic", action="store_true")
    root.add_argument("--taxonomy-id", type=_identifier)
    root.add_argument("--category-id", type=_identifier)
    root.add_argument("--dossier-id", type=_identifier)
    root.add_argument("--expected-revision", type=int)
    root.add_argument("--scenarios", type=Path, help="Explicit scenario profile, at most 128 KiB")
    root.add_argument(
        "--matching-profile", type=Path, help="Reviewed variant profile, at most 128 KiB"
    )
    root.add_argument("--content-taxonomy", type=Path, help="Reviewed theme rules, at most 128 KiB")
    root.add_argument(
        "--query-scope", help="Explicit observed query scope; never supplies missing facts"
    )
    root.add_argument("--source")
    root.add_argument("--max-rows", type=int, default=50_000)
    root.add_argument("--max-products", type=int, default=1000)
    root.add_argument("--text-limit", type=int, default=100)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        cutoff = utc(args.as_of) if args.as_of else now()
        if (
            not 1 <= args.max_rows <= 100_000
            or not 1 <= args.max_products <= 10_000
            or not 1 <= args.text_limit <= 200
            or (args.category_id is not None and args.taxonomy_id is None)
            or (
                args.query_scope is not None
                and (
                    not 1 <= len(args.query_scope) <= 1000
                    or args.query_scope != args.query_scope.strip()
                    or not args.query_scope.isprintable()
                )
            )
            or (args.source is not None and not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", args.source))
        ):
            raise ValueError("invalid_brief_scope")
        commercial = (
            args.dossier_id is not None,
            args.expected_revision is not None,
            args.scenarios is not None,
        )
        if any(commercial) and (
            not all(commercial) or not 1 <= args.expected_revision <= 2_147_483_647
        ):
            raise ValueError("brief_requires_dossier_revision_and_scenarios")
        from .commercial_sensitivity import ScenarioProfile
        from .content_insights import DEFAULT_TAXONOMY, ContentTaxonomy
        from .matching import MatchingProfile

        profile = (
            ScenarioProfile.model_validate(_object_file(args.scenarios)) if args.scenarios else None
        )
        matching = (
            MatchingProfile.model_validate(_object_file(args.matching_profile))
            if args.matching_profile
            else None
        )
        taxonomy = (
            ContentTaxonomy.model_validate(_object_file(args.content_taxonomy))
            if args.content_taxonomy
            else DEFAULT_TAXONOMY
        )
    except (OSError, ValueError, TypeError, RecursionError):
        print(
            canonical(
                {
                    "error": "invalid_brief_input",
                    "hint": "Check explicit scope, revision and bounded JSON objects; "
                    "see research brief --help.",
                }
            ),
            file=sys.stderr,
        )
        return 2
    try:
        from .collector import redact
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            report = pipeline.decision_brief().describe(
                args.product_id,
                args.target_country,
                cutoff,
                args.synthetic,
                taxonomy_id=args.taxonomy_id,
                category_id=args.category_id,
                dossier_id=args.dossier_id,
                expected_revision=args.expected_revision,
                scenario_profile=profile,
                matching_profile=matching,
                content_taxonomy=taxonomy,
                max_rows=args.max_rows,
                max_products=args.max_products,
                text_limit=args.text_limit,
                query_scope=args.query_scope,
                source=args.source,
            )
        print(canonical(redact(report)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        print(
            canonical(
                {
                    "error": "decision_brief_unavailable",
                    "hint": "Check product workspace, exact dossier revision, cutoff, "
                    "source availability and selected read budgets.",
                }
            ),
            file=sys.stderr,
        )
        return 2
