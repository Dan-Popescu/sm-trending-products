"""SQL-only operator commands for reviewed categories and observed-panel comparisons."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import NoReturn

from .contracts import canonical, now, utc

MAX_JSON_BYTES = 131072
METRICS = (
    "attention_score",
    "demand_score",
    "observed_seller_count",
    "observed_advertiser_count",
    "growth_per_hour",
    "view_count",
)


def _error(code: str) -> None:
    print(
        canonical(
            {"error": code, "hint": "Check bounded inputs and explicit module initialization."}
        ),
        file=sys.stderr,
    )


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(
            2,
            canonical(
                {
                    "error": "invalid_cohort_arguments",
                    "hint": "Use sm-trends research cohorts --help.",
                }
            )
            + "\n",
        )


def _text(value: str, maximum: int = 128) -> None:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or not value.isprintable()
    ):
        raise ValueError("Invalid bounded text")


def parse_taxonomy_categories(content: bytes) -> list[dict]:
    """Bound and validate authored JSON before settings or database construction."""
    if len(content) > MAX_JSON_BYTES:
        raise ValueError("Taxonomy JSON exceeds its byte budget")
    categories = json.loads(content)
    if isinstance(categories, dict) and set(categories) == {"categories"}:
        categories = categories["categories"]
    if not isinstance(categories, list) or not 1 <= len(categories) <= 1000:
        raise ValueError("Invalid categories")
    identifiers = set()
    for category in categories:
        if not isinstance(category, dict) or set(category) != {"category_id", "name"}:
            raise ValueError("Invalid category")
        _text(category["category_id"])
        _text(category["name"])
        if category["category_id"] in identifiers:
            raise ValueError("Duplicate category")
        identifiers.add(category["category_id"])
    return categories


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="sm-trends research cohorts",
        description="Review categories and compare a bounded observed target-market panel.",
        epilog="Examples: sm-trends research cohorts init; "
        "sm-trends research cohorts compare --taxonomy-id physical --country FR. "
        "Counter units remain uncertified; the default compares dimensionless attention scores.",
    )
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser(
        "init",
        help="Explicitly initialize optional cohort SQL tables",
        epilog="Example: sm-trends research cohorts init",
    )
    save = actions.add_parser(
        "taxonomy-save",
        help="Append a reviewed taxonomy revision",
        epilog="Example: sm-trends research cohorts taxonomy-save --taxonomy-id physical "
        "--file categories.json --expected-revision 0 --request-id taxonomy-1 "
        "--reviewer operator --rationale 'Reviewed physical-product categories'",
    )
    save.add_argument("--taxonomy-id", required=True)
    save.add_argument(
        "--file", type=Path, required=True, help="Category list JSON, at most 128 KiB"
    )
    assign = actions.add_parser(
        "assign",
        help="Review or withdraw a product category",
        epilog="Example: sm-trends research cohorts assign --product-id LAMP "
        "--taxonomy-id physical --category-id lighting --taxonomy-revision 1 "
        "--expected-revision 0 --request-id lamp-category-1 --reviewer operator "
        "--rationale 'Reviewed product specification'",
    )
    assign.add_argument("--product-id", required=True)
    assign.add_argument("--taxonomy-id", required=True)
    category = assign.add_mutually_exclusive_group(required=True)
    category.add_argument("--category-id")
    category.add_argument("--withdraw", action="store_true")
    assign.add_argument("--taxonomy-revision", type=int, required=True)
    assign.add_argument("--synthetic", action="store_true")
    for mutation in (save, assign):
        mutation.add_argument("--expected-revision", type=int, required=True)
        mutation.add_argument("--request-id", required=True)
        mutation.add_argument("--reviewer", required=True)
        mutation.add_argument("--rationale", required=True)
    show = actions.add_parser(
        "show-taxonomy",
        aliases=["showtaxonomy"],
        help="Read the taxonomy known at a cutoff",
        epilog="Example: sm-trends research cohorts show-taxonomy --taxonomy-id physical",
    )
    show.add_argument("--taxonomy-id", required=True)
    show.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    compare = actions.add_parser(
        "compare",
        help="Compare compact current assessment summaries",
        epilog="Example: sm-trends research cohorts compare --taxonomy-id physical --country FR "
        "--metric attention_score --max-products 1000 --min-peers 3",
    )
    compare.add_argument("--taxonomy-id", required=True)
    compare.add_argument("--category-id")
    compare.add_argument("--country", help="Declared product target country, never viewer country")
    compare.add_argument("--metric", choices=METRICS, default="attention_score")
    compare.add_argument("--max-products", type=int, default=1000)
    compare.add_argument("--min-peers", type=int, default=3)
    compare.add_argument("--max-assessment-age-hours", type=float, default=72)
    compare.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    compare.add_argument("--synthetic", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code or 0)
    try:
        categories = None
        cutoff = utc(args.as_of) if getattr(args, "as_of", None) else now()
        for field in ("taxonomy_id", "product_id", "category_id", "request_id", "reviewer"):
            if getattr(args, field, None) is not None:
                _text(getattr(args, field))
        if args.action in {"taxonomy-save", "assign"}:
            _text(args.rationale, 4000)
            if not 0 <= args.expected_revision < 2_147_483_647:
                raise ValueError("Invalid revision")
        if args.action == "taxonomy-save":
            if not args.file.is_file():
                raise ValueError("Taxonomy input must be a regular file")
            with args.file.open("rb") as stream:
                categories = parse_taxonomy_categories(stream.read(MAX_JSON_BYTES + 1))
        if args.action == "assign" and not 1 <= args.taxonomy_revision <= 2_147_483_647:
            raise ValueError("Invalid taxonomy revision")
        if args.action == "compare" and (
            not 1 <= args.max_products <= 10000
            or not 2 <= args.min_peers <= 1000
            or not math.isfinite(args.max_assessment_age_hours)
            or not 0 < args.max_assessment_age_hours <= 8760
            or (args.country is not None and not re.fullmatch(r"[A-Z]{2}", args.country))
        ):
            raise ValueError("Invalid cohort bounds")
    except Exception:
        _error("invalid_cohort_input")
        return 2
    try:
        from .catalog import Catalog
        from .collector import redact
        from .product_cohorts import CohortInsights
        from .settings import Settings

        settings = Settings()  # type: ignore[call-arg]
        catalog = Catalog(settings.database_url.get_secret_value())
        try:
            service = CohortInsights(catalog)
            result: dict | None
            if args.action == "init":
                result = service.initialize()
            elif args.action == "taxonomy-save":
                assert categories is not None
                result = service.save_taxonomy(
                    args.taxonomy_id,
                    categories,
                    args.expected_revision,
                    args.request_id,
                    reviewer=args.reviewer,
                    rationale=args.rationale,
                )
            elif args.action == "assign":
                result = service.assign(
                    args.product_id,
                    args.taxonomy_id,
                    args.category_id,
                    args.expected_revision,
                    args.request_id,
                    taxonomy_revision=args.taxonomy_revision,
                    synthetic=args.synthetic,
                    reviewer=args.reviewer,
                    rationale=args.rationale,
                )
            elif args.action in {"show-taxonomy", "showtaxonomy"}:
                result = service.taxonomy(args.taxonomy_id, cutoff)
            else:
                result = service.compare(
                    cutoff,
                    args.synthetic,
                    taxonomy_id=args.taxonomy_id,
                    category_id=args.category_id,
                    country=args.country,
                    metric=args.metric,
                    max_products=args.max_products,
                    min_peers=args.min_peers,
                    max_assessment_age_hours=args.max_assessment_age_hours,
                )
        finally:
            catalog.close()
        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:
        _error("cohort_operation_failed")
        return 2
