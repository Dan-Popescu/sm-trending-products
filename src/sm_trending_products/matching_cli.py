"""Explicit, read-only candidate review; never confirms product ownership."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .contracts import canonical, now, utc


def main(argv: list[str] | None = None) -> int:
    root = argparse.ArgumentParser(
        prog="sm-trends match-products",
        description="Explain bounded multilingual product-link candidates without confirming them.",
        epilog="Example: sm-trends match-products --product-id LAMP "
        "--profile profile.json --limit 20",
    )
    action = root.add_mutually_exclusive_group(required=True)
    action.add_argument("--product-id")
    action.add_argument(
        "--initialize-indexes",
        action="store_true",
        help="Explicitly build sparse identifier indexes online",
    )
    root.add_argument("--as-of", help="ISO timestamp with timezone; default current UTC")
    root.add_argument("--synthetic", action="store_true")
    root.add_argument(
        "--profile", type=Path, help="Explicit aliases/identifiers/brand/variants JSON"
    )
    root.add_argument("--limit", type=int, default=20)
    root.add_argument("--candidate-limit", type=int, default=500)
    args = root.parse_args(argv)
    try:
        from .catalog import Catalog
        from .matching import MatchingProfile, ProductMatcher
        from .settings import Settings

        profile = None
        if args.profile:
            with args.profile.open("rb") as file:
                content = file.read(65_537)
            if len(content) > 65_536:
                print(
                    canonical({"error": "profile_too_large", "maximum_bytes": 65_536}),
                    file=sys.stderr,
                )
                return 2
            profile = MatchingProfile.model_validate_json(content)
        settings = Settings()  # type: ignore[call-arg]
        catalog = Catalog(settings.database_url.get_secret_value())
        try:
            matcher = ProductMatcher(catalog)
            result = (
                matcher.initialize_indexes()
                if args.initialize_indexes
                else matcher.suggest(
                    args.product_id,
                    utc(args.as_of) if args.as_of else now(),
                    args.synthetic,
                    profile=profile,
                    limit=args.limit,
                    candidate_limit=args.candidate_limit,
                )
            )
        finally:
            catalog.close()
        from .collector import redact

        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from pydantic import ValidationError

        from .catalog import InvalidationPending
        from .matching import MatchingIndexesNotReady

        code = (
            "invalid_matching_profile"
            if isinstance(error, ValidationError)
            else "matching_indexes_not_ready"
            if isinstance(error, MatchingIndexesNotReady)
            else "evidence_temporarily_unavailable"
            if isinstance(error, InvalidationPending)
            else "matching_failed"
        )
        print(
            canonical(
                {
                    "error": code,
                    "hint": "Check product, cutoff and profile. "
                    "Identifier search requires explicit "
                    "sm-trends match-products --initialize-indexes.",
                }
            ),
            file=sys.stderr,
        )
        return 2
