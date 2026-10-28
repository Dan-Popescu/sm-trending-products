"""SQL-only operator access to bounded, explicitly supplied economic scenarios."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import NoReturn

from pydantic import ValidationError

from .commercial_sensitivity import EconomicInsights, ScenarioProfile

PROFILE_MAX_BYTES = 128 * 1024
PROGRAM = "sm-trends research economics"


class SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # Argument diagnostics can contain an arbitrary submitted flag or value.
        # Keep parser failures as private as JSON/model validation failures.
        print(
            json.dumps({"error": "invalid_arguments", "hint": f"Run {PROGRAM} --help."}),
            file=sys.stderr,
        )
        raise SystemExit(2)


class ProfileInputError(ValueError):
    """A fixed error code which never includes private JSON keys or values."""

    def __init__(self, code: str = "invalid_economic_profile"):
        self.code = code
        super().__init__(code)


def profile_from_bytes(content: bytes) -> ScenarioProfile:
    """Parse only a bounded JSON object; never expose Pydantic input diagnostics."""
    if len(content) > PROFILE_MAX_BYTES:
        raise ProfileInputError("economic_profile_too_large")
    try:
        payload = json.loads(content)
        if not isinstance(payload, dict):
            raise ProfileInputError()
        return ScenarioProfile.model_validate(payload)
    except (ValueError, ValidationError, UnicodeError, RecursionError) as error:
        raise ProfileInputError() from error


def _dossier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][\w.:-]{0,127}", value):
        raise argparse.ArgumentTypeError("Use a valid dossier identifier")
    return value


def _revision(value: str) -> int:
    if not re.fullmatch(r"[0-9]{1,10}", value) or not 1 <= int(value) <= 2_147_483_647:
        raise argparse.ArgumentTypeError("Expected revision must be between 1 and 2147483647")
    return int(value)


def parser() -> argparse.ArgumentParser:
    result = SafeParser(
        prog=PROGRAM,
        description="Read economic breakpoints and cash exposure from declared scenarios.",
        epilog="Example: sm-trends research economics --dossier-id PRODUCT-FR "
        "--expected-revision 3 --file /private/economics.json; "
        "add --synthetic for the isolated simulated workspace. "
        "Amounts are exact decimal strings. Missing facts remain unknown. "
        "No store, supplier or payment action is performed.",
    )
    result.add_argument("--dossier-id", type=_dossier, required=True)
    result.add_argument("--expected-revision", type=_revision, required=True)
    result.add_argument(
        "--file", type=Path, required=True, help="Scenario JSON object, at most 128 KiB"
    )
    result.add_argument("--synthetic", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if not arguments.file.is_file():
            raise ProfileInputError("economic_profile_unreadable")
        with arguments.file.open("rb") as source:
            profile = profile_from_bytes(source.read(PROFILE_MAX_BYTES + 1))
        # File and type validation precede configuration, SQL and any credentials.
        from .catalog import Catalog
        from .commerce import LaunchService
        from .settings import Settings

        settings = Settings()  # type: ignore[call-arg]
        catalog = Catalog(settings.database_url.get_secret_value())
        try:
            report = EconomicInsights(LaunchService(catalog)).describe(
                arguments.dossier_id,
                arguments.expected_revision,
                profile,
                synthetic=arguments.synthetic,
            )
        finally:
            catalog.close()
        from .collector import redact
        from .contracts import canonical

        print(canonical(redact(report)), flush=True)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from .commerce import LaunchError
        from .contracts import canonical

        code = "economic_insights_unavailable"
        if isinstance(error, ProfileInputError):
            code = error.code
        elif isinstance(error, LaunchError):
            code = error.code
        elif isinstance(error, OSError):
            code = "economic_profile_unreadable"
        # Error locations can contain arbitrary submitted dictionary keys. Return
        # fixed categories only, without input values, SQL errors or file paths.
        print(canonical({"error": code}), file=sys.stderr)
        return 2
