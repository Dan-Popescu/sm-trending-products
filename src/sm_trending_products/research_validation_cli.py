"""Noninteractive private operations for governed research; no source requests."""

from __future__ import annotations

import argparse
import os
import stat
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

from .contracts import canonical, utc

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .research_validation import ResearchValidation, ValidationProtocol

MAX_PROFILE_BYTES = 131072
PROGRAM = "sm-trends research validation"
SAFE_ERRORS = {
    "invalid_request",
    "real_clock_cannot_be_supplied",
    "request_conflict",
    "protocol_revision_conflict",
    "invalid_previous_protocol",
    "opened_protocol_cannot_be_revised",
    "protocol_not_found",
    "policy_not_registered",
    "policy_already_locked",
    "lock_precedes_registration",
    "lock_precedes_development_evidence",
    "attempt_capacity_exhausted",
    "partition_not_registered",
    "attempt_precedes_registration",
    "future_decision",
    "heldout_requires_prior_selected_policy_lock",
    "evaluation_invalidated",
    "attempt_not_found",
    "invalid_page",
    "unknown_or_wrong_namespace_product",
    "family_registry_capacity_exceeded",
    "ambiguous_family_timestamp",
    "family_or_entity_crosses_partitions",
    "lineage_heldout_already_opened",
}


class ProfileError(ValueError):
    """An operator-input error with a fixed, safe code."""


class SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # argparse's default can echo arbitrary supplied arguments and credentials.
        print(
            canonical({"error": "invalid_arguments", "hint": f"Run {PROGRAM} --help."}),
            file=sys.stderr,
        )
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    root = SafeParser(
        prog=PROGRAM,
        description="Register and execute bounded research over existing private evidence.",
        epilog=f"Examples: {PROGRAM} example; {PROGRAM} init; {PROGRAM} run --help",
    )
    commands = root.add_subparsers(dest="action", required=True)
    examples = {
        "example": f"{PROGRAM} example > protocol.json",
        "init": f"{PROGRAM} init",
        "register": f"{PROGRAM} register --file protocol.json --request-id REGISTER-1",
        "show": f"{PROGRAM} show --protocol-id PROTOCOL",
        "lock": f"{PROGRAM} lock --protocol-id PROTOCOL --policy-id attention --request-id LOCK-1",
        "run": f"{PROGRAM} run --protocol-id PROTOCOL --partition development "
        "--policy-id attention --request-id RUN-1",
        "attempt": f"{PROGRAM} attempt --attempt-id ATTEMPT",
        "list-attempts": f"{PROGRAM} list-attempts --protocol-id PROTOCOL --limit 20 --offset 0",
    }
    descriptions = {
        "example": "Print an editable protocol example without configuration",
        "init": "Explicitly initialize or upgrade the optional module",
        "register": "Register an immutable protocol from a regular JSON file, at most 128 KiB",
        "show": "Inspect a protocol and its actual chronology",
        "lock": "Irreversibly select one registered policy before opening heldout data",
        "run": "Execute the declared subset and retain the attempt, including failures",
        "attempt": "Inspect one attempt; a failed stored attempt is still a successful read",
        "list-attempts": "Read one bounded page of durable attempts",
    }
    for name, example in examples.items():
        child = commands.add_parser(
            name,
            help=descriptions[name],
            description=descriptions[name],
            epilog="Example: " + example,
        )
        if name in {"example", "init"}:
            continue
        child.add_argument(
            "--synthetic", action="store_true", help="Use the isolated fixture workspace"
        )
        if name == "attempt":
            child.add_argument("--attempt-id", required=True)
        elif name != "register":
            child.add_argument("--protocol-id", required=True)
        if name in {"register", "lock", "run"}:
            child.add_argument(
                "--request-id", required=True, help="Stable ID; replay with identical input"
            )
            child.add_argument("--as-of", help="Synthetic clock only: ISO timestamp with timezone")
        if name == "register":
            child.add_argument("--file", type=Path, required=True)
        if name in {"lock", "run"}:
            child.add_argument("--policy-id", required=True)
        if name == "run":
            child.add_argument(
                "--partition", choices=("development", "test", "holdout"), required=True
            )
        if name == "list-attempts":
            child.add_argument("--limit", type=int, default=20, help="1–128, default 20")
            child.add_argument("--offset", type=int, default=0, help="Nonnegative page offset")
    return root


def example_profile() -> dict:
    return {
        "name": "physical-products-pilot",
        "revision": 1,
        "config": {
            "target": "attention",
            "threshold": 1000,
            "top_k": 3,
            "horizon_days": [3],
            "launch_delay_hours": [12, 24, 48],
            "max_boundary_gap_hours": 24,
        },
        "policies": {"attention": {"version": "attention-v1", "weights": {"attention": 1}}},
        "partitions": {
            name: {"product_ids": [name + "-product"], "decision_times": [date]}
            for name, date in (
                ("development", "2027-01-01T00:00:00Z"),
                ("test", "2027-01-06T00:00:00Z"),
                ("holdout", "2027-01-11T00:00:00Z"),
            )
        },
        "measurement_sample_size": 2,
        "seed": 42,
        "max_attempts": 32,
    }


def decode_profile(content: bytes) -> ValidationProtocol:
    from .research_validation import ValidationProtocol

    if len(content) > MAX_PROFILE_BYTES:
        raise ProfileError("protocol_file_too_large")
    return ValidationProtocol.model_validate_json(content)


def read_profile(path: Path) -> ValidationProtocol:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as reader:
            if not stat.S_ISREG(os.fstat(reader.fileno()).st_mode):
                raise ProfileError("protocol_file_not_regular")
            content = reader.read(MAX_PROFILE_BYTES + 1)
    except OSError:
        raise ProfileError("protocol_file_unreadable") from None
    return decode_profile(content)


def synthetic_clock(value: str | None, synthetic: bool) -> datetime | None:
    if not value:
        return None
    if not synthetic:
        raise ProfileError("real_clock_cannot_be_supplied")
    try:
        return utc(value)
    except (TypeError, ValueError):
        raise ProfileError("invalid_synthetic_clock") from None


def service_for(pipeline: Pipeline) -> ResearchValidation:
    from .research_validation import ResearchValidation

    return ResearchValidation(pipeline.catalog, pipeline.history, pipeline.identity)


def safe_error(error: Exception) -> dict:
    from pydantic import ValidationError

    from .catalog import InvalidationPending
    from .contracts import AssessmentPolicy
    from .evaluation import EvaluationConfig, ProfitContract
    from .research_validation import ResearchError, ValidationPartition, ValidationProtocol

    if isinstance(error, ValidationError):
        fields = set().union(
            *(
                set(model.model_fields)
                for model in (
                    ValidationProtocol,
                    ValidationPartition,
                    AssessmentPolicy,
                    EvaluationConfig,
                    ProfitContract,
                )
            )
        ) | {"development", "test", "holdout", "attention", "demand", "competition", "feasibility"}
        return {
            "error": "invalid_validation_protocol",
            "fields": [
                [
                    part
                    if (type(part) is int and 0 <= part <= 500)
                    or (isinstance(part, str) and part in fields)
                    else "*"
                    for part in item["loc"]
                ]
                for item in error.errors(include_input=False)[:20]
            ],
        }
    code = "research_validation_failed"
    if isinstance(error, ProfileError):
        code = str(error)
    elif isinstance(error, ResearchError) and str(error) in SAFE_ERRORS:
        code = str(error)
    elif isinstance(error, InvalidationPending):
        code = "evidence_temporarily_unavailable"
    return {"error": code, "hint": f"Check inputs, evidence and explicit {PROGRAM} init."}


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        from .collector import redact

        if args.action == "example":
            print(canonical(redact(example_profile())))
            return 0
        profile = read_profile(args.file) if args.action == "register" else None
        synthetic = getattr(args, "synthetic", False)
        clock = synthetic_clock(getattr(args, "as_of", None), synthetic)
        for field in ("request_id", "protocol_id", "policy_id", "attempt_id"):
            value = getattr(args, field, None)
            if value is not None and (
                not value.strip() or value != value.strip() or len(value) > 128
            ):
                raise ProfileError("invalid_request")
        if args.action == "list-attempts" and (not 1 <= args.limit <= 128 or args.offset < 0):
            raise ProfileError("invalid_page")
        from .pipeline import Pipeline

        with Pipeline.from_env() as pipeline:
            service = service_for(pipeline)
            result: Any
            if args.action == "init":
                result = service.initialize()
            elif args.action == "register":
                assert profile is not None
                result = service.register(
                    profile, args.request_id, synthetic=synthetic, as_of=clock
                )
            elif args.action == "show":
                result = service.protocol(args.protocol_id, synthetic)
            elif args.action == "lock":
                result = service.lock(
                    args.protocol_id, args.policy_id, args.request_id, synthetic, as_of=clock
                )
            elif args.action == "run":
                result = service.run(
                    args.protocol_id,
                    args.partition,
                    args.policy_id,
                    args.request_id,
                    synthetic=synthetic,
                    as_of=clock,
                )
            elif args.action == "attempt":
                result = service.attempt(args.attempt_id, synthetic)
            else:
                result = {
                    "attempts": service.attempts(
                        args.protocol_id, synthetic, args.limit, args.offset
                    )
                }
        print(canonical(redact(result)), flush=True)
        return (
            1
            if args.action == "run"
            and (result.get("state") != "completed" or result.get("result_unavailable"))
            else 0
        )
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from .collector import redact

        print(canonical(redact(safe_error(error))), file=sys.stderr)
        return 2
