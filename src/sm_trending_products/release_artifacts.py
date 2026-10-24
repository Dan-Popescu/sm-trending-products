"""Build signed-release inputs only from immutable, verified main revisions."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

from .contracts import canonical


def verified_main_run(document: dict, revision: str, repository: str) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Release revision must be an immutable full lowercase commit SHA")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Invalid release repository")
    for run in document.get("workflow_runs", []):
        if not isinstance(run, dict):
            continue
        head = run.get("head_repository") or {}
        if (
            run.get("head_sha") == revision
            and run.get("head_branch") == "main"
            and run.get("event") == "push"
            and str(run.get("path", "")).split("@", 1)[0] == ".github/workflows/ci.yml"
            and run.get("status") == "completed"
            and run.get("conclusion") == "success"
            and isinstance(head, dict)
            and str(head.get("full_name", "")).casefold() == repository.casefold()
            and type(run.get("id")) is int
            and run["id"] > 0
        ):
            return run["id"]
    raise ValueError("No verified main push workflow matches the exact release revision")


def build_manifest(image: str, revision: str, compatibility: dict) -> bytes:
    from .release_control import ReleaseManifest, release_manifest_bytes

    manifest = ReleaseManifest(image=image, revision=revision, **compatibility)
    return release_manifest_bytes(manifest)


def build_request(manifest: bytes, target: str, sequence: int) -> bytes:
    from .release_control import (
        DeploymentRequest,
        ReleaseManifest,
        deployment_request_bytes,
        release_manifest_bytes,
    )

    candidate = ReleaseManifest.model_validate_json(manifest)
    if manifest != release_manifest_bytes(candidate):
        raise ValueError("Only canonical signed manifest bytes can be requested")
    request = DeploymentRequest(
        target=target,
        sequence=sequence,
        revision=candidate.revision,
        manifest_sha256=hashlib.sha256(manifest).hexdigest(),
    )
    return deployment_request_bytes(request)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify-ci", help="Require the exact successful main push CI")
    verify.add_argument("--runs", type=Path, required=True)
    verify.add_argument("--revision", required=True)
    verify.add_argument("--repository", required=True)
    manifest = commands.add_parser("manifest", help="Write canonical immutable release metadata")
    manifest.add_argument("--image", required=True)
    manifest.add_argument("--revision", required=True)
    manifest.add_argument("--compatibility", type=Path, required=True)
    manifest.add_argument("--output", type=Path, required=True)
    request = commands.add_parser(
        "request", help="Bind an ordered deployment request to a manifest"
    )
    request.add_argument("--manifest", type=Path, required=True)
    request.add_argument("--target", required=True)
    request.add_argument("--sequence", type=int, required=True)
    request.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "verify-ci":
        run_id = verified_main_run(
            json.loads(args.runs.read_text()), args.revision, args.repository
        )
        print(canonical({"verified_ci_run": run_id, "revision": args.revision}))
    elif args.command == "manifest":
        body = build_manifest(args.image, args.revision, json.loads(args.compatibility.read_text()))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(body)
        print(canonical({"manifest_written": True, "revision": args.revision}))
    else:
        body = build_request(args.manifest.read_bytes(), args.target, args.sequence)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(body)
        print(
            canonical({"request_written": True, "target": args.target, "sequence": args.sequence})
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError):
        print(canonical({"error": "invalid_release_input"}), file=sys.stderr)
        raise SystemExit(2) from None
