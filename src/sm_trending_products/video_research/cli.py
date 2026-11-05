"""Explicit operator effects; preparing manifests and help need no external access."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

from ..contracts import canonical
from .contracts import ExecutionConfig, VideoRequest, VideoResearchDenied, VideoSelection


def load(path: Path) -> dict:
    with path.open("rb") as file:
        data = file.read(64001)
    if len(data) > 64000:
        raise ValueError("Manifest exceeds its byte limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("A JSON object is required")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sm-trends video-research",
        description="Selected comments, opt-in video download and local Large-v3 transcription.",
        epilog="Examples: video-research init; video-research videos --product-id PRODUCT; "
        "video-research run --request request.json --config private.json --execute. "
        "See docs/operations/selected-video-enrichment.md.",
    )
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("init", help="Initialize only the optional video research metadata")
    doctor = actions.add_parser(
        "doctor", help="Check local optional setup without network or database"
    )
    doctor.add_argument("--config", type=Path, required=True)
    listing = actions.add_parser("videos", help="Page through confirmed product videos")
    listing.add_argument("--product-id", required=True)
    listing.add_argument("--limit", type=int, default=20)
    listing.add_argument("--after", default="")
    listing.add_argument("--synthetic", action="store_true")
    run = actions.add_parser("run", help="Prepare a request; --execute explicitly admits real work")
    run.add_argument("--request", type=Path, required=True)
    run.add_argument("--config", type=Path, required=True)
    run.add_argument("--execute", action="store_true")
    for name in ("show", "import-transcript"):
        child = actions.add_parser(
            name,
            help="Read a saved result"
            if name == "show"
            else "Explicitly import machine text into transcript insights",
        )
        child.add_argument("--request-id", required=True)
        child.add_argument("--product-id", required=True)
        child.add_argument("--config", type=Path, required=True)
        child.add_argument("--synthetic", action="store_true")
        if name == "show":
            child.add_argument("--limit", type=int, default=20)
            child.add_argument("--after", default="")
    export = actions.add_parser(
        "export", help="Copy one verified media artifact without overwriting"
    )
    export.add_argument("--job-id", required=True)
    export.add_argument("--selection", type=Path, required=True)
    export.add_argument("--config", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        config = (
            ExecutionConfig.model_validate(load(args.config)) if hasattr(args, "config") else None
        )
        if args.action == "doctor":
            assert config is not None
            result = {
                "enabled": config.enabled,
                "model": config.model,
                "automatic_downloads": False,
                "model_downloads": False,
                "dependencies": {
                    name: importlib.util.find_spec(name) is not None
                    for name in ("TikTokApi", "yt_dlp", "faster_whisper")
                },
                "model_files_present": config.model_dir is not None
                and all(
                    (config.model_dir / name).is_file()
                    for name in (
                        "model.bin",
                        "config.json",
                        "tokenizer.json",
                        "preprocessor_config.json",
                    )
                ),
                "research_approval_recorded": config.research is not None,
                "live_access_verified": False,
            }
        else:
            request = (
                VideoRequest.model_validate(load(args.request)) if args.action == "run" else None
            )
            if args.action == "run" and not args.execute:
                assert request is not None
                result = {
                    "status": "prepared",
                    "external_execution": False,
                    "source_authority_checked": False,
                    "operation": request.operation,
                    "request_id": request.request_id,
                }
            else:
                from ..pipeline import Pipeline

                with Pipeline.from_env() as pipeline:
                    service = pipeline.video_research()
                    if args.action == "init":
                        result = service.initialize()
                    elif args.action == "videos":
                        result = service.videos(
                            args.product_id,
                            synthetic=args.synthetic,
                            after=args.after,
                            limit=args.limit,
                        )
                    elif args.action == "run":
                        assert config is not None and request is not None
                        result = service.run(request, config)
                    elif args.action == "show":
                        assert config is not None
                        result = service.result(
                            args.request_id,
                            args.product_id,
                            config,
                            synthetic=args.synthetic,
                            limit=args.limit,
                            after=args.after,
                        )
                    elif args.action == "export":
                        assert config is not None
                        selection = VideoSelection.model_validate(load(args.selection))
                        result = service.export_media(args.job_id, selection, config, args.output)
                    else:
                        assert config is not None
                        result = service.import_transcription(
                            args.request_id, args.product_id, config, synthetic=args.synthetic
                        )
        from ..collector import redact

        print(canonical(redact(result)))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        from .execution import re_safe_reason

        reason = str(error) if isinstance(error, VideoResearchDenied) else "video_research_failed"
        reason = reason if re_safe_reason(reason) else "video_research_failed"
        print(
            canonical(
                {
                    "error": reason,
                    "hint": "Check explicit initialization, selected source/ownership, "
                    "permission, provider approval, local dependencies/model and budgets. "
                    "Uncertain requests are never retried automatically.",
                }
            ),
            file=sys.stderr,
        )
        return 2
