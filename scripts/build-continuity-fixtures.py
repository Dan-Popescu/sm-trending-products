#!/usr/bin/env python3
"""Build and locally register two fixture images without rebuilding dependencies."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


def command(arguments, *, cwd=None, timeout=300):
    environment = dict(os.environ)
    # Build metadata needs a writable directory in managed workspaces; retain
    # DOCKER_CONFIG and its registry credentials unchanged.
    environment.setdefault(
        "BUILDX_CONFIG", str(Path(tempfile.gettempdir()) / "sm-continuity-buildx")
    )
    for key in (
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
    ):
        environment.pop(key, None)
    return subprocess.run(
        arguments,
        cwd=cwd,
        env=environment,
        check=True,
        text=True,
        capture_output=True,
        timeout=timeout,
    ).stdout


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Example: scripts/build-continuity-fixtures.py --base-image app:cached "
        "--collector-binary /tmp/sm-collector --output /tmp/fixture-images.json. "
        "Requires an isolated local registry listening on localhost:80.",
    )
    parser.add_argument(
        "--base-image", required=True, help="Existing dependency-compatible runtime image"
    )
    parser.add_argument(
        "--collector-binary",
        type=Path,
        help="Compiled collector override; defaults to extracting the built runtime's collector",
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Write the controlled fixture manifest JSON here"
    )
    parser.add_argument(
        "--repository",
        default="localhost/sm-continuity-fixture",
        help="Isolated local registry repository",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"localhost/[a-z0-9][a-z0-9/_-]{1,120}", args.repository):
        parser.error("Fixture images must use an isolated localhost registry")
    if args.collector_binary is not None and not args.collector_binary.is_file():
        parser.error("Collector binary does not exist")
    root = Path(__file__).resolve().parents[1]
    revision = command(["git", "rev-parse", "HEAD"], cwd=root).strip()
    changed = command(
        ["git", "diff", "HEAD", "--name-only", "--", "src", "collector/src"], cwd=root
    )
    if changed.strip():
        parser.error("Commit runtime source before building immutable fixture images")
    docker = ["docker", "--host=unix:///var/run/docker.sock"]
    driver = command([*docker, "info", "--format", "{{.Driver}}"]).strip()
    if driver == "vfs" and shutil.disk_usage(root).free < 8 * 1024**3:
        parser.error(
            "VFS fixture builds require at least 8 GiB free; remove obsolete fixture images first"
        )
    installed = command(
        [
            *docker,
            "run",
            "--rm",
            "--entrypoint",
            "/app/.venv/bin/python",
            args.base_image,
            "-c",
            "import sm_trending_products;print(sm_trending_products.__path__[0])",
        ]
    ).strip()
    if not installed.startswith("/app/.venv/lib/") or not installed.endswith(
        "/site-packages/sm_trending_products"
    ):
        parser.error("Unexpected installed package layout in the runtime foundation")
    images = {}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    collector = args.collector_binary
    if collector is None:
        collector = args.output.with_suffix(".collector")
        container = command(
            [*docker, "create", "--entrypoint", "/bin/true", args.base_image]
        ).strip()
        try:
            command([*docker, "cp", f"{container}:/usr/local/bin/sm-collector", str(collector)])
            collector.chmod(0o755)
        finally:
            command([*docker, "rm", container])
    with tempfile.TemporaryDirectory(prefix="sm-continuity-image-") as temporary:
        context = Path(temporary)
        overlay = context / "rootfs"
        shutil.copytree(
            root / "src/sm_trending_products",
            overlay / installed.removeprefix("/"),
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        shutil.copytree(root / "deploy", overlay / "app/deploy")
        shutil.copytree(root / "scripts", overlay / "app/scripts")
        binary = overlay / "usr/local/bin/sm-collector"
        binary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(collector, binary)
        binary.chmod(0o755)
        for slot in ("blue", "green"):
            (context / "Dockerfile").write_text(
                f"FROM {args.base_image}\nCOPY --chown=10001:10001 rootfs/ /\n"
                f"LABEL io.sm-trends.controlled-fixture={slot} "
                f"org.opencontainers.image.revision={revision}\n"
            )
            tag = f"{args.repository}:{revision[:12]}-{slot}"
            command([*docker, "build", "--network=none", "--tag", tag, str(context)])
            command(
                [
                    *docker,
                    "run",
                    "--rm",
                    "--entrypoint",
                    "/usr/local/bin/sm-collector",
                    tag,
                    "capture",
                    "--help",
                ]
            )
            command(
                [
                    *docker,
                    "run",
                    "--rm",
                    "--entrypoint",
                    "/app/.venv/bin/sm-trends",
                    tag,
                    "capture",
                    "--help",
                ]
            )
            command([*docker, "push", tag])
            digests = json.loads(
                command([*docker, "image", "inspect", "--format", "{{json .RepoDigests}}", tag])
            )
            images[slot] = next(
                value for value in digests if value.startswith(args.repository + "@sha256:")
            )
    if images["blue"] == images["green"]:
        raise RuntimeError("Fixture variants must have distinct immutable images")
    args.output.write_text(
        json.dumps(
            {
                "revision": revision,
                "images": images,
                "collector_binary": str(collector.resolve()),
                "controlled_fixture": True,
                "production_dockerfile_build": False,
                "base_image": args.base_image,
            },
            indent=2,
        )
        + "\n"
    )
    print(
        json.dumps(
            {"ok": True, "manifest": str(args.output), "revision": revision, "images": images}
        )
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.SubprocessError) as error:
        # Tool output may include registry credentials; preserve a safe diagnostic.
        raise SystemExit(
            f"Fixture image command failed ({type(error).__name__}); "
            "inspect the local Docker daemon"
        ) from None
