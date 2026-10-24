"""Opt-in native inventory probe; object counts are real, provider timing is not.

Requires TEST_DATABASE_URL pointing at a task-owned PostgreSQL database. Source
objects use the existing staged/committed derived-publication path and HTTP Moto.
The optional baseline script should be the immutable prior object_snapshot.py.
All schemas, object service and local payload files are owned and removed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import resource
import runpy
import socket
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
from moto.server import ThreadedMotoServer
from psycopg import sql

from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings

ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def environment(values):
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def scoped_url(database_url, schema):
    from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-c search_path={schema}"
    return urlunsplit((*parsed[:3], urlencode(query, quote_via=quote), parsed.fragment))


def run(objects, output, baseline, deadline_seconds):
    database_url = os.environ["TEST_DATABASE_URL"]
    schema = "backup_inventory_" + uuid4().hex
    report = {
        "ok": False,
        "requested_objects": objects,
        "seeded_objects": 0,
        "scope": "native PostgreSQL + HTTP Moto; derived objects, not product history",
        "deadline_seconds": deadline_seconds,
        "measurements": {},
    }
    deadline = time.monotonic() + deadline_seconds

    def save():
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")

    with tempfile.TemporaryDirectory(prefix="backup-inventory-", dir=output.parent) as directory:
        directory = Path(directory)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
        server.start()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        settings = Settings(
            database_url=scoped_url(database_url, schema),
            s3_endpoint_url=f"http://127.0.0.1:{port}",
            s3_bucket="backup-inventory-" + uuid4().hex,
            cache_dir=directory / "cache",
            spool_dir=directory / "spool",
        )
        try:
            with (
                environment(
                    {
                        "DATABASE_URL": settings.database_url.get_secret_value(),
                        "S3_BUCKET": settings.s3_bucket,
                        "S3_ENDPOINT_URL": settings.s3_endpoint_url,
                        "AWS_ACCESS_KEY_ID": "inventory-fixture",
                        "AWS_SECRET_ACCESS_KEY": "inventory-fixture",
                        "AWS_EC2_METADATA_DISABLED": "true",
                    }
                ),
                Pipeline(settings) as pipeline,
            ):
                pipeline.initialize(create_bucket=True)
                started = time.monotonic()
                for index in range(objects):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Inventory seeding deadline reached")
                    body = canonical({"fixture": "backup-inventory", "index": index}).encode()
                    checksum = hashlib.sha256(body).hexdigest()
                    key = f"inventory/synthetic/{checksum}.json"
                    pipeline.catalog.stage_derivation(key, [])
                    pipeline.history.put(key, body, content_type="application/json")
                    pipeline.catalog.execute(
                        "UPDATE derived_publications SET state='committed',"
                        "object_sha256=%s,object_bytes=%s WHERE object_key=%s",
                        (checksum, len(body), key),
                    )
                    report["seeded_objects"] = index + 1
                report["seed_seconds"] = time.monotonic() - started
                state = {"active": False, "heads": 0, "gets": 0, "lists": 0, "held_heads": 0}

                def count_head(**_kwargs):
                    state["heads"] += 1
                    state["held_heads"] += int(state["active"])

                def count_get(**_kwargs):
                    state["gets"] += 1

                def count_list(**_kwargs):
                    state["lists"] += 1

                s3 = pipeline.history.client
                s3.meta.events.register("before-call.s3.HeadObject", count_head)
                s3.meta.events.register("before-call.s3.GetObject", count_get)
                s3.meta.events.register("before-call.s3.ListObjectsV2", count_list)
                scripts = {"treatment": ROOT / "scripts/object_snapshot.py"}
                if baseline:
                    scripts = {"baseline": baseline, **scripts}
                for label, path in scripts.items():
                    module = runpy.run_path(str(path))
                    module["capture"].__globals__["client"] = lambda: s3
                    original_guard = module["_checkpoint_guard"]

                    @contextmanager
                    def guard(enabled, _guard=original_guard):
                        with _guard(enabled) as connection:
                            state["active"] = True
                            checkpoint_started = time.monotonic()
                            try:
                                yield connection
                            finally:
                                state["active"] = False
                                state["checkpoint_seconds"] = time.monotonic() - checkpoint_started

                    module["capture"].__globals__["_checkpoint_guard"] = guard
                    base = None
                    for phase in ("full", "unchanged"):
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Inventory capture deadline reached")
                        for counter in ("heads", "gets", "lists", "held_heads"):
                            state[counter] = 0
                        proceed = directory / f"{label}-{phase}.continue"
                        proceed.write_text("controlled writer resumption")
                        target = directory / label / phase / "objects"
                        started = time.monotonic()
                        captured = module["capture"](
                            target,
                            copy_objects=True,
                            base=base,
                            checkpoint_signal=directory / f"{label}-{phase}.ready",
                            continue_signal=proceed,
                            deadline_seconds=max(1, int(deadline - time.monotonic())),
                        )
                        assert captured["object_count"] == objects
                        report["measurements"][f"{label}_{phase}"] = {
                            **{key: value for key, value in state.items() if key != "active"},
                            "elapsed_seconds": time.monotonic() - started,
                            "downloaded_bytes": captured["downloaded_bytes"],
                            "script_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        }
                        base = target
                        save()
                treatment = report["measurements"]["treatment_unchanged"]
                assert treatment["heads"] == treatment["gets"] == treatment["held_heads"] == 0
                assert report["measurements"]["treatment_full"]["held_heads"] == 0
                report["ok"] = True
        except Exception as error:
            report["error_type"] = type(error).__name__
            raise
        finally:
            server.stop()
            with psycopg.connect(database_url, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            report["peak_process_rss_bytes"] = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            )
            save()
    report["cleanup"] = {"schema_removed": True, "owned_files_removed": not directory.exists()}
    save()
    return report


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=1001)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-script", type=Path)
    parser.add_argument("--deadline-seconds", type=int, default=600)
    args = parser.parse_args()
    if not 1 <= args.objects <= 10000 or not 1 <= args.deadline_seconds <= 3600:
        parser.error("objects must be 1–10000; deadline must be 1–3600 seconds")
    if args.output.exists():
        parser.error("output must not already exist")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = run(args.objects, args.output.resolve(), args.baseline_script, args.deadline_seconds)
    print(json.dumps({"ok": result["ok"], "objects": result["seeded_objects"]}))
