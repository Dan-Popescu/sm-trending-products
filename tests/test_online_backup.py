"""A real application snapshot can be backed up while one-shot capture proceeds."""

import json
import os
import runpy
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from botocore.exceptions import ClientError
from psycopg import sql
from pydantic import SecretStr
from test_object_backup import configure
from test_pipeline_integration import ACCOUNT, controlled_source, video_observation

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration
snapshot = runpy.run_path(str(Path(__file__).parents[1] / "scripts/object_snapshot.py"))


def online_options(tmp_path):
    return {
        "online": True,
        "checkpoint_signal": tmp_path / "ready.json",
        "continue_signal": tmp_path / "continue",
        "catalog_state": tmp_path / "catalog-state.json",
        "deadline_seconds": 30,
    }


def test_online_capture_requires_the_dump_handshake_before_creating_state(tmp_path):
    output = tmp_path / "objects"
    with pytest.raises(ValueError, match="requires a dump handshake"):
        snapshot["capture"](output, copy_objects=True, online=True)
    assert not output.exists()


def test_backup_refuses_conflicting_modes_before_creating_output(tmp_path):
    output = tmp_path / "new-backup"
    result = subprocess.run(
        ["bash", "scripts/backup.sh", str(output), "--online", "--offline"],
        cwd=Path(__file__).parents[1],
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 2 and "Choose one backup mode" in result.stderr
    assert not output.exists()


def await_ready(future, path):
    until = time.monotonic() + 10
    while not path.exists() and time.monotonic() < until:
        if future.done():
            future.result()
        time.sleep(0.02)
    assert path.exists()
    return json.loads(path.read_text())


def dump_during_one_shot_capture(pipeline, command, dump, schema, snapshot_id):
    """Hold pg_dump on a probe relation while the ordinary collector progresses."""
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    application = "online_backup_" + uuid4().hex
    process = None
    response = {
        "data": {"videos": [{"id": "after-cut", "view_count": 99}], "has_more": False},
        "error": {"code": "ok"},
    }
    try:
        with dump.open("wb") as stream:
            with psycopg.connect(pipeline.settings.database_url.get_secret_value()) as gate:
                gate.execute("LOCK TABLE online_backup_dump_gate IN ACCESS EXCLUSIVE MODE")
                process = subprocess.Popen(
                    command[:-1]
                    + ["--env", f"PGAPPNAME={application}", command[-1]]
                    + [
                        "pg_dump",
                        "--format=custom",
                        "--no-owner",
                        "--no-acl",
                        "--schema",
                        schema,
                        "--snapshot",
                        snapshot_id,
                        "--username",
                        parsed.username,
                        "--dbname",
                        parsed.path.lstrip("/"),
                    ],
                    stdout=stream,
                    stderr=subprocess.PIPE,
                )
                until = time.monotonic() + 10
                while time.monotonic() < until:
                    waiting = pipeline.catalog.one(
                        "SELECT 1 FROM pg_stat_activity WHERE application_name=%s "
                        "AND wait_event_type='Lock'",
                        (application,),
                    )
                    if waiting:
                        break
                    assert process.poll() is None
                    time.sleep(0.02)
                assert waiting, "pg_dump did not reach its isolated relation-lock gate"
                with controlled_source([(200, response, {}), (410, {}, {})]) as (
                    endpoint,
                    requests,
                ):
                    pipeline.enqueue(
                        "tiktok_display",
                        ACCOUNT,
                        "list",
                        {"endpoint_override": endpoint},
                        synthetic=True,
                    )
                    captured = pipeline.collect_batch(1, synthetic=True)
                    assert captured["processed"] == 1 and captured["errors"] == []
                    assert len(requests) == 1 and process.poll() is None
                pipeline.create_product("after-cut", "After cut", synthetic=True)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors
    finally:
        if process is not None and process.poll() is None:
            pipeline.catalog.one(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=%s",
                (application,),
            )
            process.kill()
            process.wait(timeout=5)


def test_online_inventory_uses_one_snapshot_while_capture_and_migration_waiter_coexist(
    pipeline, tmp_path, monkeypatch
):
    s3, _ = configure(pipeline, monkeypatch)
    # configure() addresses its own runpy module; keep this isolated probe explicit.
    monkeypatch.setitem(snapshot["capture"].__globals__, "client", lambda: s3)
    initial = pipeline.import_records([video_observation("before-cut", now(), 10)], synthetic=True)[
        0
    ]
    frozen, listing = threading.Event(), threading.Event()
    expected = snapshot["capture"].__globals__["_expected_objects"]

    def checkpoint_expected(connection, db):
        expected(connection, db)
        frozen.set()
        assert listing.wait(10)

    monkeypatch.setitem(snapshot["capture"].__globals__, "_expected_objects", checkpoint_expected)
    response = {
        "data": {"videos": [{"id": "one-shot", "view_count": 42}], "has_more": False},
        "error": {"code": "ok"},
    }
    with controlled_source([(200, response, {}), (410, {}, {})]) as (endpoint, requests):
        pipeline.enqueue(
            "tiktok_display", ACCOUNT, "list", {"endpoint_override": endpoint}, synthetic=True
        )
        before = pipeline.catalog.export_state()
        options = online_options(tmp_path)
        output = tmp_path / "online/objects"
        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(snapshot["capture"], output, copy_objects=True, **options)
            try:
                assert frozen.wait(10)
                # No retention lock is held by the exporter. A retention waiter
                # therefore cannot queue subsequent captures behind the backup.
                with psycopg.connect(
                    pipeline.settings.database_url.get_secret_value(), autocommit=True
                ) as control:
                    assert control.execute("SELECT pg_try_advisory_lock(492371205)").fetchone()[0]
                    control.execute("SELECT pg_advisory_unlock(492371205)")
                    assert not control.execute("SELECT pg_try_advisory_lock(492371204)").fetchone()[
                        0
                    ]
                collected = pipeline.collect_batch(1, synthetic=True)
                assert collected["processed"] == 1 and collected["errors"] == []
                assert len(requests) == 1
                after = pipeline.catalog.export_state()
                assert after != before
                listing.set()
                ready = await_ready(future, options["checkpoint_signal"])
                assert pipeline.catalog.export_state(ready["snapshot_id"]) == before
                assert json.loads(options["catalog_state"].read_text()) == before
            finally:
                listing.set()
                options["continue_signal"].touch()
            result = future.result(timeout=15)
    assert result["complete"] and result["inventory_scope"] == "catalog_snapshot"
    inventory = [json.loads(row) for row in (output / "objects.jsonl").read_text().splitlines()]
    original = pipeline.catalog.one("SELECT manifest FROM datasets WHERE dataset_id=%s", (initial,))
    assert {row["key"] for row in inventory} == {
        original["manifest"]["raw"]["key"],
        original["manifest"]["manifest_key"],
        *(row["key"] for row in original["manifest"]["normalized"]),
    }
    assert pipeline.catalog.export_state() == after
    assert pipeline.verify()["ok"]


def test_online_custom_dump_restores_exact_catalog_cut_without_later_observations(
    pipeline, tmp_path, monkeypatch
):
    container = os.environ.get("TEST_POSTGRES_CONTAINER")
    if not container:
        pytest.skip("Set TEST_POSTGRES_CONTAINER for isolated native dump/restore")
    s3, _ = configure(pipeline, monkeypatch)
    monkeypatch.setitem(snapshot["capture"].__globals__, "client", lambda: s3)
    monkeypatch.setitem(snapshot["restore"].__globals__, "client", lambda: s3)
    pipeline.import_records([video_observation("before-cut", now(), 10)], synthetic=True)
    pipeline.create_product("before-cut", "Before cut", synthetic=True)
    pipeline.catalog.execute("CREATE TABLE online_backup_dump_gate(value integer)")
    before = pipeline.catalog.export_state()
    options = online_options(tmp_path)
    output = tmp_path / "online/objects"
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    schema = parse_qs(parsed.query)["options"][0].removeprefix("-c search_path=")
    target = "online_restore_" + uuid4().hex
    dump = tmp_path / "application.dump"
    command = ["docker", "--host=unix:///var/run/docker.sock", "exec", "-i", container]
    with ThreadPoolExecutor(max_workers=1) as workers:
        future = workers.submit(snapshot["capture"], output, copy_objects=True, **options)
        try:
            ready = await_ready(future, options["checkpoint_signal"])
            dump_during_one_shot_capture(pipeline, command, dump, schema, ready["snapshot_id"])
        finally:
            options["continue_signal"].touch()
        result = future.result(timeout=15)
    assert result["complete"]
    assert pipeline.catalog.export_state() != before
    admin_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    with psycopg.connect(admin_url, autocommit=True) as control:
        control.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target)))
    try:
        with dump.open("rb") as stream:
            subprocess.run(
                command
                + [
                    "pg_restore",
                    "--exit-on-error",
                    "--single-transaction",
                    "--no-owner",
                    "--no-acl",
                    "--username",
                    parsed.username,
                    "--dbname",
                    target,
                ],
                stdin=stream,
                capture_output=True,
                check=True,
                timeout=20,
            )
        destination = "online-restore-" + uuid4().hex
        snapshot["restore"](output, bucket=destination, create_bucket=True)
        target_url = urlunsplit((parsed.scheme, parsed.netloc, "/" + target, parsed.query, ""))
        settings = pipeline.settings.model_copy(
            update={
                "database_url": SecretStr(target_url),
                "s3_bucket": destination,
                "cache_dir": tmp_path / "restored-cache",
            }
        )
        with Pipeline(settings) as restored:
            assert restored.catalog.export_state() == before
            assert restored.verify()["ok"]
            observations, _ = restored.history.observations(now(), True)
            assert [row.entity_id for row in observations] == ["before-cut"]
        live, _ = pipeline.history.observations(now(), True)
        assert {row.entity_id for row in live} == {"before-cut", "after-cut"}
    finally:
        with psycopg.connect(admin_url, autocommit=True) as control:
            control.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target)))


def test_online_retention_race_fails_incomplete_without_blocking_new_publication(
    pipeline, tmp_path, monkeypatch
):
    s3, _ = configure(pipeline, monkeypatch)
    monkeypatch.setitem(snapshot["capture"].__globals__, "client", lambda: s3)
    first = pipeline.import_records([video_observation("expired", now(), 10)], synthetic=True)[0]
    options = online_options(tmp_path)
    output = tmp_path / "online/objects"
    with ThreadPoolExecutor(max_workers=1) as workers:
        future = workers.submit(snapshot["capture"], output, copy_objects=True, **options)
        try:
            await_ready(future, options["checkpoint_signal"])
            row = pipeline.catalog.one(
                "SELECT manifest FROM datasets WHERE dataset_id=%s", (first,)
            )
            pipeline.catalog.invalidate_dataset(first, "source_expired")
            s3.delete_object(Bucket=pipeline.history.bucket, Key=row["manifest"]["raw"]["key"])
            pipeline.import_records([video_observation("continuing", now(), 20)], synthetic=True)
        finally:
            options["continue_signal"].touch()
        with pytest.raises(ClientError):
            future.result(timeout=15)
    assert not json.loads((output / "snapshot.json").read_text())["complete"]
    observations, _ = pipeline.history.observations(now(), True)
    assert [row.entity_id for row in observations] == ["continuing"]
