"""Opt-in real PostgreSQL custom-dump recovery of a pending continuation."""

import os
import subprocess
from urllib.parse import parse_qs, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pydantic import SecretStr
from test_large_invalidation import seed_fanout

from sm_trending_products.catalog import InvalidationPending
from sm_trending_products.invalidation import SourceInvalidation
from sm_trending_products.pipeline import Pipeline


@pytest.mark.integration
@pytest.mark.parametrize("pending", [False, True])
def test_native_custom_dump_keeps_gate_and_resumes_persisted_cursor(
    pipeline, tmp_path, monkeypatch, pending
):
    container = os.environ.get("TEST_POSTGRES_CONTAINER")
    if not container:
        pytest.skip("Set TEST_POSTGRES_CONTAINER for an isolated native dump/restore probe")
    source = seed_fanout(pipeline, 2500)
    work = SourceInvalidation(pipeline.catalog)
    if pending:
        work.begin(source, "source_deleted")
        assert work.resume(max_batches=1, max_seconds=1)["status"] == "partial"
    fingerprint = pipeline.catalog.export_state()
    assert ("source_invalidations" in fingerprint) is pending
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    schema = parse_qs(parsed.query)["options"][0].removeprefix("-c search_path=")
    target = "invalidation_restore_" + uuid4().hex
    dump = tmp_path / "pending.dump"
    command = ["docker", "exec", "-i", container]
    with dump.open("wb") as output:
        subprocess.run(
            command
            + [
                "pg_dump",
                "--format=custom",
                "--no-owner",
                "--no-acl",
                "--schema",
                schema,
                "--username",
                parsed.username,
                "--dbname",
                parsed.path.removeprefix("/"),
            ],
            stdout=output,
            stderr=subprocess.PIPE,
            check=True,
            timeout=30,
        )
    admin_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    with psycopg.connect(admin_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target)))
    try:
        with dump.open("rb") as input_file:
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
                stdin=input_file,
                capture_output=True,
                check=True,
                timeout=30,
            )
        target_url = urlunsplit((parsed.scheme, parsed.netloc, "/" + target, parsed.query, ""))
        settings = pipeline.settings.model_copy(update={"database_url": SecretStr(target_url)})
        with Pipeline(settings) as restored:
            restored.initialize()
            assert restored.catalog.export_state() == fingerprint
            if not pending:
                SourceInvalidation(restored.catalog).begin(source, "source_deleted")
                changed = restored.catalog.export_state()
                assert "source_invalidations" in changed and "source_invalidation_work" in changed
                assert changed != fingerprint
                return
            original = restored.catalog.resume_invalidations
            monkeypatch.setattr(restored.catalog, "resume_invalidations", lambda **kwargs: None)
            with pytest.raises(InvalidationPending):
                restored.latest_assessments(True)
            assert restored.latest_assessments(False) == []
            monkeypatch.setattr(restored.catalog, "resume_invalidations", original)
            assert original(max_batches=1000, max_seconds=30)["status"] == "complete"
            assert (
                restored.catalog.one(
                    "SELECT count(*) AS n FROM derived_publications WHERE state='invalidated'"
                )["n"]
                == 2500
            )
            assert restored.latest_assessments(True) == []
            assert (
                restored.catalog.one("SELECT count(*) AS n FROM source_invalidation_work")["n"] == 0
            )
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target))
            )
