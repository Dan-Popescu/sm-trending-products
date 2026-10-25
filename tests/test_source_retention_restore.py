"""Native backups preserve unlimited policy and immutable withdrawal authority."""

import os
import subprocess
from urllib.parse import parse_qs, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pydantic import SecretStr
from test_capture_spool import local_capture
from test_raw_capture import ACCOUNT, enqueue, one_shot_pages, page, service
from test_source_retention import old_history

from sm_trending_products.collector import CollectionError
from sm_trending_products.pipeline import Pipeline


@pytest.mark.integration
def test_native_dump_restores_unlimited_history_finite_expiry_and_withdrawal(pipeline, tmp_path):
    container = os.environ.get("TEST_POSTGRES_CONTAINER")
    if not container:
        pytest.skip("Set TEST_POSTGRES_CONTAINER for isolated native dump/restore")
    retained, expired = old_history(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified,details) "
        "VALUES('tiktok_display',NULL,true,'{\"source_retention_evidence\":\"source-contract\"}')"
    )
    with one_shot_pages([page()]) as (endpoint, _):
        enqueue(pipeline, endpoint)
        _, receipt, _ = local_capture(pipeline)
    acquisition = service(pipeline)
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    policies = pipeline.catalog.all("SELECT * FROM source_policies ORDER BY source")
    certificates = pipeline.catalog.all("SELECT * FROM capture_spool_retirements")
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    schema = parse_qs(parsed.query)["options"][0].removeprefix("-c search_path=")
    target = "source_retention_restore_" + uuid4().hex
    dump = tmp_path / "retention.dump"
    command = ["docker", "--host=unix:///var/run/docker.sock", "exec", "-i", container]
    with dump.open("wb") as stream:
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
            stdout=stream,
            stderr=subprocess.PIPE,
            check=True,
            timeout=30,
        )
    admin_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    with psycopg.connect(admin_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target)))
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
                timeout=30,
            )
        target_url = urlunsplit((parsed.scheme, parsed.netloc, "/" + target, parsed.query, ""))
        settings = pipeline.settings.model_copy(update={"database_url": SecretStr(target_url)})
        with Pipeline(settings) as restored:
            restored.initialize()
            assert restored.catalog.all("SELECT * FROM source_policies ORDER BY source") == policies
            assert restored.catalog.all("SELECT * FROM capture_spool_retirements") == certificates
            assert set(restored.retention()["dataset_ids"]) == {expired}
            restored.retention(apply=True)
            assert restored.replay(retained)["records"] == 1
            with pytest.raises(CollectionError, match="capture_source_removed"):
                with restored.catalog.transaction() as connection:
                    service(restored).spool.assert_publishable(connection, receipt)
            assert restored.verify()["ok"]
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target))
            )
