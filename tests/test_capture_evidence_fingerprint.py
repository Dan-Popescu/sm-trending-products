"""Logical backups retain legal collection evidence at the exported SQL cut."""

from __future__ import annotations

import hashlib
import os
import subprocess
from urllib.parse import parse_qs, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pydantic import SecretStr
from test_capture_spool import local_capture
from test_raw_capture import ACCOUNT, one_shot_pages, page, service

from sm_trending_products.capture_contracts import CaptureContracts, CapturePolicy
from sm_trending_products.contracts import canonical, now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration
LEGAL_TABLES = {
    "capture_policies",
    "capture_obligations",
    "capture_spool_revocations",
    "capture_spool_retirements",
}


def seed_certificate(pipeline, key):
    CaptureContracts(pipeline.catalog).register(
        CapturePolicy(
            source="tiktok_display",
            account_id=ACCOUNT,
            operation="list",
            version=key,
            cadence_seconds=60,
            lateness_seconds=10,
            recoverability="snapshot",
            verification_ref="controlled-backup-contract",
            verified_at=now(),
        ),
        synthetic=True,
    )
    with one_shot_pages([page(key)]) as (endpoint, _):
        pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint},
            synthetic=True,
            idempotency_key=key,
        )
        _, receipt, _ = local_capture(pipeline)
    acquisition = service(pipeline)
    acquisition.request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    assert acquisition.maintain_spool(1000, apply=True)["retired"] == 1
    return receipt["capture"]["capture_id"]


def test_empty_or_pre_migration_legal_tables_preserve_legacy_fingerprint(pipeline):
    before = pipeline.catalog.export_state()
    assert not LEGAL_TABLES.intersection(before)
    pipeline.catalog.execute(
        "DROP TABLE capture_spool_retirements,capture_spool_revocations,"
        "capture_obligations,capture_policies CASCADE"
    )
    assert pipeline.catalog.export_state() == before


def test_populated_legal_evidence_hashes_exact_semantic_rows(pipeline):
    seed_certificate(pipeline, "legal-before")
    fingerprint = pipeline.catalog.export_state()
    assert LEGAL_TABLES.issubset(fingerprint)
    for table in LEGAL_TABLES:
        rows = pipeline.catalog.all(
            sql.SQL("SELECT row_to_json(t) AS row FROM {} t ORDER BY row_to_json(t)::text").format(
                sql.Identifier(table)
            )
        )
        assert rows
        assert fingerprint[table] == hashlib.sha256(canonical(rows).encode()).hexdigest()
    service(pipeline).request_spool_removal(
        "tiktok_display", "other-account", True, "source_opt_out"
    )
    changed = pipeline.catalog.export_state()
    assert {name for name in changed if changed[name] != fingerprint[name]} == {
        "capture_spool_revocations"
    }


def test_each_legal_table_enters_the_fingerprint_only_when_populated(pipeline):
    CaptureContracts(pipeline.catalog).register(
        CapturePolicy(
            source="tiktok_display",
            account_id=ACCOUNT,
            operation="list",
            version="independent-inclusion",
            cadence_seconds=60,
        ),
        synthetic=True,
    )
    assert LEGAL_TABLES.intersection(pipeline.catalog.export_state()) == {"capture_policies"}
    pipeline.enqueue("tiktok_display", ACCOUNT, "list", {}, synthetic=True)
    assert LEGAL_TABLES.intersection(pipeline.catalog.export_state()) == {
        "capture_policies",
        "capture_obligations",
    }
    service(pipeline).request_spool_removal("tiktok_display", ACCOUNT, True, "source_opt_out")
    assert LEGAL_TABLES.intersection(pipeline.catalog.export_state()) == LEGAL_TABLES - {
        "capture_spool_retirements"
    }


def test_exported_snapshot_and_actual_restore_preserve_cut_retirement_evidence(pipeline, tmp_path):
    container = os.environ.get("TEST_POSTGRES_CONTAINER")
    if not container:
        pytest.skip("Set TEST_POSTGRES_CONTAINER for isolated native dump/restore")
    original_id = seed_certificate(pipeline, "certificate-before-cut")
    original = pipeline.catalog.one(
        "SELECT * FROM capture_spool_retirements WHERE capture_id=%s", (original_id,)
    )
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    schema = parse_qs(parsed.query)["options"][0].removeprefix("-c search_path=")
    command = ["docker", "exec", "-i", container]
    dump = tmp_path / "legal-evidence.dump"
    with pipeline.catalog.transaction() as exporter:
        exporter.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        snapshot_id = exporter.execute("SELECT pg_export_snapshot() AS snapshot").fetchone()[
            "snapshot"
        ]
        before = pipeline.catalog.export_state(snapshot_id)
        seed_certificate(pipeline, "certificate-after-cut")
        after = pipeline.catalog.export_state()
        assert {name for name in after if after[name] != before[name]} == LEGAL_TABLES
        assert pipeline.catalog.export_state(snapshot_id) == before
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
                    "--snapshot",
                    snapshot_id,
                    "--username",
                    parsed.username,
                    "--dbname",
                    parsed.path.lstrip("/"),
                ],
                stdout=stream,
                stderr=subprocess.PIPE,
                check=True,
                timeout=30,
            )
    target = "legal_restore_" + uuid4().hex
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
        restored_url = urlunsplit((parsed.scheme, parsed.netloc, "/" + target, parsed.query, ""))
        settings = pipeline.settings.model_copy(update={"database_url": SecretStr(restored_url)})
        with Pipeline(settings) as restored:
            assert restored.catalog.export_state() == before
            assert restored.catalog.all("SELECT * FROM capture_spool_retirements") == [original]
            assert restored.catalog.one("SELECT count(*) AS n FROM capture_obligations")["n"] == 1
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target))
            )
