"""Optional research restore qualification uses real application SQL and object reads."""

import hashlib
import json
import os
import runpy
import subprocess
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from pydantic import SecretStr

from sm_trending_products.enrichment import EnrichmentBudget, TranscriptInsights
from sm_trending_products.product_cohorts import CohortInsights
from sm_trending_products.research_validation import ResearchValidation
from sm_trending_products.text_selection import TextSelection

FIXTURE = runpy.run_path(str(Path(__file__).parents[1] / "scripts/research_recovery_fixture.py"))
seed, verify = FIXTURE["seed"], FIXTURE["verify"]


def test_read_only_verification_and_seed_replay_do_not_add_work(pipeline, monkeypatch):
    receipt = seed(pipeline)
    assert len(receipt["checks"]) == 10 and all(receipt["checks"].values())
    assert seed(pipeline) == receipt
    before = FIXTURE["_snapshot"](pipeline)

    def forbidden(*args, **kwargs):
        raise AssertionError("Verification attempted a mutation or source/provider work")

    for owner, names in (
        (ResearchValidation, ("initialize", "register", "lock", "run")),
        (CohortInsights, ("initialize", "save_taxonomy", "assign")),
        (TranscriptInsights, ("initialize", "import_transcript", "analyze", "configure_budget")),
        (TextSelection, ("execute",)),
    ):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)
    monkeypatch.setattr(pipeline.collector, "run", forbidden)
    checked = verify(pipeline, receipt)
    assert checked["status"] == "verified"
    assert checked["new_source_requests"] == checked["paid_provider_calls"] == 0
    assert FIXTURE["_snapshot"](pipeline) == before


def test_existing_commerce_fixture_and_checkpoint_recovery_remain_compatible(pipeline):
    from sm_trending_products.commerce_rehearsal import rehearse, verify_rehearsal

    commerce = rehearse(pipeline, synthetic=True, run_id="ci-commerce-v1")
    assert commerce["status"] == "completed"
    receipt = seed(pipeline)
    checkpoint = runpy.run_path(str(Path(__file__).parents[1] / "scripts/recovery_fixture.py"))
    checkpoint["seed"](pipeline)
    assert checkpoint["resume"](pipeline.settings)["status"] == "verified"
    commerce_check = verify_rehearsal(pipeline, synthetic=True, run_id="ci-commerce-v1")
    assert commerce_check["status"] == "verified"
    assert verify(pipeline, receipt)["status"] == "verified"


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM research_policy_locks",
        "DELETE FROM research_lineage_freezes",
        "DELETE FROM research_attempts",
        "DELETE FROM cohort_assignment_revisions",
        "UPDATE cohort_assignments SET revision=2",
        "UPDATE cohort_taxonomies SET revision=2",
        "UPDATE enrichment_transcripts SET metadata='{}'::jsonb,context_sha256=''",
        "UPDATE enrichment_analyses SET state='uncertain'",
        "UPDATE application_module_migrations SET sha256=repeat('0',64) "
        "WHERE module='research_validation' AND version=1",
    ],
)
def test_damaged_restored_state_is_detected(pipeline, statement):
    receipt = seed(pipeline)
    pipeline.catalog.execute(statement)
    with pytest.raises(ValueError):
        verify(pipeline, receipt)


@pytest.mark.parametrize("action", (seed, lambda application: verify(application, {})))
def test_live_product_refused_before_optional_tables_or_objects(pipeline, action):
    pipeline.create_product("live", "Real product", synthetic=False)
    with pytest.raises(ValueError, match="refuses live"):
        action(pipeline)
    assert (
        pipeline.catalog.one("SELECT to_regclass('application_module_migrations') AS relation")[
            "relation"
        ]
        is None
    )


def test_receipt_checksum_is_validated_before_constructing_application(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    path = tmp_path / "receipt.json"
    path.write_text("{}")
    with pytest.raises(SystemExit) as error:
        FIXTURE["main"](["verify", "--receipt", str(path), "--expected-sha256", "0" * 64])
    assert error.value.code == 2


@pytest.mark.parametrize("encoded", (b"[]", b"not-json", b"x" * 65537))
def test_receipt_shape_and_actual_read_size_reject_before_settings(tmp_path, monkeypatch, encoded):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    path = tmp_path / "receipt.json"
    path.write_bytes(encoded)
    with pytest.raises(SystemExit) as error:
        FIXTURE["main"](
            [
                "verify",
                "--receipt",
                str(path),
                "--expected-sha256",
                hashlib.sha256(encoded).hexdigest(),
            ]
        )
    assert error.value.code == 2


def test_cloud_store_and_enabled_source_are_refused(pipeline, monkeypatch):
    with monkeypatch.context() as cloud:
        cloud.setattr(pipeline.settings, "s3_endpoint_url", "https://s3.amazonaws.com")
        with pytest.raises(ValueError, match="controlled local"):
            seed(pipeline)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
        "VALUES('fixture','account','collect','available','{}')"
    )
    with pytest.raises(ValueError, match="enabled source"):
        seed(pipeline)


def test_live_optional_budget_without_any_product_is_refused(pipeline):
    texts = pipeline.transcript_insights()
    texts.initialize()
    texts.configure_budget("local-rules", EnrichmentBudget(), synthetic=False)
    with pytest.raises(ValueError, match="live optional"):
        seed(pipeline)


@pytest.mark.skipif(
    not os.environ.get("RESEARCH_TEST_PG_CONTAINER"),
    reason="Set RESEARCH_TEST_PG_CONTAINER for an existing disposable PostgreSQL test container",
)
def test_actual_pg_dump_and_restore_preserve_optional_research(pipeline, tmp_path):
    from sm_trending_products.pipeline import Pipeline

    receipt = seed(pipeline)
    encoded = json.dumps(receipt).encode()
    assert len(encoded) < 65536 and hashlib.sha256(encoded).hexdigest()
    container = os.environ["RESEARCH_TEST_PG_CONTAINER"]
    original = urlsplit(pipeline.settings.database_url.get_secret_value())
    schema = parse_qs(original.query)["options"][0].removeprefix("-c search_path=")
    restored_name = "research_restore_" + uuid4().hex
    command = ["docker", "exec", container]
    dump = tmp_path / "research.dump"
    with dump.open("wb") as output:
        subprocess.run(
            command
            + [
                "pg_dump",
                "--format=custom",
                "--no-owner",
                "--schema",
                schema,
                "--username",
                original.username,
                "--dbname",
                original.path.lstrip("/"),
            ],
            check=True,
            stdout=output,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    subprocess.run(
        command + ["createdb", "--username", original.username, restored_name],
        check=True,
        capture_output=True,
        timeout=30,
    )
    try:
        with dump.open("rb") as data:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "pg_restore",
                    "--exit-on-error",
                    "--single-transaction",
                    "--no-owner",
                    "--no-privileges",
                    "--username",
                    original.username,
                    "--dbname",
                    restored_name,
                ],
                check=True,
                stdin=data,
                capture_output=True,
                timeout=60,
            )
        restored_url = urlunsplit(original._replace(path="/" + restored_name))
        restored_settings = pipeline.settings.model_copy(
            update={"database_url": SecretStr(restored_url)}
        )
        with Pipeline(restored_settings) as restored:
            assert verify(restored, receipt)["status"] == "verified"
            restored.catalog.execute("DELETE FROM research_lineage_freezes")
            with pytest.raises(ValueError):
                verify(restored, receipt)
    finally:
        subprocess.run(
            command + ["dropdb", "--force", "--username", original.username, restored_name],
            check=True,
            capture_output=True,
            timeout=30,
        )
