"""Qualify optional research state only in an explicitly synthetic restore rehearsal."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from sm_trending_products.content_insights import ContentInsights
from sm_trending_products.contracts import AssessmentPolicy, Observation, canonical, digest
from sm_trending_products.enrichment import EnrichmentDenied, TranscriptContext, TranscriptInput
from sm_trending_products.evaluation import EvaluationConfig
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.product_cohorts import CohortInsights
from sm_trending_products.research_validation import ValidationPartition, ValidationProtocol
from sm_trending_products.text_selection import TextSelection, TextSelectionConfig

PRODUCT = "ci-research-development"
TAXONOMY = "ci-research-types"
REUSE_REQUEST = "ci-research-canonical-reuse"
BASE = datetime(2099, 1, 1, tzinfo=UTC)
CUTOFF = BASE + timedelta(days=10)
TEXT = "This can save time. Easy to use."
PARTITIONS = ("development", "test", "holdout")


def _isolated(application: Pipeline) -> None:
    endpoint = urlsplit(application.settings.s3_endpoint_url or "")
    host = endpoint.hostname or ""
    try:
        local = (
            host in {"localhost", "objectstore"}
            or bool(re.fullmatch(r"sm-trends-restore-objects-[0-9a-f]{12}", host))
            or ipaddress.ip_address(host).is_loopback
        )
    except ValueError:
        local = False
    if endpoint.scheme != "http" or not local or endpoint.username or endpoint.password:
        raise ValueError("Research recovery fixture requires a controlled local object store")
    for table in (
        "products",
        "entities",
        "datasets",
        "collection_tasks",
        "evaluation_runs",
        "assessment_runs",
    ):
        if application.catalog.one(f"SELECT 1 FROM {table} WHERE NOT synthetic LIMIT 1"):
            raise ValueError("Research recovery fixture refuses live state")
    if application.catalog.one("SELECT 1 FROM source_capabilities WHERE state='available'"):
        raise ValueError("Research recovery fixture refuses enabled source capabilities")
    for table in (
        "research_protocols",
        "enrichment_transcripts",
        "enrichment_analyses",
        "enrichment_requests",
        "enrichment_daily_budgets",
        "commerce_dossiers",
        "commerce_delivery_connections",
        "commerce_order_connections",
        "commercial_tests",
    ):
        relation = application.catalog.one("SELECT to_regclass(%s) AS relation", (table,))
        if (
            relation
            and relation["relation"]
            and application.catalog.one(f"SELECT 1 FROM {table} WHERE NOT synthetic LIMIT 1")
        ):
            raise ValueError("Research recovery fixture refuses live optional state")


def _protocol() -> ValidationProtocol:
    return ValidationProtocol(
        name="ci-research-recovery-v1",
        config=EvaluationConfig(
            top_k=1,
            horizon_days=(1,),
            launch_delay_days=(0,),
            max_boundary_gap_hours=1,
        ),
        policies={"declared": AssessmentPolicy()},
        partitions={
            name: ValidationPartition(
                product_ids=(f"ci-research-{name}",),
                decision_times=(BASE + timedelta(days=2 * index),),
            )
            for index, name in enumerate(PARTITIONS)
        },
    )


def seed(application: Pipeline) -> dict:
    _isolated(application)
    for name in PARTITIONS:
        application.create_product(
            f"ci-research-{name}",
            f"Synthetic research {name}",
            synthetic=True,
            market="FR",
        )
    validation = application.research_validation()
    validation.initialize()
    registered = validation.register(
        _protocol(),
        "ci-research-register",
        synthetic=True,
        as_of=BASE - timedelta(days=1),
    )
    identifier = registered["protocol_id"]
    validation.lock(
        identifier, "declared", "ci-research-lock", True, as_of=BASE + timedelta(days=1)
    )
    attempt = validation.run(
        identifier,
        "test",
        "declared",
        "ci-research-test",
        synthetic=True,
        as_of=BASE + timedelta(days=3, hours=2),
    )
    if attempt["state"] != "completed":
        raise ValueError("Research recovery fixture attempt did not complete")
    cohorts = CohortInsights(application.catalog)
    cohorts.initialize()
    cohorts.save_taxonomy(
        TAXONOMY,
        [{"category_id": "physical", "name": "Synthetic physical products"}],
        0,
        "ci-research-taxonomy",
        reviewer="synthetic-reviewer",
        rationale="Synthetic fixture",
    )
    cohorts.assign(
        PRODUCT,
        TAXONOMY,
        "physical",
        0,
        "ci-research-assignment",
        taxonomy_revision=1,
        synthetic=True,
        reviewer="synthetic-reviewer",
        rationale="Synthetic reviewed category",
    )
    observation = Observation(
        observation_id="ci-research-text-observation",
        source="ci_research",
        entity_type="video",
        entity_id="reviewed-text",
        synthetic=True,
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    dataset = application.import_records([observation.model_dump(mode="json")], synthetic=True)[0]
    application.review_link(PRODUCT, observation.entity_key)
    product = application.catalog.one(
        "SELECT created_at FROM products WHERE product_id=%s", (PRODUCT,)
    )
    texts = application.transcript_insights()
    texts.initialize()
    imported = texts.import_transcript(
        TranscriptInput(
            product_id=PRODUCT,
            entity_key=observation.entity_key,
            dataset_id=dataset,
            observation_id=observation.observation_id,
            synthetic=True,
            text=TEXT,
            source_kind="human_transcript",
            language="en",
            permission_reference="synthetic-local-only",
            context=TranscriptContext(
                reference="synthetic-reviewed-context",
                known_at=product["created_at"],
                creator_id="synthetic-creator",
                creator_role="seller",
                variant="synthetic-blue",
                known_theme_ids=("time", "ease"),
            ),
        )
    )
    try:
        texts.result(REUSE_REQUEST, PRODUCT, synthetic=True)
    except EnrichmentDenied as error:
        if str(error) != "enrichment_request_unavailable":
            raise
        selection = TextSelection(application.catalog, application.provenance)
        plan = selection.preview(
            [PRODUCT], synthetic=True, config=TextSelectionConfig(max_selected=1)
        )
        applied = selection.execute(plan, request_prefix="ci-research-selection")
        if len(applied["results"]) != 1 or applied["state"] != "completed":
            raise ValueError("Research recovery fixture local selection did not complete") from None
        reused = texts.analyze(REUSE_REQUEST, PRODUCT, imported["transcript_id"], synthetic=True)
        if reused["analysis_id"] != applied["results"][0]["analysis_id"]:
            raise ValueError("Research recovery fixture did not reuse canonical analysis") from None
    return _snapshot(application)


def _snapshot(application: Pipeline) -> dict:
    """Small known-row receipt, separate from capture's compatibility fingerprint."""
    protocol_id = digest({"protocol": _protocol().model_dump(mode="json"), "synthetic": True})
    validation = application.research_validation()
    protocol = validation.protocol(protocol_id, True)
    attempts = validation.attempts(protocol_id, True)
    content = ContentInsights(application.catalog, application.provenance).aggregate(
        PRODUCT,
        synthetic=True,
        as_of=CUTOFF,
        limit=2,
    )
    cohort = CohortInsights(application.catalog).compare(
        CUTOFF,
        True,
        taxonomy_id=TAXONOMY,
        category_id="physical",
        country="FR",
        max_products=10,
    )
    selection = TextSelection(application.catalog, application.provenance).preview(
        [PRODUCT],
        synthetic=True,
        as_of=CUTOFF,
        config=TextSelectionConfig(max_selected=1),
    )
    analysis = application.transcript_insights().result(REUSE_REQUEST, PRODUCT, synthetic=True)
    catalog = application.catalog
    rows = {
        "protocol": catalog.all(
            "SELECT * FROM research_protocols WHERE protocol_id=%s", (protocol_id,)
        ),
        "lock": catalog.all(
            "SELECT * FROM research_policy_locks WHERE protocol_id=%s", (protocol_id,)
        ),
        "lineage": catalog.all(
            "SELECT * FROM research_lineage_freezes WHERE protocol_id=%s", (protocol_id,)
        ),
        "attempt": catalog.all(
            "SELECT * FROM research_attempts WHERE protocol_id=%s ORDER BY attempt_id",
            (protocol_id,),
        ),
        "assignment": catalog.all(
            "SELECT * FROM cohort_assignment_revisions WHERE product_id=%s "
            "AND taxonomy_id=%s ORDER BY revision",
            (PRODUCT, TAXONOMY),
        ),
        "assignment_head": catalog.all(
            "SELECT * FROM cohort_assignments WHERE product_id=%s AND taxonomy_id=%s",
            (PRODUCT, TAXONOMY),
        ),
        "taxonomy_head": catalog.all(
            "SELECT * FROM cohort_taxonomies WHERE taxonomy_id=%s",
            (TAXONOMY,),
        ),
        "taxonomy_revisions": catalog.all(
            "SELECT * FROM cohort_taxonomy_revisions WHERE taxonomy_id=%s ORDER BY revision",
            (TAXONOMY,),
        ),
        "transcripts": catalog.all(
            "SELECT * FROM enrichment_transcripts WHERE synthetic "
            "AND entity_key='ci_research:video:reviewed-text' ORDER BY transcript_id",
        ),
        "requests": catalog.all(
            "SELECT * FROM enrichment_requests WHERE product_id=%s ORDER BY request_id", (PRODUCT,)
        ),
        "analyses": catalog.all(
            "SELECT a.* FROM enrichment_analyses a WHERE EXISTS(SELECT 1 "
            "FROM enrichment_requests r WHERE r.analysis_id=a.analysis_id "
            "AND r.product_id=%s) ORDER BY a.analysis_id",
            (PRODUCT,),
        ),
        "budgets": catalog.all(
            "SELECT * FROM enrichment_daily_budgets WHERE synthetic "
            "AND provider_id='local-rules' ORDER BY budget_day"
        ),
        "modules": catalog.all(
            "SELECT * FROM application_module_migrations WHERE module IN "
            "('research_validation','product_cohorts','transcript_insights') "
            "ORDER BY module,version"
        ),
    }
    checks = {
        "protocol": protocol["protocol"] == _protocol().model_dump(mode="json"),
        "locked_policy": protocol["locked_policy_id"] == "declared" and len(rows["lock"]) == 1,
        "lineage_owner": len(rows["lineage"]) == 1
        and rows["lineage"][0]["policy_id"] == "declared",
        "completed_attempt": len(attempts) == 1
        and attempts[0]["state"] == "completed"
        and "result" in attempts[0],
        "reviewed_assignment": len(cohort["rows"]) == 1
        and cohort["rows"][0]["assignment_revision"] == 1,
        "reviewed_context": len(content["texts"]) == 1
        and content["texts"][0]["context_status"] == "operator_attested"
        and content["texts"][0]["creator_role"] == "seller"
        and content["texts"][0]["creator_id"] == "synthetic-creator",
        "exact_quotes": any(
            mention["quote"] == "save time"
            for text in content["texts"]
            for mention in text["mentions"]
        ),
        "local_analysis": analysis["state"] == "completed"
        and analysis["provider_id"] == "local-rules"
        and str(analysis["reserved_cost"]) == "0",
        "canonical_reuse": len(rows["requests"]) == 2
        and len(rows["analyses"]) == 1
        and not selection["items"]
        and any(row["reason"] == "analysis_completed" for row in selection["decisions"]),
        "module_versions": len(rows["modules"]) == 5,
    }
    if not all(checks.values()):
        raise ValueError(
            "Research recovery fixture checks failed: "
            + ",".join(key for key, ok in checks.items() if not ok)
        )
    return {
        "version": "synthetic-research-recovery-v1",
        "synthetic": True,
        "checks": checks,
        "protocol_id": protocol_id,
        "hashes": {key: digest(value) for key, value in rows.items()}
        | {
            "public_protocol": digest(protocol),
            "public_attempts": digest(attempts),
            "public_cohort": digest(cohort),
            "public_content": digest(content),
            "public_selection": digest(selection),
            "public_analysis": digest(analysis),
        },
    }


def verify(application: Pipeline, receipt: dict) -> dict:
    _isolated(application)
    if receipt != _snapshot(application):
        raise ValueError("Restored research state differs from its seeded receipt")
    return {"status": "verified", **receipt, "new_source_requests": 0, "paid_provider_calls": 0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("seed", "verify"))
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--expected-sha256")
    args = parser.parse_args(argv)
    receipt = None
    if args.action == "verify":
        if args.receipt is None or args.expected_sha256 is None:
            parser.error("Verification requires its explicit receipt and SHA-256")
        try:
            if not args.receipt.is_file():
                raise ValueError
            with args.receipt.open("rb") as stream:
                encoded = stream.read(65537)
            if len(encoded) > 65536:
                raise ValueError
        except (OSError, ValueError):
            parser.error("Receipt must be a regular file no larger than 64 KiB")
        if hashlib.sha256(encoded).hexdigest() != args.expected_sha256:
            parser.error("Fixture receipt checksum mismatch")
        try:
            receipt = json.loads(encoded)
        except (ValueError, UnicodeDecodeError):
            parser.error("Fixture receipt requires a valid JSON object")
        if (
            not isinstance(receipt, dict)
            or receipt.get("version") != "synthetic-research-recovery-v1"
            or receipt.get("synthetic") is not True
        ):
            parser.error("Fixture receipt requires its explicit synthetic version")
    with Pipeline.from_env() as application:
        report = seed(application) if args.action == "seed" else verify(application, receipt)
        print(canonical(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
