"""Synthetic, bounded inputs and concrete public report cases for offline measurement."""

from __future__ import annotations

import ipaddress
import os
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from psycopg.conninfo import conninfo_to_dict
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import digest, utc
from .costs import Store, _store_charges
from .enrichment import TranscriptContext, TranscriptInput
from .text_selection import TextSelectionConfig

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .settings import Settings

CUTOFF = datetime(2099, 1, 9, tzinfo=UTC)
CASES = ("trend", "brief", "content", "selection", "combined", "changes", "progression", "research")
NEW_REPORT_CASES = {"changes", "progression", "research"}


class Workload(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )

    case: Literal[
        "trend", "brief", "content", "selection", "combined", "changes", "progression", "research"
    ] = "combined"
    products: int = Field(default=2, ge=1, le=10)
    entities: int = Field(default=2, ge=1, le=20)
    samples: int = Field(default=8, ge=2, le=169)
    panel_size: int = Field(default=2, ge=0, le=20)
    texts: int = Field(default=2, ge=0, le=20)
    text_characters: int = Field(default=128, ge=32, le=16000)
    shape: Literal["regular", "skewed", "revisions"] = "regular"
    batch_rows: int = Field(default=1000, ge=1, le=1000)
    max_rows: int = Field(default=10000, ge=1, le=100000)
    text_limit: int = Field(default=100, ge=1, le=200)

    @model_validator(mode="before")
    @classmethod
    def research_defaults(cls, values: Any) -> Any:
        if isinstance(values, dict) and values.get("case") in NEW_REPORT_CASES:
            values = dict(values)
            for name, default in (("products", 1), ("entities", 8), ("texts", 8)):
                values.setdefault(name, default)
        return values

    @property
    def research_reports(self) -> bool:
        return self.case in NEW_REPORT_CASES

    @property
    def rows(self) -> int:
        return self.products * (
            self.entities * (self.samples + (self.shape == "revisions")) + self.panel_size * 2
        )

    @model_validator(mode="after")
    def bounded(self):
        if self.research_reports and self.texts > self.entities:
            raise ValueError("New report profiles admit at most one supplied text per video")
        if self.case in {"progression", "research"} and (
            self.entities < 4 or self.texts != self.entities
        ):
            raise ValueError("Progression profiles need at least four videos, each with one text")
        if self.rows > 10000 or (self.rows + self.batch_rows - 1) // self.batch_rows > 100:
            raise ValueError("At most 10000 rows and 100 import batches")
        if self.products * self.texts * self.text_characters > 320000:
            raise ValueError("At most 320000 supplied text characters")
        return self


def _loopback(host: str | None) -> bool:
    try:
        return ipaddress.ip_address(host or "").is_loopback
    except ValueError:
        return (host or "").lower() == "localhost"


def require_local_test_database(database_url: str) -> dict[str, str]:
    """Validate libpq's actual overrides, including inherited address/service defaults."""
    info: dict[str, str] = {}
    for key, value in conninfo_to_dict(database_url).items():
        if not isinstance(value, str):
            raise ValueError("Use explicit URI connection fields")
        info[key] = value
    address = info.get("hostaddr") or os.environ.get("PGHOSTADDR")
    if (
        "service" in info
        or os.environ.get("PGSERVICE")
        or not _loopback(info.get("host"))
        or (address and not _loopback(address))
        or not re.fullmatch(r"[A-Za-z0-9_.-]*test[A-Za-z0-9_.-]*", info.get("dbname", ""), re.I)
    ):
        raise ValueError(
            "Require an explicit loopback native test database without service indirection"
        )
    return info


def require_scratch_settings(settings: Settings) -> str:
    """Validate ownership and local endpoints before constructing or initializing services."""
    endpoint = urlsplit(settings.s3_endpoint_url or "")
    connection = require_local_test_database(settings.database_url.get_secret_value())
    options = connection.get("options", "")
    match = re.fullmatch(r"-c search_path=(research_workload_[0-9a-f]{32})", options)
    if (
        not match
        or endpoint.scheme != "http"
        or not _loopback(endpoint.hostname)
        or endpoint.username
        or endpoint.password
        or settings.acquisition_enabled
        or settings.s3_bucket != match[1].replace("_", "-")
    ):
        raise ValueError("Research workload requires its owned synthetic loopback scratch state")
    return match[1]


def require_empty_scratch(application: Pipeline) -> None:
    """Only a completely empty bootstrap schema may receive initialization writes."""
    schema = require_scratch_settings(application.settings)
    state = application.catalog.one(
        "SELECT current_schema() AS schema,EXISTS(SELECT 1 FROM pg_depend "
        "WHERE refclassid='pg_namespace'::regclass "
        "AND refobjid=current_schema()::regnamespace) AS occupied"
    )
    if state is None or state["schema"] != schema or state["occupied"]:
        raise ValueError("Research workload requires its existing empty owned bootstrap schema")


def require_scratch(application: Pipeline) -> None:
    """Never adopt an operator workspace, even when its visible products are synthetic."""
    schema = require_scratch_settings(application.settings)
    current = application.catalog.one("SELECT current_schema() AS schema")
    if current is None or current["schema"] != schema:
        raise ValueError("Research workload refuses a mismatched database schema")
    if application.catalog.one("SELECT 1 FROM source_capabilities WHERE state='available'"):
        raise ValueError("Research workload refuses enabled source capabilities")
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
            raise ValueError("Research workload refuses live optional state")
    for table in (
        "products",
        "entities",
        "datasets",
        "collection_tasks",
        "evaluation_runs",
        "assessment_runs",
    ):
        if application.catalog.one(f"SELECT 1 FROM {table} WHERE NOT synthetic LIMIT 1"):
            raise ValueError("Research workload refuses live state")


def seed(application: Pipeline, workload: Workload) -> dict:
    require_scratch(application)
    if application.catalog.one("SELECT 1 FROM products UNION ALL SELECT 1 FROM datasets LIMIT 1"):
        raise ValueError("Research workload requires empty scratch state")
    rows: list[dict[str, Any]] = []
    links: list[tuple[str, str]] = []
    latest_video: dict[int, dict[str, Any]] = {}
    first_video: dict[tuple[int, int], dict[str, Any]] = {}
    for product in range(workload.products):
        product_id = f"workload-{product}"
        application.create_product(product_id, product_id, synthetic=True, market="FR")
        for entity in range(workload.entities):
            key = f"p{product}-video{entity}"
            links.append((product_id, f"workload:video:{key}"))
            for sample in range(workload.samples):
                # Skew concentrates growth in one entity, retaining the same complete row count.
                timestamp = CUTOFF - timedelta(
                    hours=168 * (workload.samples - sample - 1) / (workload.samples - 1)
                )
                views = 100 + sample * (100 if workload.shape == "skewed" and entity == 0 else 1)
                row: dict[str, Any] = {
                    "observation_id": f"{key}-{sample}",
                    "source": "workload",
                    "entity_type": "video",
                    "entity_id": key,
                    "synthetic": True,
                    "observed_at": timestamp.isoformat(),
                    "available_at": timestamp.isoformat(),
                    "market": "FR",
                    "metrics": {"view_count": {"lower": views, "upper": views}},
                    "attributes": {"creator_id": key, "title": "Synthetic physical product"},
                }
                if workload.research_reports:
                    step = 100 if workload.shape == "skewed" and entity == 0 else entity + 1
                    views = 100 + sample * step + (step if sample == workload.samples - 1 else 0)
                    row["metrics"] = {"view_count": {"lower": views, "upper": views}}
                    row["published_at"] = (CUTOFF - timedelta(hours=170)).isoformat()
                    row["attributes"].update(
                        distribution="public_video", distribution_context="public_video"
                    )
                rows.append(row)
                latest_video[product] = row
                if sample == 0:
                    first_video[(product, entity)] = row
            if workload.shape == "revisions":
                rows.append(
                    {
                        **row,
                        "observation_id": f"{key}-correction",
                        "available_at": (CUTOFF - timedelta(hours=1)).isoformat(),
                        "observed_at": (CUTOFF - timedelta(hours=168)).isoformat(),
                        "metrics": {"view_count": {"lower": 90, "upper": 90}},
                    }
                )
        for offer in range(workload.panel_size):
            key = f"p{product}-offer{offer}"
            links.append((product_id, f"workload:offer:{key}"))
            for sample in range(2):
                timestamp = CUTOFF - timedelta(hours=24 * (1 - sample))
                rows.append(
                    {
                        "observation_id": f"{key}-{sample}",
                        "source": "workload",
                        "entity_type": "offer",
                        "entity_id": key,
                        "synthetic": True,
                        "observed_at": timestamp.isoformat(),
                        "available_at": timestamp.isoformat(),
                        "market": "FR",
                        "metrics": {},
                        "attributes": {
                            "seller_id": key,
                            "query_scope": "workload-fr",
                            "coverage_scope": "declared-panel",
                            "distribution": "offer_listing",
                            "price": str(20 - sample),
                            "currency": "EUR",
                            "variant_id": "one",
                            "price_basis": "per_item",
                            "tax_basis": "tax_included",
                            "shipping_basis": "shipping_excluded",
                        },
                    }
                )
    datasets = []
    for offset in range(0, len(rows), workload.batch_rows):
        batch = rows[offset : offset + workload.batch_rows]
        imported = application.import_records(batch, synthetic=True)
        datasets.extend(imported)
        # import_records may split a batch by source/day: authoritative membership is SQL.
    for linked_product, linked_entity in links:
        application.review_link(linked_product, linked_entity)
    if workload.texts:
        texts = application.transcript_insights()
        texts.initialize()
        text_inputs = [
            (
                product,
                index,
                first_video[(product, index)]
                if workload.research_reports
                else latest_video[product],
            )
            for product in range(workload.products)
            for index in range(workload.texts)
        ]
        membership: dict[str, str] = {}
        for product, index, observation in text_inputs:
            observation_id = observation["observation_id"]
            if observation_id not in membership:
                member = application.catalog.one(
                    "SELECT n.dataset_id FROM observation_deliveries o JOIN normalized_objects n "
                    "ON n.object_id=o.object_id WHERE o.observation_id=%s "
                    "ORDER BY n.dataset_id LIMIT 1",
                    (observation_id,),
                )
                if member is None:
                    raise ValueError("Synthetic transcript membership is unavailable")
                membership[observation_id] = member["dataset_id"]
            candidate = index < workload.entities // 2
            prefix = (
                ("It can save time. " if candidate else "A small blue item. ")
                if workload.research_reports
                else f"Synthetic text {index}: can save time. Easy to use. "
            )
            text = (prefix + "x" * workload.text_characters)[: workload.text_characters]
            known_at = utc(observation["observed_at"]) if workload.research_reports else CUTOFF
            texts.import_transcript(
                TranscriptInput(
                    product_id=f"workload-{product}",
                    entity_key=f"workload:video:{observation['entity_id']}",
                    dataset_id=membership[observation_id],
                    observation_id=observation_id,
                    synthetic=True,
                    text=text,
                    source_kind="human_transcript",
                    language="en",
                    permission_reference="synthetic-local-only",
                    context=TranscriptContext(
                        reference="synthetic-context",
                        known_at=known_at,
                        creator_id=observation["attributes"]["creator_id"]
                        if workload.research_reports
                        else f"creator-{product}",
                        creator_role="seller",
                        variant=f"variant-{index}",
                        known_theme_ids=("time",)
                        if candidate or not workload.research_reports
                        else (),
                    ),
                ),
                as_of=known_at,
            )
    return {
        "rows": len(rows),
        "datasets": len(datasets),
        "linked_entities": len(links),
        "supplied_texts": workload.products * workload.texts,
        "supplied_text_characters": workload.products * workload.texts * workload.text_characters,
        "fixture_sha256": digest(rows),
        "text_timing": "known_by_first_metric" if workload.research_reports else "known_at_cutoff",
        "cutoff": CUTOFF.isoformat(),
        "source_requests": 0,
        "paid_provider_calls": 0,
    }


def reports(application: Pipeline, workload: Workload) -> dict:
    """Concrete adapters only; each public module retains its guards and full output."""
    require_scratch(application)
    results = {}
    cases = (
        ("trend", "brief", "content", "selection")
        if workload.case == "combined"
        else ("changes", "progression")
        if workload.case == "research"
        else (workload.case,)
    )
    for case in cases:
        for product in range(workload.products):
            product_id = f"workload-{product}"
            if case == "trend":
                report = application.trend_insights().describe(
                    product_id, CUTOFF, True, max_rows=workload.max_rows
                )
            elif case == "brief":
                report = application.decision_brief().describe(
                    product_id,
                    "FR",
                    CUTOFF,
                    True,
                    max_rows=workload.max_rows,
                    text_limit=workload.text_limit,
                )
            elif case == "content":
                report = application.content_insights().aggregate(
                    product_id, synthetic=True, as_of=CUTOFF, limit=workload.text_limit
                )
            elif case == "changes":
                report = application.decision_changes().compare(
                    product_id,
                    "FR",
                    CUTOFF - timedelta(hours=24),
                    CUTOFF,
                    True,
                    max_rows=workload.max_rows,
                    text_limit=workload.text_limit,
                )
            elif case == "progression":
                report = application.content_progression().describe(
                    product_id,
                    CUTOFF,
                    True,
                    window_hours=24,
                    max_rows=workload.max_rows,
                    max_texts=workload.text_limit,
                    max_strata=16,
                )
            else:
                report = application.text_selection().preview(
                    [product_id],
                    synthetic=True,
                    as_of=CUTOFF,
                    config=TextSelectionConfig(
                        per_product_candidates=min(100, workload.text_limit)
                    ),
                )
            results[f"{case}:{product_id}"] = report
    return results


def result_identity(results: dict) -> dict:
    return {
        "result_sha256": digest(results),
        "reports": len(results),
        "public_ids": {
            key: value.get("report_id", value.get("plan_id", value.get("aggregate_id")))
            for key, value in results.items()
        },
        "report_sha256": {key: digest(value) for key, value in results.items()},
        "reported_work": reported_work(results),
    }


def reported_work(results: dict) -> dict:
    """Retain module-reported work, without recomputing comparison or association rules."""
    work = {}
    for key, report in results.items():
        if key.startswith("changes:"):
            work[key] = {"windows": report["trajectory"]["windows"]}
        elif key.startswith("progression:"):
            work[key] = {
                "comparison_status": report["comparison_status"],
                "eligible_video_count": report["eligible_video_count"],
                "distinct_text_count": report["distinct_text_count"],
                "exclusion_counts": report["exclusion_counts"],
                "source_unavailable_count": report["source_unavailable_count"],
                "comparison_reasons": sorted(
                    {
                        reason
                        for theme in report["themes"]
                        for stratum in theme["strata"]
                        for reason in stratum["reasons"]
                    }
                ),
                "associations": [
                    {"theme_id": theme["theme_id"], **stratum}
                    for theme in report["themes"]
                    for stratum in theme["strata"]
                    if stratum["status"] == "observed_association"
                ],
            }
    return work


def require_measured_reports(results: dict) -> None:
    """Refuse an unavailable fast path as a successful measured new-report case."""
    for key, work in reported_work(results).items():
        if key.startswith("changes:") and not any(
            row["window_hours"] == 24 and row["comparison_status"] == "measured"
            for row in work["windows"]
        ):
            raise ValueError("Synthetic changes case did not reach a measured 24-hour panel")
        if key.startswith("progression:") and not work["associations"]:
            raise ValueError("Synthetic progression case did not reach an observed association")


def price_usage(usage: dict, store: Store) -> dict:
    """Existing price arithmetic, with no monthly retention duration inferred from a run."""
    quantities = {
        "source_store_bytes": None,
        "source_store_egress_bytes": Decimal(usage["download_bytes"]),
        "source_read_requests": Decimal(usage["get_requests"] + usage["head_requests"]),
        "source_write_requests": Decimal(usage["put_requests"]),
    }
    charges = _store_charges(store, quantities, "source", free=False)
    return {
        "observed_on": str(store.observed_on),
        "currency": store.currency,
        "price_assumptions": store.model_dump(mode="json"),
        "charges": {key: None if value is None else str(value) for key, value in charges.items()},
        "scope": "Marginal request/egress scenario; no account free tier; HTTP retries excluded",
        "production_cost": None,
    }
