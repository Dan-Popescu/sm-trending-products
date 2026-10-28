"""Atomic selected-video enrichment with existing source retirement authority."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from uuid import uuid4

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..contracts import digest, utc
from ..identity_rules import LINK_STATE_CTES
from ..module_schema import initialize_module, require_module
from ..provenance import Provenance
from .contracts import (
    IDENTIFIER,
    EnrichmentBudget,
    EnrichmentDenied,
    InsightOutput,
    ProviderSpec,
    TranscriptAdapter,
    TranscriptInput,
    revalidate,
)
from .extractor import LocalTranscriptAdapter
from .schema import MIGRATIONS


class TranscriptInsights:
    def __init__(self, catalog: Catalog, provenance: Provenance):
        self.catalog, self.provenance = catalog, provenance

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "transcript_insights", MIGRATIONS)

    def configure_budget(
        self, provider: str, budget: EnrichmentBudget, *, synthetic: bool = False
    ) -> dict:
        budget = revalidate(EnrichmentBudget, budget)
        require_module(self.catalog, "transcript_insights")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", provider):
            raise ValueError("Invalid enrichment provider identifier")
        with self.catalog.transaction() as connection:
            row = self._budget(connection, provider, synthetic, budget)
        return {
            "provider": provider,
            "day": str(row["budget_day"]),
            **budget.model_dump(mode="json"),
        }

    @staticmethod
    def _budget(connection, provider, synthetic, budget=None, *, require_same=True):
        day = connection.execute(
            "SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date AS day"
        ).fetchone()["day"]
        if budget is not None:
            connection.execute(
                "INSERT INTO enrichment_daily_budgets"
                "(synthetic,provider_id,budget_day,configuration) "
                "VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (synthetic, provider, day, Jsonb(budget.model_dump(mode="json"))),
            )
        row = connection.execute(
            "SELECT * FROM enrichment_daily_budgets WHERE synthetic=%s AND provider_id=%s "
            "AND budget_day=%s FOR UPDATE",
            (synthetic, provider, day),
        ).fetchone()
        if row is None:
            raise EnrichmentDenied("enrichment_budget_not_configured")
        if (
            require_same
            and budget is not None
            and revalidate(EnrichmentBudget, row["configuration"]) != budget
        ):
            raise EnrichmentDenied("enrichment_daily_budget_already_defined")
        return row

    @staticmethod
    def _clock(connection, as_of, synthetic):
        checked = connection.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        cutoff = checked if as_of is None else utc(as_of)
        if not synthetic and cutoff > checked:
            raise EnrichmentDenied("enrichment_future_live_cutoff")
        return cutoff

    def _scope(self, connection, product, entity, dataset, synthetic, cutoff, observation=None):
        self.catalog.guard_namespace(connection, synthetic)
        try:
            self.catalog.guard_inputs(connection, [dataset])
        except ValueError:
            raise EnrichmentDenied("enrichment_source_unavailable") from None
        owner = connection.execute(
            "WITH "
            + LINK_STATE_CTES
            + "SELECT product_id,confidence FROM resolved_link_owners WHERE entity_key=%s",
            (synthetic, cutoff, cutoff, entity),
        ).fetchone()
        if not owner or owner["product_id"] != product or owner["confidence"] < 0.9:
            raise EnrichmentDenied("enrichment_video_not_owned_at_cutoff")
        source = connection.execute(
            "SELECT d.*,p.verified,p.retention_days,clock_timestamp() AS checked_at "
            "FROM datasets d LEFT JOIN source_policies p USING(source) "
            "JOIN entities e ON e.entity_key=%s AND e.synthetic=d.synthetic "
            "AND e.source=d.source WHERE d.dataset_id=%s AND d.synthetic=%s",
            (entity, dataset, synthetic),
        ).fetchone()
        if not source or source["created_at"] > cutoff:
            raise EnrichmentDenied("enrichment_source_unavailable")
        if not synthetic:
            if source["verified"] is not True:
                raise EnrichmentDenied("enrichment_source_policy_unverified")
            if (
                source["retention_days"] is not None
                and (source["checked_at"] - source["created_at"]).total_seconds()
                >= source["retention_days"] * 86400
            ):
                raise EnrichmentDenied("enrichment_source_retention_expired")
        if observation is not None:
            delivered = connection.execute(
                "SELECT 1 FROM observation_deliveries o JOIN normalized_objects n "
                "ON n.object_id=o.object_id WHERE n.dataset_id=%s AND o.entity_key=%s "
                "AND o.observation_id=%s AND o.available_at<=%s LIMIT 1",
                (dataset, entity, observation, cutoff),
            ).fetchone()
            if not delivered:
                raise EnrichmentDenied("enrichment_source_observation_unavailable")

    def import_transcript(self, value: TranscriptInput, *, as_of: datetime | None = None) -> dict:
        value = revalidate(TranscriptInput, value)
        require_module(self.catalog, "transcript_insights")
        content_key = digest(
            {
                "entity_key": value.entity_key,
                "synthetic": value.synthetic,
                "source_kind": value.source_kind,
                "language": value.language,
                "text_sha256": hashlib.sha256(value.text.encode()).hexdigest(),
            }
        )
        with self.catalog.evidence_transaction(value.synthetic) as connection:
            cutoff = self._clock(connection, as_of, value.synthetic)
            self._scope(
                connection,
                value.product_id,
                value.entity_key,
                value.dataset_id,
                value.synthetic,
                cutoff,
                value.observation_id,
            )
            cached = connection.execute(
                "SELECT t.* FROM enrichment_transcripts t JOIN datasets d "
                "ON d.dataset_id=t.source_dataset_id AND d.state='committed' "
                "LEFT JOIN source_policies policy ON policy.source=d.source "
                "JOIN derived_publications p "
                "ON p.object_key=t.descriptor#>>'{root,key}' AND p.state='committed' "
                "WHERE t.synthetic=%s AND t.content_key=%s AND t.available_at<=%s "
                "AND (t.synthetic OR (policy.verified AND (policy.retention_days IS NULL OR "
                "d.created_at>=clock_timestamp()-policy.retention_days*interval '1 day'))) "
                "ORDER BY t.available_at DESC,t.transcript_id LIMIT 1",
                (value.synthetic, content_key, cutoff),
            ).fetchone()
            if cached:
                self._scope(
                    connection,
                    value.product_id,
                    cached["entity_key"],
                    cached["source_dataset_id"],
                    value.synthetic,
                    cutoff,
                )
                self.provenance.guard(connection, self.provenance.roots(cached["descriptor"]))
                return self._transcript_summary(cached, reused=True)
        transcript_id = digest(
            {
                "content_key": content_key,
                "dataset": value.dataset_id,
                "observation": value.observation_id,
            }
        )
        source_refs = self.provenance.references(
            [value.observation_id], {value.observation_id: value.dataset_id}, value.synthetic
        )
        descriptor = self.provenance.records(
            {
                "transcript": {
                    "text": value.text,
                    "source_kind": value.source_kind,
                    "language": value.language,
                    "permission_reference": value.permission_reference,
                    "text_normalization": "nfc-lf-trim-v1",
                    "input_provenance": source_refs,
                }
            },
            value.synthetic,
        )
        with self.catalog.evidence_transaction(value.synthetic) as connection:
            cutoff = self._clock(connection, as_of, value.synthetic)
            self._scope(
                connection,
                value.product_id,
                value.entity_key,
                value.dataset_id,
                value.synthetic,
                cutoff,
                value.observation_id,
            )
            self.provenance.guard(connection, self.provenance.roots(descriptor))
            connection.execute(
                "INSERT INTO enrichment_transcripts(transcript_id,content_key,entity_key,"
                "source_dataset_id,observation_id,synthetic,descriptor) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    transcript_id,
                    content_key,
                    value.entity_key,
                    value.dataset_id,
                    value.observation_id,
                    value.synthetic,
                    Jsonb(descriptor),
                ),
            )
            row = connection.execute(
                "SELECT * FROM enrichment_transcripts WHERE transcript_id=%s", (transcript_id,)
            ).fetchone()
            if row is None or row["descriptor"] != descriptor:
                raise EnrichmentDenied("enrichment_transcript_identity_conflict")
            return self._transcript_summary(row, reused=False)

    @staticmethod
    def _transcript_summary(row, *, reused):
        return {
            key: row[key] for key in ("transcript_id", "entity_key", "synthetic", "available_at")
        } | {"reused": reused}

    def _transcript(self, connection, transcript_id, product, synthetic, cutoff):
        row = connection.execute(
            "SELECT * FROM enrichment_transcripts WHERE transcript_id=%s AND synthetic=%s "
            "AND available_at<=%s",
            (transcript_id, synthetic, cutoff),
        ).fetchone()
        if not row:
            raise EnrichmentDenied("enrichment_transcript_unavailable")
        self._scope(
            connection, product, row["entity_key"], row["source_dataset_id"], synthetic, cutoff
        )
        try:
            self.provenance.guard(connection, self.provenance.roots(row["descriptor"]))
        except ValueError:
            raise EnrichmentDenied("enrichment_transcript_unavailable") from None
        return row

    def analyze(
        self,
        request_id: str,
        product_id: str,
        transcript_id: str,
        *,
        synthetic: bool = False,
        as_of: datetime | None = None,
        adapter: TranscriptAdapter | None = None,
    ) -> dict:
        require_module(self.catalog, "transcript_insights")
        if not re.fullmatch(IDENTIFIER, request_id) or not re.fullmatch(IDENTIFIER, product_id):
            raise ValueError("Invalid enrichment request or product identifier")
        provider = adapter or LocalTranscriptAdapter()
        spec = revalidate(ProviderSpec, provider.spec)
        if not spec.enabled:
            raise EnrichmentDenied("enrichment_provider_disabled")
        if adapter is not None and (spec.provider_id == "local-rules" or spec.reserved_cost <= 0):
            raise EnrichmentDenied("enrichment_external_cost_bound_required")
        fingerprint = digest(
            {
                "product": product_id,
                "transcript": transcript_id,
                "provider": spec.provider_id,
                "version": spec.version,
            }
        )
        token = None
        with self.catalog.evidence_transaction(synthetic) as connection:
            cutoff = self._clock(connection, as_of, synthetic)
            transcript = self._transcript(connection, transcript_id, product_id, synthetic, cutoff)
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371246))",
                (f"enrichment-request:{synthetic}:{request_id}",),
            )
            existing = connection.execute(
                "SELECT * FROM enrichment_requests WHERE request_id=%s AND synthetic=%s",
                (request_id, synthetic),
            ).fetchone()
            if existing:
                if existing["request_sha256"] != fingerprint:
                    raise EnrichmentDenied("enrichment_request_identity_conflict")
            else:
                cache_key = digest(
                    {
                        "content": transcript["content_key"],
                        "provider": spec.provider_id,
                        "version": spec.version,
                    }
                )
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,492371247))",
                    (f"enrichment-content:{synthetic}:{cache_key}",),
                )
                analysis = connection.execute(
                    "SELECT a.* FROM enrichment_analyses a JOIN enrichment_transcripts t "
                    "ON t.transcript_id=a.transcript_id JOIN datasets d "
                    "ON d.dataset_id=t.source_dataset_id "
                    "LEFT JOIN source_policies policy ON policy.source=d.source "
                    "JOIN derived_publications p "
                    "ON p.object_key=t.descriptor#>>'{root,key}' "
                    "LEFT JOIN derived_publications r ON r.object_key=a.descriptor#>>'{root,key}' "
                    "WHERE a.synthetic=%s AND a.cache_key=%s "
                    "AND (a.state<>'completed' OR (p.state='committed' AND r.state='committed' "
                    "AND d.state='committed' AND (a.synthetic OR (policy.verified AND "
                    "(policy.retention_days IS NULL OR d.created_at>=clock_timestamp()-"
                    "policy.retention_days*interval '1 day'))))) "
                    "ORDER BY a.created_at DESC,a.analysis_id LIMIT 1",
                    (synthetic, cache_key),
                ).fetchone()
                if analysis is None:
                    document = self.provenance.selected_records(
                        transcript["descriptor"], ["transcript"], connection=connection
                    )["transcript"]
                    default_budget = EnrichmentBudget() if adapter is None else None
                    daily = self._budget(
                        connection, spec.provider_id, synthetic, default_budget, require_same=False
                    )
                    budget = revalidate(EnrichmentBudget, daily["configuration"])
                    if adapter is not None and not budget.allow_external:
                        raise EnrichmentDenied("enrichment_external_provider_not_allowed")
                    if (
                        spec.currency != budget.currency
                        or spec.reserved_cost > budget.maximum_request_cost
                    ):
                        raise EnrichmentDenied("enrichment_request_cost_exceeds_budget")
                    if len(document["text"]) > budget.max_text_characters:
                        raise EnrichmentDenied("enrichment_text_exceeds_run_budget")
                    if daily["requests_reserved"] >= budget.daily_requests or (
                        daily["cost_reserved"] + spec.reserved_cost > budget.daily_cost_limit
                    ):
                        raise EnrichmentDenied("enrichment_daily_budget_exhausted")
                    connection.execute(
                        "UPDATE enrichment_daily_budgets SET requests_reserved=requests_reserved+1,"
                        "cost_reserved=cost_reserved+%s WHERE synthetic=%s AND provider_id=%s "
                        "AND budget_day=%s",
                        (spec.reserved_cost, synthetic, spec.provider_id, daily["budget_day"]),
                    )
                    token = uuid4().hex
                    analysis_id = digest({"cache": cache_key, "transcript": transcript_id})
                    analysis = connection.execute(
                        "INSERT INTO enrichment_analyses(analysis_id,cache_key,transcript_id,"
                        "synthetic,provider_id,provider_version,state,run_token,"
                        "reserved_cost,currency) "
                        "VALUES(%s,%s,%s,%s,%s,%s,'running',%s,%s,%s) RETURNING *",
                        (
                            analysis_id,
                            cache_key,
                            transcript_id,
                            synthetic,
                            spec.provider_id,
                            spec.version,
                            token,
                            spec.reserved_cost,
                            spec.currency,
                        ),
                    ).fetchone()
                if analysis is None:
                    raise RuntimeError("Enrichment reservation disappeared")
                connection.execute(
                    "INSERT INTO enrichment_requests(request_id,synthetic,product_id,transcript_id,"
                    "analysis_id,request_sha256) VALUES(%s,%s,%s,%s,%s,%s)",
                    (
                        request_id,
                        synthetic,
                        product_id,
                        transcript_id,
                        analysis["analysis_id"],
                        fingerprint,
                    ),
                )
        if token is not None:
            assert analysis is not None
            try:
                source_text_length = len(document["text"])
                output = revalidate(InsightOutput, provider.analyze(document))
                if any(segment.end > source_text_length for segment in output.segments):
                    raise ValueError("Provider segment is outside supplied text")
                descriptor = self.provenance.records(
                    {
                        "insight": {
                            "output": output.model_dump(mode="json"),
                            "input_provenance": transcript["descriptor"],
                        }
                    },
                    synthetic,
                )
                with self.catalog.evidence_transaction(synthetic) as connection:
                    self.provenance.guard(connection, self.provenance.roots(descriptor))
                    changed = connection.execute(
                        "UPDATE enrichment_analyses SET state='completed',descriptor=%s,"
                        "finished_at=clock_timestamp() WHERE analysis_id=%s AND run_token=%s "
                        "AND state='running' RETURNING analysis_id",
                        (Jsonb(descriptor), analysis["analysis_id"], token),
                    ).fetchone()
                    if changed is None:
                        raise EnrichmentDenied("enrichment_analysis_ownership_changed")
            except Exception:
                with self.catalog.transaction() as connection:
                    connection.execute(
                        "UPDATE enrichment_analyses SET state='uncertain' WHERE analysis_id=%s "
                        "AND run_token=%s AND state='running'",
                        (analysis["analysis_id"], token),
                    )
                raise EnrichmentDenied("enrichment_provider_outcome_uncertain") from None
        return self.result(request_id, product_id, synthetic=synthetic, as_of=as_of)

    def result(
        self,
        request_id: str,
        product_id: str,
        *,
        synthetic: bool = False,
        as_of: datetime | None = None,
    ) -> dict:
        require_module(self.catalog, "transcript_insights")
        with self.catalog.evidence_transaction(synthetic) as connection:
            cutoff = self._clock(connection, as_of, synthetic)
            row = connection.execute(
                "SELECT r.*,a.state,a.provider_id,a.provider_version,a.reserved_cost,a.currency,"
                "a.descriptor,a.finished_at,a.transcript_id AS analyzed_transcript "
                "FROM enrichment_requests r "
                "JOIN enrichment_analyses a USING(analysis_id) WHERE r.request_id=%s "
                "AND r.product_id=%s AND r.synthetic=%s AND r.created_at<=%s",
                (request_id, product_id, synthetic, cutoff),
            ).fetchone()
            if not row:
                raise EnrichmentDenied("enrichment_request_unavailable")
            transcript = self._transcript(
                connection, row["transcript_id"], product_id, synthetic, cutoff
            )
            if row["analyzed_transcript"] != row["transcript_id"]:
                transcript = self._transcript(
                    connection, row["analyzed_transcript"], product_id, synthetic, cutoff
                )
            response = {
                key: row[key]
                for key in (
                    "request_id",
                    "analysis_id",
                    "state",
                    "provider_id",
                    "provider_version",
                    "reserved_cost",
                    "currency",
                )
            }
            response["reservation_is_actual_bill"] = False
            if row["state"] == "completed":
                if row["finished_at"] > cutoff:
                    raise EnrichmentDenied("enrichment_result_not_available_at_cutoff")
                try:
                    self.provenance.guard(connection, self.provenance.roots(row["descriptor"]))
                except ValueError:
                    raise EnrichmentDenied("enrichment_result_unavailable") from None
                content = self.provenance.selected_records(
                    transcript["descriptor"], ["transcript"], connection=connection
                )["transcript"]
                output = self.provenance.selected_records(
                    row["descriptor"], ["insight"], connection=connection
                )["insight"]["output"]
                response.update(
                    source_kind=content["source_kind"],
                    language=content["language"],
                    speech_evidence=content["source_kind"] != "description",
                    classification_status="unvalidated_text_interpretation",
                    effectiveness="not_measured",
                    segments=[
                        {**segment, "quote": content["text"][segment["start"] : segment["end"]]}
                        for segment in output["segments"]
                    ],
                    method=output["method"],
                )
            return response

    def summaries(
        self,
        product_id: str,
        *,
        synthetic: bool = False,
        limit: int = 100,
        after: str | None = None,
    ) -> dict:
        require_module(self.catalog, "transcript_insights")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise ValueError("Enrichment page limit must be between 1 and 200")
        rows = self.catalog.all(
            "SELECT request_id FROM enrichment_requests WHERE product_id=%s AND synthetic=%s "
            "AND request_id>%s ORDER BY request_id LIMIT %s",
            (product_id, synthetic, after or "", limit + 1),
        )
        visible = []
        for row in rows[:limit]:
            try:
                result = self.result(row["request_id"], product_id, synthetic=synthetic)
                visible.append({key: value for key, value in result.items() if key != "segments"})
            except EnrichmentDenied:
                continue
        return {
            "items": visible,
            "next_cursor": rows[limit - 1]["request_id"] if len(rows) > limit else None,
        }
