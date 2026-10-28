"""Metadata-only plans and explicit, bounded execution through the free analyzer."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from .catalog import Catalog
from .contracts import digest, utc
from .enrichment import EnrichmentDenied, TranscriptInsights
from .enrichment.contracts import IDENTIFIER, _EnrichmentContract, revalidate
from .provenance import Provenance

METHOD = "metadata-text-selection-v1"


class TextSelectionConfig(_EnrichmentContract):
    version: str = Field(default="bounded-local-selection-v1", pattern=IDENTIFIER)
    max_products: int = Field(default=10, ge=1, le=10, strict=True)
    max_candidates: int = Field(default=100, ge=1, le=200, strict=True)
    per_product_candidates: int = Field(default=20, ge=1, le=100, strict=True)
    max_selected: int = Field(default=10, ge=1, le=20, strict=True)
    max_total_text_characters: int = Field(default=80_000, ge=1, le=320_000, strict=True)
    desired_theme_ids: tuple[str, ...] = Field(default=(), max_length=16)

    @field_validator("desired_theme_ids")
    @classmethod
    def theme_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not re.fullmatch(IDENTIFIER, theme) for theme in value):
            raise ValueError("Desired themes require bounded identifiers")
        return tuple(sorted(set(value)))


DEFAULT_SELECTION_CONFIG = TextSelectionConfig()


class _SelectedText(_EnrichmentContract):
    product_id: str = Field(pattern=IDENTIFIER)
    transcript_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_key: str = Field(pattern=r"^[a-f0-9]{64}$")
    entity_key: str = Field(max_length=512)
    text_characters: int = Field(ge=1, le=16_000, strict=True)
    reasons: tuple[str, ...] = Field(max_length=24)


class TextSelectionPlan(_EnrichmentContract):
    plan_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    method: Literal["metadata-text-selection-v1"]
    config: TextSelectionConfig
    config_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    product_ids: tuple[str, ...] = Field(min_length=1, max_length=10)
    synthetic: bool = Field(strict=True)
    as_of: datetime
    after: dict[str, str] = Field(max_length=10)
    items: tuple[_SelectedText, ...] = Field(max_length=20)
    decisions: tuple[dict[str, Any], ...] = Field(max_length=200)
    pages: tuple[dict[str, Any], ...] = Field(max_length=10)
    selected_text_characters: int = Field(ge=0, le=320_000, strict=True)
    evidence_scope: Literal["supplied_text_metadata_only"]
    theme_coverage: Literal["operator_attested_metadata_only"]
    external_execution: Literal["disabled"]

    @field_validator("as_of")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def bounded(self):
        if (
            len(self.product_ids) > self.config.max_products
            or len(set(self.product_ids)) != len(self.product_ids)
            or any(not re.fullmatch(IDENTIFIER, product) for product in self.product_ids)
            or set(self.after) - set(self.product_ids)
            or any(not re.fullmatch(r"[a-f0-9]{64}", cursor) for cursor in self.after.values())
            or len(self.decisions) > self.config.max_candidates
            or len(self.items) > self.config.max_selected
            or self.selected_text_characters != sum(item.text_characters for item in self.items)
            or self.selected_text_characters > self.config.max_total_text_characters
            or any(item.product_id not in self.product_ids for item in self.items)
            or len({(item.product_id, item.content_key) for item in self.items}) != len(self.items)
        ):
            raise ValueError("Text selection plan exceeds its declared scope or budget")
        return self


def _dimensions(candidate: dict) -> dict[str, set[str]]:
    metadata = candidate["metadata"]
    context = metadata.get("context") or {}
    creator = context.get("creator_id")
    return {
        "language": {metadata["language"]} if metadata.get("language") else set(),
        "variant": {context["variant"]} if context.get("variant") else set(),
        "creator": {candidate["entity_key"].split(":", 1)[0] + ":" + creator} if creator else set(),
        "theme": set(context.get("known_theme_ids", ())),
    }


def _selection(candidates: list[dict], config: TextSelectionConfig) -> tuple[list, list, int]:
    covered: dict[str, set[str]] = {
        key: set() for key in ("language", "variant", "creator", "theme")
    }
    products: set[str] = set()
    decisions, pending = [], []
    # Metadata revisions and distinct observation deliveries share logical work.
    canonical = {}
    for candidate in sorted(
        candidates, key=lambda row: (row["available_at"], row["transcript_id"])
    ):
        canonical[(candidate["product_id"], candidate["content_key"])] = candidate
    for candidate in candidates:
        reason = None
        metadata = candidate["metadata"]
        state = candidate["local_analysis_state"]
        if canonical[(candidate["product_id"], candidate["content_key"])] is not candidate:
            reason = "duplicate_canonical_text"
        elif metadata.get("source_kind") not in {"subtitles", "human_transcript", "description"}:
            reason = "not_reviewed_metadata"
        elif (metadata.get("language") or "unknown").split("-", 1)[0] not in {"en", "fr"}:
            reason = "unsupported_or_unknown_language"
        elif (
            type(metadata.get("text_characters")) is not int
            or not 1 <= metadata["text_characters"] <= 16_000
        ):
            reason = "not_reviewed_text_length"
        elif state != "not_analyzed":
            reason = "analysis_" + state
            if state == "completed":
                for dimension, values in _dimensions(candidate).items():
                    covered[dimension].update(values)
                products.add(candidate["product_id"])
        if reason:
            decisions.append(
                {
                    "product_id": candidate["product_id"],
                    "transcript_id": candidate["transcript_id"],
                    "reason": reason,
                }
            )
        else:
            pending.append(candidate)
    selected: list[dict] = []
    characters = 0
    desired = set(config.desired_theme_ids)
    while pending and len(selected) < config.max_selected:

        def rank(candidate):
            dimensions = _dimensions(candidate)
            novelty = {key: values - covered[key] for key, values in dimensions.items()}
            # All pending rows are eligible unanalysed content. Diversify products
            # first, then explicit missing themes, language, variant and creator.
            return (
                -(candidate["product_id"] not in products),
                -len(novelty["theme"] & desired),
                -bool(novelty["language"]),
                -bool(novelty["variant"]),
                -bool(novelty["creator"]),
                candidate["product_id"],
                candidate["content_key"],
                candidate["transcript_id"],
            )

        candidate = min(pending, key=rank)
        pending.remove(candidate)
        count = candidate["metadata"]["text_characters"]
        if characters + count > config.max_total_text_characters:
            decisions.append(
                {
                    "product_id": candidate["product_id"],
                    "transcript_id": candidate["transcript_id"],
                    "reason": "text_character_budget",
                }
            )
            continue
        dimensions = _dimensions(candidate)
        reasons = ["eligible_unanalyzed_text"]
        if candidate["product_id"] not in products:
            reasons.append("product_diversity")
        for dimension, values in dimensions.items():
            novel = values - covered[dimension]
            if dimension == "theme":
                novel &= desired
            if novel:
                reasons.append("new_known_" + dimension)
            covered[dimension].update(values)
        if not dimensions["theme"]:
            reasons.append("theme_coverage_unknown")
        products.add(candidate["product_id"])
        selected.append(
            {
                key: candidate[key]
                for key in ("product_id", "transcript_id", "content_key", "entity_key")
            }
            | {"text_characters": count, "reasons": reasons}
        )
        characters += count
    decisions.extend(
        {
            "product_id": candidate["product_id"],
            "transcript_id": candidate["transcript_id"],
            "reason": "selection_count_budget",
        }
        for candidate in pending
    )
    return selected, decisions, characters


class TextSelection:
    def __init__(self, catalog: Catalog, provenance: Provenance):
        self.texts = TranscriptInsights(catalog, provenance)

    def preview(
        self,
        product_ids: Sequence[str],
        *,
        config: TextSelectionConfig = DEFAULT_SELECTION_CONFIG,
        synthetic: bool = False,
        as_of: datetime | None = None,
        after: dict[str, str] | None = None,
    ) -> dict:
        config = revalidate(TextSelectionConfig, config)
        if (
            isinstance(product_ids, str | bytes)
            or type(synthetic) is not bool
            or not 1 <= len(product_ids) <= config.max_products
            or len(set(product_ids)) != len(product_ids)
            or any(not re.fullmatch(IDENTIFIER, product) for product in product_ids)
        ):
            raise ValueError("Provide a bounded distinct product shortlist and explicit namespace")
        after = after or {}
        if set(after) - set(product_ids):
            raise ValueError("Selection cursors must belong to the selected products")
        candidates: list[dict] = []
        pages: list[dict] = []
        cutoff = as_of
        for product in product_ids:
            remaining = config.max_candidates - sum(page["scanned"] for page in pages)
            if remaining <= 0:
                pages.append(
                    {"product_id": product, "status": "candidate_budget_exhausted", "scanned": 0}
                )
                continue
            page = self.texts.eligible_texts(
                product,
                synthetic=synthetic,
                as_of=cutoff,
                limit=min(remaining, config.per_product_candidates),
                after=after.get(product),
            )
            cutoff = page["as_of"]
            candidates.extend({"product_id": product, **item} for item in page["items"])
            pages.append(
                {
                    "product_id": product,
                    "scanned": page["scanned"],
                    "next_cursor": page["next_cursor"],
                    "unavailable": page["unavailable"],
                    "status": "available"
                    if page["items"]
                    else "no_eligible_supplied_text_at_cutoff",
                }
            )
        items, decisions, characters = _selection(candidates, config)
        payload = {
            "method": METHOD,
            "config": config.model_dump(mode="json"),
            "config_sha256": digest(config.model_dump(mode="json")),
            "product_ids": list(product_ids),
            "synthetic": synthetic,
            "as_of": cutoff,
            "after": after,
            "items": items,
            "decisions": decisions,
            "pages": pages,
            "selected_text_characters": characters,
            "evidence_scope": "supplied_text_metadata_only",
            "theme_coverage": "operator_attested_metadata_only",
            "external_execution": "disabled",
        }
        return TextSelectionPlan.model_validate({"plan_id": digest(payload), **payload}).model_dump(
            mode="json"
        )

    def execute(self, plan: TextSelectionPlan | dict, *, request_prefix: str) -> dict:
        """Explicit application only; no caller can supply an external adapter."""
        plan = revalidate(TextSelectionPlan, plan)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}", request_prefix):
            raise ValueError("A stable bounded request prefix is required")
        expected = self.preview(
            plan.product_ids,
            config=plan.config,
            synthetic=plan.synthetic,
            as_of=plan.as_of,
            after=plan.after,
        )
        if expected != plan.model_dump(mode="json"):
            raise ValueError("Text selection plan changed or its evidence is no longer eligible")
        results = []
        for item in plan.items:
            request_id = (
                request_prefix
                + ":"
                + digest(
                    {
                        "plan": plan.plan_id,
                        "product": item.product_id,
                        "transcript": item.transcript_id,
                    }
                )[:32]
            )
            try:
                # Current source policy/ownership and atomic daily budgets are
                # authoritative even when the selection was made in the past.
                result = self.texts.analyze(
                    request_id, item.product_id, item.transcript_id, synthetic=plan.synthetic
                )
                results.append(
                    {
                        "product_id": item.product_id,
                        "transcript_id": item.transcript_id,
                        "request_id": request_id,
                        "state": result["state"],
                        "analysis_id": result["analysis_id"],
                    }
                )
            except EnrichmentDenied as error:
                results.append(
                    {
                        "product_id": item.product_id,
                        "transcript_id": item.transcript_id,
                        "request_id": request_id,
                        "state": "denied",
                        "reason": str(error),
                    }
                )
        return {
            "plan_id": plan.plan_id,
            "synthetic": plan.synthetic,
            "provider": "local-rules",
            "state": "completed"
            if all(row["state"] == "completed" for row in results)
            else "partial",
            "results": results,
        }
