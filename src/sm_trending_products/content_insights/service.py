"""Bounded content themes with exact quotes and deliberately narrow claims."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from typing import Any

from ..catalog import Catalog
from ..contracts import digest, utc
from ..enrichment import TranscriptInsights
from ..enrichment.contracts import revalidate
from ..provenance import Provenance
from .contracts import DEFAULT_TAXONOMY, ContentTaxonomy

METHOD = "quoted-theme-mentions-v2"
MAX_MENTIONS_PER_TEXT = 24


def _fold_with_offsets(text: str) -> tuple[str, list[int]]:
    chars, positions = [], []
    for index, char in enumerate(text):
        for folded in unicodedata.normalize("NFKD", char.casefold().replace("’", "'")):
            if not unicodedata.combining(folded):
                chars.append(folded)
                positions.append(index)
    return "".join(chars), positions


def _status(text: str, start: int, end: int) -> str:
    # Conservative scope markers, not a general syntactic or sentiment parser.
    prefix = re.split(r"[.!?;\n]", text[max(0, start - 64) : start])[-1]
    suffix = re.split(r"[.!;\n]", text[end : end + 64])[0]
    if re.search(r"\b(?:not only|pas seulement)\b", prefix):
        return "ambiguous"
    if re.search(
        r"\b(?:not|never|no|without|doesn't|isn't|can't|don't|won't|pas|sans|jamais|aucun)\b",
        prefix,
    ):
        return "negated"
    if "?" in suffix or re.search(r"\b(?:may|might|maybe|could|perhaps|peut|pourrait)\b", prefix):
        return "ambiguous"
    return "candidate"


def classify(text: str, language: str | None, taxonomy: ContentTaxonomy) -> dict:
    language = (language or "unknown").split("-", 1)[0]
    coverage = {
        rule.theme_id: "not_scanned"
        if any(phrase.language == language for phrase in rule.phrases)
        else "unsupported_language"
        for rule in taxonomy.themes
    }
    if all(value == "unsupported_language" for value in coverage.values()):
        return {
            "status": "unsupported_language",
            "mentions": [],
            "truncated": False,
            "theme_coverage": coverage,
        }
    folded, offsets = _fold_with_offsets(text)
    mentions: list[dict] = []
    for rule in taxonomy.themes:
        if coverage[rule.theme_id] == "unsupported_language":
            continue
        for phrase in rule.phrases:
            if phrase.language != language:
                continue
            phrase_text = _fold_with_offsets(phrase.text)[0]
            pattern = r"(?<!\w)" + re.escape(phrase_text).replace(r"\ ", r"\s+") + r"(?!\w)"
            for match in re.finditer(pattern, folded):
                if len(mentions) == MAX_MENTIONS_PER_TEXT:
                    coverage[rule.theme_id] = "partial"
                    return {
                        "status": "mentions",
                        "mentions": mentions,
                        "truncated": True,
                        "theme_coverage": coverage,
                    }
                start, end = offsets[match.start()], offsets[match.end() - 1] + 1
                mentions.append(
                    {
                        "theme_id": rule.theme_id,
                        "category": phrase.category,
                        "status": _status(folded, match.start(), match.end()),
                        "start": start,
                        "end": end,
                        "quote": text[start:end],
                    }
                )
        coverage[rule.theme_id] = "complete"
    mentions.sort(key=lambda item: (item["start"], item["end"], item["theme_id"], item["category"]))
    # Synonyms within a manually merged theme cannot count the same span twice.
    mentions = list(
        {(m["theme_id"], m["category"], m["start"], m["end"]): m for m in mentions}.values()
    )
    return {
        "status": "mentions" if mentions else "no_mention",
        "mentions": mentions,
        "truncated": False,
        "theme_coverage": coverage,
    }


class ContentInsights:
    def __init__(self, catalog: Catalog, provenance: Provenance):
        self.texts = TranscriptInsights(catalog, provenance)

    def aggregate(
        self,
        product_id: str,
        *,
        taxonomy: ContentTaxonomy = DEFAULT_TAXONOMY,
        synthetic: bool = False,
        as_of: datetime | None = None,
        limit: int = 100,
        after: str | None = None,
    ) -> dict:
        cutoff = utc(as_of) if as_of is not None else self.texts.resolve_cutoff(synthetic=synthetic)
        taxonomy = revalidate(ContentTaxonomy, taxonomy)
        if taxonomy.reviewed_at > cutoff:
            raise ValueError("Content taxonomy was not reviewed at this cutoff")
        page = self.texts.read_text_page(
            product_id, synthetic=synthetic, as_of=cutoff, limit=limit, after=after
        )
        themes: dict[str, dict[str, Any]] = {
            rule.theme_id: {
                "theme_id": rule.theme_id,
                "label": rule.label,
                "mentions": [],
                "texts": set(),
                "creators": defaultdict(set),
                "source_kinds": defaultdict(set),
                "languages": defaultdict(set),
                "roles": defaultdict(set),
            }
            for rule in taxonomy.themes
        }
        texts, scripts = [], defaultdict(list)
        for item in sorted(page["items"], key=lambda item: item["content_key"]):
            context = item["context"] or {}
            attribution = {
                "creator_id": context.get("creator_id"),
                "creator_role": context.get("creator_role", "unknown"),
                "context_status": "operator_attested" if context else "unknown",
                "context_reference": context.get("reference"),
                "context_known_at": context.get("known_at"),
            }
            result = classify(item["text"], item["language"], taxonomy)
            record = (
                {
                    key: item[key]
                    for key in (
                        "transcript_id",
                        "content_key",
                        "entity_key",
                        "available_at",
                        "source_kind",
                        "language",
                    )
                }
                | attribution
                | result
            )
            texts.append(record)
            folded = _fold_with_offsets(item["text"])[0]
            script_key = hashlib.sha256(" ".join(re.findall(r"\w+", folded)).encode()).hexdigest()
            scripts[script_key].append(item["content_key"])
            for mention in result["mentions"]:
                theme = themes[mention["theme_id"]]
                theme["mentions"].append(
                    {
                        **mention,
                        "transcript_id": item["transcript_id"],
                        "content_key": item["content_key"],
                        "entity_key": item["entity_key"],
                        "source_kind": item["source_kind"],
                        "language": item["language"],
                        **attribution,
                    }
                )
                theme["texts"].add(item["content_key"])
                if attribution["creator_id"] is not None:
                    creator = (item["entity_key"].split(":", 1)[0], attribution["creator_id"])
                    theme["creators"][creator].add(item["content_key"])
                theme["source_kinds"][item["source_kind"]].add(item["content_key"])
                theme["languages"][item["language"] or "unknown"].add(item["content_key"])
                theme["roles"][attribution["creator_role"]].add(item["content_key"])
        results = []
        for theme in themes.values():
            coverage = [item["theme_coverage"][theme["theme_id"]] for item in texts]
            supported_count = sum(value != "unsupported_language" for value in coverage)
            incomplete_count = sum(value in {"partial", "not_scanned"} for value in coverage)
            results.append(
                {
                    "theme_id": theme["theme_id"],
                    "label": theme["label"],
                    "status": "quoted_mentions"
                    if theme["mentions"]
                    else "not_fully_scanned"
                    if incomplete_count
                    else "no_mention_in_supported_texts"
                    if supported_count
                    else "unsupported_language"
                    if texts
                    else "not_available",
                    "distinct_text_count": len(theme["texts"]),
                    "supported_text_count": supported_count,
                    "unsupported_text_count": len(coverage) - supported_count,
                    "fully_scanned_text_count": coverage.count("complete"),
                    "unscanned_or_partial_text_count": incomplete_count,
                    "known_creator_count": len(theme["creators"]),
                    "by_known_creator": [
                        {"source": source, "creator_id": creator, "distinct_text_count": len(keys)}
                        for (source, creator), keys in sorted(theme["creators"].items())
                    ],
                    "by_source_kind": {
                        key: len(value) for key, value in sorted(theme["source_kinds"].items())
                    },
                    "by_language": {
                        key: len(value) for key, value in sorted(theme["languages"].items())
                    },
                    "by_role": {key: len(value) for key, value in sorted(theme["roles"].items())},
                    "by_status": {
                        status: len(
                            {m["content_key"] for m in theme["mentions"] if m["status"] == status}
                        )
                        for status in ("candidate", "negated", "ambiguous")
                    },
                    "mentions": theme["mentions"],
                }
            )
        unavailable = page["unavailable"]
        unsupported = sum(item["status"] == "unsupported_language" for item in texts)
        truncated = sum(item["truncated"] for item in texts)
        response = {
            "product_id": product_id,
            "synthetic": synthetic,
            "as_of": page["as_of"],
            "method": METHOD,
            "taxonomy_version": taxonomy.version,
            "taxonomy_sha256": digest(taxonomy.model_dump(mode="json")),
            "taxonomy_reviewed_at": taxonomy.reviewed_at,
            "taxonomy_review_reference": taxonomy.review_reference,
            "scope": "bounded_page_of_eligible_supplied_texts",
            "status": "available" if texts else "not_available",
            "distinct_text_count": len(texts),
            "normalized_script_count": len(scripts),
            "unsupported_text_count": unsupported,
            "truncated_text_count": truncated,
            "texts": texts,
            "themes": results,
            "text_similarity_groups": [
                {
                    "normalized_text_sha256": key,
                    "content_keys": values,
                    "meaning": "normalized_text_similarity_only",
                }
                for key, values in sorted(scripts.items())
                if len(values) > 1
            ],
            "same_creative": "not_verified",
            "independent_people": "not_established",
            "claims": "unverified_text_mentions",
            "buyer_intent": "not_established",
            "effectiveness": "not_measured",
            "unavailable": unavailable,
            "next_cursor": page["next_cursor"],
            "scanned": page["scanned"],
            "gaps": (["no_eligible_supplied_text_at_cutoff"] if not texts else [])
            + (["unsupported_or_unknown_language"] if unsupported else [])
            + (
                ["theme_language_coverage_incomplete"]
                if any(theme["unsupported_text_count"] for theme in results)
                else []
            )
            + (["text_mention_limit_reached"] if truncated else [])
            + (["source_evidence_unavailable"] if unavailable else [])
            + (["bounded_page_has_more"] if page["next_cursor"] else []),
        }
        response["aggregate_id"] = digest(response)
        return response
