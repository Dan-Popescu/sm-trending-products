"""Bounded, explained product-link hypotheses; ownership is never changed here."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import Field, field_validator

from . import _matching_indexes
from ._matching_indexes import MatchingIndexesNotReady as MatchingIndexesNotReady
from .catalog import Catalog
from .contracts import Contract, ProductLink, utc
from .identity_rules import resolve_links

METHOD = "review-candidates-v2"
OWNERSHIP_PAIR_LIMIT = 10000
TITLE_LIMIT = 4096
Text = Annotated[str, Field(strict=True, min_length=1, max_length=256)]
IDENTIFIERS = {"gtin", "ean", "upc", "sku", "mpn", "model"}
VARIANTS = {"size", "color", "capacity", "voltage", "material"}
STOP_WORDS = frozenset(
    "a an and the for with to of in on new viral trending tiktok video "
    "le la les un une de des du et pour avec en au aux sur "
    "der die das ein eine und mit fur fuer von zu "
    "el los las una unos unas del para con y "
    "il lo gli uno di per e da "
    "o os as um uma do dos das com "
    "een het den van voor bij".split()
)
UNITS = {
    "ml": ("volume", Decimal("1")),
    "cl": ("volume", Decimal("10")),
    "l": ("volume", Decimal("1000")),
    "mm": ("length", Decimal("1")),
    "cm": ("length", Decimal("10")),
    "m": ("length", Decimal("1000")),
    "g": ("mass", Decimal("1")),
    "kg": ("mass", Decimal("1000")),
    "v": ("voltage", Decimal("1")),
    "w": ("power", Decimal("1")),
    "gb": ("storage", Decimal("1")),
    "tb": ("storage", Decimal("1000")),
}
QUANTITY = re.compile(
    r"(?<![\w.,-])(\d{1,12}(?:[.,]\d{1,6})?)\s*(ml|cl|l|mm|cm|m|kg|g|v|w|gb|tb)\b", re.I
)


class MatchingProfile(Contract):
    """Operator-supplied translations and facts, not inferred product identity."""

    aliases: tuple[Text, ...] = Field(default=(), max_length=8)
    identifiers: dict[str, Text] = Field(default_factory=dict, max_length=6)
    brand: Text | None = None
    variants: dict[str, Text] = Field(default_factory=dict, max_length=5)

    @field_validator("identifiers", "variants")
    @classmethod
    def known_fields(cls, value: dict[str, str], info) -> dict[str, str]:
        allowed = IDENTIFIERS if info.field_name == "identifiers" else VARIANTS
        if set(value) - allowed or any(not item.strip() for item in value.values()):
            raise ValueError("Only bounded explicit product facts are supported")
        if info.field_name == "identifiers":
            if any(
                len(unicodedata.normalize("NFKC", item.strip())) > 256 for item in value.values()
            ):
                raise ValueError("Normalized identifiers must contain at most 256 characters")
            for key, item in value.items():
                if key in {"gtin", "ean", "upc"} and _barcode(item.strip()) is None:
                    raise ValueError("GTIN/EAN/UPC must have a valid supported check digit")
            barcodes = {
                _barcode(item.strip())
                for key, item in value.items()
                if key in {"gtin", "ean", "upc"}
            }
            if len(barcodes) > 1:
                raise ValueError("Supplied GTIN/EAN/UPC facts must describe the same product")
        return {key: item.strip() for key, item in value.items()}

    @field_validator("brand")
    @classmethod
    def nonblank_brand(cls, value: str | None) -> str | None:
        if value is not None and not _fold(value):
            raise ValueError("Brand must contain a value")
        return value.strip() if value is not None else None

    @field_validator("aliases")
    @classmethod
    def nonblank_aliases(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() for item in value):
            raise ValueError("Aliases must contain product terms")
        return tuple(dict.fromkeys(item.strip() for item in value))


def _fold(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return " ".join(
        re.findall(r"[^\W_]+", "".join(c for c in normalized if not unicodedata.combining(c)))
    )


def _terms(value: str) -> set[str]:
    return {term for term in _fold(value).split() if term not in STOP_WORDS}


def _search_query(aliases: Sequence[str], connection) -> str:
    variants = list(
        dict.fromkeys(
            text
            for alias in aliases
            for text in (
                unicodedata.normalize("NFKC", alias).lower(),
                " ".join(re.findall(r"[^\W_]+", unicodedata.normalize("NFKC", alias).lower())),
                _fold(alias),
                "".join(
                    char
                    for char in unicodedata.normalize("NFKD", alias.casefold())
                    if not unicodedata.combining(char)
                ),
            )
        )
    )
    # Native forms preserve decimals/versions; normalized words retain historical
    # recall for spaced/hyphenated titles. At most 36 variants share one bounded batch.
    parsed = connection.execute(
        "SELECT a.ordinality,array_agg(t.lexeme ORDER BY t.positions[1],t.lexeme) AS terms "
        "FROM unnest(%s::text[]) WITH ORDINALITY a(title,ordinality) "
        "CROSS JOIN LATERAL unnest(to_tsvector('simple',a.title)) t "
        "GROUP BY a.ordinality ORDER BY a.ordinality",
        (variants,),
    ).fetchall()
    clauses = set()
    for row in parsed:
        terms = [
            term for term in row["terms"] if _fold(term) not in STOP_WORDS and len(term) <= 64
        ][:16]
        if terms:
            escaped = [term.replace("\\", "\\\\").replace("'", "\\'") for term in terms]
            clauses.add("(" + " & ".join("'" + term + "'" for term in escaped) + ")")
    if not clauses:
        raise ValueError("Provide at least one informative product name or alias")
    return " | ".join(sorted(clauses))


def _barcode(value: str) -> str | None:
    if not re.fullmatch(r"(?:\d{8}|\d{12}|\d{13}|\d{14})", value, flags=re.ASCII):
        return None
    checksum = sum(
        int(char) * (3 if index % 2 == 0 else 1) for index, char in enumerate(reversed(value[:-1]))
    )
    return value.zfill(14) if (10 - checksum % 10) % 10 == int(value[-1]) else None


def _quantities(value: str) -> dict[str, set[Decimal]]:
    quantities: dict[str, set[Decimal]] = {}
    for amount, unit in QUANTITY.findall(value):
        dimension, factor = UNITS[unit.lower()]
        quantities.setdefault(dimension, set()).add(Decimal(amount.replace(",", ".")) * factor)
    return quantities


def _scalar(value: Any) -> str | None:
    if isinstance(value, str) and value.strip() and len(value) <= 256:
        normalized = unicodedata.normalize("NFKC", value.strip())
        return normalized if len(normalized) <= 256 else None
    return None


def _facts(
    profile: MatchingProfile,
    attributes: Mapping[str, Any],
    name: str,
    title: str,
    alias: str,
) -> tuple[list[dict], list[dict], list[str]]:
    matches, conflicts, missing = [], [], []
    for key, expected in profile.identifiers.items():
        keys = ("gtin", "ean", "upc") if key in {"gtin", "ean", "upc"} else (key,)
        supplied = [
            value for field in keys if (value := _scalar(attributes.get(field))) is not None
        ]
        if key in {"gtin", "ean", "upc"}:
            actual = {code for value in supplied if (code := _barcode(value)) is not None}
            expected_value = _barcode(expected)
        else:
            actual, expected_value = set(supplied), unicodedata.normalize("NFKC", expected)
        if not actual:
            missing.append(key)
        elif actual == {expected_value}:
            matches.append({"kind": "identifier", "field": key, "value": expected})
        else:
            conflicts.append(
                {
                    "kind": "identifier_mismatch",
                    "field": key,
                    "expected": expected,
                    "observed": sorted(actual),
                }
            )
    if profile.brand:
        actual_brand = _scalar(attributes.get("brand"))
        if actual_brand is None:
            missing.append("brand")
        elif _fold(profile.brand) == _fold(actual_brand):
            matches.append({"kind": "brand", "value": actual_brand})
        else:
            conflicts.append(
                {"kind": "brand_mismatch", "expected": profile.brand, "observed": actual_brand}
            )
    for key, expected in profile.variants.items():
        actual_variant = _scalar(attributes.get(key))
        if actual_variant is None:
            missing.append(key)
        elif (
            key in {"size", "capacity", "voltage"}
            and QUANTITY.fullmatch(expected)
            and QUANTITY.fullmatch(actual_variant)
            and _quantities(expected) == _quantities(actual_variant)
        ) or _fold(expected) == _fold(actual_variant):
            matches.append({"kind": "variant", "field": key, "value": actual_variant})
        else:
            conflicts.append(
                {
                    "kind": "variant_value_difference",
                    "field": key,
                    "expected": expected,
                    "observed": actual_variant,
                }
            )
    expected_texts = {
        "canonical_name": name,
        "matched_alias": alias,
        **{f"profile.{key}": value for key, value in profile.variants.items()},
    }
    for index, text in enumerate(profile.aliases):
        if text not in expected_texts.values():
            expected_texts[f"alias.{index}"] = text
    actual_quantities = _quantities(title)
    actual_numbers = _numbers(title)
    for origin, expected_text in expected_texts.items():
        expected_quantities = _quantities(expected_text)
        for dimension in sorted(expected_quantities.keys() & actual_quantities.keys()):
            if not expected_quantities[dimension] & actual_quantities[dimension]:
                conflicts.append(
                    {
                        "kind": "numeric_variant_difference",
                        "origin": origin,
                        "dimension": dimension,
                        "expected": sorted(map(str, expected_quantities[dimension])),
                        "observed": sorted(map(str, actual_quantities[dimension])),
                    }
                )
        expected_numbers = _numbers(expected_text)
        if expected_numbers and actual_numbers and not expected_numbers & actual_numbers:
            conflicts.append(
                {
                    "kind": "numeric_title_difference",
                    "origin": origin,
                    "expected": sorted(map(str, expected_numbers)),
                    "observed": sorted(map(str, actual_numbers)),
                }
            )
    return matches, conflicts, sorted(set(missing))


def _numbers(value: str) -> set[Decimal]:
    return {
        Decimal(number.replace(",", "."))
        for number in re.findall(
            r"(?<![\w.,-])\d{1,12}(?:[.,]\d{1,6})?(?![\w.,])", QUANTITY.sub(" ", value)
        )
    }


def _signal(value: str) -> tuple[set[str], set[str]]:
    folded = _fold(value)
    padded = "  " + folded + "  "
    return _terms(value), {padded[index : index + 3] for index in range(len(padded) - 2)}


def _similarity(left: tuple[set[str], set[str]], right: tuple[set[str], set[str]]) -> float:
    terms_a, grams_a = left
    terms_b, grams_b = right
    union = terms_a | terms_b
    overlap = len(terms_a & terms_b) / len(union) if union else 0.0
    grams = 2 * len(grams_a & grams_b) / (len(grams_a) + len(grams_b))
    return 0.7 * overlap + 0.3 * grams


class ProductMatcher:
    """Search bounded current projections and resolve candidate ownership at a cutoff.

    Historical projections replaced by later source delivery are excluded rather
    than reconstructed from S3. Text recall is limited to indexed aliases; accent
    folding improves ranking and query variants without an unindexed corpus scan.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def initialize_indexes(self) -> dict:
        """Explicitly prepare optional sparse indexes online, without capture migration changes."""
        return _matching_indexes.initialize(self.catalog)

    def suggest(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        profile: MatchingProfile | dict | None = None,
        limit: int = 20,
        candidate_limit: int = 500,
    ) -> dict:
        cutoff = utc(as_of)
        if not isinstance(product_id, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", product_id
        ):
            raise ValueError("A bounded canonical product ID is required")
        if not isinstance(synthetic, bool) or any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in (limit, candidate_limit)
        ):
            raise ValueError("Workspace and result bounds require explicit booleans/integers")
        if not 1 <= limit <= 100 or not 1 <= candidate_limit <= 1000 or limit > candidate_limit:
            raise ValueError("Result limit must be 1..100 and within the 1..1000 candidate bound")
        supplied = profile.model_dump() if isinstance(profile, MatchingProfile) else profile
        target = MatchingProfile.model_validate({} if supplied is None else supplied)
        with self.catalog.evidence_transaction(synthetic) as connection:
            product = connection.execute(
                "SELECT product_id,name,market FROM products "
                "WHERE product_id=%s AND synthetic=%s AND created_at<=%s",
                (product_id, synthetic, cutoff),
            ).fetchone()
            if product is None:
                raise ValueError("Product is not known in this workspace at the selected cutoff")
            aliases = tuple(dict.fromkeys((product["name"], *target.aliases)))
            try:
                query = _search_query(aliases, connection)
            except ValueError:
                if not target.identifiers:
                    raise
                query = None
            identifier_query, identifier_params = (
                _matching_indexes.lookup(connection, target.identifiers)
                if target.identifiers
                else ("false", [])
            )
            text_query = "to_tsvector('simple',e.title) @@ to_tsquery('simple',%s)"
            retrieval = "(" + text_query + " OR " + identifier_query + ")"
            rows = list(
                connection.execute(
                    "SELECT COALESCE((" + identifier_query + "),false) AS identifier_hit, "
                    "e.entity_key,left(e.title,%s) AS title, "
                    "e.source,e.entity_type,e.synthetic, "
                    "e.origin->>'dataset_id' AS metadata_dataset_id, "
                    "COALESCE((SELECT jsonb_object_agg(f.key,e.attributes->>f.key) "
                    "FROM unnest(%s::text[]) f(key) "
                    "WHERE jsonb_typeof(e.attributes->f.key)='string' "
                    "AND length(e.attributes->>f.key)<=256),'{}'::jsonb) AS attributes "
                    "FROM entities e LEFT JOIN datasets d ON d.dataset_id=e.origin->>'dataset_id' "
                    "WHERE e.synthetic=%s "
                    "AND e.entity_type IN ('video','ad','commercial_content','offer') "
                    "AND e.discovered_at<=%s "
                    "AND (e.last_observed_at IS NULL OR e.last_observed_at<=%s) "
                    "AND (NOT (e.origin ? 'dataset_id') OR (d.state='committed' "
                    "AND d.synthetic=e.synthetic "
                    "AND d.source=e.source AND d.committed_at<=%s AND d.max_available_at<=%s)) "
                    "AND " + retrieval + " ORDER BY identifier_hit DESC,e.entity_key LIMIT %s",
                    (
                        *identifier_params,
                        TITLE_LIMIT,
                        sorted(IDENTIFIERS | VARIANTS | {"brand"}),
                        synthetic,
                        cutoff,
                        cutoff,
                        cutoff,
                        cutoff,
                        query,
                        *identifier_params,
                        candidate_limit + 1,
                    ),
                ).fetchall()
            )
            truncated = len(rows) > candidate_limit
            rows = rows[:candidate_limit]
            pairs = []
            if rows:
                # Ownership needs no evidence payload; keep large source references out of RAM.
                pairs = list(
                    connection.execute(
                        "SELECT * FROM (SELECT DISTINCT ON (l.product_id,l.entity_key) "
                        "l.id,l.product_id,l.entity_key,l.available_at,l.status,l.confidence, "
                        "'ownership_projection' AS method,'[]'::jsonb AS evidence_refs, "
                        "l.decision_id "
                        "FROM product_links l JOIN products p USING(product_id) "
                        "WHERE p.synthetic=%s "
                        "AND p.created_at<=%s AND l.available_at<=%s AND l.entity_key=ANY(%s) "
                        "ORDER BY l.product_id,l.entity_key,l.available_at DESC,l.id DESC) pairs "
                        "ORDER BY available_at,id LIMIT %s",
                        (
                            synthetic,
                            cutoff,
                            cutoff,
                            [row["entity_key"] for row in rows],
                            OWNERSHIP_PAIR_LIMIT + 1,
                        ),
                    ).fetchall()
                )
        ownership_truncated = len(pairs) > OWNERSHIP_PAIR_LIMIT
        links = (
            []
            if ownership_truncated
            else [
                ProductLink.model_validate(
                    {key: value for key, value in row.items() if key != "id"}
                )
                for row in pairs
            ]
        )
        owners = resolve_links(links, cutoff)
        confirmed_entities = {link.entity_key for link in links if link.status == "confirmed"}
        signals = {alias: _signal(alias) for alias in aliases}
        suggestions = []
        for row in rows:
            title_signal = _signal(row["title"])
            score, alias = max(
                (_similarity(signal, title_signal), alias) for alias, signal in signals.items()
            )
            attributes = row.pop("attributes")
            matches, conflicts, missing = _facts(
                target, attributes, product["name"], row["title"], alias
            )
            if any(item["kind"] == "identifier" for item in matches):
                score = max(score, 0.9)
            owner = owners.get(row["entity_key"])
            if owner and owner.product_id != product_id:
                conflicts.append({"kind": "owned_by_other_product", "product_id": owner.product_id})
            if ownership_truncated:
                ownership_status = "unresolved_budget_exceeded"
            elif owner:
                ownership_status = "confirmed"
            elif row["entity_key"] in confirmed_entities:
                ownership_status = "ambiguous"
                conflicts.append({"kind": "ambiguous_ownership"})
            else:
                ownership_status = "unowned"
            if conflicts:
                score = min(score, 0.25)
            suggestions.append(
                {
                    **row,
                    "score": round(score, 6),
                    "score_kind": "heuristic_not_identity_probability",
                    "method": METHOD,
                    "requires_review": True,
                    "matched_alias": alias,
                    "matched_terms": sorted(signals[alias][0] & title_signal[0]),
                    "fact_matches": matches,
                    "conflicts": conflicts,
                    "missing_facts": missing,
                    "current_owner": owner.product_id if owner else None,
                    "ownership_status": ownership_status,
                    "already_linked": bool(owner and owner.product_id == product_id),
                }
            )
        return {
            "product_id": product_id,
            "market": product["market"],
            "synthetic": synthetic,
            "as_of": cutoff.isoformat(),
            "method": METHOD,
            "candidate_count": len(rows),
            "candidate_limit": candidate_limit,
            "candidate_truncated": truncated,
            "ownership_truncated": ownership_truncated,
            "projection_scope": "eligible_current_metadata_only",
            "notes": [
                "Recall uses explicit aliases and optional indexed identifiers",
                "Similarity cannot verify identity, creator independence or creative fingerprints",
                "Past source metadata replaced after the cutoff is excluded, not reconstructed",
                "Ranking uses at most 4096 title characters and bounded explicit string facts",
            ],
            "suggestions": sorted(
                suggestions, key=lambda item: (-item["score"], item["entity_key"])
            )[:limit],
        }
