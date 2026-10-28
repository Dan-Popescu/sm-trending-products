"""Bounded observations of competition, with explicit country/query coverage.

New observations are not market entry, missing observations are not departure,
and observed retail-price changes do not establish a supplier-margin change.
"""

from __future__ import annotations

import math
import re
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

from .analytics import _price_change, _price_quote
from .catalog import Catalog
from .contracts import Capability, Observation, digest, utc
from .evaluation import eligible_evidence
from .history import History
from .identity_rules import LINK_STATE_CTES

MAX_METADATA_ROWS = 10000
MAX_FAMILY_PRODUCTS = 1000
PREVIEW_LIMIT = 200
MAX_SCOPES = 200


def _declared(value: Any) -> str | None:
    return (
        value
        if isinstance(value, str)
        and value.strip()
        and len(value) <= 1000
        and value == value.strip()
        else None
    )


def _country(value: str | None) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[A-Z]{2}", value) else None


def _distribution(value: Any) -> str | None:
    declared = _declared(value)
    return (
        declared
        if declared is not None
        and len(declared) <= 128
        and declared.isprintable()
        and declared.casefold() not in {"unknown", "mixed", "unspecified", "not_available"}
        else None
    )


def _scope(
    row: Observation,
) -> tuple[str | None, str, str | None, str | None, str | None, str | None]:
    return (
        _country(row.market),
        row.source,
        _declared(row.attributes.get("query_scope")),
        _declared(row.attributes.get("coverage_scope")),
        _distribution(row.attributes.get("distribution")),
        _declared(row.account_id),
    )


def _scope_report(scope: tuple) -> dict:
    return dict(
        zip(
            (
                "observed_country",
                "source",
                "query_scope",
                "coverage_scope",
                "distribution",
                "source_account",
            ),
            scope,
            strict=True,
        )
    ) | {
        "source_account_scope": "explicit_account" if scope[5] is not None else "unknown",
        "source_account_independence": "not_established",
    }


def _scope_reason(scope: tuple) -> str | None:
    return (
        "unknown_observed_country"
        if scope[0] is None
        else "undeclared_query_scope"
        if scope[2] is None
        else "undeclared_coverage_scope"
        if scope[3] is None
        else "unknown_distribution_scope"
        if scope[4] is None
        else None
    )


def _capabilities(
    connection, observations: list[Observation], cutoff: datetime
) -> list[Capability]:
    accounts = sorted({(row.source, row.account_id or "") for row in observations})
    if not accounts:
        return []
    if len(accounts) > MAX_METADATA_ROWS:
        raise ValueError("Competition source metadata exceeds budget")
    rows = list(
        connection.execute(
            "SELECT DISTINCT ON(c.source,c.account_id,c.capability) "
            "c.source,c.account_id,c.capability,c.state,c.available_at,c.details "
            "FROM source_capabilities c JOIN unnest(%s::text[],%s::text[]) "
            "AS selected(source,account_id) USING(source,account_id) WHERE c.available_at<=%s "
            "ORDER BY c.source,c.account_id,c.capability,c.available_at DESC,c.id DESC LIMIT %s",
            (
                [pair[0] for pair in accounts],
                [pair[1] for pair in accounts],
                cutoff,
                MAX_METADATA_ROWS + 1,
            ),
        ).fetchall()
    )
    unscoped = sorted({pair[0] for pair in accounts} - {row["source"] for row in rows})
    if unscoped:
        # A grant for another account or only after the cutoff still establishes
        # a scoped source; lack of an applicable grant never makes it unrestricted.
        rows.extend(
            connection.execute(
                "SELECT c.* FROM unnest(%s::text[]) AS selected(source) CROSS JOIN LATERAL ("
                "SELECT source,account_id,capability,state,available_at,details "
                "FROM source_capabilities WHERE source=selected.source "
                "ORDER BY account_id,capability,available_at DESC,id DESC LIMIT 1) c",
                (unscoped,),
            ).fetchall()
        )
    if len(rows) > MAX_METADATA_ROWS:
        raise ValueError("Competition capability metadata exceeds budget")
    return [Capability.model_validate(row) for row in rows]


class CompetitionInsights:
    """One guarded product/family history read, reused by every timeline and price check."""

    def __init__(self, catalog: Catalog, history: History):
        self.catalog, self.history = catalog, history

    def describe(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        family: bool = False,
        country: str | None = None,
        source: str | None = None,
        query_scope: str | None = None,
        window_days: int = 30,
        max_age_hours: float = 72,
        max_rows: int = 50000,
        min_link_confidence: float = 0.9,
    ) -> dict[str, Any]:
        cutoff = utc(as_of)
        if (
            type(synthetic) is not bool
            or type(family) is not bool
            or type(window_days) is not int
            or not 1 <= window_days <= 366
            or type(max_age_hours) not in (int, float)
            or not math.isfinite(max_age_hours)
            or not 0 < max_age_hours <= 8760
            or type(max_rows) is not int
            or not 1 <= max_rows <= 100000
            or type(min_link_confidence) not in (int, float)
            or not math.isfinite(min_link_confidence)
            or not 0 <= min_link_confidence <= 1
            or (country is not None and _country(country) is None)
            or (source is not None and (_declared(source) is None or len(source) > 128))
            or (
                query_scope is not None
                and (_declared(query_scope) is None or len(query_scope) > 1000)
            )
        ):
            raise ValueError("Invalid bounded competition configuration")
        since = cutoff - timedelta(days=window_days)
        # This outer read holds the namespace and retirement fences through
        # computation. Retirement cannot race a released history read and leak
        # already-cached source identifiers through a newly assembled report.
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            product = connection.execute(
                "SELECT product_id,name,market FROM products "
                "WHERE product_id=%s AND synthetic=%s AND created_at<=%s",
                (product_id, synthetic, cutoff),
            ).fetchone()
            if product is None:
                raise ValueError("Product is unavailable in this workspace and cutoff")
            products = [product_id]
            family_id = None
            if family:
                membership = connection.execute(
                    "SELECT family_id FROM family_memberships WHERE product_id=%s "
                    "AND available_at<=%s ORDER BY available_at DESC,id DESC LIMIT 1",
                    (product_id, cutoff),
                ).fetchone()
                if membership:
                    family_id = membership["family_id"]
                    related = connection.execute(
                        "SELECT p.product_id FROM products p JOIN LATERAL ("
                        "SELECT family_id FROM family_memberships WHERE product_id=p.product_id "
                        "AND available_at<=%s ORDER BY available_at DESC,id DESC LIMIT 1) f "
                        "ON f.family_id=%s WHERE p.synthetic=%s AND p.created_at<=%s "
                        "ORDER BY p.product_id LIMIT %s",
                        (cutoff, family_id, synthetic, cutoff, MAX_FAMILY_PRODUCTS + 1),
                    ).fetchall()
                    if len(related) > MAX_FAMILY_PRODUCTS:
                        raise ValueError("Competition family exceeds product budget")
                    products = [row["product_id"] for row in related]
            candidates = connection.execute(
                "SELECT DISTINCT entity_key FROM product_links WHERE product_id=ANY(%s) "
                "AND available_at<=%s ORDER BY entity_key LIMIT %s",
                (products, cutoff, MAX_METADATA_ROWS + 1),
            ).fetchall()
            if len(candidates) > MAX_METADATA_ROWS:
                raise ValueError("Competition identity exceeds metadata budget")
            links = (
                list(
                    connection.execute(
                        "WITH product_links AS (SELECT * FROM product_links "
                        "WHERE entity_key=ANY(%s)), "
                        + LINK_STATE_CTES
                        + "SELECT entity_key,product_id,decision_id,confidence "
                        "FROM resolved_link_owners WHERE product_id=ANY(%s) AND confidence>=%s "
                        "ORDER BY entity_key LIMIT %s",
                        (
                            [row["entity_key"] for row in candidates],
                            synthetic,
                            cutoff,
                            cutoff,
                            products,
                            min_link_confidence,
                            MAX_METADATA_ROWS + 1,
                        ),
                    ).fetchall()
                )
                if candidates
                else []
            )
            keys = {
                row["entity_key"]
                for row in links
                if row["entity_key"].split(":", 2)[1] in {"offer", "ad", "commercial_content"}
                and (source is None or row["entity_key"].split(":", 1)[0] == source)
            }
            with self.history.read_session():
                observations = (
                    self.history.read(
                        self.catalog.iter_manifests(
                            synthetic=synthetic,
                            as_of=cutoff,
                            entity_keys=keys,
                            since=since,
                            until=cutoff,
                        ),
                        as_of=cutoff,
                        since=since,
                        until=cutoff,
                        entity_keys=keys,
                        max_rows=min(max_rows, self.history.settings.analysis_max_rows_per_batch),
                    )
                    if keys
                    else []
                )
            eligible = eligible_evidence(
                observations,
                cutoff,
                synthetic,
                None,
                _capabilities(connection, observations, cutoff),
                since=since,
            )
            report = _describe(eligible, cutoff, since, country, source, query_scope, max_age_hours)
            report.update(
                {
                    "version": "observed-competition-v1",
                    "product_id": product_id,
                    "name": product["name"],
                    "synthetic": synthetic,
                    "as_of": cutoff.isoformat(),
                    "window_start": since.isoformat(),
                    "family_requested": family,
                    "family_id": family_id,
                    "product_ids": products,
                    "requested_scope": {
                        "country": country,
                        "source": source,
                        "query_scope": query_scope,
                    },
                    "input_observation_count": len(observations),
                    "eligible_observation_count": len(eligible),
                    "source_excluded_observation_count": len(observations) - len(eligible),
                    "identity": {
                        "version": digest(links),
                        "decision_count": len(links),
                        "decision_refs_preview": [
                            row["decision_id"] for row in links[:PREVIEW_LIMIT]
                        ],
                        "preview_truncated": len(links) > PREVIEW_LIMIT,
                    },
                }
            )
            if family and family_id is None:
                report["limitations"].append(
                    "Unknown family: report covers the selected product only"
                )
            report["report_id"] = digest(report)
            return report


def _resolved_points(
    observations: list[Observation],
) -> tuple[list[Observation], int, set[str]]:
    groups: dict[tuple[str, datetime], list[Observation]] = defaultdict(list)
    for row in observations:
        groups[row.entity_key, utc(row.observed_at)].append(row)
    result = []
    revisions = 0
    conflicts: set[str] = set()
    for rows in groups.values():
        newest = max(utc(row.available_at) for row in rows)
        latest = [row for row in rows if utc(row.available_at) == newest]
        # Observation-ID order cannot resolve two simultaneous competing facts.
        signatures = {
            digest(row.model_dump(exclude={"observation_id", "raw_dataset_id"})) for row in latest
        }
        revisions += len(rows) - 1
        if len(signatures) > 1:
            conflicts.add(rows[0].entity_key)
            continue
        result.append(min(latest, key=lambda row: row.observation_id))
    return (
        sorted(result, key=lambda row: (utc(row.observed_at), row.entity_key)),
        revisions,
        conflicts,
    )


def _describe(observations, cutoff, since, country, source, query_scope, max_age):
    points, revisions, conflicts = _resolved_points(observations)
    groups: dict[tuple, list[Observation]] = defaultdict(list)
    excluded: Counter[str] = Counter()
    entity_timelines: dict[str, list[Observation]] = defaultdict(list)
    for row in points:
        scope = _scope(row)
        entity_timelines[row.entity_key].append(row)
        if country is not None and scope[0] != country:
            excluded["unknown_observed_country" if scope[0] is None else "other_country"] += 1
            continue
        if source is not None and scope[1] != source:
            excluded["other_source"] += 1
            continue
        if query_scope is not None and scope[2] != query_scope:
            excluded["undeclared_query_scope" if scope[2] is None else "other_query_scope"] += 1
            continue
        groups[scope].append(row)
        if len(groups) > MAX_SCOPES:
            raise ValueError("Competition report exceeds scope budget; narrow country/source/query")
    scopes = [
        _timeline(scope, rows, cutoff, max_age, entity_timelines, conflicts)
        for scope, rows in sorted(
            groups.items(), key=lambda item: tuple(value or "" for value in item[0])
        )
    ]
    changes = []
    for entity_key, rows in sorted(entity_timelines.items()):
        for previous, latest in zip(rows, rows[1:], strict=False):
            if _scope(previous) != _scope(latest):
                changes.append(
                    {
                        "entity_key": entity_key,
                        "from": _scope_report(_scope(previous)),
                        "to": _scope_report(_scope(latest)),
                        "evidence_refs": [previous.observation_id, latest.observation_id],
                        "reason": "changed_scope_is_not_competition_growth",
                    }
                )
    comparable = [row for row in scopes if row["status"] == "observed"]
    return {
        "status": "observed" if comparable else "not_comparable",
        "reason": None if comparable else "no_fresh_declared_comparable_scope",
        "scopes": scopes,
        "resolved_observation_count": len(points),
        "revision_count": revisions,
        "conflicting_entity_count": len(conflicts),
        "excluded_observations": dict(sorted(excluded.items())),
        "scope_change_count": len(changes),
        "scope_changes_preview": changes[:PREVIEW_LIMIT],
        "scope_changes_preview_truncated": len(changes) > PREVIEW_LIMIT,
        "market_saturation_percent": None,
        "competition_growth": None,
        "limitations": [
            "Counts cover observed linked candidates within the bounded window, "
            "not the whole market",
            "First seen in this report is newly observed, not market entry or a launch date",
            "Missing or stale captures do not establish departure; departures are not estimated",
            "Declared query and coverage scope must match; "
            "wider discovery is not competition growth",
            "Capture timestamps are not complete query snapshots; "
            "collection completeness is unknown",
            "Retail quote changes are observed price pressure, "
            "not measured supplier margin or profit",
        ],
    }


def _timeline(scope, rows, cutoff, max_age, all_timelines, conflicting_entities):
    reason = _scope_reason(scope)
    events: dict[datetime, list[Observation]] = defaultdict(list)
    entities: dict[str, list[Observation]] = defaultdict(list)
    for row in rows:
        events[utc(row.observed_at)].append(row)
        entities[row.entity_key].append(row)
    last_at = max(events)
    age = (cutoff - last_at).total_seconds() / 3600
    reason = reason or (
        "stale_latest_capture"
        if age > max_age
        else "insufficient_capture_history"
        if len(events) < 2
        else None
    )
    seen_sellers: set[str] = set()
    seen_advertisers: set[str] = set()
    seen_offers: set[str] = set()
    seen_ads: set[str] = set()
    timeline = []
    previous_at = None
    interruptions = sorted(
        {
            utc(row.observed_at)
            for key in entities
            for row in all_timelines[key]
            if _scope(row) != scope
        }
    )
    for timestamp, observations in sorted(events.items()):
        offers = {row.entity_key for row in observations if row.entity_type == "offer"}
        ads = {row.entity_key for row in observations if row.entity_type != "offer"}
        sellers = {
            value
            for row in observations
            if row.entity_type == "offer"
            if (value := _declared(row.attributes.get("seller_id")))
        }
        advertisers = {
            value
            for row in observations
            if row.entity_type != "offer"
            if (value := _declared(row.attributes.get("advertiser_id")))
        }
        gap_hours = (
            (timestamp - previous_at).total_seconds() / 3600 if previous_at is not None else None
        )
        interval_reason = (
            "first_capture_in_window"
            if previous_at is None
            else "capture_gap"
            if gap_hours > max_age
            else _scope_reason(scope)
        )
        if previous_at is not None:
            position = bisect_right(interruptions, previous_at)
            if position < len(interruptions) and interruptions[position] < timestamp:
                interval_reason = "coverage_context_interrupted"
        known_scope = _scope_reason(scope) is None
        item = {
            "observed_at": timestamp.isoformat(),
            "observation_age_hours": (cutoff - timestamp).total_seconds() / 3600,
            "gap_hours": gap_hours,
            "interval_status": "not_comparable" if interval_reason else "observed",
            "interval_reason": interval_reason,
            "observed_offer_count": len(offers),
            "observed_ad_count": len(ads),
            "observed_named_seller_count": len(sellers),
            "observed_named_advertiser_count": len(advertisers),
            "unknown_seller_offer_count": len(offers)
            - sum(
                bool(_declared(row.attributes.get("seller_id")))
                for row in observations
                if row.entity_type == "offer"
            ),
            "unknown_advertiser_ad_count": len(ads)
            - sum(
                bool(_declared(row.attributes.get("advertiser_id")))
                for row in observations
                if row.entity_type != "offer"
            ),
            "newly_observed_seller_count": len(sellers - seen_sellers) if known_scope else None,
            "continuing_observed_seller_count": len(sellers & seen_sellers)
            if known_scope
            else None,
            "newly_observed_advertiser_count": (
                len(advertisers - seen_advertisers) if known_scope else None
            ),
            "continuing_observed_advertiser_count": (
                len(advertisers & seen_advertisers) if known_scope else None
            ),
            "newly_observed_offer_count": len(offers - seen_offers) if known_scope else None,
            "continuing_observed_offer_count": len(offers & seen_offers) if known_scope else None,
            "newly_observed_ad_count": len(ads - seen_ads) if known_scope else None,
            "continuing_observed_ad_count": len(ads & seen_ads) if known_scope else None,
            "evidence_refs_preview": sorted(row.observation_id for row in observations)[
                :PREVIEW_LIMIT
            ],
            "evidence_count": len(observations),
            "evidence_preview_truncated": len(observations) > PREVIEW_LIMIT,
        }
        timeline.append(item)
        seen_sellers.update(sellers)
        seen_advertisers.update(advertisers)
        seen_offers.update(offers)
        seen_ads.update(ads)
        previous_at = timestamp
    prices = [
        _prices(key, all_timelines[key], cutoff, scope, key in conflicting_entities)
        for key, observations in sorted(entities.items())
        if observations[0].entity_type == "offer"
    ]
    fresh_entities = sum(
        (cutoff - utc(observations[-1].observed_at)).total_seconds() <= max_age * 3600
        for observations in entities.values()
    )
    comparable_intervals = sum(row["interval_status"] == "observed" for row in timeline)
    reason = reason or ("no_comparable_capture_interval" if not comparable_intervals else None)
    return {
        **_scope_report(scope),
        "scope_id": digest(scope),
        "status": "not_comparable" if reason else "observed",
        "reason": reason,
        "coverage": "observed_candidates_only",
        "collection_completeness": "unknown",
        "latest_observation_age_hours": age,
        "first_capture_at": min(events).isoformat(),
        "last_capture_at": last_at.isoformat(),
        "capture_count": len(events),
        "observed_entity_count": len(entities),
        "fresh_entity_count": fresh_entities,
        "stale_entity_count": len(entities) - fresh_entities,
        "observed_seller_count": len(seen_sellers) if _scope_reason(scope) is None else None,
        "observed_advertiser_count": len(seen_advertisers)
        if _scope_reason(scope) is None
        else None,
        "timeline_preview": timeline[:PREVIEW_LIMIT],
        "timeline_preview_truncated": len(timeline) > PREVIEW_LIMIT,
        "comparable_interval_count": comparable_intervals,
        "price_offers_preview": prices[:PREVIEW_LIMIT],
        "price_offer_count": len(prices),
        "price_preview_truncated": len(prices) > PREVIEW_LIMIT,
        "price_interval_count": sum(row["comparable_interval_count"] for row in prices),
        "observed_price_decrease_count": sum(row["observed_decrease_count"] for row in prices),
        "observed_price_increase_count": sum(row["observed_increase_count"] for row in prices),
    }


def _quote(row: Observation):
    quote, reason = _price_quote(row)
    if quote is None:
        return None, reason
    scope_reason = _scope_reason(_scope(row))
    if scope_reason is not None:
        return None, scope_reason
    if _declared(quote["variant_id"]) is None:
        return None, "unknown_variant"
    if _declared(row.attributes.get("price_basis")) is None:
        return None, "unknown_price_basis"
    tax = row.attributes.get("tax_basis")
    shipping = row.attributes.get("shipping_basis")
    if tax not in ("tax_included", "tax_excluded", "not_applicable"):
        return None, "unknown_tax_basis"
    if shipping not in ("shipping_included", "shipping_excluded", "not_applicable"):
        return None, "unknown_shipping_basis"
    quote["signature"] = (
        _scope(row),
        quote["currency"],
        quote["variant_id"],
        quote["price_basis"],
        tax,
        shipping,
    )
    return quote, None


def _prices(entity_key, observations, cutoff, selected_scope, conflicting):
    previous = None
    changes = []
    exclusions: Counter[str] = Counter()
    matching = [row for row in observations if _scope(row) == selected_scope]
    context = None
    collection_scope_id = digest(selected_scope)
    for row in observations:
        if _scope(row) != selected_scope:
            previous = None
            exclusions["other_scope_breaks_price_history"] += 1
            continue
        quote, reason = _quote(row)
        if quote is None:
            exclusions[reason] += 1
            previous = None
            continue
        context = {
            "currency": quote["currency"],
            "variant_id": quote["variant_id"],
            "price_basis": quote["price_basis"],
            "tax_basis": row.attributes["tax_basis"],
            "shipping_basis": row.attributes["shipping_basis"],
        }
        if previous is not None:
            if previous["signature"] != quote["signature"]:
                exclusions["changed_variant_currency_tax_shipping_or_price_basis"] += 1
            else:
                changes.append(
                    {
                        **_price_change(previous, quote),
                        "price_context": context,
                        "collection_scope_id": collection_scope_id,
                    }
                )
        previous = quote
    if conflicting:
        exclusions["conflicting_revision_history"] += 1
        changes = []
    return {
        "entity_key": entity_key,
        "status": "observed" if changes else "not_comparable",
        "reason": None if changes else "insufficient_compatible_price_history",
        "price_context": context,
        "price_context_scope": "latest_valid_quote",
        "last_observation_age_hours": (cutoff - utc(matching[-1].observed_at)).total_seconds()
        / 3600,
        "quote_count": len(matching),
        "comparable_interval_count": len(changes),
        "observed_decrease_count": sum(row["direction"] == "decrease" for row in changes),
        "observed_increase_count": sum(row["direction"] == "increase" for row in changes),
        "uncertain_change_count": sum(row["direction"] == "uncertain" for row in changes),
        "excluded_quotes_or_intervals": dict(sorted(exclusions.items())),
        "intervals_preview": changes[:PREVIEW_LIMIT],
        "intervals_preview_truncated": len(changes) > PREVIEW_LIMIT,
        "interpretation": "Displayed retail price on declared tax/shipping basis; "
        "no margin inference",
    }
