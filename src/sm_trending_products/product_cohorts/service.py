"""Category review and compact comparisons without a global raw-history scan."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..contracts import digest, utc
from ..module_schema import initialize_module, require_module
from ._schema import MIGRATIONS

METRICS = {
    "growth_per_hour": ("features", "growth_per_hour"),
    "view_count": ("features", "view_count"),
    "attention_score": ("attention", "score"),
    "demand_score": ("demand", "score"),
    "observed_seller_count": ("competition", "observed_seller_count"),
    "observed_advertiser_count": ("competition", "observed_advertiser_count"),
}


def _text(value: str, label: str, maximum: int = 128) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(f"Invalid {label}")
    return value


def _revision(expected_revision: int) -> None:
    if type(expected_revision) is not int or not 0 <= expected_revision < 2_147_483_647:
        raise ValueError("Invalid expected_revision")


def _record(row: dict) -> dict:
    return {
        **row["payload"],
        "revision": row["revision"],
        "available_at": row["available_at"].isoformat(),
    }


class CohortInsights:
    """Compare reviewed categories within an explicitly labelled target-market scope.

    Current assessment pointers are the only metric input. Historical requests
    never substitute a newer pointer; missing older snapshots remain explicit.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "product_cohorts", MIGRATIONS)

    def save_taxonomy(
        self,
        taxonomy_id: str,
        categories: list[dict],
        expected_revision: int,
        request_id: str,
        *,
        reviewer: str,
        rationale: str,
    ) -> dict:
        """Append a reviewed complete category list with immutable request replay."""
        _text(taxonomy_id, "taxonomy_id")
        _text(request_id, "request_id")
        _revision(expected_revision)
        _text(reviewer, "reviewer")
        _text(rationale, "rationale", 4000)
        if not isinstance(categories, list) or not 1 <= len(categories) <= 1000:
            raise ValueError("A taxonomy requires between one and 1000 categories")
        normalized = []
        for category in categories:
            if not isinstance(category, dict) or set(category) != {"category_id", "name"}:
                raise ValueError("Categories require category_id and name")
            normalized.append({key: _text(category[key], key) for key in ("category_id", "name")})
        normalized.sort(key=lambda item: item["category_id"])
        if len({item["category_id"] for item in normalized}) != len(normalized):
            raise ValueError("Duplicate category_id")
        payload = {
            "taxonomy_id": taxonomy_id,
            "categories": normalized,
            "reviewer": reviewer,
            "rationale": rationale,
        }
        require_module(self.catalog, "product_cohorts")
        fingerprint = digest({"payload": payload, "expected_revision": expected_revision})
        with self.catalog.transaction() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended("
                "current_schema()||':cohort-taxonomy:'||%s,492371261))",
                (taxonomy_id,),
            )
            replay = connection.execute(
                "SELECT * FROM cohort_taxonomy_revisions WHERE taxonomy_id=%s AND request_id=%s",
                (taxonomy_id, request_id),
            ).fetchone()
            if replay:
                if replay["request_hash"] != fingerprint:
                    raise ValueError("request_conflict")
                return _record(replay)
            current = connection.execute(
                "SELECT revision FROM cohort_taxonomies WHERE taxonomy_id=%s FOR UPDATE",
                (taxonomy_id,),
            ).fetchone()
            if expected_revision != (current["revision"] if current else 0):
                raise ValueError("revision_conflict")
            connection.execute(
                "INSERT INTO cohort_taxonomies VALUES(%s,%s) ON CONFLICT(taxonomy_id) "
                "DO UPDATE SET revision=EXCLUDED.revision",
                (taxonomy_id, expected_revision + 1),
            )
            row = connection.execute(
                "INSERT INTO cohort_taxonomy_revisions VALUES(%s,%s,%s,%s,%s,clock_timestamp()) "
                "RETURNING *",
                (taxonomy_id, expected_revision + 1, request_id, fingerprint, Jsonb(payload)),
            ).fetchone()
            assert row is not None
            return _record(row)

    def taxonomy(self, taxonomy_id: str, as_of: datetime) -> dict | None:
        require_module(self.catalog, "product_cohorts")
        _text(taxonomy_id, "taxonomy_id")
        row = self.catalog.one(
            "SELECT * FROM cohort_taxonomy_revisions WHERE taxonomy_id=%s AND available_at<=%s "
            "ORDER BY revision DESC LIMIT 1",
            (taxonomy_id, utc(as_of)),
        )
        return _record(row) if row else None

    def assign(
        self,
        product_id: str,
        taxonomy_id: str,
        category_id: str | None,
        expected_revision: int,
        request_id: str,
        *,
        taxonomy_revision: int,
        synthetic: bool = False,
        reviewer: str,
        rationale: str,
    ) -> dict:
        """Review a product category; None explicitly withdraws an assignment."""
        for key, value in (
            ("product_id", product_id),
            ("taxonomy_id", taxonomy_id),
            ("request_id", request_id),
            ("reviewer", reviewer),
        ):
            _text(value, key)
        _text(rationale, "rationale", 4000)
        if category_id is not None:
            _text(category_id, "category_id")
        _revision(expected_revision)
        if (
            type(synthetic) is not bool
            or type(taxonomy_revision) is not int
            or taxonomy_revision < 1
        ):
            raise ValueError("Invalid assignment scope")
        require_module(self.catalog, "product_cohorts")
        payload = {
            "product_id": product_id,
            "taxonomy_id": taxonomy_id,
            "category_id": category_id,
            "taxonomy_revision": taxonomy_revision,
            "synthetic": synthetic,
            "reviewer": reviewer,
            "rationale": rationale,
        }
        fingerprint = digest({"payload": payload, "expected_revision": expected_revision})
        with self.catalog.evidence_transaction(synthetic) as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended("
                "current_schema()||':cohort-assignment:'||%s,492371262))",
                (digest([product_id, taxonomy_id]),),
            )
            product = connection.execute(
                "SELECT synthetic FROM products WHERE product_id=%s FOR SHARE", (product_id,)
            ).fetchone()
            if product is None or product["synthetic"] != synthetic:
                raise ValueError("Product is unavailable in this workspace")
            replay = connection.execute(
                "SELECT * FROM cohort_assignment_revisions WHERE product_id=%s "
                "AND taxonomy_id=%s AND request_id=%s",
                (product_id, taxonomy_id, request_id),
            ).fetchone()
            if replay:
                if replay["request_hash"] != fingerprint:
                    raise ValueError("request_conflict")
                return _record(replay)
            taxonomy = connection.execute(
                "SELECT payload FROM cohort_taxonomy_revisions "
                "WHERE taxonomy_id=%s AND revision=%s",
                (taxonomy_id, taxonomy_revision),
            ).fetchone()
            if taxonomy is None or (
                category_id is not None
                and category_id
                not in {item["category_id"] for item in taxonomy["payload"]["categories"]}
            ):
                raise ValueError("Unknown reviewed taxonomy category")
            current = connection.execute(
                "SELECT revision FROM cohort_assignments WHERE product_id=%s AND taxonomy_id=%s "
                "FOR UPDATE",
                (product_id, taxonomy_id),
            ).fetchone()
            if expected_revision != (current["revision"] if current else 0):
                raise ValueError("revision_conflict")
            connection.execute(
                "INSERT INTO cohort_assignments VALUES(%s,%s,%s) "
                "ON CONFLICT(product_id,taxonomy_id) DO UPDATE SET revision=EXCLUDED.revision",
                (product_id, taxonomy_id, expected_revision + 1),
            )
            row = connection.execute(
                "INSERT INTO cohort_assignment_revisions VALUES("
                "%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp()) RETURNING *",
                (
                    product_id,
                    taxonomy_id,
                    expected_revision + 1,
                    taxonomy_revision,
                    category_id,
                    request_id,
                    fingerprint,
                    Jsonb(payload),
                ),
            ).fetchone()
            assert row is not None
            return _record(row)

    def compare(
        self,
        as_of: datetime,
        synthetic: bool = False,
        *,
        taxonomy_id: str,
        category_id: str | None = None,
        country: str | None = None,
        metric: str = "attention_score",
        max_products: int = 1000,
        min_peers: int = 3,
        max_assessment_age_hours: float = 72,
    ) -> dict[str, Any]:
        cutoff = utc(as_of)
        if (
            type(synthetic) is not bool
            or not isinstance(metric, str)
            or metric not in METRICS
            or type(max_products) is not int
            or not 1 <= max_products <= 10000
            or type(min_peers) is not int
            or not 2 <= min_peers <= 1000
            or type(max_assessment_age_hours) not in (int, float)
            or not math.isfinite(max_assessment_age_hours)
            or not 0 < max_assessment_age_hours <= 8760
        ):
            raise ValueError("Invalid bounded cohort configuration")
        if country is not None and (
            not isinstance(country, str) or not re.fullmatch(r"[A-Z]{2}", country)
        ):
            raise ValueError("Country must be an explicit uppercase two-letter code")
        taxonomy = self.taxonomy(taxonomy_id, cutoff)
        if taxonomy is None:
            raise ValueError("Taxonomy was unavailable at this cutoff")
        if category_id is not None and category_id not in {
            item["category_id"] for item in taxonomy["categories"]
        }:
            raise ValueError("Unknown category_id")
        dimension, field = METRICS[metric]
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            rows = list(
                connection.execute(
                    "SELECT p.product_id,p.name,p.market,a.category_id,a.taxonomy_revision,"
                    "a.revision AS assignment_revision,f.family_id,r.run_id,r.as_of AS assessed_at,"
                    "r.policy,CASE WHEN r.run_id IS NOT NULL THEN c.payload#>%s END AS metric,"
                    "CASE WHEN r.run_id IS NOT NULL THEN c.payload#>'{features,age_hours}' END "
                    "AS age_hours,CASE WHEN r.run_id IS NOT NULL THEN "
                    "c.payload#>'{features,growth_upper_per_hour}' END AS growth_upper_per_hour "
                    "FROM products p LEFT JOIN LATERAL (SELECT * FROM cohort_assignment_revisions "
                    "WHERE product_id=p.product_id AND taxonomy_id=%s AND available_at<=%s "
                    "ORDER BY revision DESC LIMIT 1) a ON true "
                    "LEFT JOIN LATERAL (SELECT family_id FROM family_memberships "
                    "WHERE product_id=p.product_id AND available_at<=%s "
                    "ORDER BY available_at DESC,id DESC LIMIT 1) f ON true "
                    "LEFT JOIN current_assessments c ON c.product_id=p.product_id "
                    "AND c.synthetic=%s "
                    "LEFT JOIN assessment_runs r ON r.run_id=c.run_id AND NOT r.invalidated "
                    "AND r.as_of<=%s AND r.created_at<=%s "
                    "LEFT JOIN derived_publications d ON d.object_key=r.object_key "
                    "WHERE p.synthetic=%s AND p.created_at<=%s "
                    "AND (%s::text IS NULL OR a.category_id=%s) "
                    "AND (%s::text IS NULL OR upper(p.market)=%s) "
                    "AND (r.run_id IS NULL OR d.state IS NULL OR d.state='committed') "
                    "ORDER BY p.product_id LIMIT %s",
                    (
                        [dimension, field],
                        taxonomy_id,
                        cutoff,
                        cutoff,
                        synthetic,
                        cutoff,
                        cutoff,
                        synthetic,
                        cutoff,
                        category_id,
                        category_id,
                        country,
                        country,
                        max_products + 1,
                    ),
                ).fetchall()
            )
            if len(rows) > max_products:
                raise ValueError("Cohort exceeds product budget; narrow category or country")
            return _compare(
                rows, taxonomy, cutoff, synthetic, metric, min_peers, max_assessment_age_hours
            )


def _compare(rows, taxonomy, cutoff, synthetic, metric, min_peers, max_age):
    groups: dict[tuple, list[dict]] = defaultdict(list)
    result = []
    for row in rows:
        age = row["age_hours"]
        value = row["metric"]
        assessed = row["assessed_at"]
        country = row["market"].upper() if isinstance(row["market"], str) else None
        age_hours = (
            age + (cutoff - assessed).total_seconds() / 3600
            if isinstance(age, (float, int)) and assessed
            else None
        )
        age_bucket = (
            None
            if age_hours is None
            else "under_1d"
            if age_hours < 24
            else "1_to_7d"
            if age_hours < 168
            else "over_7d"
        )
        reason = (
            "unreviewed_category"
            if not row["category_id"]
            else "taxonomy_revision_requires_review"
            if row["taxonomy_revision"] != taxonomy["revision"]
            else "unknown_target_country"
            if not country or not re.fullmatch(r"[A-Z]{2}", country)
            else "no_current_assessment_at_cutoff"
            if assessed is None
            else "stale_assessment"
            if (cutoff - assessed).total_seconds() > max_age * 3600
            else "unknown_age"
            if age_bucket is None
            else "missing_or_nonfinite_metric"
            if type(value) not in (float, int)
            or (type(value) is float and not math.isfinite(value))
            else "bounded_growth"
            if metric == "growth_per_hour" and row["growth_upper_per_hour"] != value
            else "uncertified_metric_unit"
            if metric in {"growth_per_hour", "view_count"}
            else None
        )
        item = {
            "product_id": row["product_id"],
            "name": row["name"],
            "category_id": row["category_id"],
            "assignment_revision": row["assignment_revision"],
            "target_country": country,
            "country_basis": "product_target_market",
            "observed_audience_country": None,
            "audience_coverage": "unknown",
            "source_scope": "assessment_aggregate_mixed_or_unknown",
            "distribution_scope": "assessment_aggregate_mixed_or_unknown",
            "metric_unit": "uncertified"
            if metric in {"growth_per_hour", "view_count"}
            else "score_points"
            if metric.endswith("_score")
            else "count",
            "age_bucket": age_bucket,
            "metric": metric,
            "value": value,
            "assessment_run_id": row["run_id"],
            "assessed_at": assessed.isoformat() if assessed else None,
            "family_id": row["family_id"],
            "percentile": None,
            "peer_count": 0,
            "status": "not_comparable" if reason else "pending",
            "reason": reason,
            "comparability": "not_comparable" if reason else "limited",
            "descriptive_within_observed_panel_percentile": None,
        }
        result.append(item)
        if reason is None:
            key = (row["category_id"], country, age_bucket, digest(row["policy"]), assessed)
            groups[key].append(item)
    for members in groups.values():
        representatives: dict[tuple[str, str], dict] = {}
        for item in members:
            family = (
                ("family", item["family_id"])
                if item["family_id"]
                else ("product", item["product_id"])
            )
            representatives.setdefault(family, item)
        for item in members:
            family = (
                ("family", item["family_id"])
                if item["family_id"]
                else ("product", item["product_id"])
            )
            peers = [peer for key, peer in representatives.items() if key != family]
            item["peer_count"] = len(peers)
            item["cohort_product_count"] = len(members)
            item["cohort_independent_count"] = len(representatives)
            item["unknown_family_count"] = sum(peer["family_id"] is None for peer in members)
            item["tie_count"] = sum(peer["value"] == item["value"] for peer in peers)
            reason = (
                "undersized_peer_group"
                if len(peers) < min_peers
                else ("all_peers_tied" if item["tie_count"] == len(peers) else None)
            )
            item["status"] = "not_comparable" if reason else "observed"
            item["comparability"] = "not_comparable" if reason else "limited"
            item["reason"] = reason
            if reason is None:
                item["percentile"] = (
                    100
                    * (sum(peer["value"] < item["value"] for peer in peers) + item["tie_count"] / 2)
                    / len(peers)
                )
                item["descriptive_within_observed_panel_percentile"] = item["percentile"]
    report = {
        "version": "target-market-cohorts-v1",
        "as_of": cutoff.isoformat(),
        "requested_as_of": cutoff.isoformat(),
        "eligibility_scope": "assessment_cutoff_eligibility",
        "live_source_eligibility_reassessed": False,
        "synthetic": synthetic,
        "taxonomy": taxonomy,
        "metric": metric,
        "rows": result,
        "coverage": {
            "candidate_product_count": len(rows),
            "comparable_product_count": sum(item["percentile"] is not None for item in result),
            "scope": "bounded_current_assessment_candidates",
            "global_coverage": "unknown",
        },
        "interpretation": [
            "Country is the declared product target market, not viewer country or country demand",
            "Source and distribution remain assessment aggregates; "
            "no channel comparison is implied",
            "Peers share reviewed category, target country, age bucket, "
            "assessment cutoff and policy",
            "One lexicographically first product represents each known family; "
            "own family is excluded",
            "Percentile is a midpoint tie rank among measured peers, "
            "not opportunity or sales probability",
            "Compact assessments do not certify original counter units; "
            "raw growth and view values cannot receive a percentile",
            "Current pointers newer than this cutoff are unavailable; "
            "no historical backfill is invented",
        ],
    }
    report["report_id"] = digest(report)
    return report
