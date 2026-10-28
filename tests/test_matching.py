"""Product matching hypotheses through the native catalog evidence boundary."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep

import psycopg
import pytest
from psycopg.types.json import Jsonb
from pydantic import ValidationError
from test_identity import identity as identity

from sm_trending_products import matching
from sm_trending_products.catalog import InvalidationPending
from sm_trending_products.identity import Identity
from sm_trending_products.invalidation import SourceInvalidation
from sm_trending_products.matching import MatchingIndexesNotReady, MatchingProfile, ProductMatcher

pytestmark = pytest.mark.integration


def add_candidate(
    identity: Identity,
    suffix: str,
    title: str,
    *,
    attributes: dict | None = None,
    synthetic: bool = False,
    discovered_at: datetime | None = None,
    observed_at: datetime | None = None,
    origin: dict | None = None,
    entity_type: str = "video",
) -> str:
    key = f"tiktok_display:{entity_type}:{suffix}"
    identity.catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,entity_type,title,attributes,"
        "discovered_at,last_observed_at,origin) VALUES(%s,%s,'tiktok_display',%s,%s,%s,%s,%s,%s)",
        (
            key,
            synthetic,
            entity_type,
            title,
            Jsonb(attributes or {}),
            discovered_at or datetime.now(UTC),
            observed_at,
            Jsonb(origin or {}),
        ),
    )
    return key


def candidates(result: dict) -> dict[str, dict]:
    return {row["entity_key"].rsplit(":", 1)[-1]: row for row in result["suggestions"]}


def kinds(row: dict) -> set[str]:
    return {conflict["kind"] for conflict in row["conflicts"]}


def test_multilingual_aliases_explain_matches_without_writing_identity(identity: Identity) -> None:
    identity.create_product("brush", "Electric toothbrush", market="FR")
    add_candidate(identity, "fr", "Nouvelle brosse à dents électrique", attributes={"creator": "A"})
    add_candidate(identity, "de", "Elektrische Zahnbürste für unterwegs")
    add_candidate(identity, "es", "Cepillo eléctrico portátil")
    add_candidate(identity, "folded", "Brosse a dents electrique compacte")
    add_candidate(identity, "unrelated", "Kitchen chopping board")
    add_candidate(identity, "fixture", "Electric toothbrush", synthetic=True)
    add_candidate(identity, "cost-record", "Electric toothbrush", entity_type="cost")
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    profile = MatchingProfile(
        aliases=("brosse à dents électrique", "elektrische Zahnbürste", "cepillo eléctrico")
    )
    before = identity.catalog.all("SELECT * FROM entities ORDER BY entity_key")
    result = matcher.suggest("brush", cutoff, profile=profile)
    assert result == matcher.suggest("brush", cutoff, profile=profile)
    assert set(candidates(result)) == {"fr", "de", "es", "folded"}
    assert result["method"] == "review-candidates-v2"
    assert result["market"] == "FR"
    for row in result["suggestions"]:
        assert row["requires_review"] is True
        assert row["matched_alias"] in profile.aliases
        assert row["matched_terms"]
        assert row["score_kind"] == "heuristic_not_identity_probability"
        assert row["current_owner"] is None
        assert row["ownership_status"] == "unowned"
        assert "creator" not in row and "creative_fingerprint" not in row
    assert identity.link_history(cutoff) == []
    assert identity.families(cutoff) == []
    assert identity.catalog.all("SELECT * FROM entities ORDER BY entity_key") == before
    # Translation is supplied explicitly; no model/provider fabricates cross-language recall.
    assert matcher.suggest("brush", cutoff)["suggestions"] == []


def test_identifier_brand_and_variant_facts_remain_explicit(identity: Identity) -> None:
    identity.create_product("bottle", "Travel bottle 350 ml")
    profile = MatchingProfile(
        aliases=("Travel bottle",),
        identifiers={"ean": "4006381333931", "sku": "BT-035"},
        brand="Éco",
        variants={"capacity": "350 ml", "color": "blue"},
    )
    add_candidate(
        identity,
        "same",
        "Travel bottle 0.35 l",
        attributes={
            "gtin": "04006381333931",
            "sku": "BT-035",
            "brand": "Eco",
            "capacity": "0,35 l",
            "color": "blue",
            "creator_id": "never-inferred",
        },
    )
    add_candidate(
        identity,
        "wrong",
        "Travel bottle 500 ml",
        attributes={
            "ean": "036000291452",
            "sku": "bt-035",
            "brand": "Elsewhere",
            "capacity": "500 ml",
            "color": "red",
        },
    )
    add_candidate(identity, "unknown", "Travel bottle compact", attributes={"ean": 4006381333931})
    matcher = ProductMatcher(identity.catalog)
    matcher.initialize_indexes()
    result = candidates(matcher.suggest("bottle", datetime.now(UTC), profile=profile))
    same, wrong, unknown = result["same"], result["wrong"], result["unknown"]
    assert not same["conflicts"]
    assert {fact["kind"] for fact in same["fact_matches"]} == {"identifier", "brand", "variant"}
    assert same["score"] >= 0.9
    assert kinds(wrong) == {
        "identifier_mismatch",
        "brand_mismatch",
        "variant_value_difference",
        "numeric_variant_difference",
    }
    assert wrong["score"] <= 0.25
    assert unknown["missing_facts"] == ["brand", "capacity", "color", "ean", "sku"]
    assert not unknown["fact_matches"] and not unknown["conflicts"]
    assert all(row["requires_review"] for row in result.values())
    assert identity.links(datetime.now(UTC)) == []


def test_model_numbers_and_textual_variant_differences_are_not_discarded(
    identity: Identity,
) -> None:
    identity.create_product("lamp", "Desk lamp 2")
    add_candidate(identity, "different", "Desk lamp 3", attributes={"color": "red 10 cm"})
    row = ProductMatcher(identity.catalog).suggest(
        "lamp",
        datetime.now(UTC),
        profile={"aliases": ["Desk lamp"], "variants": {"color": "blue 10 cm"}},
    )["suggestions"][0]
    assert kinds(row) == {"numeric_title_difference", "variant_value_difference"}
    assert row["score"] <= 0.25


def test_global_ownership_transfer_is_resolved_at_each_cutoff(identity: Identity) -> None:
    identity.create_product("original", "Desk lamp")
    identity.create_product("replacement", "Different desk lamp")
    key = add_candidate(identity, "transfer", "Desk lamp with clamp")
    first = identity.review_link("original", key)
    second = identity.review_link("replacement", key)
    before = identity.link_history(datetime.now(UTC))
    matcher = ProductMatcher(identity.catalog)
    historical = matcher.suggest("original", first["available_at"])["suggestions"][0]
    current = matcher.suggest("original", second["available_at"])["suggestions"][0]
    assert historical["current_owner"] == "original" and historical["already_linked"]
    assert not historical["conflicts"]
    assert current["current_owner"] == "replacement" and not current["already_linked"]
    assert current["ownership_status"] == "confirmed"
    assert kinds(current) == {"owned_by_other_product"}
    assert current["score"] <= 0.25
    assert identity.link_history(datetime.now(UTC)) == before


def test_ambiguous_owner_and_pair_budget_do_not_choose_an_incomplete_winner(
    identity: Identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity.create_product("first", "Desk lamp")
    identity.create_product("second", "Desk lamp")
    key = add_candidate(identity, "tie", "Desk lamp")
    cutoff = datetime.now(UTC)
    for product in ("first", "second"):
        identity.catalog.execute(
            "INSERT INTO product_links(product_id,entity_key,available_at,status,confidence,"
            "method,decision_id) VALUES(%s,%s,%s,'confirmed',1,'fixture',%s)",
            (product, key, cutoff, f"tie-{product}"),
        )
    matcher = ProductMatcher(identity.catalog)
    row = matcher.suggest("first", cutoff)["suggestions"][0]
    assert row["current_owner"] is None
    assert row["ownership_status"] == "ambiguous"
    assert kinds(row) == {"ambiguous_ownership"}
    monkeypatch.setattr(matching, "OWNERSHIP_PAIR_LIMIT", 1)
    result = matcher.suggest("first", cutoff)
    row = result["suggestions"][0]
    assert result["ownership_truncated"] is True
    assert row["current_owner"] is None and not row["already_linked"]
    assert row["ownership_status"] == "unresolved_budget_exceeded"
    assert row["requires_review"] is True
    assert len(identity.link_history(cutoff)) == 2


def test_current_projection_source_availability_and_workspaces(identity: Identity) -> None:
    identity.create_product("lamp", "Desk lamp")
    identity.create_product("synthetic-lamp", "Desk lamp", synthetic=True)
    cutoff = datetime.now(UTC)
    past, future = cutoff - timedelta(seconds=1), cutoff + timedelta(seconds=1)
    add_candidate(identity, "manual", "Desk lamp", discovered_at=past)
    add_candidate(identity, "discovered-later", "Desk lamp", discovered_at=future)
    add_candidate(identity, "observed-later", "Desk lamp", discovered_at=past, observed_at=future)
    add_candidate(identity, "synthetic", "Desk lamp", synthetic=True, discovered_at=past)
    for suffix, state, committed, available, synthetic, source in (
        ("usable", "committed", past, past, False, "tiktok_display"),
        ("late-commit", "committed", future, past, False, "tiktok_display"),
        ("late-availability", "committed", past, future, False, "tiktok_display"),
        ("invalidated", "invalidated", past, past, False, "tiktok_display"),
        ("deleted", "deleted", past, past, False, "tiktok_display"),
        ("staged", "staged", None, past, False, "tiktok_display"),
        ("wrong-workspace", "committed", past, past, True, "tiktok_display"),
        ("wrong-source", "committed", past, past, False, "other"),
    ):
        identity.catalog.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest,committed_at,"
            "max_available_at) VALUES(%s,%s,%s,%s,'{}',%s,%s)",
            (suffix, source, synthetic, state, committed, available),
        )
        add_candidate(
            identity,
            suffix,
            "Desk lamp",
            discovered_at=past,
            observed_at=past,
            origin={"dataset_id": suffix},
        )
    add_candidate(
        identity,
        "missing-dataset",
        "Desk lamp",
        discovered_at=past,
        origin={"dataset_id": "absent"},
    )
    matcher = ProductMatcher(identity.catalog)
    assert set(candidates(matcher.suggest("lamp", cutoff))) == {"manual", "usable"}
    assert set(candidates(matcher.suggest("synthetic-lamp", cutoff, synthetic=True))) == {
        "synthetic"
    }
    with pytest.raises(ValueError, match="workspace"):
        matcher.suggest("synthetic-lamp", cutoff)
    with pytest.raises(ValueError, match="cutoff"):
        matcher.suggest("lamp", past - timedelta(days=1))


def test_candidate_bound_is_deterministic_and_payload_is_bounded(identity: Identity) -> None:
    identity.create_product("lamp", "Desk lamp")
    for index in range(45):
        add_candidate(
            identity,
            f"{index:03}",
            "Desk lamp " + "verbose " * 1000,
            attributes={"brand": "z" * 10000, "unrelated": "y" * 10000},
        )
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    result = matcher.suggest(
        "lamp", cutoff, limit=2, candidate_limit=10, profile={"brand": "Known"}
    )
    assert result == matcher.suggest(
        "lamp", cutoff, limit=2, candidate_limit=10, profile={"brand": "Known"}
    )
    assert result["candidate_count"] == 10 and result["candidate_truncated"] is True
    assert len(result["suggestions"]) == 2
    assert set(candidates(result)) == {"000", "001"}
    assert all(len(row["title"]) == 4096 for row in result["suggestions"])
    assert all(row["missing_facts"] == ["brand"] for row in result["suggestions"])
    assert "attributes" not in result["suggestions"][0]


@pytest.mark.parametrize(
    "options",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"limit": 1.0},
        {"candidate_limit": 0},
        {"candidate_limit": 1001},
        {"candidate_limit": False},
        {"limit": 3, "candidate_limit": 2},
        {"synthetic": 0},
        {"profile": []},
        {"profile": False},
    ],
)
def test_invalid_request_bounds_fail_before_catalog_access(options: dict) -> None:
    # None exposes any accidental database access while validating the request.
    with pytest.raises((ValueError, ValidationError)):
        ProductMatcher(None).suggest("lamp", datetime.now(UTC), **options)


@pytest.mark.parametrize(
    "profile",
    [
        {"aliases": ["   "]},
        {"aliases": ["a"] * 9},
        {"brand": " "},
        {"identifiers": {"ean": "4006381333932"}},
        {"identifiers": {"ean": "4006381333931", "upc": "036000291452"}},
        {"identifiers": {"ean": 4006381333931}},
        {"identifiers": {"sku": "ﷺ" * 256}},
        {"identifiers": {"unknown": "x"}},
        {"variants": {"other": "x"}},
        {"variants": {"size": " "}},
        {"aliases": ["x" * 257]},
        {"unknown": "x"},
    ],
)
def test_invalid_profiles_are_rejected(profile: dict) -> None:
    with pytest.raises(ValidationError):
        MatchingProfile.model_validate(profile)


def test_noninformative_or_operator_query_has_no_unbounded_fallback(identity: Identity) -> None:
    identity.create_product("empty", "the viral video")
    matcher = ProductMatcher(identity.catalog)
    with pytest.raises(ValueError, match="informative"):
        matcher.suggest("empty", datetime.now(UTC), profile={"aliases": ["& ! : * | ()"]})
    # SQL-like and tsquery operators are parsed as bounded words, never executed.
    identity.create_product("injected", "x'); SELECT pg_sleep(10); --")
    result = matcher.suggest("injected", datetime.now(UTC))
    assert result["candidate_count"] == 0


def test_pending_invalidation_blocks_suggestions_only_in_its_workspace(
    identity: Identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity.create_product("live-lamp", "Desk lamp")
    identity.create_product("fixture-lamp", "Desk lamp", synthetic=True)
    add_candidate(identity, "live", "Desk lamp")
    add_candidate(identity, "fixture", "Desk lamp", synthetic=True)
    identity.catalog.execute(
        "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
        "VALUES('removing','tiktok_display',true,'committed','{}')"
    )
    SourceInvalidation(identity.catalog).begin("removing", "source_deleted")
    # Retain the durable job to exercise the real reader fence without completing maintenance.
    monkeypatch.setattr(identity.catalog, "resume_invalidations", lambda **kwargs: None)
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    with pytest.raises(InvalidationPending):
        matcher.suggest("fixture-lamp", cutoff, synthetic=True)
    assert set(candidates(matcher.suggest("live-lamp", cutoff))) == {"live"}
    assert identity.catalog.pending_invalidations()


def test_repeated_titles_never_infer_creator_independence_or_fingerprints(
    identity: Identity,
) -> None:
    identity.create_product("lamp", "Desk lamp")
    for suffix, creator in (("a", "creator-one"), ("b", "creator-one"), ("c", "creator-two")):
        add_candidate(identity, suffix, "Desk lamp", attributes={"creator_id": creator})
    result = ProductMatcher(identity.catalog).suggest("lamp", datetime.now(UTC))
    assert len(result["suggestions"]) == 3
    assert len({row["score"] for row in result["suggestions"]}) == 1
    assert all(row["requires_review"] for row in result["suggestions"])
    assert all(
        "creative_fingerprint" not in row and "independent_creators" not in row
        for row in result["suggestions"]
    )
    assert identity.links(datetime.now(UTC)) == []


@pytest.mark.parametrize(
    "profile",
    [
        {"aliases": ["Travel bottle 350 ml"]},
        {"variants": {"capacity": "350 ml"}},
        {"aliases": ["Travel bottle 350 ml"], "variants": {"capacity": "350 ml"}},
    ],
)
def test_alias_and_declared_variant_conflicts_do_not_require_source_attributes(
    identity: Identity, profile: dict
) -> None:
    identity.create_product("bottle", "Travel bottle")
    add_candidate(identity, "wrong-size", "Travel bottle 500 ml")
    row = ProductMatcher(identity.catalog).suggest("bottle", datetime.now(UTC), profile=profile)[
        "suggestions"
    ][0]
    # A generic canonical name can win text ranking; supplied aliases still declare numbers.
    assert "numeric_variant_difference" in kinds(row)
    assert row["score"] <= 0.25
    if "variants" in profile:
        assert "capacity" in row["missing_facts"]
    assert row["requires_review"] is True


def test_identifier_lookup_requires_explicit_online_indexes_and_survives_no_text_match(
    identity: Identity,
) -> None:
    identity.create_product("bottle", "Travel bottle")
    identity.create_product("fixture-bottle", "Travel bottle", synthetic=True)
    add_candidate(
        identity, "foreign-title", "Gourde de voyage", attributes={"ean": "4006381333931"}
    )
    add_candidate(identity, "gtin", "Reiseflasche", attributes={"gtin": "04006381333931"})
    add_candidate(
        identity, "fixture", "Gourde de voyage", synthetic=True, attributes={"ean": "4006381333931"}
    )
    add_candidate(
        identity,
        "future",
        "Gourde de voyage",
        attributes={"ean": "4006381333931"},
        discovered_at=datetime.now(UTC) + timedelta(days=1),
    )
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    assert matcher.suggest("bottle", cutoff)["suggestions"] == []
    with pytest.raises(MatchingIndexesNotReady, match="initialize_indexes") as error:
        matcher.suggest("bottle", cutoff, profile={"identifiers": {"ean": "4006381333931"}})
    assert error.value.code == "matching_indexes_not_ready" and len(error.value.indexes) == 6
    schema_versions = identity.catalog.all("SELECT version FROM schema_migrations ORDER BY version")
    setup = matcher.initialize_indexes()
    assert setup["status"] == "ready" and len(setup["created"]) == 6
    assert len(setup["indexes"]) == 6 and all(item["bytes"] > 0 for item in setup["indexes"])
    assert matcher.initialize_indexes()["created"] == []
    assert (
        identity.catalog.all("SELECT version FROM schema_migrations ORDER BY version")
        == schema_versions
    )
    profile = {"identifiers": {"ean": "4006381333931"}}
    rows = candidates(matcher.suggest("bottle", cutoff, profile=profile))
    assert set(rows) == {"foreign-title", "gtin"}
    assert all(row["identifier_hit"] and row["score"] >= 0.9 for row in rows.values())
    assert all(row["matched_terms"] == [] and row["requires_review"] for row in rows.values())
    assert set(candidates(matcher.suggest("fixture-bottle", cutoff, True, profile=profile))) == {
        "fixture"
    }
    assert identity.links(datetime.now(UTC)) == []


def test_identifier_candidates_have_priority_over_a_full_text_window(identity: Identity) -> None:
    identity.create_product("bottle", "Travel bottle")
    for suffix in ("aaa", "bbb", "ccc"):
        add_candidate(identity, suffix, "Travel bottle")
    add_candidate(identity, "zzz", "Gourde de voyage", attributes={"ean": "4006381333931"})
    identity.create_product("no-title", "the viral video")
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    matcher.initialize_indexes()
    profile = {"identifiers": {"ean": "4006381333931"}}
    result = matcher.suggest("bottle", cutoff, profile=profile, candidate_limit=1, limit=1)
    assert result["candidate_truncated"] is True
    assert set(candidates(result)) == {"zzz"}
    assert set(candidates(matcher.suggest("no-title", cutoff, profile=profile))) == {"zzz"}


def test_exact_sku_mpn_model_and_compatibility_spelling_have_indexed_recall(
    identity: Identity,
) -> None:
    identity.create_product("bottle", "Travel bottle")
    add_candidate(identity, "sku", "Gourde de voyage", attributes={"sku": " Ａ-12 "})
    add_candidate(identity, "mpn", "Reiseflasche", attributes={"mpn": "MPN-123"})
    add_candidate(identity, "model", "Gourde", attributes={"model": "M 3"})
    add_candidate(identity, "case-difference", "Gourde", attributes={"sku": "a-12"})
    matcher, cutoff = ProductMatcher(identity.catalog), datetime.now(UTC)
    matcher.initialize_indexes()
    for field, value, expected in (
        ("sku", "A-12", "sku"),
        ("mpn", "MPN-123", "mpn"),
        ("model", "M 3", "model"),
    ):
        result = matcher.suggest("bottle", cutoff, profile={"identifiers": {field: value}})
        assert set(candidates(result)) == {expected}
        assert result["suggestions"][0]["fact_matches"] == [
            {"kind": "identifier", "field": field, "value": value}
        ]


def test_online_setup_allows_entity_writes_and_recovers_an_interrupted_build(
    identity: Identity,
) -> None:
    identity.create_product("lamp", "Desk lamp")
    matcher = ProductMatcher(identity.catalog)
    conninfo = identity.catalog.pool.conninfo
    with psycopg.connect(conninfo() if callable(conninfo) else conninfo) as writer:
        writer.execute(
            "INSERT INTO entities(entity_key,synthetic,source,entity_type,title) "
            "VALUES('tiktok_display:video:held',false,'tiktok_display','video','Desk lamp')"
        )
        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(matcher.initialize_indexes)
            try:
                deadline = monotonic() + 1.5
                progress = None
                while monotonic() < deadline:
                    progress = identity.catalog.one(
                        "SELECT pid,phase FROM pg_stat_progress_create_index "
                        "WHERE relid='entities'::regclass"
                    )
                    if progress:
                        break
                    sleep(0.01)
                assert progress, "Concurrent index build did not reach PostgreSQL"
                # An ordinary CREATE INDEX would block this row write behind the held writer.
                with identity.catalog.transaction() as connection:
                    connection.execute("SET LOCAL lock_timeout='500ms'")
                    connection.execute(
                        "INSERT INTO entities(entity_key,synthetic,source,entity_type,title) "
                        "VALUES('tiktok_display:video:during',false, "
                        "'tiktok_display','video','Desk lamp')"
                    )
                identity.catalog.one("SELECT pg_cancel_backend(%s)", (progress["pid"],))
                with pytest.raises(psycopg.errors.QueryCanceled):
                    future.result(timeout=10)
            finally:
                writer.commit()
    cutoff = datetime.now(UTC)
    assert len(matcher.suggest("lamp", cutoff)["suggestions"]) == 2
    with pytest.raises(MatchingIndexesNotReady):
        matcher.suggest("lamp", cutoff, profile={"identifiers": {"sku": "LAMP"}})
    assert matcher.initialize_indexes()["status"] == "ready"
    assert (
        matcher.suggest("lamp", cutoff, profile={"identifiers": {"sku": "LAMP"}})["candidate_count"]
        == 2
    )


def test_identifier_retrieval_uses_fact_indexes_with_a_large_unrelated_catalog(
    identity: Identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity.create_product("bottle", "Travel bottle", synthetic=True)
    add_candidate(
        identity, "exact", "Gourde de voyage", synthetic=True, attributes={"ean": "4006381333931"}
    )
    identity.catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,entity_type,title,attributes) "
        "SELECT 'tiktok_display:video:unrelated-'||g,true,'tiktok_display','video', "
        "'Other content '||g,jsonb_build_object('ean',lpad(g::text,13,'0'),'sku','ITEM-'||g) "
        "FROM generate_series(1,10000) g"
    )
    matcher = ProductMatcher(identity.catalog)
    matcher.initialize_indexes()
    identity.catalog.execute("ANALYZE entities")
    original = identity.catalog.evidence_transaction
    plans = []

    @contextmanager
    def recording_transaction(synthetic):
        with original(synthetic) as connection:

            class RecordingConnection:
                def execute(self, query, params=()):
                    if query.startswith("SELECT COALESCE(("):
                        plans.append(
                            connection.execute("EXPLAIN (FORMAT JSON) " + query, params).fetchone()[
                                "QUERY PLAN"
                            ]
                        )
                    return connection.execute(query, params)

            yield RecordingConnection()

    monkeypatch.setattr(identity.catalog, "evidence_transaction", recording_transaction)
    result = matcher.suggest(
        "bottle", datetime.now(UTC), True, profile={"identifiers": {"ean": "4006381333931"}}
    )
    assert set(candidates(result)) == {"exact"}

    def nodes(plan):
        yield plan
        for child in plan.get("Plans", []):
            yield from nodes(child)

    actual_nodes = list(nodes(plans[0][0]["Plan"]))
    used = [node for node in actual_nodes if node.get("Index Name", "").startswith("entity_match_")]
    assert used
    assert any("4006381333931" in node.get("Index Cond", "") for node in used)
    assert not any(
        node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "entities"
        for node in actual_nodes
    )


def test_expanding_identifier_unicode_cannot_break_entity_writes_after_index_setup(
    identity: Identity,
) -> None:
    identity.create_product("bottle", "Travel bottle")
    matcher = ProductMatcher(identity.catalog)
    matcher.initialize_indexes()
    # Each compatibility character expands to 18 characters. It must stay outside
    # the B-tree key rather than exceeding PostgreSQL's index tuple limit on capture.
    add_candidate(identity, "large-key", "Travel bottle", attributes={"sku": "ﷺ" * 256})
    result = matcher.suggest("bottle", datetime.now(UTC), profile={"identifiers": {"sku": "BT-35"}})
    assert set(candidates(result)) == {"large-key"}
    assert result["suggestions"][0]["identifier_hit"] is False
    assert result["suggestions"][0]["missing_facts"] == ["sku"]
