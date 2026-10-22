"""Identity decisions through the catalog review boundary and real PostgreSQL."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from sm_trending_products.catalog import Catalog
from sm_trending_products.contracts import FamilyMembership, Product, ProductLink
from sm_trending_products.identity import Identity

pytestmark = pytest.mark.integration


@pytest.fixture
def identity() -> Iterator[Identity]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for real PostgreSQL identity scenarios")
    schema = f"identity_test_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    catalog = Catalog(make_conninfo(database_url, options=f"-c search_path={schema}"))
    try:
        catalog.initialize()
        yield Identity(catalog)
    finally:
        catalog.close()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def add_entity(
    identity: Identity,
    entity_key: str,
    *,
    synthetic: bool = False,
    title: str = "Portable desk lamp",
) -> None:
    source, entity_type, _ = entity_key.split(":", 2)
    identity.catalog.execute(
        "INSERT INTO entities(entity_key,synthetic,source,entity_type,title,attributes) "
        "VALUES(%s,%s,%s,%s,%s,%s)",
        (entity_key, synthetic, source, entity_type, title, Jsonb({})),
    )


def confirmed_owners(identity: Identity, cutoff: datetime) -> dict[str, str]:
    return {
        row["entity_key"]: row["product_id"]
        for row in identity.links(cutoff)
        if row["status"] == "confirmed"
    }


def test_reviewed_evidence_has_no_identity_before_the_decision(identity: Identity) -> None:
    product = identity.create_product("desk-lamp", "Desk lamp", market="FR")
    Product.model_validate(product)
    add_entity(identity, "tiktok_display:video:one")
    before = datetime.now(UTC)
    decision = identity.review_link("desk-lamp", "tiktok_display:video:one")
    ProductLink.model_validate(decision)
    assert identity.links(before) == []
    assert confirmed_owners(identity, decision["available_at"]) == {
        "tiktok_display:video:one": "desk-lamp"
    }


def test_rejected_hypothesis_preserves_owner_and_transfer_preserves_history(
    identity: Identity,
) -> None:
    identity.create_product("desk-lamp", "Desk lamp")
    identity.create_product("similar-lamp", "Different lamp")
    add_entity(identity, "tiktok_display:video:one")
    original = identity.review_link("desk-lamp", "tiktok_display:video:one")
    rejection = identity.review_link("similar-lamp", "tiktok_display:video:one", status="rejected")
    assert confirmed_owners(identity, rejection["available_at"]) == {
        "tiktok_display:video:one": "desk-lamp"
    }
    transferred = identity.review_link("similar-lamp", "tiktok_display:video:one")
    assert confirmed_owners(identity, transferred["available_at"]) == {
        "tiktok_display:video:one": "similar-lamp"
    }
    assert confirmed_owners(identity, original["available_at"]) == {
        "tiktok_display:video:one": "desk-lamp"
    }
    history = identity.link_history(transferred["available_at"])
    assert len(history) == 4
    assert history[0]["status"] == "confirmed"
    assert history[-1]["product_id"] == "similar-lamp"
    assert any(row["product_id"] == "desk-lamp" and row["status"] == "rejected" for row in history)


def test_synthetic_evidence_cannot_be_confirmed_for_a_live_product(identity: Identity) -> None:
    identity.create_product("live-lamp", "Desk lamp")
    identity.create_product("fixture-lamp", "Desk lamp", synthetic=True)
    add_entity(identity, "tiktok_display:video:synthetic", synthetic=True)
    with pytest.raises(ValueError, match="workspace"):
        identity.review_link("live-lamp", "tiktok_display:video:synthetic")
    decision = identity.review_link("fixture-lamp", "tiktok_display:video:synthetic")
    assert identity.links(decision["available_at"], synthetic=False) == []
    assert (
        identity.links(decision["available_at"], synthetic=True)[0]["product_id"] == "fixture-lamp"
    )


def test_multi_entity_transfer_rolls_back_when_one_entity_is_missing(identity: Identity) -> None:
    identity.create_product("source-product", "Original product")
    identity.create_product("target-product", "Corrected product")
    add_entity(identity, "tiktok_display:video:one")
    identity.review_link("source-product", "tiktok_display:video:one")
    with pytest.raises(ValueError, match="workspace"):
        identity.reassign_entities(
            ["tiktok_display:video:one", "tiktok_display:video:missing"], "target-product"
        )
    assert confirmed_owners(identity, datetime.now(UTC)) == {
        "tiktok_display:video:one": "source-product"
    }


def test_concurrent_transfers_leave_one_confirmed_owner(identity: Identity) -> None:
    identity.create_product("first-product", "First product")
    identity.create_product("second-product", "Second product")
    add_entity(identity, "tiktok_display:video:one")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(identity.review_link, product_id, "tiktok_display:video:one")
            for product_id in ["first-product", "second-product"]
        ]
        results = [future.result() for future in futures]
    latest = max(results, key=lambda row: row["available_at"])
    assert confirmed_owners(identity, datetime.now(UTC)) == {
        "tiktok_display:video:one": latest["product_id"]
    }


def test_family_changes_are_reconstructible_without_future_memberships(identity: Identity) -> None:
    identity.create_product("desk-lamp", "Desk lamp")
    first = identity.set_family("desk-lamp", "portable-lighting")
    second = identity.set_family("desk-lamp", "desk-lighting")
    FamilyMembership.model_validate(first)
    FamilyMembership.model_validate(second)
    assert identity.families(first["available_at"])[0]["family_id"] == "portable-lighting"
    assert identity.families(second["available_at"])[0]["family_id"] == "desk-lighting"
    assert len(identity.family_history(second["available_at"])) == 2


def test_text_candidates_are_bounded_deterministic_and_never_confirmed(identity: Identity) -> None:
    identity.create_product("desk-lamp", "Desk lamp")
    add_entity(identity, "tiktok_display:video:one", title="Portable desk lamp")
    add_entity(identity, "tiktok_display:video:two", title="Desk lamp with clamp")
    add_entity(identity, "tiktok_display:video:fixture", synthetic=True, title="Desk lamp")
    first = identity.suggest_links("desk lamp", limit=1)
    assert first == identity.suggest_links("desk lamp", limit=1)
    assert len(first) == 1
    assert first[0]["entity_key"] in {"tiktok_display:video:one", "tiktok_display:video:two"}
    assert first[0]["requires_review"] is True
    assert identity.links(datetime.now(UTC)) == []


def test_sql_and_python_resolve_the_same_historical_ownership(identity: Identity) -> None:
    from sm_trending_products.identity_rules import resolve_links

    identity.create_product("first", "First product")
    identity.create_product("second", "Second product")
    base = identity.catalog.one("SELECT clock_timestamp() AS ts")["ts"]
    cases = {
        "ambiguous": [("first", 0, "confirmed", 1), ("second", 0, "confirmed", 0.2)],
        "takeover": [("first", 0, "confirmed", 1), ("second", 1, "confirmed", 0.2)],
        "alternative": [("first", 0, "confirmed", 1), ("second", 1, "rejected", 1)],
        "downgraded": [("first", 0, "confirmed", 1), ("first", 1, "confirmed", 0.2)],
        "future": [("first", 0, "confirmed", 1), ("second", 2, "confirmed", 1)],
        "event-order": [("first", 0, "confirmed", 1), ("first", 0, "rejected", 1)],
    }
    for name, events in cases.items():
        entity_key = f"tiktok_display:video:{name}"
        add_entity(identity, entity_key)
        for index, (product_id, hour, status, confidence) in enumerate(events):
            identity.catalog.execute(
                "INSERT INTO product_links(decision_id,product_id,entity_key,available_at,"
                "status,confidence,method) VALUES(%s,%s,%s,%s,%s,%s,'fixture')",
                (
                    f"{name}-{index}",
                    product_id,
                    entity_key,
                    base + timedelta(hours=hour),
                    status,
                    confidence,
                ),
            )
    cutoff = base + timedelta(hours=1)
    expected = {
        "tiktok_display:video:takeover": "second",
        "tiktok_display:video:alternative": "first",
        "tiktok_display:video:downgraded": "first",
        "tiktok_display:video:future": "first",
    }
    history = [ProductLink.model_validate(row) for row in identity.link_history(cutoff)]
    assert confirmed_owners(identity, cutoff) == expected
    assert {key: row.product_id for key, row in resolve_links(history, cutoff).items()} == expected
    assert {
        key: row.product_id
        for key, row in resolve_links(history, cutoff, min_confidence=0.9).items()
    } == {
        "tiktok_display:video:alternative": "first",
        "tiktok_display:video:future": "first",
    }
