"""Ownership semantics shared by assessments, evaluations, and SQL planning."""

from datetime import UTC, datetime, timedelta
from itertools import permutations

from sm_trending_products.contracts import ProductLink
from sm_trending_products.identity_rules import resolve_links

BASE = datetime(2026, 10, 22, tzinfo=UTC)
ENTITY = "tiktok_display:video:one"


def decision(product_id: str, hour: int = 0, **changes: object) -> ProductLink:
    return ProductLink.model_validate(
        {
            "product_id": product_id,
            "entity_key": ENTITY,
            "available_at": BASE + timedelta(hours=hour),
            **changes,
        }
    )


def test_latest_owner_is_chosen_before_confidence_is_filtered() -> None:
    links = [decision("previous", confidence=1), decision("current", 1, confidence=0.4)]
    assert resolve_links(links, BASE + timedelta(hours=1), min_confidence=0.9) == {}
    assert resolve_links(links, BASE + timedelta(hours=1))[ENTITY].product_id == "current"
    assert resolve_links(links, BASE, min_confidence=0.9)[ENTITY].product_id == "previous"


def test_simultaneous_owners_are_ambiguous_regardless_of_order_or_confidence() -> None:
    links = [decision("first", confidence=1), decision("second", confidence=0.2)]
    for ordered in permutations(links):
        assert resolve_links(ordered, BASE) == {}
        assert resolve_links(ordered, BASE, min_confidence=0.9) == {}


def test_latest_pair_state_supersedes_older_confirmations_before_filtering() -> None:
    links = [
        decision("owner", confidence=1),
        decision("owner", 1, confidence=0.4),
        decision("alternative", 2, status="rejected"),
        decision("other-alternative", 3, status="proposed"),
    ]
    assert resolve_links(links, BASE + timedelta(hours=3), min_confidence=0.9) == {}
    assert resolve_links(links, BASE + timedelta(hours=3))[ENTITY].product_id == "owner"


def test_rejected_alternative_does_not_erase_confirmed_owner() -> None:
    links = [decision("owner"), decision("alternative", 1, status="rejected")]
    assert resolve_links(links, BASE + timedelta(hours=1))[ENTITY].product_id == "owner"


def test_pair_timestamp_ties_preserve_the_supplied_database_event_sequence() -> None:
    confirmed = decision("owner", decision_id="z-earlier")
    rejected = decision("owner", status="rejected", decision_id="a-later")
    assert resolve_links([confirmed, rejected], BASE) == {}
    assert resolve_links([rejected, confirmed], BASE)[ENTITY].status == "confirmed"
