"""Frozen decisions and temporal replay through the real persistence facade."""

from __future__ import annotations

import time
from datetime import timedelta

import pytest

from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def record(entity_id, observed_at, views, *, available_at=None, account_id=None):
    return {
        "observation_id": f"{entity_id}-{observed_at.isoformat()}-{views}",
        "source": "fixture_import",
        "entity_type": "video",
        "entity_id": entity_id,
        "synthetic": True,
        "observed_at": observed_at.isoformat(),
        "available_at": (available_at or observed_at).isoformat(),
        "published_at": (observed_at - timedelta(days=1)).isoformat(),
        "account_id": account_id,
        "metrics": {"view_count": {"lower": views, "upper": views}},
        "attributes": {"creator_id": entity_id, "title": "Physical product evidence"},
    }


def seed_product(pipeline, product_id, *, base=None, account_id=None):
    base = base or now()
    entity_id = f"video-{product_id}"
    datasets = pipeline.import_records(
        [
            record(entity_id, base - timedelta(hours=2), 100, account_id=account_id),
            record(entity_id, base - timedelta(hours=1), 200, account_id=account_id),
        ],
        synthetic=True,
    )
    pipeline.create_product(product_id, product_id, synthetic=True)
    entity_key = f"fixture_import:video:{entity_id}"
    pipeline.review_link(product_id, entity_key)
    return entity_id, entity_key, datasets


def outcome_config(**changes):
    return {"top_k": 1, "horizon_days": [3], "launch_delay_days": [0], "threshold": 1000, **changes}


def test_persisted_forward_decision_is_immutable_and_outcomes_are_idempotent(pipeline):
    entity_id, _, _ = seed_product(pipeline, "first-product")
    decision_time = now() + timedelta(minutes=1)
    pipeline.import_records(
        [
            record(entity_id, decision_time, 300),
            record(entity_id, decision_time + timedelta(days=3), 1800),
        ],
        synthetic=True,
    )
    frozen = pipeline.freeze(decision_time, synthetic=True, config=outcome_config())
    assert frozen["result"]["selected"] == ["first-product"]
    pending = pipeline.mature(frozen["evaluation_id"], decision_time + timedelta(days=2))
    assert pending["result"]["status"] == "pending"
    matured = pipeline.mature(frozen["evaluation_id"], decision_time + timedelta(days=3))
    assert matured["result"]["scenarios"][0]["methods"]["policy"]["precision_at_k"] == 1
    repeated = pipeline.mature(frozen["evaluation_id"], decision_time + timedelta(days=3))
    assert repeated["evaluation_id"] == matured["evaluation_id"]
    stored = pipeline.evaluations(synthetic=True)
    original = next(row for row in stored if row["evaluation_id"] == frozen["evaluation_id"])
    assert original["payload"] == frozen["result"]
    assert {row["kind"] for row in stored} == {"forward", "outcome"}
    assert pipeline.evaluations(synthetic=False) == []


def test_replay_uses_old_identity_and_excludes_a_later_available_counter(pipeline):
    entity_id, entity_key, _ = seed_product(pipeline, "original-owner")
    pipeline.create_product("later-owner", "Later owner", synthetic=True)
    decision_time = now()
    time.sleep(0.01)
    pipeline.review_link("later-owner", entity_key)
    pipeline.import_records(
        [
            record(
                entity_id,
                decision_time - timedelta(minutes=30),
                100000,
                available_at=decision_time + timedelta(days=1),
            )
        ],
        synthetic=True,
    )
    result = pipeline.backtest(
        [decision_time], decision_time + timedelta(days=4), synthetic=True, config=outcome_config()
    )["result"]
    snapshot = result["snapshots"][0]
    assert snapshot["selected"] == ["original-owner"]
    assert snapshot["universe"][0]["features"]["view_count"] == 200
    assert snapshot["entity_bindings"][entity_key] == "original-owner"


def test_historical_shared_family_survives_later_family_changes(pipeline):
    seed_product(pipeline, "family-one")
    seed_product(pipeline, "family-two")
    pipeline.set_family("family-one", "shared-original-family")
    pipeline.set_family("family-two", "shared-original-family")
    decision_time = now()
    time.sleep(0.01)
    pipeline.set_family("family-one", "separate-later-one")
    pipeline.set_family("family-two", "separate-later-two")
    result = pipeline.backtest(
        [decision_time],
        now() + timedelta(seconds=1),
        synthetic=True,
        config=outcome_config(top_k=2),
    )["result"]
    snapshot = result["snapshots"][0]
    assert snapshot["coverage"]["family_count"] == 1
    assert len(snapshot["selected"]) == 1
    assert {row["family_id"] for row in snapshot["universe"]} == {"shared-original-family"}


def test_capability_history_keeps_a_later_grant_out_of_an_earlier_decision(pipeline):
    seed_product(pipeline, "scoped-product", account_id="controlled-account")
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
        "VALUES('fixture_import','controlled-account','collect','denied','{}')"
    )
    decision_time = now()
    time.sleep(0.01)
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
        "VALUES('fixture_import','controlled-account','collect','available','{}')"
    )
    evaluation = pipeline.backtest(
        [decision_time], now() + timedelta(seconds=1), synthetic=True, config=outcome_config()
    )["result"]
    assert evaluation["snapshots"][0]["selected"] == []
    assert evaluation["snapshots"][0]["status"] == "insufficient_history"
    assert pipeline.capabilities()[0]["state"] == "available"


def test_invalidating_evidence_invalidates_saved_decisions_and_outcomes(pipeline):
    _, _, datasets = seed_product(pipeline, "retained-product")
    decision_time = now() + timedelta(seconds=1)
    frozen = pipeline.freeze(decision_time, synthetic=True, config=outcome_config())
    outcome = pipeline.mature(frozen["evaluation_id"], decision_time + timedelta(days=3))
    pipeline.catalog.invalidate_dataset(datasets[0], "source_retention_expired")
    evaluations = {row["evaluation_id"]: row for row in pipeline.evaluations(synthetic=True)}
    assert evaluations[frozen["evaluation_id"]]["invalidated"] is True
    assert evaluations[outcome["evaluation_id"]]["invalidated"] is True
    with pytest.raises(ValueError, match="No valid frozen decision"):
        pipeline.mature(frozen["evaluation_id"], decision_time + timedelta(days=4))


def test_normalization_replay_does_not_duplicate_or_change_backtest_evidence(pipeline):
    _, entity_key, datasets = seed_product(pipeline, "replayed-product")
    decision_time = now() + timedelta(seconds=1)
    before = pipeline.backtest(
        [decision_time], decision_time + timedelta(days=4), synthetic=True, config=outcome_config()
    )
    replay = pipeline.replay(datasets[0])
    after = pipeline.backtest(
        [decision_time], decision_time + timedelta(days=4), synthetic=True, config=outcome_config()
    )
    assert replay["records"] == 2
    assert len(pipeline.datasets()) == 2
    assert before["result"]["evaluation_id"] == after["result"]["evaluation_id"]
    assert before["evaluation_id"] == after["evaluation_id"]
    assert after["result"]["snapshots"][0]["entity_bindings"][entity_key] == "replayed-product"
    assert len(after["result"]["snapshots"][0]["evidence_refs"]) == 2


def test_live_forward_decisions_cannot_be_backdated_or_populated_by_synthetic_history(pipeline):
    seed_product(pipeline, "synthetic-product")
    with pytest.raises(ValueError, match="cannot be retrospectively backdated"):
        pipeline.freeze(now() - timedelta(days=1), synthetic=False, config=outcome_config())
    frozen = pipeline.freeze(now() + timedelta(seconds=1), synthetic=False, config=outcome_config())
    assert frozen["result"]["status"] == "insufficient_history"
    assert frozen["result"]["selected"] == []
    assert frozen["result"]["evidence_refs"] == []
