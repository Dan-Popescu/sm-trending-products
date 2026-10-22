"""Frozen decisions and temporal replay through the real persistence facade."""

from __future__ import annotations

import time
from datetime import timedelta

import pytest

from sm_trending_products.contracts import Product, ProductLink, now
from sm_trending_products.evaluation import EvaluationConfig, freeze_decision

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


def test_prepared_history_adequacy_matches_raw_decisions_with_metadata_revisions(pipeline):
    cutoff = now() + timedelta(minutes=1)
    entity_id, entity_key, _ = seed_product(pipeline, "revision-history", base=cutoff)
    config = outcome_config(minimum_history_observations=3)
    original = pipeline.freeze(cutoff, synthetic=True, config=config)["result"]
    assert original["status"] == "insufficient_history"

    revisions = []
    for hours, views in ((2, 100), (1, 200)):
        row = record(
            entity_id,
            cutoff - timedelta(hours=hours),
            views,
            available_at=cutoff - timedelta(seconds=1),
        )
        row["observation_id"] += "-normalizer-v2"
        row["attributes"]["normalizer_version"] = "2"
        revisions.append(row)
    pipeline.import_records(revisions, synthetic=True)
    assert pipeline.freeze(cutoff, synthetic=True, config=config)["result"] == original

    late = record(
        entity_id,
        cutoff - timedelta(minutes=30),
        300,
        available_at=cutoff + timedelta(days=1),
    )
    pipeline.import_records([late], synthetic=True)
    assert pipeline.freeze(cutoff, synthetic=True, config=config)["result"] == original
    genuine = {
        **late,
        "observation_id": "genuine-third-known-time",
        "available_at": (cutoff - timedelta(microseconds=1)).isoformat(),
    }
    pipeline.import_records([genuine], synthetic=True)
    persisted = pipeline.freeze(cutoff, synthetic=True, config=config)["result"]
    evidence, _ = pipeline.history.observations(cutoff, synthetic=True, entity_keys={entity_key})
    direct = freeze_decision(
        evidence,
        [Product.model_validate(row) for row in pipeline.products(synthetic=True)],
        [ProductLink.model_validate(row) for row in pipeline.links(cutoff, synthetic=True)],
        cutoff,
        EvaluationConfig.model_validate(config),
        synthetic=True,
    )
    assert persisted == direct
    assert persisted["status"] == "frozen"
    assert persisted["selected"] == ["revision-history"]
    assert set(persisted["evidence_refs"]) == {row.observation_id for row in evidence}
    assert len(persisted["evidence_refs"]) == 5
    assert late["observation_id"] not in persisted["evidence_refs"]
    assert set(persisted["universe"][0]["attention"]["evidence_refs"]) == {
        row["observation_id"] for row in [*revisions, genuine]
    }
    assert persisted["universe"][0]["features"]["view_count"] == 300
    assert persisted["config"]["minimum_history_observations"] == 3
    assert persisted["policy"]["min_observations"] == 2


def test_live_forward_decisions_cannot_be_backdated_or_populated_by_synthetic_history(pipeline):
    seed_product(pipeline, "synthetic-product")
    with pytest.raises(ValueError, match="cannot be retrospectively backdated"):
        pipeline.freeze(now() - timedelta(days=1), synthetic=False, config=outcome_config())
    frozen = pipeline.freeze(now() + timedelta(seconds=1), synthetic=False, config=outcome_config())
    assert frozen["result"]["status"] == "insufficient_history"
    assert frozen["result"]["selected"] == []
    assert frozen["result"]["evidence_refs"] == []


def test_evaluation_partitions_ignore_unrelated_history_under_a_small_row_budget(pipeline):
    pipeline.settings.analysis_max_rows_per_batch = 100
    pipeline.settings.analysis_product_batch_size = 100
    cutoff = now() + timedelta(minutes=1)
    records = []
    for product in range(3):
        entity = f"partition-video-{product}"
        records.extend(
            record(entity, cutoff - timedelta(hours=39 - index), 100 + index * (product + 1))
            for index in range(40)
        )
        records.append(record(entity, cutoff + timedelta(days=3), 2100 + 39 * (product + 1)))
    records.extend(
        record("unrelated-unlinked-video", cutoff - timedelta(minutes=index), 1000 - index)
        for index in range(200)
    )
    pipeline.import_records(records, synthetic=True)
    for product in range(3):
        product_id = f"partition-product-{product}"
        pipeline.create_product(product_id, product_id, synthetic=True)
        pipeline.review_link(product_id, f"fixture_import:video:partition-video-{product}")
    published = pipeline.assess(cutoff, synthetic=True)
    frozen = pipeline.freeze(cutoff, synthetic=True, config=outcome_config())
    assert len(published["rows"]) == 3
    assert {row["product_id"]: row["score"] for row in published["rows"]} == {
        row["product_id"]: row["score"] for row in frozen["result"]["universe"]
    }
    assert len(frozen["result"]["universe"]) == 3
    assert frozen["result"]["selected"] == ["partition-product-2"]
    assert len(frozen["result"]["evidence_refs"]) == 120
    matured = pipeline.mature(frozen["evaluation_id"], cutoff + timedelta(days=4))
    assert matured["result"]["scenarios"][0]["methods"]["policy"]["precision_at_k"] == 1
    replayed = pipeline.backtest(
        [cutoff], cutoff + timedelta(days=4), synthetic=True, config=outcome_config()
    )
    assert replayed["result"]["snapshots"][0] == frozen["result"]
    assert replayed["result"]["reports"][0] == matured["result"]
    assert sum(row["row_count"] for row in pipeline.datasets()) > 300


def test_durable_maturation_frontier_advances_old_decisions_beyond_one_hundred(pipeline):
    cutoff = now() + timedelta(minutes=1)
    decisions = [
        pipeline.freeze(cutoff + timedelta(seconds=index), synthetic=True, config=outcome_config())[
            "evaluation_id"
        ]
        for index in range(105)
    ]
    first = pipeline.mature_due(limit=100, synthetic=True, as_of=cutoff + timedelta(days=1))
    assert first["errors"] == []
    assert first["completed"] == 100
    assert first["decisions"] == decisions[:100]
    second = pipeline.mature_due(limit=100, synthetic=True, as_of=cutoff + timedelta(days=1))
    assert second["errors"] == []
    assert second["completed"] == 5
    assert second["decisions"] == decisions[100:]
    assert (
        pipeline.mature_due(limit=100, synthetic=True, as_of=cutoff + timedelta(days=1))[
            "processed"
        ]
        == 0
    )


def test_missing_outcomes_retry_until_the_frozen_boundary_grace_then_finish_censored(pipeline):
    seed_product(pipeline, "missing-outcome-product")
    cutoff = now() + timedelta(seconds=1)
    frozen = pipeline.freeze(cutoff, synthetic=True, config=outcome_config())
    first = pipeline.mature_due(limit=10, synthetic=True, as_of=cutoff + timedelta(days=3))
    assert first["errors"] == []
    assert first["pending"] == 1
    assert first["completed"] == 0
    assert first["decisions"] == [frozen["evaluation_id"]]
    assert (
        pipeline.mature_due(limit=10, synthetic=True, as_of=cutoff + timedelta(days=3, minutes=30))[
            "processed"
        ]
        == 0
    )
    finished = pipeline.mature_due(
        limit=10, synthetic=True, as_of=cutoff + timedelta(days=4, hours=1)
    )
    assert finished["errors"] == []
    assert finished["completed"] == 1
    evaluations = {row["evaluation_id"]: row for row in pipeline.evaluations(synthetic=True)}
    report = evaluations[finished["evaluations"][0]]["payload"]
    assert report["scenarios"][0]["methods"]["policy"]["status_counts"] == {"censored": 1}
    assert (
        pipeline.mature_due(limit=10, synthetic=True, as_of=cutoff + timedelta(days=5))["processed"]
        == 0
    )


def test_native_postgres_claims_forward_frontier_without_duplicate_workers(pipeline):
    import os
    from concurrent.futures import ThreadPoolExecutor

    from sm_trending_products.pipeline import Pipeline

    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Forward SKIP LOCKED concurrency requires native PostgreSQL CI")
    cutoff = now() + timedelta(minutes=1)
    decisions = {
        pipeline.freeze(cutoff + timedelta(seconds=index), synthetic=True, config=outcome_config())[
            "evaluation_id"
        ]
        for index in range(20)
    }

    def run_worker():
        with Pipeline(pipeline.settings) as worker:
            return worker.mature_due(limit=10, synthetic=True, as_of=cutoff + timedelta(days=1))

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = list(executor.map(lambda _: run_worker(), range(2)))
    assert first["errors"] == second["errors"] == []
    assert set(first["decisions"]).isdisjoint(second["decisions"])
    assert set(first["decisions"]) | set(second["decisions"]) == decisions
