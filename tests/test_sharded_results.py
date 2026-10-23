"""Compact publication boundaries, paged audit, fencing and legacy integrity."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path

import pytest
from analytical_results import backtest_reports, backtest_snapshots, reference_ids
from test_evaluation_delivery import outcome_config, seed_product

import sm_trending_products.provenance as provenance_module
from sm_trending_products.contracts import Observation, canonical, now, utc
from sm_trending_products.evaluation import mature_decision
from sm_trending_products.pipeline import Pipeline


def seed(pipeline, *, products=1, videos=2, points=4):
    cutoff = now() + timedelta(minutes=1)
    records = []
    for product in range(products):
        pipeline.create_product(f"p{product}", f"Product {product}", synthetic=True)
        for video in range(videos):
            entity = f"p{product}-v{video}"
            for point in range(points):
                observed = cutoff - timedelta(hours=6 * (points - point - 1))
                records.append(
                    {
                        "observation_id": hashlib.sha256(f"{entity}-{point}".encode()).hexdigest(),
                        "source": "provenance_fixture",
                        "entity_type": "video",
                        "entity_id": entity,
                        "observed_at": observed.isoformat(),
                        "available_at": observed.isoformat(),
                        "synthetic": True,
                        "metrics": {"view_count": {"lower": point * 100, "upper": point * 100}},
                        "attributes": {"creator_id": entity},
                    }
                )
    datasets = pipeline.import_records(records, synthetic=True)
    for product in range(products):
        for video in range(videos):
            pipeline.review_link(f"p{product}", f"provenance_fixture:video:p{product}-v{video}")
    return cutoff, records, datasets


@pytest.mark.parametrize("target", ["attention", "profit"])
def test_legacy_snapshot_json_configuration_and_whole_report_hashes_are_unchanged(target):
    fixture = json.loads((Path(__file__).parent / "fixtures/legacy_evaluation_v1.json").read_text())
    case = next(case for case in fixture["cases"] if case["target"] == target)
    snapshot = case["snapshot"]
    original = canonical(snapshot)
    rows = [Observation.model_validate(row) for row in fixture["observations"]]
    report = mature_decision(snapshot, rows, utc(fixture["end"]))
    assert canonical(snapshot) == original == case["snapshot_bytes"]
    assert canonical(snapshot["config"]) == case["config_bytes"]
    assert report == case["report"]
    assert report["report_id"] == case["report"]["report_id"]
    assert "profit_contract" not in snapshot["config"]


@pytest.mark.integration
def test_large_reference_scopes_are_bounded_and_inspectable_after_restart(pipeline, monkeypatch):
    cutoff, records, _datasets = seed(pipeline, videos=20, points=120)
    published = pipeline.assess(cutoff, True)
    row = published["rows"][0]
    assert published["schema_version"] == "2"
    assert row["evidence_refs"]["count"] == 2400
    assert len(canonical(row).encode()) < 6000
    assert len(canonical(published).encode()) < 8000
    assert reference_ids(pipeline, row["evidence_refs"]) == sorted(
        record["observation_id"] for record in records
    )
    nodes = pipeline.catalog.all(
        "SELECT object_key,object_bytes,object_sha256 FROM derived_publications "
        "WHERE object_key LIKE 'provenance/%%'"
    )
    assert nodes and max(node["object_bytes"] for node in nodes) <= provenance_module.MAX_NODE_BYTES
    assert all(node["object_sha256"] and node["object_bytes"] for node in nodes)
    with Pipeline(pipeline.settings) as reopened:
        pinned = []
        original = provenance_module.pin_cached

        def counted(history, descriptor):
            pinned.append(descriptor["key"])
            return original(history, descriptor)

        monkeypatch.setattr(provenance_module, "pin_cached", counted)
        page = reopened.analytical_page("assessment", "p0", True, limit=25, offset=100)
        assert len(page["references"]) == 25
        assert page["count"] == 2400
        assert len(pinned) < len(nodes) // 2
        assert reopened.assessment("p0", True) == row
        with pytest.raises(ValueError, match="selected workspace"):
            reopened.analytical_page("assessment", "p0", False)


@pytest.mark.integration
def test_unchanged_provenance_reuses_nodes_and_an_append_rewrites_one_day_path(pipeline):
    cutoff, _records, _datasets = seed(pipeline, videos=20, points=120)
    first = pipeline.assess(cutoff, True)
    before = pipeline.history.metrics["put_requests"]
    second = pipeline.assess(cutoff + timedelta(minutes=1), True)
    # Only the new, compact assessment publication is uploaded.
    assert pipeline.history.metrics["put_requests"] - before == 1
    assert first["rows"][0]["evidence_refs"] == second["rows"][0]["evidence_refs"]
    new_cutoff = cutoff + timedelta(hours=1)
    pipeline.import_records(
        [
            {
                "observation_id": f"next-wave-{video}",
                "source": "provenance_fixture",
                "entity_type": "video",
                "entity_id": f"p0-v{video}",
                "observed_at": new_cutoff.isoformat(),
                "available_at": new_cutoff.isoformat(),
                "synthetic": True,
                "metrics": {"view_count": {"lower": 12100, "upper": 12100}},
            }
            for video in range(20)
        ],
        synthetic=True,
    )
    old_nodes = pipeline.catalog.one(
        "SELECT count(*) AS count FROM derived_publications WHERE object_key LIKE 'provenance/%%'"
    )["count"]
    updated = pipeline.assess(new_cutoff, True)
    new_nodes = pipeline.catalog.one(
        "SELECT count(*) AS count FROM derived_publications WHERE object_key LIKE 'provenance/%%'"
    )["count"]
    assert updated["rows"][0]["evidence_refs"]["count"] == 2420
    assert 1 <= new_nodes - old_nodes <= 4


@pytest.mark.integration
def test_record_lookup_and_nested_backtest_paths_do_not_hydrate_the_universe(pipeline, monkeypatch):
    cutoff, _records, _datasets = seed(pipeline, products=3, videos=2, points=4)
    forward = pipeline.freeze(cutoff, True, {"horizon_days": [3], "launch_delay_days": [0]})
    report = pipeline.mature(forward["evaluation_id"], cutoff + timedelta(days=3))
    tested = pipeline.backtest(
        [cutoff],
        cutoff + timedelta(days=3),
        True,
        {"horizon_days": [3], "launch_delay_days": [0]},
    )
    assert tested["result"]["schema_version"] == "2"
    assert backtest_snapshots(pipeline, tested["result"]) == [forward["result"]]
    assert backtest_reports(pipeline, tested["result"]) == [report["result"]]
    snapshot_id = forward["result"]["snapshot_id"]
    product = pipeline.analytical_record(
        "evaluation",
        tested["evaluation_id"],
        "p1",
        True,
        path=("snapshots", snapshot_id, "universe"),
    )
    assert product["product_id"] == "p1"
    page = pipeline.analytical_page(
        "evaluation",
        tested["evaluation_id"],
        True,
        path=("snapshots", snapshot_id, "universe", "p1", "evidence_refs"),
        limit=3,
    )
    assert page["count"] == 8 and len(page["references"]) == 3
    # A separate larger record index proves direct lookup opens only its route.
    rows = pipeline.provenance.records(
        {f"product-{i}": {"product_id": i} for i in range(1000)}, True
    )
    pinned = []
    original = provenance_module.pin_cached
    monkeypatch.setattr(
        provenance_module,
        "pin_cached",
        lambda history, descriptor: (
            pinned.append(descriptor["key"]) or original(history, descriptor)
        ),
    )
    assert pipeline.provenance.record(rows, "product-777") == {"product_id": 777}
    assert len(pinned) <= 4


@pytest.mark.integration
@pytest.mark.parametrize("policy_retirement", [False, True])
def test_source_invalidation_closes_shared_artifacts_and_blocks_cached_pages(
    pipeline, policy_retirement
):
    cutoff, _records, datasets = seed(pipeline, products=2)
    published = pipeline.assess(cutoff, True)
    frozen = pipeline.freeze(cutoff, True)
    tested = pipeline.backtest([cutoff], cutoff, True)
    root = published["rows"][0]["evidence_refs"]
    pipeline.provenance.page(root, limit=2)
    if policy_retirement:
        pipeline.catalog.execute(
            "INSERT INTO source_policies(source,retention_days,verified) "
            "VALUES('provenance_fixture',1,true)"
        )
        pipeline.catalog.execute(
            "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' "
            "WHERE dataset_id=ANY(%s)",
            (datasets,),
        )
        assert pipeline.retention(apply=True)["pending_objects"] == 0
    else:
        pipeline.catalog.invalidate_dataset(datasets[0], "source_deleted")
    assert pipeline.assessment("p0", True) is None
    assert pipeline.evaluation(frozen["evaluation_id"], True)["invalidated"]
    assert pipeline.evaluation(tested["evaluation_id"], True)["invalidated"]
    with pytest.raises(ValueError, match="invalidated"):
        pipeline.provenance.page(root)


@pytest.mark.integration
def test_retirement_between_artifact_staging_and_upload_cannot_resurrect_provenance(
    pipeline, monkeypatch
):
    cutoff, _records, datasets = seed(pipeline)
    observations, _manifests = pipeline.history.observations(cutoff, True)
    put = pipeline.history.put
    retired = False

    def intercepted(key, body, **kwargs):
        nonlocal retired
        if key.startswith("provenance/") and not retired:
            retired = True
            pipeline.catalog.invalidate_dataset(datasets[0], "removed_during_publication")
        return put(key, body, **kwargs)

    monkeypatch.setattr(pipeline.history, "put", intercepted)
    with pytest.raises(ValueError, match="unavailable|invalidated"):
        pipeline.provenance.compact(
            {"evidence_refs": [row.observation_id for row in observations]}, observations, True
        )
    assert retired
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS count FROM derived_publications WHERE state='committed'"
        )["count"]
        == 0
    )


@pytest.mark.integration
def test_invalidating_a_compacted_parent_hides_children_and_restores_fresh_original(pipeline):
    cutoff = now() + timedelta(minutes=1)
    datasets = []
    for point in range(2):
        datasets.extend(
            pipeline.import_records(
                [
                    {
                        "source": "provenance_fixture",
                        "entity_type": "video",
                        "entity_id": "shared",
                        "observation_id": f"point-{point}",
                        "synthetic": True,
                        "observed_at": (cutoff - timedelta(hours=2 - point)).isoformat(),
                        "available_at": (cutoff - timedelta(hours=2 - point)).isoformat(),
                        "metrics": {"view_count": {"lower": point * 100, "upper": point * 100}},
                    }
                ],
                synthetic=True,
            )
        )
    pipeline.create_product("p", "Product", synthetic=True)
    pipeline.review_link("p", "provenance_fixture:video:shared")
    child = pipeline.compact(True)["dataset_id"]
    pipeline.assess(cutoff, True)
    pipeline.catalog.invalidate_dataset(datasets[0], "source_deleted")
    assert (
        pipeline.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (child,))["state"]
        == "invalidated"
    )
    updated = pipeline.assess(cutoff, True)
    assert updated["rows"][0]["coverage"]["observations"] == 1
    assert reference_ids(pipeline, updated["rows"][0]["evidence_refs"]) == ["point-1"]
    with pytest.raises(ValueError, match="completed|committed|invalidated"):
        pipeline.replay(child)


@pytest.mark.integration
def test_invalidating_latest_incomplete_ledger_invalidates_censored_outcome(pipeline):
    decision = now() + timedelta(minutes=1)
    end = decision + timedelta(days=3)
    seed_product(pipeline, "profit", base=decision)

    def ledger(identifier, observed, available, attributes):
        return {
            "observation_id": identifier,
            "source": "cost_import",
            "entity_type": "cost",
            "entity_id": "ledger",
            "synthetic": True,
            "observed_at": observed.isoformat(),
            "available_at": available.isoformat(),
            "attributes": attributes,
        }

    pipeline.import_records(
        [
            ledger(
                "quote",
                decision - timedelta(hours=1),
                decision - timedelta(hours=1),
                {"currency": "EUR"},
            )
        ],
        synthetic=True,
    )
    pipeline.review_link("profit", "cost_import:cost:ledger")
    frozen = pipeline.freeze(
        decision,
        True,
        outcome_config(
            target="profit",
            currency="EUR",
            threshold=30,
            profit_contract={
                "version": "realized-profit-v1",
                "ledgers": {"cost_import:cost:ledger": ["goods", "fees"]},
            },
        ),
    )
    attrs = {
        "period_start": decision.isoformat(),
        "period_end": end.isoformat(),
        "currency": "EUR",
        "realized": True,
        "costs_complete": True,
        "realized_revenue": "100.00",
        "realized_costs": {"goods": "40.00", "fees": "20.00"},
    }
    pipeline.import_records(
        [ledger("complete", end, end + timedelta(minutes=1), attrs)], synthetic=True
    )
    successful = pipeline.mature(frozen["evaluation_id"], end + timedelta(minutes=2))
    assert successful["result"]["scenarios"][0]["methods"]["policy"]["status_counts"] == {
        "success": 1
    }
    correction = pipeline.import_records(
        [
            ledger(
                "incomplete",
                end,
                end + timedelta(hours=1),
                {**attrs, "realized_costs": {"goods": "40.00"}},
            )
        ],
        synthetic=True,
    )[0]
    censored = pipeline.mature(frozen["evaluation_id"], end + timedelta(hours=2))
    assert censored["result"]["scenarios"][0]["methods"]["policy"]["status_counts"] == {
        "censored": 1
    }
    pipeline.catalog.invalidate_dataset(correction, "independent-invalid-input")
    state = pipeline.catalog.one(
        "SELECT invalidated FROM evaluation_runs WHERE evaluation_id=%s",
        (censored["evaluation_id"],),
    )
    assert state["invalidated"] is True, "Censored report must depend on its incomplete correction"
    assert (
        pipeline.catalog.one(
            "SELECT invalidated FROM evaluation_runs WHERE evaluation_id=%s",
            (successful["evaluation_id"],),
        )["invalidated"]
        is False
    )


@pytest.mark.integration
def test_deleted_artifact_generations_leave_only_current_source_removal_work(pipeline):
    timestamp = now()

    def record(identifier):
        return {
            "observation_id": identifier,
            "source": "provenance_fixture",
            "entity_type": "video",
            "entity_id": identifier,
            "observed_at": timestamp.isoformat(),
            "available_at": timestamp.isoformat(),
            "synthetic": True,
            "metrics": {"view_count": {"lower": 1, "upper": 1}},
        }

    first = pipeline.import_records([record(f"a-{i}") for i in range(4)], synthetic=True)[0]
    second = pipeline.import_records([record("b")], synthetic=True)[0]
    current = pipeline.provenance.references(["b"], {"b": second}, True)
    for index in range(4):
        identifier = f"a-{index}"
        other = pipeline.provenance.references([identifier], {identifier: first}, True)
        current = pipeline.provenance.combine([current, other], True)
    pipeline.catalog.execute(
        "INSERT INTO source_policies(source,retention_days,verified) "
        "VALUES('provenance_fixture',1,true)"
    )
    pipeline.catalog.execute(
        "UPDATE datasets SET created_at=clock_timestamp()-interval '2 days' WHERE dataset_id=%s",
        (first,),
    )
    assert pipeline.retention(apply=True)["pending_objects"] == 0
    assert pipeline.catalog.all(
        "SELECT state,count(*) AS count FROM derived_publications GROUP BY state ORDER BY state"
    ) == [{"state": "committed", "count": 1}, {"state": "deleted", "count": 8}]
    assert (
        pipeline.catalog.all(
            "SELECT object_key FROM analytical_artifact_ancestors(ARRAY[%s])", (first,)
        )
        == []
    )
    remaining = pipeline.catalog.all(
        "SELECT object_key FROM analytical_artifact_ancestors(ARRAY[%s])", (second,)
    )
    assert len(remaining) == 1
    pipeline.catalog.invalidate_dataset(second, "later-source-removal")
    assert pipeline.catalog.all(
        "SELECT state,count(*) AS count FROM derived_publications GROUP BY state ORDER BY state"
    ) == [{"state": "deleted", "count": 8}, {"state": "invalidated", "count": 1}]
