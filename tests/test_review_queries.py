"""Review list projections stay bounded and preserve workspace/identity rules."""

from datetime import timedelta

import pytest
from psycopg.types.json import Jsonb

from sm_trending_products.contracts import now

pytestmark = pytest.mark.integration


def test_review_lists_project_large_payloads_and_fetch_only_selected_details(pipeline):
    for synthetic, prefix in ((True, "test"), (False, "live")):
        run_id = f"{prefix}-run"
        pipeline.catalog.execute(
            "INSERT INTO assessment_runs(run_id,as_of,synthetic,policy,object_key,"
            "evidence_datasets) "
            "VALUES(%s,%s,%s,%s,%s,%s)",
            (run_id, now(), synthetic, Jsonb({}), f"{prefix}/result", Jsonb([])),
        )
        for index in range(3):
            product_id = f"{prefix}-{index}"
            pipeline.create_product(product_id, product_id, synthetic=synthetic)
            payload = {
                "product_id": product_id,
                "name": product_id,
                "score": 100 - index,
                "as_of": now().isoformat(),
                "policy_version": "v1",
                "features": {"large": "x" * 100_000},
                "evidence_refs": ["observation-1"] * 1000,
                "attention": {"score": 90, "status": "observed", "large": "x" * 100_000},
                "demand": {"score": None, "status": "unknown"},
            }
            pipeline.catalog.execute(
                "INSERT INTO current_assessments(product_id,run_id,synthetic,score,payload) "
                "VALUES(%s,%s,%s,%s,%s)",
                (product_id, run_id, synthetic, payload["score"], Jsonb(payload)),
            )
            pipeline.catalog.execute(
                "INSERT INTO alerts(alert_id,product_id,run_id,synthetic,payload) "
                "VALUES(%s,%s,%s,%s,%s)",
                (product_id, product_id, run_id, synthetic, Jsonb(payload)),
            )
            pipeline.catalog.execute(
                "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,payload) "
                "VALUES(%s,'forward',%s,%s)",
                (product_id, synthetic, Jsonb({"status": "pending", "large": "x" * 100_000})),
            )
    page = pipeline.assessment_summaries(True, limit=1, offset=1)
    assert len(page) == 1 and page[0]["product_id"] == "test-1"
    assert "features" not in page[0] and "evidence_refs" not in page[0]
    assert page[0]["attention"] == {"score": 90, "status": "observed"}
    assert pipeline.assessment("test-1", True)["features"]["large"] == "x" * 100_000
    assert pipeline.assessment("test-1", False) is None
    evaluations = pipeline.evaluation_summaries(True, limit=1, offset=1)
    assert len(evaluations) == 1 and evaluations[0]["evaluation_id"].startswith("test-")
    assert "payload" not in evaluations[0]
    assert pipeline.evaluation(evaluations[0]["evaluation_id"], True)["payload"]["large"]
    assert pipeline.evaluation(evaluations[0]["evaluation_id"], False) is None
    alerts = pipeline.alert_summaries(True, limit=1)
    assert len(alerts) == 1 and "payload" not in alerts[0]
    assert pipeline.alert(alerts[0]["alert_id"], True)["payload"]["features"]
    assert pipeline.alert(alerts[0]["alert_id"], False) is None
    for reader in (
        pipeline.assessment_summaries,
        pipeline.evaluation_summaries,
        pipeline.alert_summaries,
    ):
        with pytest.raises(ValueError, match="Review pages"):
            reader(True, limit=201)
        with pytest.raises(ValueError, match="Review pages"):
            reader(True, offset=-1)
    pipeline.catalog.execute("UPDATE assessment_runs SET invalidated=true WHERE run_id='test-run'")
    assert pipeline.assessment_summaries(True) == []
    assert pipeline.assessment("test-1", True) is None
    assert pipeline.assessment_summaries(False)


def test_product_link_page_resolves_transfers_before_filtering_product(pipeline):
    pipeline.create_product("first", "First", synthetic=True)
    pipeline.create_product("second", "Second", synthetic=True)
    for index in range(3):
        key = f"tiktok_display:video:review-{index}"
        pipeline.catalog.execute(
            "INSERT INTO entities(entity_key,synthetic,source,entity_type,title,attributes) "
            "VALUES(%s,true,'tiktok_display','video','Review',%s)",
            (key, Jsonb({})),
        )
        pipeline.review_link("first", key)
    original_cutoff = now()
    pipeline.review_link("second", "tiktok_display:video:review-0")
    cutoff = now() + timedelta(seconds=1)
    page = pipeline.product_links("first", cutoff, True, limit=1)
    assert len(page) == 1
    # The old owner is explicitly rejected on transfer, rather than resurrected.
    assert not any(
        row["entity_key"] == "tiktok_display:video:review-0" and row["status"] == "confirmed"
        for row in pipeline.product_links("first", cutoff, True)
    )
    assert [row["product_id"] for row in pipeline.product_links("second", cutoff, True)] == [
        "second"
    ]
    assert len(pipeline.product_links("first", original_cutoff, True)) == 3
    assert pipeline.product_links("first", cutoff, False) == []
    assert pipeline.product_links("first", cutoff, True, limit=1, offset=1) != page


def test_status_reads_capabilities_once(pipeline, monkeypatch):
    calls = []

    def capabilities():
        calls.append(True)
        return [{"state": "available"}]

    monkeypatch.setattr(pipeline, "capabilities", capabilities)
    status = pipeline.status()
    assert calls == [True]
    assert status["capabilities"] == [{"state": "available"}]
    assert status["pending_live_acceptance"] is False
