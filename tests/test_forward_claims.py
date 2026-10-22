"""Native frontier recovery must not block healthy work or cross workspaces."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from test_evaluation_integration import outcome_config

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def dirty_decision(pipeline, cutoff, synthetic, kind):
    identifier = pipeline.freeze(cutoff, synthetic, outcome_config())["evaluation_id"]
    if kind in {"expired", "active"}:
        pipeline.catalog.execute(
            "UPDATE forward_frontier SET state='running',lease_token='original-owner',"
            "lease_until=clock_timestamp()+%s WHERE evaluation_id=%s",
            (timedelta(minutes=-1 if kind == "expired" else 60), identifier),
        )
    else:
        pipeline.catalog.execute(
            "UPDATE evaluation_runs SET invalidated=true WHERE evaluation_id=%s", (identifier,)
        )
    return identifier


@pytest.mark.parametrize("synthetic", [False, True])
@pytest.mark.parametrize("kind", ["expired", "invalidated", "active"])
def test_forward_cleanup_only_changes_the_requested_workspace(pipeline, synthetic, kind):
    cutoff = now() + timedelta(seconds=1)
    other = dirty_decision(pipeline, cutoff, not synthetic, kind)
    ready = pipeline.freeze(cutoff, synthetic, outcome_config())["evaluation_id"]
    before = pipeline.catalog.one("SELECT * FROM forward_frontier WHERE evaluation_id=%s", (other,))
    result = pipeline.mature_due(limit=1, synthetic=synthetic, as_of=cutoff + timedelta(days=1))
    assert result["errors"] == [] and result["decisions"] == [ready]
    assert (
        pipeline.catalog.one("SELECT * FROM forward_frontier WHERE evaluation_id=%s", (other,))
        == before
    )
    recovered = pipeline.mature_due(
        limit=1, synthetic=not synthetic, as_of=cutoff + timedelta(days=1)
    )
    assert recovered["errors"] == []
    assert recovered["processed"] == (1 if kind == "expired" else 0)
    assert (
        pipeline.catalog.one("SELECT state FROM forward_frontier WHERE evaluation_id=%s", (other,))[
            "state"
        ]
        == {"expired": "complete", "invalidated": "invalidated", "active": "running"}[kind]
    )


@pytest.mark.parametrize("kind", ["expired", "invalidated"])
def test_forward_claim_skips_locked_cleanup_and_completes_another_due_decision(pipeline, kind):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Independent frontier locks require native PostgreSQL")
    cutoff = now() + timedelta(seconds=1)
    blocked = dirty_decision(pipeline, cutoff, True, kind)
    ready = pipeline.freeze(cutoff + timedelta(seconds=1), True, outcome_config())["evaluation_id"]

    def run_worker():
        with Pipeline(pipeline.settings) as worker:
            return worker.mature_due(limit=1, synthetic=True, as_of=cutoff + timedelta(days=1))

    with ThreadPoolExecutor(max_workers=1) as executor:
        with pipeline.catalog.transaction() as connection:
            connection.execute(
                "SELECT evaluation_id FROM forward_frontier WHERE evaluation_id=%s FOR UPDATE",
                (blocked,),
            )
            future = executor.submit(run_worker)
            completed = future.result(timeout=2)
            assert completed["errors"] == [] and completed["decisions"] == [ready]
    recovered = pipeline.mature_due(limit=1, synthetic=True, as_of=cutoff + timedelta(days=1))
    assert recovered["errors"] == []
    assert recovered["processed"] == (1 if kind == "expired" else 0)
