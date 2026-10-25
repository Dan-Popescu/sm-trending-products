"""Current rows and frozen alerts publish atomically through bounded SQL batches."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
from threading import Event
from time import monotonic, sleep

import psycopg
import pytest
from test_assessment_archive import logical_body

from sm_trending_products.assessment_publisher import AssessmentPublisher
from sm_trending_products.contracts import AssessmentPolicy, canonical, now
from sm_trending_products.provenance import PublicationScope

pytestmark = pytest.mark.integration


def stage(pipeline, count=3, *, cutoff=None, policy=None):
    body = logical_body(count, cutoff=cutoff)
    body["policy"] = (policy or AssessmentPolicy(alert_threshold=0)).model_dump(mode="json")
    for row in body["rows"]:
        pipeline.create_product(row["product_id"], row["name"], synthetic=True)
    return pipeline._assessment_archive.stage(body, PublicationScope([], ()))


def publisher(pipeline):
    return AssessmentPublisher(pipeline.catalog, pipeline._assessment_archive)


def test_publisher_preserves_all_rows_exact_archive_and_alerts_across_large_batches(pipeline):
    publication = stage(pipeline, 425)
    original = canonical(publication.body)
    publisher(pipeline).publish(publication)
    assert canonical(publication.body) == original
    assert pipeline.latest_assessments(True) == publication.body["rows"]
    assert (
        pipeline.assessment_snapshot(publication.run_id, True)["rows"] == publication.body["rows"]
    )
    alerts = pipeline.catalog.all("SELECT product_id,payload FROM alerts")
    assert {row["product_id"]: row["payload"] for row in alerts} == {
        row["product_id"]: row for row in publication.body["rows"]
    }
    publisher(pipeline).publish(publication)
    assert pipeline.catalog.one("SELECT count(*) AS n FROM alerts")["n"] == 425


def test_old_run_keeps_current_winner_but_retains_its_existing_alert_semantics(pipeline):
    newer = stage(pipeline, cutoff=now(), policy=AssessmentPolicy(alert_threshold=100))
    older = stage(pipeline, cutoff=newer.body["as_of"] - timedelta(hours=1))
    publisher(pipeline).publish(newer)
    publisher(pipeline).publish(older)
    assert pipeline.assessment("product-000002", True) == newer.body["rows"][0]
    assert {row["run_id"] for row in pipeline.alerts(True)} == {older.run_id}


def test_failure_rolls_back_current_alerts_and_archive_commit_together(pipeline):
    publication = stage(pipeline)
    pipeline.catalog.execute("""
        CREATE FUNCTION reject_alert_batch() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'controlled_alert_failure'; END $$;
        CREATE TRIGGER reject_alert_batch BEFORE INSERT ON alerts
        FOR EACH ROW EXECUTE FUNCTION reject_alert_batch();
    """)
    with pytest.raises(psycopg.errors.RaiseException, match="controlled_alert_failure"):
        publisher(pipeline).publish(publication)
    assert pipeline.catalog.all("SELECT * FROM current_assessments") == []
    assert pipeline.catalog.all("SELECT * FROM alerts") == []
    assert pipeline.catalog.all("SELECT * FROM assessment_runs") == []
    assert (
        pipeline.catalog.one(
            "SELECT state FROM derived_publications WHERE object_key=%s",
            (publication.descriptor["key"],),
        )["state"]
        == "staged"
    )


def test_concurrent_first_publications_keep_newest_cutoff_and_one_alert_per_product(
    pipeline, monkeypatch
):
    older = stage(pipeline, 2)
    newer = stage(pipeline, 2, cutoff=older.body["as_of"] + timedelta(minutes=1))
    entered, release = Event(), Event()
    waiting, waiter = Event(), []
    original = AssessmentPublisher._current
    execute = psycopg.Connection.execute

    def observe_wait(connection, query, params=None, **kwargs):
        if (
            entered.is_set()
            and isinstance(query, str)
            and query.startswith("SELECT product_id FROM products")
        ):
            waiter.append(connection.info.backend_pid)
            waiting.set()
        return execute(connection, query, params, **kwargs)

    def paused(connection, publication, rows):
        result = original(connection, publication, rows)
        if publication.run_id == older.run_id:
            entered.set()
            assert release.wait(10)
        return result

    monkeypatch.setattr(AssessmentPublisher, "_current", staticmethod(paused))
    monkeypatch.setattr(psycopg.Connection, "execute", observe_wait)
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(publisher(pipeline).publish, older)
        try:
            assert entered.wait(5)
            second = workers.submit(publisher(pipeline).publish, newer)
            assert waiting.wait(5)
            deadline = monotonic() + 5
            while not pipeline.catalog.one(
                "SELECT cardinality(pg_blocking_pids(%s)) AS n", (waiter[0],)
            )["n"]:
                assert monotonic() < deadline
                sleep(0.01)
        finally:
            release.set()
        first.result(timeout=10)
        second.result(timeout=10)
    assert pipeline.latest_assessments(True) == newer.body["rows"]
    assert len(pipeline.alerts(True)) == 2


def test_reversed_rank_orders_across_batches_publish_without_deadlock(pipeline):
    older = stage(pipeline, 425)
    body = deepcopy(older.body)
    body["as_of"] += timedelta(minutes=1)
    body["rows"].reverse()
    for index, row in enumerate(body["rows"]):
        row["score"] = 1000 - index
        row["as_of"] = body["as_of"].isoformat()
    newer = pipeline._assessment_archive.stage(body, PublicationScope([], ()))
    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(publisher(pipeline).publish, value) for value in (older, newer)]
        for future in futures:
            future.result(timeout=15)
    assert pipeline.latest_assessments(True) == body["rows"]
    assert len(pipeline.alerts(True)) == 425


def test_payload_byte_bound_and_existing_current_redaction_are_preserved(pipeline, monkeypatch):
    original = stage(pipeline, 12)
    body = deepcopy(original.body)
    body["as_of"] += timedelta(minutes=1)
    for row in body["rows"]:
        row["unknown_future_field"] = "x" * 100000
        row["token"] = "synthetic-field-for-redaction-parity"
    publication = pipeline._assessment_archive.stage(body, PublicationScope([], ()))
    execute = psycopg.Connection.execute
    sizes = []

    def counted(connection, query, params=None, **kwargs):
        if isinstance(query, str) and query.startswith("INSERT INTO current_assessments"):
            sizes.append(sum(len(params[index].encode()) for index in range(4, len(params) - 1, 5)))
        return execute(connection, query, params, **kwargs)

    monkeypatch.setattr(psycopg.Connection, "execute", counted)
    publisher(pipeline).publish(publication)
    assert len(sizes) == 2 and max(sizes) <= 1024 * 1024
    assert all(row["token"] == "[REDACTED]" for row in pipeline.latest_assessments(True))
    assert all(
        row["payload"]["token"] == "synthetic-field-for-redaction-parity"
        for row in pipeline.alerts(True)
    )
    assert pipeline.assessment_snapshot(publication.run_id, True)["rows"] == body["rows"]


def test_empty_current_assessment_still_publishes_its_exact_archive(pipeline):
    result = pipeline.assess(synthetic=True)
    assert result["rows"] == [] and pipeline.latest_assessments(True) == []
    assert canonical(pipeline.assessment_snapshot(result["run_id"], True)) == canonical(result)


def test_cooldown_invalidated_alerts_and_null_score_keep_existing_behavior(pipeline):
    first = stage(pipeline)
    publisher(pipeline).publish(first)
    second = stage(pipeline, cutoff=first.body["as_of"] + timedelta(hours=1))
    publisher(pipeline).publish(second)
    assert len(pipeline.alerts(True)) == 3
    pipeline.catalog.execute("UPDATE alerts SET invalidated=true")
    changed = deepcopy(second.body)
    changed["as_of"] += timedelta(minutes=1)
    changed["rows"][0]["score"] = None
    publication = pipeline._assessment_archive.stage(changed, PublicationScope([], ()))
    publisher(pipeline).publish(publication)
    assert pipeline.catalog.one("SELECT count(*) AS n FROM alerts WHERE NOT invalidated")["n"] == 2
