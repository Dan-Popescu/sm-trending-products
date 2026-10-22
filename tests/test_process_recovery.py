"""Terminate actual workers around publication and recover native storage state."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from test_pipeline_integration import ACCOUNT, controlled_source

from sm_trending_products.contracts import now
from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration
CRASH_EXIT_CODE = 81


def _crashing_worker(settings, boundary):
    application = Pipeline(settings)
    if boundary == "before_publication":
        application.history.publish = lambda *args, **kwargs: os._exit(CRASH_EXIT_CODE)
    elif boundary == "after_raw_object":
        original_put = application.history._put

        def put(key, body, **kwargs):
            result = original_put(key, body, **kwargs)
            if key.startswith("raw/"):
                os._exit(CRASH_EXIT_CODE)
            return result

        application.history._put = put
    else:
        original_transaction = application.catalog.transaction

        class CrashAtCheckpoint:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, query, params=()):
                if str(query).startswith("UPDATE collection_tasks SET state=%s,payload=%s"):
                    os._exit(CRASH_EXIT_CODE)
                return self.connection.execute(query, params)

        @contextmanager
        def transaction():
            with original_transaction() as connection:
                yield CrashAtCheckpoint(connection)

        application.catalog.transaction = transaction
    application.collect_batch(1, synthetic=True)
    application.close()


@pytest.mark.parametrize(
    "boundary", ["before_publication", "after_raw_object", "before_checkpoint"]
)
def test_process_termination_recovers_pagination_without_lost_or_duplicate_evidence(
    pipeline, boundary
):
    def page(identifier, more, cursor):
        return (
            200,
            {
                "data": {
                    "videos": [{"id": identifier, "view_count": 10}],
                    "has_more": more,
                    "cursor": cursor,
                },
                "error": {"code": "ok"},
            },
            {},
        )

    first, second = page("first", True, 7), page("second", False, 0)
    responses = [first, first, second] if boundary == "before_publication" else [first, second]
    with controlled_source(responses) as (endpoint, requests):
        task_id = pipeline.enqueue(
            "tiktok_display",
            ACCOUNT,
            "list",
            {"endpoint_override": endpoint, "fixture_observed_at": now().isoformat()},
            synthetic=True,
        )
        settings = pipeline.settings.model_dump(mode="json")
        settings["database_url"] = pipeline.settings.database_url.get_secret_value()
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; from sm_trending_products.settings import Settings; "
                "from test_process_recovery import _crashing_worker; "
                "_crashing_worker(Settings.model_validate_json(sys.stdin.read()),sys.argv[1])",
                boundary,
            ],
            input=json.dumps(settings),
            text=True,
            capture_output=True,
            timeout=30,
            env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)},
        )
        assert child.returncode == CRASH_EXIT_CODE
        assert pipeline.catalog.manifests(synthetic=True) == []
        staged = pipeline.datasets()
        if boundary == "before_publication":
            assert staged == []
        else:
            # Publication intent is visible before recovery; it must be repaired
            # in place, rather than leaving an orphaned staging record behind.
            assert len(staged) == 1 and staged[0]["state"] == "staged"
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert task["state"] == "leased" and task["page_index"] == 0
        assert "cursor" not in task["payload"]
        pipeline.catalog.execute(
            "UPDATE collection_tasks SET lease_until=clock_timestamp()-interval '1 second' "
            "WHERE task_id=%s",
            (task_id,),
        )
        recovered = pipeline.collect_batch(1, synthetic=True)
        assert recovered["processed"] == 1 and recovered["errors"] == []
        task = pipeline.catalog.one("SELECT * FROM collection_tasks WHERE task_id=%s", (task_id,))
        assert task["state"] == "pending" and task["payload"]["cursor"] == 7
        assert pipeline.collect_batch(1, synthetic=True)["processed"] == 1
        assert pipeline.collect_batch(1, synthetic=True)["processed"] == 0
        assert requests[-1]["cursor"] == 7
        assert len(requests) == len(responses)
    datasets = pipeline.datasets()
    assert len(datasets) == 2 and all(row["state"] == "committed" for row in datasets)
    assert {row["dataset_id"] for row in staged} <= {row["dataset_id"] for row in datasets}
    listing = pipeline.history.client.list_objects_v2(Bucket=pipeline.history.bucket)
    assert not listing["IsTruncated"]
    assert {item["Key"] for item in listing["Contents"]} == {
        row["object_key"]
        for row in pipeline.catalog.all("SELECT object_key FROM dataset_object_refs")
    }
    observations, _ = pipeline.history.observations(now(), synthetic=True)
    assert sorted(row.entity_id for row in observations) == ["first", "second"]
    assert pipeline.verify()["ok"] is True
