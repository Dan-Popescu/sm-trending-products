"""Exercise a staged synthetic checkpoint only in the isolated restore rehearsal."""

from __future__ import annotations

import argparse
from datetime import timedelta

from sm_trending_products.contracts import Observation, canonical, digest, now
from sm_trending_products.pipeline import Pipeline

ACCOUNT = "ci-restored-checkpoint"
OBSERVATION = "ci-restored-checkpoint-observation"


def seed(application: Pipeline) -> dict:
    timestamp = now()
    application.catalog.configure_budget("tiktok_display", ACCOUNT, 10, 10)
    task_id = application.enqueue(
        "tiktok_display",
        ACCOUNT,
        "list",
        {"fixture_observed_at": timestamp.isoformat()},
        synthetic=True,
        idempotency_key=ACCOUNT,
    )
    task = application.catalog.claim("ci-interrupted-writer", 180, True, 8)
    assert task is not None and task["task_id"] == task_id
    dataset_id = digest({"task": task_id, "page_index": 0})
    observation = Observation(
        observation_id=OBSERVATION,
        source="tiktok_display",
        entity_type="video",
        entity_id="restored-checkpoint",
        observed_at=timestamp,
        available_at=timestamp,
        synthetic=True,
    )
    original_commit = application.catalog.commit_dataset

    def interrupted_commit(*args, **kwargs):
        raise RuntimeError("controlled_before_checkpoint_interruption")

    application.catalog.commit_dataset = interrupted_commit
    try:
        application.history.publish(
            [observation],
            {
                "schema_version": "1",
                "records": [observation.model_dump(mode="json")],
                "observed_at": timestamp.isoformat(),
                "next_payload": None,
            },
            source="tiktok_display",
            synthetic=True,
            dataset_id=dataset_id,
            task=task,
        )
    except RuntimeError as error:
        assert str(error) == "controlled_before_checkpoint_interruption"
    else:
        raise AssertionError("Controlled checkpoint interruption was not exercised")
    finally:
        application.catalog.commit_dataset = original_commit
    application.catalog.execute(
        "UPDATE collection_tasks SET lease_until=%s WHERE task_id=%s",
        (now() - timedelta(seconds=1), task_id),
    )
    assert (
        application.catalog.one("SELECT state FROM datasets WHERE dataset_id=%s", (dataset_id,))[
            "state"
        ]
        == "staged"
    )
    return {"task_id": task_id, "dataset_id": dataset_id, "observation_id": OBSERVATION}


def resume(settings) -> dict:
    def no_source_request(*args, **kwargs):
        raise AssertionError("Restored checkpoint recovery attempted a source request")

    with Pipeline(settings) as application:
        # This opt-in exercise is restricted to an entirely synthetic recovery set.
        for table in ("collection_tasks", "datasets", "products", "evaluation_runs"):
            if application.catalog.one(f"SELECT 1 FROM {table} WHERE NOT synthetic LIMIT 1"):
                raise ValueError("Synthetic recovery exercise refuses live state")
        if application.catalog.one("SELECT 1 FROM source_capabilities WHERE state='available'"):
            raise ValueError("Synthetic recovery exercise refuses enabled source capabilities")
        tasks = application.catalog.all("SELECT * FROM collection_tasks")
        if len(tasks) != 1 or tasks[0]["account_id"] != ACCOUNT:
            raise ValueError("Synthetic recovery exercise requires its dedicated fixture task")
        task = tasks[0]
        assert task["state"] == "leased" and task["page_index"] == 0
        dataset_id = digest({"task": task["task_id"], "page_index": 0})
        assert (
            application.catalog.one(
                "SELECT state FROM datasets WHERE dataset_id=%s", (dataset_id,)
            )["state"]
            == "staged"
        )
        original = application.catalog.export_state()
        committed = application.catalog.all("SELECT * FROM datasets WHERE state='committed'")
        observations, _ = application.history.observations(now(), True)
        original_ids = {row.observation_id for row in observations}
        application.collector.run = no_source_request
        result = application.collect_batch(1, synthetic=True)
        assert result["processed"] == 1 and result["errors"] == []
        recovered_task = application.catalog.one(
            "SELECT * FROM collection_tasks WHERE task_id=%s", (task["task_id"],)
        )
        assert recovered_task["state"] == "done" and recovered_task["page_index"] == 0
        assert recovered_task["completed_at"] is not None
        assert (
            application.catalog.one(
                "SELECT state FROM datasets WHERE dataset_id=%s", (dataset_id,)
            )["state"]
            == "committed"
        )
        for before in committed:
            assert (
                application.catalog.one(
                    "SELECT * FROM datasets WHERE dataset_id=%s", (before["dataset_id"],)
                )
                == before
            )
        after = application.catalog.export_state()
        assert {key: value for key, value in after.items() if key != "datasets"} == {
            key: value for key, value in original.items() if key != "datasets"
        }
        assert application.verify()["ok"]
    with Pipeline(settings) as restarted:
        restarted.collector.run = no_source_request
        assert restarted.collect_batch(1, synthetic=True)["processed"] == 0
        observations, _ = restarted.history.observations(now(), True)
        ids = [row.observation_id for row in observations]
        assert set(ids) == original_ids | {OBSERVATION} and len(ids) == len(set(ids))
        assert restarted.catalog.export_state() == after
        assert restarted.verify()["ok"]
    return {
        "status": "verified",
        "synthetic": True,
        "source_requests": 0,
        "task_id": task["task_id"],
        "dataset_id": dataset_id,
        "recovered_observations": 1,
        "restart_duplicate_observations": 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("seed", "resume"))
    arguments = parser.parse_args()
    with Pipeline.from_env() as application:
        if arguments.action == "seed":
            print(canonical(seed(application)))
        else:
            print(canonical(resume(application.settings)))
