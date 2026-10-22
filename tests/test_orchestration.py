"""Execute Dagster's asset boundary, including failed and deferred work."""

from __future__ import annotations

import pytest
from dagster import materialize

from sm_trending_products import orchestration


@pytest.mark.parametrize("category,success", [("access_denied", False), ("quota_exhausted", True)])
def test_collection_asset_distinguishes_failure_from_budget_wait(monkeypatch, category, success):
    class ControlledPipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def schedule(self, *args, **kwargs):
            return {"scheduled": 0}

        def collect_batch(self, *args, **kwargs):
            return {"processed": 0, "errors": [{"category": category}]}

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize(
        [orchestration.collection_schedule, orchestration.observation_batch],
        raise_on_error=False,
    )
    assert result.success is success
    if success:
        event = result.asset_materializations_for_node("observation_batch")[0]
        assert event.metadata["outcome"].value == "waiting"
    else:
        assert result.failure_data_for_node("observation_batch") is not None


@pytest.mark.parametrize("pending", [0, 1])
def test_retention_records_actual_counts_and_fails_pending_deletion(monkeypatch, pending):
    class ControlledPipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def retention(self, **kwargs):
            return {"dataset_count": 2, "deleted_objects": 5, "pending_objects": pending}

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize([orchestration.source_retention], raise_on_error=False)
    assert result.success is (pending == 0)
    if not pending:
        metadata = result.asset_materializations_for_node("source_retention")[0].metadata
        assert metadata["dataset_count"].value == 2
        assert metadata["deleted_objects"].value == 5
