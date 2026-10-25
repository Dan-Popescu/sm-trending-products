"""Execute Dagster's asset boundary, including failed and deferred work."""

from __future__ import annotations

import importlib
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import pytest
import yaml
from dagster import AssetKey, AssetMaterialization, DagsterInstance, materialize
from dagster_postgres.utils import get_conn_string

from sm_trending_products import orchestration


@pytest.mark.parametrize("independent", ["false", "true"])
def test_complete_deployed_repository_loads_in_both_acquisition_modes(monkeypatch, independent):
    try:
        with monkeypatch.context() as environment:
            environment.setenv("ACQUISITION_ENABLED", independent)
            definitions = importlib.reload(orchestration).defs
            repository = definitions.get_repository_def()
            # The daemon/code server loads every job, including the implicit asset
            # job. Resolving one selected job misses cross-job definition conflicts.
            repository.load_all_definitions()
            compaction = repository.get_schedule_def("history_compaction_schedule")
            assert repository.get_job(compaction.job_name).asset_layer.executable_asset_keys == {
                AssetKey("history_compaction")
            }
    finally:
        importlib.reload(orchestration)


def test_independent_mode_removes_the_automatic_collection_schedule(monkeypatch):
    try:
        with monkeypatch.context() as environment:
            environment.setenv("ACQUISITION_ENABLED", "true")
            independent = importlib.reload(orchestration)
            jobs = {schedule.job_name for schedule in independent.defs.schedules}
            assert "collection_batches" not in jobs
            assert "opportunity_rankings" in jobs
            assert "retention_policy" in jobs
    finally:
        importlib.reload(orchestration)


@pytest.mark.parametrize("asset", ["collection_schedule", "observation_batch"])
def test_independent_acquisition_rejects_legacy_collection_before_access(monkeypatch, asset):
    class ForbiddenPipeline:
        @classmethod
        def from_env(cls):
            raise AssertionError("Legacy collection must not open the acquisition dependencies")

    monkeypatch.setenv("ACQUISITION_ENABLED", "true")
    monkeypatch.setattr(orchestration, "Pipeline", ForbiddenPipeline)
    result = materialize([getattr(orchestration, asset)], raise_on_error=False)
    assert not result.success
    failure = result.failure_data_for_node(asset)
    assert "Dedicated acquisition is enabled" in failure.error.message


def test_independent_analytics_runs_without_collection(monkeypatch):
    class AnalyticalPipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def assess(self, *args, **kwargs):
            return {"run_id": "existing-captured-evidence", "rows": []}

    monkeypatch.setenv("ACQUISITION_ENABLED", "true")
    monkeypatch.setattr(orchestration, "Pipeline", AnalyticalPipeline)
    result = materialize([orchestration.opportunity_assessments])
    assert result.success
    assert [event.step_key for event in result.get_step_success_events()] == [
        "opportunity_assessments"
    ]


@pytest.mark.integration
def test_deployed_dagster_storage_records_native_asset_events(pipeline, tmp_path):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        pytest.skip("Dagster event storage requires native PostgreSQL notification semantics")
    parsed = urlsplit(pipeline.settings.database_url.get_secret_value())
    config = yaml.safe_load(Path("deploy/dagster/dagster.yaml").read_text())
    scheme = config["storage"]["postgres"]["postgres_db"]["scheme"]
    config["storage"]["postgres"] = {
        "postgres_url": get_conn_string(
            username=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            hostname=parsed.hostname or "",
            db_name=parsed.path.lstrip("/"),
            port=str(parsed.port or 5432),
            params=dict(parse_qsl(parsed.query)),
            scheme=scheme,
        )
    }
    (tmp_path / "dagster.yaml").write_text(yaml.safe_dump(config))
    with DagsterInstance.from_config(str(tmp_path)) as instance:
        key = AssetKey("native-storage-contract")
        instance.report_runless_asset_event(AssetMaterialization(key))
        assert len(instance.fetch_materializations(key, limit=10).records) == 1


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

        def retention_drain(self, **kwargs):
            return {
                "dataset_count": 2,
                "deleted_objects": 5,
                "pending_objects": pending,
                "failed_objects": pending,
                "pending_source_invalidations": [
                    {"phase": "discover", "created_at": datetime.now(UTC)}
                ],
            }

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize([orchestration.source_retention], raise_on_error=False)
    assert result.success is (pending == 0)
    if not pending:
        metadata = result.asset_materializations_for_node("source_retention")[0].metadata
        assert metadata["dataset_count"].value == 2
        assert metadata["deleted_objects"].value == 5
        assert metadata["pending_source_invalidations"].value == 1


def test_collection_asset_reports_quota_wait_without_an_error_or_attempt(monkeypatch):
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
            return {"processed": 0, "errors": [], "quota_waiting": 3, "waiting": True}

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize([orchestration.collection_schedule, orchestration.observation_batch])
    assert result.success
    event = result.asset_materializations_for_node("observation_batch")[0]
    assert event.metadata["outcome"].value == "waiting"
    assert event.metadata["quota_waiting"].value == 3
    assert event.metadata["errors"].value == 0
