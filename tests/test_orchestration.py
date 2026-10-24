"""Execute Dagster's asset boundary, including failed and deferred work."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import pytest
import yaml
from dagster import AssetKey, AssetMaterialization, DagsterInstance, materialize
from dagster_postgres.utils import get_conn_string

from sm_trending_products import orchestration


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
            }

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize([orchestration.source_retention], raise_on_error=False)
    assert result.success is (pending == 0)
    if not pending:
        metadata = result.asset_materializations_for_node("source_retention")[0].metadata
        assert metadata["dataset_count"].value == 2
        assert metadata["deleted_objects"].value == 5


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
