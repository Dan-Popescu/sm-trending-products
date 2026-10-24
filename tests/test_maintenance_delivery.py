"""Operator jobs distinguish resource-limited maintenance from failed deletion."""

import pytest
from dagster import materialize

from sm_trending_products import orchestration
from sm_trending_products.cli import dispatch, parser
from sm_trending_products.settings import Settings


@pytest.mark.parametrize(
    ("status", "failed", "success"),
    [("complete", 0, True), ("partial", 0, True), ("partial", 1, False), ("blocked", 0, False)],
)
def test_retention_job_preserves_budget_backlog_and_fails_actual_errors(
    monkeypatch, status, failed, success
):
    class ControlledPipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def retention_drain(self):
            return {
                "status": status,
                "stop_reason": "object_limit",
                "failed_objects": failed,
                "pending_objects": 250,
                "oldest_pending_object_age_seconds": 5400,
                "expired_root_count": 120,
                "oldest_expired_root_age_seconds": 7200,
            }

    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    result = materialize([orchestration.source_retention], raise_on_error=False)
    assert result.success is success
    if success:
        metadata = result.asset_materializations_for_node("source_retention")[0].metadata
        assert metadata["pending_objects"].value == 250
        assert metadata["oldest_pending_object_age_seconds"].value == 5400
        assert metadata["expired_root_count"].value == 120
        assert metadata["status"].value == status


def test_cli_passes_explicit_drain_limits_and_keeps_preview_separate():
    class Commands:
        def retention(self, *, apply):
            return {"preview": not apply}

        def retention_drain(self, **limits):
            return limits

        def compact_drain(self, **limits):
            return limits

    commands = Commands()
    assert dispatch(commands, parser().parse_args(["retention"])) == {"preview": True}
    result = dispatch(
        commands,
        parser().parse_args(
            [
                "retention",
                "--apply",
                "--drain",
                "--max-objects",
                "123",
                "--max-seconds",
                "5",
            ]
        ),
    )
    assert result["max_objects"] == 123 and result["max_seconds"] == 5
    result = dispatch(
        commands,
        parser().parse_args(
            [
                "compact",
                "--synthetic",
                "--drain",
                "--max-batches",
                "7",
            ]
        ),
    )
    assert result["synthetic"] is True and result["max_batches"] == 7
    for arguments in (
        ["retention", "--drain"],
        ["retention", "--apply", "--max-seconds", "5"],
        ["compact", "--max-batches", "2"],
    ):
        with pytest.raises(ValueError):
            dispatch(commands, parser().parse_args(arguments))


@pytest.mark.parametrize(("interval", "seconds"), [(1, 30), (1, 3600), (15, 450), (60, 1800)])
def test_profile_rejects_maintenance_budgets_that_fill_or_exceed_the_shared_interval(
    interval, seconds
):
    with pytest.raises(ValueError, match="Combined retention and compaction budgets"):
        Settings(
            database_url="postgresql://unused",
            maintenance_schedule_minutes=interval,
            maintenance_max_seconds=seconds,
        )


@pytest.mark.parametrize(("interval", "seconds"), [(1, 29), (15, 120), (60, 1799)])
def test_profile_accepts_maintenance_budgets_leaving_time_for_other_serialized_jobs(
    interval, seconds
):
    settings = Settings(
        database_url="postgresql://unused",
        maintenance_schedule_minutes=interval,
        maintenance_max_seconds=seconds,
    )
    assert 2 * settings.maintenance_max_seconds < 60 * settings.maintenance_schedule_minutes


@pytest.mark.integration
def test_retention_asset_reports_physical_backlog_age_after_roots_are_retired(
    pipeline, monkeypatch
):
    from sm_trending_products.pipeline import Pipeline

    pipeline.catalog.execute(
        "INSERT INTO retention_objects(object_key,reason,created_at) "
        "SELECT key,'controlled_pending',clock_timestamp()-interval '2 hours' "
        "FROM unnest(ARRAY['pending-first','pending-second']) AS key"
    )
    settings = pipeline.settings.model_copy(
        update={
            "retention_max_objects": 1,
            "retention_max_operations": 2,
            "maintenance_max_seconds": 30,
        }
    )
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: cls(settings)))
    result = materialize([orchestration.source_retention])
    assert result.success
    metadata = result.asset_materializations_for_node("source_retention")[0].metadata
    assert metadata["pending_objects"].value == 1
    assert metadata["expired_root_count"].value == 0
    assert metadata["oldest_expired_root_age_seconds"].value is None
    assert metadata["oldest_pending_object_age_seconds"].value >= 7200
    assert metadata["status"].value == "partial"
