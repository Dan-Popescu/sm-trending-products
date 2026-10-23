"""Declared workload and actual orchestration admission before live access exists."""

import json
import os
import subprocess
import sys

import pytest

from sm_trending_products import orchestration
from sm_trending_products.settings import PlanningSettings


def capacity(*arguments, **settings):
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"DATABASE_URL", "TIKTOK_DISPLAY_ACCESS_TOKEN", "TIKTOK_CCL_ACCESS_TOKEN"}
    }
    environment.update(settings)
    return subprocess.run(
        [sys.executable, "-m", "sm_trending_products", "capacity", *arguments],
        env=environment,
        text=True,
        capture_output=True,
        timeout=15,
    )


def test_operator_can_compare_realistic_profile_with_planning_without_credentials():
    result = capacity(
        "--products",
        "10000",
        "--videos-per-product",
        "20",
        "--refreshes-per-day",
        "4",
        SCHEDULER_REQUEST_BUDGET="100",
        COLLECTION_SCHEDULE_MINUTES="15",
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["observations_per_day"] == 800000
    assert report["retained_observations"] == 72000000
    assert report["minimum_refresh_requests_per_day"] == 40000
    assert report["planning_or_declared_budget_shortfall_per_day"] == 30400
    assert report["planning_sufficient"] is False
    assert report["runtime_capacity_verified"] is False


def test_declared_source_quota_still_limits_a_faster_planning_profile():
    result = capacity(
        "--products",
        "10000",
        "--videos-per-product",
        "20",
        "--refreshes-per-day",
        "4",
        "--source-requests-per-day",
        "20000",
        SCHEDULER_REQUEST_BUDGET="500",
        COLLECTION_SCHEDULE_MINUTES="5",
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["maximum_new_tasks_planned_per_day"] == 144000
    assert report["declared_request_upper_bound_per_day"] == 20000
    assert report["planning_or_declared_budget_shortfall_per_day"] == 20000
    assert report["source_budget_supplied"] is True
    assert report["runtime_capacity_verified"] is False


@pytest.mark.parametrize("value", ["0", "7", "61"])
def test_unsupported_cadence_rejects_instead_of_misreporting_daily_capacity(value):
    result = capacity(
        "--products",
        "1",
        "--videos-per-product",
        "1",
        "--refreshes-per-day",
        "1",
        COLLECTION_SCHEDULE_MINUTES=value,
    )
    assert result.returncode == 2
    assert "invalid_configuration_or_record" in result.stderr


def test_production_collection_job_does_not_rebuild_the_analytical_universe(monkeypatch):
    calls = []

    class ControlledPipeline:
        @classmethod
        def from_env(cls):
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def schedule(self, budget, **kwargs):
            calls.append(("schedule", budget))
            return {"scheduled": budget}

        def collect_batch(self, size, **kwargs):
            calls.append(("collect", size))
            return {"processed": size, "errors": []}

        def assess(self, *args, **kwargs):
            calls.append(("assess", None))
            return {"run_id": "controlled", "rows": []}

    monkeypatch.setenv("SCHEDULER_REQUEST_BUDGET", "321")
    monkeypatch.setattr(orchestration, "Pipeline", ControlledPipeline)
    assert orchestration.defs.resolve_job_def("collection_batches").execute_in_process().success
    assert calls == [("schedule", 321), ("collect", 20)]
    assert orchestration.defs.resolve_job_def("opportunity_rankings").execute_in_process().success
    assert calls[-1] == ("assess", None)


def test_hourly_cadence_is_one_run_per_hour():
    assert PlanningSettings.cron(60) == "0 * * * *"


def test_profile_rejects_a_budget_that_the_real_scheduler_cannot_admit():
    result = capacity(
        "--products",
        "10000",
        "--videos-per-product",
        "20",
        "--refreshes-per-day",
        "4",
        SCHEDULER_REQUEST_BUDGET="10001",
    )
    assert result.returncode == 2
    assert "invalid_configuration_or_record" in result.stderr
