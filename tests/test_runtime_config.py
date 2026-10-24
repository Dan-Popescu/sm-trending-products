"""Resolve the deployed role boundaries and preserve standalone compatibility."""

import json
import os
import shutil
import subprocess

import pytest
from pydantic import ValidationError

from sm_trending_products.settings import PlanningSettings, Settings


def test_independent_acquisition_is_explicit_outside_compose(monkeypatch):
    monkeypatch.delenv("ACQUISITION_ENABLED", raising=False)
    assert not PlanningSettings().acquisition_enabled
    monkeypatch.setenv("ACQUISITION_ENABLED", "true")
    assert PlanningSettings().acquisition_enabled


def test_capture_spool_requires_room_for_a_bounded_response(monkeypatch):
    monkeypatch.delenv("CAPTURE_SPOOL_MAX_BYTES", raising=False)
    settings = Settings(database_url="postgresql://fixture@localhost/fixture")
    assert settings.capture_spool_max_bytes == 268_435_456
    with pytest.raises(ValidationError, match="capture_spool_max_bytes"):
        Settings(
            database_url="postgresql://fixture@localhost/fixture",
            capture_spool_max_bytes=33_554_431,
        )


def test_compose_resolves_independent_pins_identities_and_persistent_spool():
    if not shutil.which("docker"):
        pytest.skip("Docker Compose is required for interpolation validation")
    roles = {
        "CAPTURE_BLUE_IMAGE": "fixture/blue@sha256:" + "1" * 64,
        "CAPTURE_GREEN_IMAGE": "fixture/green@sha256:" + "2" * 64,
        "ACQUISITION_PLANNER_IMAGE": "fixture/planner@sha256:" + "3" * 64,
        "NORMALIZER_IMAGE": "fixture/normalizer@sha256:" + "4" * 64,
        "ANALYTICS_IMAGE": "fixture/analytics@sha256:" + "5" * 64,
        "DASHBOARD_IMAGE": "fixture/dashboard@sha256:" + "6" * 64,
    }
    environment = {
        **os.environ,
        **roles,
        "COMPOSE_ENV_FILES": "/dev/null",
        "COMPOSE_DISABLE_ENV_FILE": "true",
        "POSTGRES_PASSWORD": "fixture-only",
        "DATABASE_URL": "postgresql://fixture:fixture-only@postgres/fixture",
        "S3_BUCKET": "fixture-only",
        "ACQUISITION_ENABLED": "true",
        "CAPTURE_BLUE_WORKER_ID": "blue-release-17",
        "CAPTURE_GREEN_WORKER_ID": "green-release-18",
        "POSTGRES_HOST_PORT": "15432",
    }
    result = subprocess.run(
        ["docker", "compose", "--profile", "rollout", "config", "--format", "json"],
        env=environment,
        text=True,
        capture_output=True,
        check=True,
        timeout=15,
    )
    services = json.loads(result.stdout)["services"]
    for role, variable in (
        ("worker", "CAPTURE_BLUE_IMAGE"),
        ("worker-green", "CAPTURE_GREEN_IMAGE"),
        ("capture-planner", "ACQUISITION_PLANNER_IMAGE"),
        ("normalizer", "NORMALIZER_IMAGE"),
        ("dagster-daemon", "ANALYTICS_IMAGE"),
        ("dagster-webserver", "ANALYTICS_IMAGE"),
        ("dashboard", "DASHBOARD_IMAGE"),
    ):
        assert services[role]["image"] == roles[variable]
    for role, worker_id in (("worker", "blue-release-17"), ("worker-green", "green-release-18")):
        service = services[role]
        assert service["command"][:3] == ["sm-trends", "capture", "--loop"]
        assert service["environment"]["WORKER_ID"] == worker_id
        assert service["labels"]["io.sm-trends.worker-id"] == worker_id
    assert services["worker-green"]["profiles"] == ["rollout"]
    assert services["normalizer"]["command"][:3] == ["sm-trends", "publish-captures", "--loop"]
    assert services["capture-planner"]["command"] == [
        "python",
        "-m",
        "sm_trending_products.capture_coordinator",
        "--loop",
    ]
    assert services["postgres"]["ports"][0]["host_ip"] == "127.0.0.1"
    assert str(services["postgres"]["ports"][0]["published"]) == "15432"
    spool_sources = {
        volume["source"]
        for role in ("worker", "worker-green", "normalizer")
        for volume in services[role]["volumes"]
        if volume["target"] == "/var/lib/sm-trends/spool"
    }
    assert len(spool_sources) == 1
    assert all(
        "docker.sock" not in volume["target"]
        for service in services.values()
        for volume in service.get("volumes", [])
    )
