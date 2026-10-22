"""Disk pressure is observable without scanning analytical history."""

import json
import shutil

import pytest
from streamlit.testing.v1 import AppTest

from sm_trending_products.pipeline import Pipeline

pytestmark = pytest.mark.integration


def test_storage_pressure_and_unavailable_volume_remain_visible_without_history_scan(
    pipeline, monkeypatch
):
    actual = shutil.disk_usage(pipeline.settings.cache_dir)

    def disk_usage(path):
        if path == pipeline.settings.spool_dir:
            raise PermissionError("private-filesystem-detail")
        return actual._replace(free=4096)

    def no_history_scan(*args, **kwargs):
        raise AssertionError("Operational status must not scan observations")

    monkeypatch.setattr(shutil, "disk_usage", disk_usage)
    monkeypatch.setattr(pipeline.history, "observations", no_history_scan)
    capacity = pipeline.status()["storage_capacity"]
    assert capacity["cache"]["available_bytes"] == 4096
    assert capacity["cache"]["configured_cache_limit_bytes"] == pipeline.settings.cache_max_bytes
    assert capacity["spool"] == {"status": "unavailable"}
    monkeypatch.setattr(Pipeline, "from_env", classmethod(lambda cls: pipeline))
    app = AppTest.from_string("from sm_trending_products.dashboard import main\nmain()").run(
        timeout=20
    )
    app.radio(key="review_view").set_value("Health and coverage").run(timeout=20)
    assert not app.exception
    displayed = next(json.loads(row.value) for row in app.json if "storage_capacity" in row.value)
    assert displayed["storage_capacity"] == capacity
    assert "private-filesystem-detail" not in json.dumps(displayed)
    assert str(pipeline.settings.cache_dir) not in json.dumps(displayed)


def test_status_measures_spool_ancestor_without_creating_the_directory(pipeline):
    assert not pipeline.settings.spool_dir.exists()
    capacity = pipeline.status()["storage_capacity"]
    assert capacity["cache"]["scope"] == "directory_filesystem"
    assert capacity["spool"]["scope"] == "ancestor_filesystem"
    assert capacity["spool"]["total_bytes"] > 0
    assert not pipeline.settings.spool_dir.exists()
