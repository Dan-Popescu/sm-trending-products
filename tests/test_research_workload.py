"""Bounded report attribution uses real scratch SQL/HTTP history, never a live source."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from pydantic import SecretStr

from sm_trending_products.costs import Store
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.research_workload import (
    Workload,
    price_usage,
    require_scratch,
    require_scratch_settings,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "research_workload_runner", ROOT / "scripts/research-workload.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.fixture
def native_sentinel():
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Dedicated native PostgreSQL required")
    schema = "workload_test_sentinel_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_bounds_refuse_before_state_or_output(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:secret@cloud.example/prod")
    output = tmp_path / "report.json"
    report = runner.run_case(Workload(), output=output)
    assert report["status"] == "refused"
    assert report["error_type"] == "ValueError"
    assert report["cleanup"] == {"schema_dropped": None, "files_removed": None, "errors": []}
    assert "secret" not in output.read_text()
    assert "cloud.example" not in output.read_text()
    for values in (
        {"products": True},
        {"samples": 0},
        {"text_characters": 16001},
        {"products": 10, "entities": 20, "samples": 169},
    ):
        with pytest.raises(ValueError):
            Workload(**values)


def test_native_cold_warm_identity_accounting_and_cleanup(tmp_path, monkeypatch, native_sentinel):
    # Isolate behavioral admission checks from unrelated host cgroup activity.
    monkeypatch.setattr(runner.existing, "memory_headroom", lambda: 512 * runner.MIB)
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    output = tmp_path / "report.json"
    workload = Workload(
        case="combined",
        products=2,
        entities=1,
        samples=3,
        texts=2,
        text_characters=96,
        panel_size=2,
    )
    report = runner.run_case(workload, output=output, timeout=60)
    assert report["ok"], report
    assert report["setup"]["result"]["rows"] == 14
    assert report["setup"]["result"]["supplied_texts"] == 4
    assert report["setup"]["result"]["supplied_text_characters"] == 384
    cold, warm = report["phases"]
    assert [cold["name"], warm["name"]] == ["cold", "warm"]
    assert cold["result"] == warm["result"]
    assert cold["result"]["result_sha256"]
    assert cold["storage"]["get_requests"] > 0
    assert warm["storage"]["get_requests"] == 0
    assert warm["storage"]["cache_hits"] > 0
    assert cold["wall_seconds"] > 0 and cold["cpu_seconds"] > 0
    assert report["worker_lifetime_cpu_seconds"] >= sum(
        phase["cpu_seconds"] for phase in [report["setup"], cold, warm]
    )
    assert report["worker_lifetime_reaped_children_cpu_seconds"] == 0
    assert cold["memory"]["host_peak_rss_bytes"] is None
    assert report["cleanup"] == {"schema_dropped": True, "files_removed": True, "errors": []}
    assert report["optional_prices"] is None
    assert json.loads(output.read_text()) == report
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as connection:
        assert connection.execute("SELECT to_regnamespace(%s)", (native_sentinel,)).fetchone()[0]
        assert (
            connection.execute("SELECT to_regnamespace(%s)", (report["schema"],)).fetchone()[0]
            is None
        )


def test_native_row_refusal_retains_usage_and_cleanup(tmp_path, monkeypatch):
    # Isolate behavioral admission checks from unrelated host cgroup activity.
    monkeypatch.setattr(runner.existing, "memory_headroom", lambda: 512 * runner.MIB)
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    report = runner.run_case(
        Workload(case="trend", samples=4, max_rows=1), output=tmp_path / "refused.json", timeout=60
    )
    assert not report["ok"]
    assert report["status"] == "report_refused"
    assert report["phases"][0]["error_type"] == "RowBudgetExceeded"
    assert report["phases"][0]["storage"]["get_requests"] > 0
    assert report["cleanup"]["schema_dropped"] and report["cleanup"]["files_removed"]


def test_prices_are_dated_optional_and_do_not_change_usage():
    usage = {
        "get_requests": 2,
        "head_requests": 1,
        "put_requests": 4,
        "download_bytes": 1000,
        "upload_bytes": 2000,
    }
    original = dict(usage)
    store = Store(
        name="synthetic-price",
        region="synthetic",
        currency="EUR",
        observed_on="2026-10-29",
        source_url="https://example.invalid/prices",
        storage_price_per_gb_month="1",
        write_price_per_unit="2",
        write_unit="1000",
        read_price_per_unit="1",
        read_unit="1000",
        egress_price_per_gb="3",
        notes="Assumed for test only",
    )
    priced = price_usage(usage, store)
    assert usage == original
    assert priced["observed_on"] == "2026-10-29"
    assert priced["charges"]["source_storage"] is None
    assert priced["charges"]["source_reads_and_heads"] == "0.003"
    assert priced["production_cost"] is None


def test_native_deadline_keeps_unknown_inflight_usage_and_cleans(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.existing, "memory_headroom", lambda: 512 * runner.MIB)
    report = runner.run_case(Workload(), output=tmp_path / "deadline.json", timeout=0.01)
    assert not report["ok"]
    assert report["status"] == "case_deadline"
    assert report["cleanup"]["schema_dropped"] and report["cleanup"]["files_removed"]
    assert report["usage_accounting_complete"] is False
    assert report["usage"]["get_requests"] is None
    assert report["optional_prices"] is None


def test_scratch_guards_refuse_operator_or_cloud_state(pipeline, monkeypatch):
    # The ordinary test fixture is isolated too, but it is not owned by this harness.
    with pytest.raises(ValueError, match="owned"):
        require_scratch(pipeline)
    monkeypatch.setattr(pipeline.settings, "s3_endpoint_url", "https://cloud.example")
    with pytest.raises(ValueError, match="scratch"):
        require_scratch(pipeline)


def test_memory_unknowns_are_not_zero(monkeypatch):
    original = Path.read_text

    def unavailable(path, *args, **kwargs):
        if str(path) == "/proc/self/status":
            raise OSError("Unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unavailable)
    measured = runner.memory()
    assert measured["process_rss_bytes_at_end"] is None
    assert measured["process_tree_peak_rss_bytes"] is None
    assert measured["host_peak_rss_bytes"] is None


@pytest.mark.parametrize("value", ["true", "1"])
def test_acquisition_enabled_refuses_before_scratch(tmp_path, monkeypatch, value):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://test:test@127.0.0.1/test")
    monkeypatch.setenv("ACQUISITION_ENABLED", value)
    report = runner.run_case(Workload(), output=tmp_path / "refused.json")
    assert report["status"] == "refused"
    assert report["cleanup"]["schema_dropped"] is None


def test_native_owned_scratch_refuses_enabled_sources_and_live_rows(pipeline, tmp_path):
    database_url = os.environ["TEST_DATABASE_URL"]
    schema = "research_workload_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped_url = runner.existing.scoped_database_url(database_url, schema)
    settings = pipeline.settings.model_copy(
        update={
            "database_url": SecretStr(scoped_url),
            "s3_bucket": schema.replace("_", "-"),
            "cache_dir": tmp_path / "owned-cache",
            "spool_dir": tmp_path / "owned-spool",
        }
    )
    try:
        with Pipeline(settings) as scratch:
            scratch.initialize(create_bucket=True)
            require_scratch(scratch)
            scratch.catalog.execute(
                "INSERT INTO source_capabilities(source,account_id,capability,state,details) "
                "VALUES('fixture','fixture','collect','available','{}')"
            )
            with pytest.raises(ValueError, match="capabilities"):
                require_scratch(scratch)
            scratch.catalog.execute("DELETE FROM source_capabilities")
            scratch.create_product("live", "Live row", synthetic=False)
            with pytest.raises(ValueError, match="live state"):
                require_scratch(scratch)
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_direct_worker_rejects_foreign_schema_before_initialize_or_bucket_writes(
    pipeline, tmp_path, monkeypatch
):
    database_url = os.environ["TEST_DATABASE_URL"]
    schema = "foreign_workload_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    values = pipeline.settings.model_dump(mode="json")
    values.update(
        database_url=runner.existing.scoped_database_url(database_url, schema),
        s3_bucket="foreign-workload-" + uuid4().hex,
    )
    before = pipeline.history.client.list_buckets()["Buckets"]
    initialized = []
    initialize = Pipeline.initialize

    def counted_initialize(application, *args, **kwargs):
        initialized.append(1)
        return initialize(application, *args, **kwargs)

    monkeypatch.setattr(Pipeline, "initialize", counted_initialize)
    payload = {
        "settings": values,
        "workload": Workload(case="trend").model_dump(),
        "owned": str(tmp_path / "owned"),
        "max_upload_bytes": runner.MIB,
        "checkpoint": str(tmp_path / "worker.json"),
    }
    try:
        assert runner.worker(payload) == 1
        assert initialized == []
        with psycopg.connect(database_url) as connection:
            assert (
                connection.execute(
                    "SELECT to_regclass(%s)", (schema + ".schema_migrations",)
                ).fetchone()[0]
                is None
            )
        assert pipeline.history.client.list_buckets()["Buckets"] == before
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_direct_worker_rejects_populated_owned_schema_before_initialization(
    pipeline, tmp_path, monkeypatch
):
    database_url = os.environ["TEST_DATABASE_URL"]
    schema = "research_workload_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    values = pipeline.settings.model_dump(mode="json")
    values.update(
        database_url=runner.existing.scoped_database_url(database_url, schema),
        s3_bucket=schema.replace("_", "-"),
        cache_dir=str(tmp_path / "owned-cache"),
        spool_dir=str(tmp_path / "owned-spool"),
    )
    from sm_trending_products.settings import Settings

    try:
        with Pipeline(Settings.model_validate(values)) as occupied:
            occupied.initialize(create_bucket=True)
            occupied.create_product("live", "Existing live state", synthetic=False)
            before = occupied.catalog.all("SELECT * FROM products ORDER BY product_id")
            initialized = []
            initialize = Pipeline.initialize

            def counted_initialize(application, *args, **kwargs):
                initialized.append(1)
                return initialize(application, *args, **kwargs)

            monkeypatch.setattr(Pipeline, "initialize", counted_initialize)
            payload = {
                "settings": values,
                "workload": Workload(case="trend").model_dump(),
                "owned": str(tmp_path / "worker-owned"),
                "max_upload_bytes": runner.MIB,
                "checkpoint": str(tmp_path / "worker.json"),
            }
            assert runner.worker(payload) == 1
            assert initialized == []
            assert occupied.catalog.all("SELECT * FROM products ORDER BY product_id") == before
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize(
    "override",
    [
        "host=192.0.2.1",
        "hostaddr=192.0.2.1",
        "dbname=production",
        "service=production",
        "options=-c%20search_path%3Dpublic",
    ],
)
def test_effective_libpq_overrides_refuse_before_resources(tmp_path, monkeypatch, override):
    from sm_trending_products.settings import Settings

    database = "postgresql://test:test@127.0.0.1/pipeline_test"
    schema = "research_workload_" + uuid4().hex
    scoped = runner.existing.scoped_database_url(database, schema) + "&" + override
    with pytest.raises(ValueError):
        require_scratch_settings(
            Settings(
                database_url=scoped,
                s3_bucket=schema.replace("_", "-"),
                s3_endpoint_url="http://127.0.0.1:12345",
            )
        )
    monkeypatch.setenv("TEST_DATABASE_URL", database + "?" + override)
    if override.startswith("options="):
        return  # Supervisor replaces input search_path; worker validates the effective owned one.
    report = runner.run_case(Workload(), output=tmp_path / "refused.json")
    assert report["status"] == "refused"
    assert report["cleanup"]["schema_dropped"] is None


@pytest.mark.parametrize("name,value", [("PGHOSTADDR", "192.0.2.1"), ("PGSERVICE", "production")])
def test_libpq_environment_indirection_refuses_before_resources(tmp_path, monkeypatch, name, value):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://test:test@127.0.0.1/pipeline_test")
    monkeypatch.setenv(name, value)
    report = runner.run_case(Workload(), output=tmp_path / "refused.json")
    assert report["status"] == "refused" and report["cleanup"]["schema_dropped"] is None


def test_new_report_defaults_admit_measured_fixture_shape():
    for case in ("changes", "progression", "research"):
        workload = Workload(case=case)
        assert workload.products == 1 and workload.entities == workload.texts == 8
        assert workload.rows == 68
    with pytest.raises(ValueError, match="four videos"):
        Workload(case="progression", entities=2, texts=2)


@pytest.mark.parametrize("shape", ["regular", "skewed"])
def test_native_new_reports_measure_complete_panels_and_associations(tmp_path, monkeypatch, shape):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.existing, "memory_headroom", lambda: 512 * runner.MIB)
    report = runner.run_case(
        Workload(case="research", text_characters=96, shape=shape),
        output=tmp_path / "new-reports.json",
        timeout=90,
    )
    assert report["ok"], report
    assert report["setup"]["result"]["rows"] == 68
    assert report["setup"]["result"]["supplied_texts"] == 8
    assert report["complete_bytes_preserved"] and report["identity_preserved"]
    cold, warm = report["phases"]
    assert cold["result"] == warm["result"]
    assert warm["storage"]["get_requests"] == 0
    assert cold["sql_execute_calls"] > 0 and warm["sql_execute_calls"] > 0
    work = cold["result"]["reported_work"]
    compared = next(
        row for row in work["changes:workload-0"]["windows"] if row["window_hours"] == 24
    )
    assert compared["comparison_status"] == "measured"
    assert compared["continuing_entity_count"] == 8
    assert compared["pace_ratio_lower"] == compared["pace_ratio_upper"] == 2
    progression = work["progression:workload-0"]
    assert (
        progression["comparison_status"] == "available" and progression["eligible_video_count"] == 8
    )
    association = next(row for row in progression["associations"] if row["theme_id"] == "time")
    assert association["timing_scope"] == "text_known_by_interval_start"
    assert association["present"]["observed_growth_lower"] == (218 if shape == "skewed" else 20)
    assert association["absent"]["observed_growth_lower"] == 52
    for arm in ("present", "absent"):
        assert association[arm]["video_count"] == association[arm]["known_creator_count"] == 4
        assert association[arm]["minimum_coverage_fraction"] == 1
        assert association[arm]["minimum_measured_hours"] == 24
    assert report["cleanup"]["schema_dropped"] and report["cleanup"]["files_removed"]


def test_native_sparse_new_report_is_a_refusal_with_actual_result_identity(tmp_path, monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("Dedicated native PostgreSQL required")
    monkeypatch.setattr(runner.existing, "memory_headroom", lambda: 512 * runner.MIB)
    report = runner.run_case(
        Workload(case="progression", samples=2),
        output=tmp_path / "sparse-new-report.json",
        timeout=60,
    )
    assert not report["ok"] and report["status"] == "report_refused"
    assert len(report["phases"]) == 1
    cold = report["phases"][0]
    assert (
        cold["result"]["result_sha256"] and cold["result"]["public_ids"]["progression:workload-0"]
    )
    assert (
        cold["result"]["reported_work"]["progression:workload-0"]["comparison_status"]
        == "unavailable"
    )
    assert cold["storage"]["get_requests"] > 0 and cold["sql_execute_calls"] > 0
    assert report["usage_accounting_complete"]
    assert report["cleanup"]["schema_dropped"] and report["cleanup"]["files_removed"]


def test_price_fifo_refuses_without_blocking_or_creating_state(tmp_path):
    fifo = tmp_path / "prices.fifo"
    os.mkfifo(fifo)
    output = tmp_path / "report.json"
    environment = {key: value for key, value in os.environ.items() if key != "TEST_DATABASE_URL"}
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/research-workload.py"),
            "--prices",
            str(fifo),
            "--output",
            str(output),
            "--timeout",
            "0.01",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env=environment,
    )
    assert result.returncode == 2
    assert "Invalid bounded workload or price input" in result.stderr
    assert not output.exists()
