"""Real SQL and HTTP object-store test boundaries, isolated from deployed data."""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql


def unused_port() -> int:
    with socket.socket() as connection:
        connection.bind(("127.0.0.1", 0))
        return connection.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def sql_engine():
    if os.environ.get("TEST_DATABASE_URL") or not os.environ.get("TEST_PGLITE_BINARY"):
        yield
        return
    port = unused_port()
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(
            [
                os.environ["TEST_PGLITE_BINARY"],
                "--db=memory://",
                "--port",
                str(port),
                "--host",
                "127.0.0.1",
                "--max-connections",
                "16",
            ],
            stdout=log,
            stderr=log,
        )
        database_url = f"postgresql://postgres:postgres@127.0.0.1:{port}/postgres?sslmode=disable"
        try:
            for _ in range(200):
                try:
                    with psycopg.connect(database_url, connect_timeout=1) as connection:
                        connection.execute("SELECT 1")
                    break
                except psycopg.OperationalError:
                    if process.poll() is not None:
                        raise RuntimeError("Test PostgreSQL-compatible engine exited") from None
                    time.sleep(0.1)
            else:
                raise RuntimeError("Test PostgreSQL-compatible engine startup timed out")
            os.environ["TEST_DATABASE_URL"] = database_url
            os.environ["TEST_DATABASE_ENGINE"] = "pglite"
            os.environ["PG_PREPARE_THRESHOLD"] = "none"
            yield
        finally:
            os.environ.pop("TEST_DATABASE_URL", None)
            os.environ.pop("TEST_DATABASE_ENGINE", None)
            os.environ.pop("PG_PREPARE_THRESHOLD", None)
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture(autouse=True)
def isolate_wasm_backend(sql_engine):
    if os.environ.get("TEST_DATABASE_ENGINE") == "pglite":
        # The fallback multiplexes one backend and ignores per-client startup search_path.
        # This is our ephemeral in-memory engine, never a user-supplied native database.
        with psycopg.connect(
            os.environ["TEST_DATABASE_URL"], autocommit=True, prepare_threshold=None
        ) as connection:
            connection.execute("DROP SCHEMA public CASCADE")
            connection.execute("CREATE SCHEMA public")
    yield


@pytest.fixture(scope="session")
def s3_endpoint(sql_engine):
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=unused_port(), verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def pipeline(sql_engine, s3_endpoint, tmp_path, monkeypatch):
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip(
            "Set TEST_DATABASE_URL for native PostgreSQL "
            "or TEST_PGLITE_BINARY for SQL-only fallback"
        )
    from sm_trending_products.pipeline import Pipeline
    from sm_trending_products.settings import Settings

    schema = "pipeline_test_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-c search_path={schema}"
    scoped_url = urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )
    for key, value in {
        "AWS_ACCESS_KEY_ID": "fixture-access",
        "AWS_SECRET_ACCESS_KEY": "fixture-secret",
        "AWS_EC2_METADATA_DISABLED": "true",
        "TIKTOK_DISPLAY_ACCESS_TOKEN": "fixture-token",
        "TIKTOK_CCL_ACCESS_TOKEN": "fixture-ccl-token",
    }.items():
        monkeypatch.setenv(key, value)
    collector = Path(__file__).resolve().parents[1] / "collector/target/debug/sm-collector"
    monkeypatch.setenv("DATABASE_URL", scoped_url)
    settings = Settings(
        database_url=scoped_url,
        s3_bucket="fixture-" + uuid4().hex,
        s3_endpoint_url=s3_endpoint,
        cache_dir=tmp_path / "cache",
        spool_dir=tmp_path / "spool",
        collector_binary=str(collector),
        history_partition_buckets=4,
    )
    application = Pipeline(settings)
    try:
        application.initialize(create_bucket=True)
        for source in ("tiktok_display", "tiktok_ads", "tiktok_commercial"):
            application.catalog.configure_budget(source, "fixture-account", 1000, 1000)
        yield application
    finally:
        application.close()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
