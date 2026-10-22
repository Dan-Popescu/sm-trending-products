"""Measure a fresh synthetic namespace on native PostgreSQL and local HTTP Moto.

TEST_DATABASE_URL must identify an isolated test database whose role may create
and drop schemas. No production store or live-source token is used.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from moto.server import ThreadedMotoServer
from psycopg import sql

from sm_trending_products.cli import dispatch, parser
from sm_trending_products.contracts import canonical
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings


def main() -> int:
    import sys

    args = parser().parse_args(["benchmark", *sys.argv[1:]])
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        raise SystemExit("Set TEST_DATABASE_URL to an isolated native test database.")
    schema = "benchmark_" + uuid4().hex
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-c search_path={schema}"
    scoped_url = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, quote_via=quote),
            parsed.fragment,
        )
    )
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    try:
        # These values are only used by the isolated loopback test endpoints.
        for key, value in {
            "AWS_ACCESS_KEY_ID": "offline-fixture",
            "AWS_SECRET_ACCESS_KEY": "offline-fixture",
            "AWS_EC2_METADATA_DISABLED": "true",
            "TIKTOK_DISPLAY_ACCESS_TOKEN": "offline-fixture",
            "TIKTOK_CCL_ACCESS_TOKEN": "offline-fixture",
        }.items():
            os.environ[key] = value
        server.start()
        host, port = server.get_host_and_port()
        with tempfile.TemporaryDirectory(prefix="sm-trends-benchmark-") as temporary:
            settings = Settings(
                database_url=scoped_url,
                s3_bucket=schema.replace("_", "-"),
                s3_endpoint_url=f"http://{host}:{port}",
                aws_default_region="us-east-1",
                cache_dir=Path(temporary) / "cache",
                spool_dir=Path(temporary) / "spool",
                collector_binary=str(Path("collector/target/debug/sm-collector").resolve()),
                history_partition_buckets=4,
            )
            with Pipeline(settings) as application:
                application.initialize(create_bucket=True)
                result = dispatch(application, args)
                result["verification_boundary"] = {
                    "database": application.catalog.one("SELECT version() AS version")["version"],
                    "object_store": "HTTP Moto 5.2.4",
                    "synthetic": True,
                    "production_acceptance": False,
                }
                args.output.write_text(canonical(result) + "\n")
                print(canonical(result))
                return 0 if result["ok"] else 1
    finally:
        server.stop()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    raise SystemExit(main())
