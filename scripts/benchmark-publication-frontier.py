"""Measure native publication selection against isolated, metadata-only history.

This creates and drops one private schema in TEST_DATABASE_URL. Superuser access
is needed to bypass publication triggers only while seeding synthetic metadata;
the rows are not complete source captures or an observation-throughput fixture.
No HTTP source, object store, source credentials or deployed data is used.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from psycopg import sql

from sm_trending_products.catalog import Catalog
from sm_trending_products.raw_capture import RawCaptureService, _publication_claim_sql
from sm_trending_products.settings import Settings

BASELINE = "ac5ddcb9e8713e311b6772528e48b0ee8ade9b33"


def baseline_query() -> str:
    content = subprocess.run(
        ["git", "show", f"{BASELINE}:src/sm_trending_products/raw_capture.py"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    candidates = [
        node.value
        for node in ast.walk(ast.parse(content))
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith(
            "SELECT d.dataset_id FROM datasets d LEFT JOIN capture_publication_leases"
        )
    ]
    if len(candidates) != 1:
        raise ValueError("The immutable baseline must contain exactly one publication claim")
    return candidates[0]


def scoped_url(database_url: str, schema: str) -> str:
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-c search_path={schema}"
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, quote_via=quote),
            parsed.fragment,
        )
    )


def samples(operation, repetitions):
    values = []
    for _ in range(repetitions):
        started = time.perf_counter()
        assert operation() is None
        values.append((time.perf_counter() - started) * 1000)
    return {
        "median_ms": statistics.median(values),
        "p95_ms": sorted(values)[min(len(values) - 1, int(len(values) * 0.95))],
        "samples_ms": values,
    }


def explain(catalog, query, parameters):
    with catalog.transaction() as connection:
        return connection.execute(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, parameters
        ).fetchone()["QUERY PLAN"][0]


def seed(catalog, parents):
    started = time.perf_counter()
    with catalog.transaction() as connection:
        connection.execute("SET LOCAL statement_timeout='120s'")
        connection.execute("TRUNCATE datasets CASCADE")
        connection.execute("SET LOCAL session_replication_role=replica")
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
            "SELECT md5('parent-'||n)||md5('parent-'||n),'publication_fixture',true,'committed',"
            '\'{"kind":"raw_capture","acquisition_protocol":"1"}\'::jsonb '
            "FROM generate_series(1,%s) n",
            (parents,),
        )
        connection.execute(
            "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest,supersedes) "
            "SELECT md5('child-'||n)||md5('child-'||n),'publication_fixture',true,'committed',"
            "'{}'::jsonb,md5('parent-'||n)||md5('parent-'||n) "
            "FROM generate_series(1,%s) n",
            (parents,),
        )
    with catalog.transaction() as connection:
        connection.execute("ANALYZE datasets")
        connection.execute("ANALYZE capture_publication_pending")
        connection.execute("ANALYZE capture_publication_leases")
    return time.perf_counter() - started


def run_case(catalog, acquisition, old_query, parents, repetitions):
    seeded = seed(catalog, parents)
    baseline = samples(lambda: catalog.one(old_query, (True,)), repetitions)
    treatment_sql = samples(
        lambda: catalog.one(_publication_claim_sql(True), (True, True)), repetitions
    )
    treatment_method = samples(lambda: acquisition._claim_publication(True), repetitions)
    result = {
        "completed_parents": parents,
        "completed_v1_children": parents,
        "pending_parents": 0,
        "seed_seconds": seeded,
        "baseline_sql": baseline,
        "treatment_sql": treatment_sql,
        "treatment_actual_method": treatment_method,
        "baseline_explain": explain(catalog, old_query, (True,)),
        "treatment_explain": explain(catalog, _publication_claim_sql(True), (True, True)),
    }
    # Reproduce a formerly large backlog whose rows completed before ANALYZE.
    # The deliberately reconstructed-and-deleted projection is metadata only;
    # authoritative completed parents/children remain unchanged throughout.
    prior_backlog = min(parents, 5000)
    catalog.execute(
        "INSERT INTO capture_publication_pending(dataset_id,synthetic,created_at) "
        "SELECT dataset_id,synthetic,created_at FROM datasets "
        "WHERE manifest->>'kind'='raw_capture' LIMIT %s",
        (prior_backlog,),
    )
    catalog.execute("ANALYZE capture_publication_pending")
    catalog.execute("DELETE FROM capture_publication_pending")
    result["stale_statistics_prior_backlog"] = prior_backlog
    result["stale_statistics_actual_method"] = samples(
        lambda: acquisition._claim_publication(True), repetitions
    )
    result["stale_statistics_explain"] = explain(
        catalog, _publication_claim_sql(True), (True, True)
    )
    result["storage_bytes"] = catalog.one(
        "SELECT pg_total_relation_size('capture_publication_pending') AS pending_projection,"
        "pg_relation_size('capture_committed_normalization_parent') AS child_parent_index"
    )
    return result


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Example: TEST_DATABASE_URL=<isolated-native-test-url> PYTHONPATH=src "
            "python scripts/benchmark-publication-frontier.py --parents 1000 500000 "
            "--repetitions 15 --output output/publication-frontier.json"
        ),
    )
    parser.add_argument("--parents", type=int, nargs="+", default=[1000, 50000, 500000])
    parser.add_argument("--repetitions", type=int, default=15)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if not arguments.parents or any(not 1 <= value <= 500000 for value in arguments.parents):
        parser.error("--parents values must be between 1 and 500000")
    if not 3 <= arguments.repetitions <= 100:
        parser.error("--repetitions must be between 3 and 100")
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        parser.error("TEST_DATABASE_URL is required; no deployed database is selected implicitly")
    old_query = baseline_query()
    schema = "publication_frontier_benchmark_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    catalog = Catalog(scoped_url(database_url, schema))
    acquisition = None
    report = {
        "baseline_commit": BASELINE,
        "python": platform.python_version(),
        "fixture": (
            "metadata-only; publication triggers bypassed only during private-schema seeding"
        ),
        "included": (
            "native PostgreSQL exact SQL and actual Python claim "
            "including namespace guard/schema check"
        ),
        "excluded": (
            "source HTTP, S3, normalizer, observations, whole-stack CPU/RAM and VPS capacity"
        ),
        "cases": [],
    }
    try:
        catalog.initialize()
        with tempfile.TemporaryDirectory(prefix="publication-frontier-") as directory:
            settings = Settings(
                database_url=scoped_url(database_url, schema),
                cache_dir=Path(directory) / "cache",
                spool_dir=Path(directory) / "spool",
            )
            acquisition = RawCaptureService(settings, catalog, None, None, "publication-fixture")
            report["postgresql"] = catalog.one("SELECT version() AS version")["version"]
            for parents in arguments.parents:
                case = run_case(catalog, acquisition, old_query, parents, arguments.repetitions)
                report["cases"].append(case)
                arguments.output.parent.mkdir(parents=True, exist_ok=True)
                arguments.output.write_text(json.dumps(report, indent=2) + "\n")
                print(
                    json.dumps(
                        {
                            "parents": parents,
                            "baseline_sql_median_ms": case["baseline_sql"]["median_ms"],
                            "treatment_method_median_ms": case["treatment_actual_method"][
                                "median_ms"
                            ],
                            "treatment_method_p95_ms": case["treatment_actual_method"]["p95_ms"],
                        }
                    ),
                    flush=True,
                )
    finally:
        if acquisition is not None:
            acquisition.close()
        catalog.close()
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
