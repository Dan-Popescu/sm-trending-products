"""Measure actual native current-row/alert transactions in a disposable test schema.

Requires TEST_DATABASE_URL. Synthetic archive metadata replaces object staging;
the real archive SQL commit and all publication guards execute. There is no
object upload, source request, feature calculation or production dataset here.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import tempfile
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from sm_trending_products.assessment_archive import AssessmentArchive, AssessmentPublication
from sm_trending_products.assessment_publisher import AssessmentPublisher
from sm_trending_products.catalog import Catalog
from sm_trending_products.collector import redact
from sm_trending_products.contracts import AssessmentPolicy, canonical, digest, now
from sm_trending_products.history import History
from sm_trending_products.settings import Settings

BASELINE = "0f2500f"


def baseline_code():
    source = subprocess.check_output(
        ["git", "show", f"{BASELINE}:src/sm_trending_products/pipeline.py"], text=True
    )
    method = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "assess"
    )
    transaction = next(
        node
        for node in method.body
        if isinstance(node, ast.With) and "self._assessment_archive.commit" in ast.unparse(node)
    )
    return compile(ast.Module(body=[transaction], type_ignores=[]), "<immutable-baseline>", "exec")


class CountedConnection:
    def __init__(self, connection, counter):
        self.connection, self.counter = connection, counter

    def execute(self, query, parameters=None):
        self.counter.statements += 1
        return self.connection.execute(query, parameters)

    def __getattr__(self, name):
        return getattr(self.connection, name)


class CountedCatalog:
    def __init__(self, catalog):
        self.catalog, self.statements = catalog, 0

    @contextmanager
    def transaction(self):
        with self.catalog.transaction() as connection:
            yield CountedConnection(connection, self)


def prepare(catalog, count, serial, threshold):
    cutoff = now() + timedelta(microseconds=serial)
    body = {
        "schema_version": "2",
        "as_of": cutoff,
        "synthetic": True,
        "policy": AssessmentPolicy(alert_threshold=threshold).model_dump(mode="json"),
        "rows": [
            {
                "product_id": f"product-{i:06}",
                "name": f"Product {i}",
                "score": 80.0,
                "as_of": cutoff.isoformat(),
                "features": {"age_hours": i / 7},
                "evidence_refs": [],
                "detail": "Useful retained detail é 漢字 " * 20,
            }
            for i in reversed(range(count))
        ],
        "input_provenance": [],
    }
    encoded = canonical(body).encode()
    run_id = digest(body)
    key = f"assessments/synthetic/{run_id}.json"
    catalog.stage_derivation(key, ())
    catalog.execute(
        "UPDATE derived_publications SET object_sha256=%s,object_bytes=%s WHERE object_key=%s",
        (hashlib.sha256(encoded).hexdigest(), len(encoded), key),
    )
    return AssessmentPublication(
        run_id, body, {"key": key, "sha256": run_id, "bytes": len(encoded)}, ()
    )


def measure_case(catalog, archive, old, count, scenario, repetitions, serial):
    samples = {"baseline": [], "batched": []}
    for repeat in range(repetitions):
        for name in ("baseline", "batched") if repeat % 2 == 0 else ("batched", "baseline"):
            catalog.execute("TRUNCATE current_assessments,alerts")
            if scenario == "cooldown":
                seed = prepare(catalog, count, serial, 0)
                serial += 1
                AssessmentPublisher(catalog, archive).publish(seed)
            publication = prepare(catalog, count, serial, 100 if scenario == "no_alerts" else 0)
            serial += 1
            counter = CountedCatalog(catalog)
            cpu, wall = time.process_time(), time.perf_counter()
            if name == "baseline":
                exec(
                    old,
                    {
                        "self": SimpleNamespace(catalog=counter, _assessment_archive=archive),
                        "publication": publication,
                        "run_id": publication.run_id,
                        "rows": publication.body["rows"],
                        "as_of": publication.body["as_of"],
                        "synthetic": True,
                        "policy": AssessmentPolicy.model_validate(publication.body["policy"]),
                        "Jsonb": Jsonb,
                        "redact": redact,
                        "digest": digest,
                    },
                )
            else:
                AssessmentPublisher(counter, archive).publish(publication)
            sample = {
                "elapsed_ms": (time.perf_counter() - wall) * 1000,
                "client_cpu_ms": (time.process_time() - cpu) * 1000,
                "sql_execute_calls": counter.statements,
            }
            current = catalog.all("SELECT product_id,payload FROM current_assessments")
            assert {row["product_id"]: row["payload"] for row in current} == {
                row["product_id"]: row for row in publication.body["rows"]
            }
            assert catalog.one("SELECT count(*) AS n FROM alerts")["n"] == (
                0 if scenario == "no_alerts" else count
            )
            samples[name].append(sample)
    return {
        "products": count,
        "scenario": scenario,
        **{
            name: {
                "samples": values,
                **{
                    f"median_{field}": statistics.median(row[field] for row in values)
                    for field in values[0]
                },
            }
            for name, values in samples.items()
        },
    }, serial


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--products", type=int, nargs="+", default=[100, 1000])
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 10 or any(not 1 <= n <= 10000 for n in args.products):
        parser.error("Use1..10 repetitions and1..10000 products")
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        parser.error("Set TEST_DATABASE_URL to a dedicated native test database")
    schema = "assessment_publication_measure_" + uuid4().hex
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-c search_path={schema}"
    scoped = urlunsplit(
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
    try:
        with tempfile.TemporaryDirectory(prefix="publication-measure-") as directory:
            settings = Settings(
                database_url=scoped,
                cache_dir=Path(directory) / "cache",
                spool_dir=Path(directory) / "spool",
            )
            catalog = Catalog(scoped)
            try:
                catalog.initialize()
                catalog.execute(
                    "INSERT INTO products(product_id,name,synthetic) "
                    "SELECT 'product-'||lpad(n::text,6,'0'),'Product '||n,true "
                    "FROM generate_series(0,%s) n",
                    (max(args.products) - 1,),
                )
                archive = AssessmentArchive(catalog, History(settings, catalog))
                old, cases, serial = baseline_code(), [], 0
                for count in args.products:
                    for scenario in ("first_alerts", "cooldown", "no_alerts"):
                        case, serial = measure_case(
                            catalog, archive, old, count, scenario, args.repetitions, serial
                        )
                        cases.append(case)
                result = {
                    "baseline_commit": subprocess.check_output(
                        ["git", "rev-parse", BASELINE], text=True
                    ).strip(),
                    "scope": (
                        "Actual native archive/current/alert SQL transaction. Synthetic metadata "
                        "only; no S3 staging, source collection or feature computation. "
                        "execute-call count includes publication guards and excludes transaction "
                        "timeout setters. Client process CPU excludes PostgreSQL server CPU; "
                        "RSS is cumulative, not VPS consumption."
                    ),
                    "python": platform.python_version(),
                    "postgresql": catalog.one("SELECT version() AS version")["version"],
                    "implementation_sha256": hashlib.sha256(
                        Path(__file__)
                        .resolve()
                        .parents[1]
                        .joinpath("src/sm_trending_products/assessment_publisher.py")
                        .read_bytes()
                    ).hexdigest(),
                    "cases": cases,
                    "all_current_payloads_exact": True,
                    "all_alert_counts_exact": True,
                    "process_cumulative_peak_rss_kib": resource.getrusage(
                        resource.RUSAGE_SELF
                    ).ru_maxrss,
                }
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(result, indent=2) + "\n")
                print(json.dumps({"output": str(args.output), "cases": len(cases)}))
            finally:
                catalog.close()
    finally:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
