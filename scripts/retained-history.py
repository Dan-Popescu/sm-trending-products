"""Bounded real PostgreSQL/HTTP-object-store retained-history measurement.

The supervisor owns an isolated schema and working directory. Its child streams
synthetic history through the normal import/publication path; no SQL observations
or analytical results are fabricated to make a large workload look complete.
"""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import importlib.metadata
import importlib.util
import json
import logging
import os
import pstats
import resource
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "retained_runner", ROOT / "scripts/offline-benchmark.py"
)
assert SPEC and SPEC.loader
HELPER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPER)
GIB = 1024**3
MOTO_KEY_BUFFER_BYTES = 32768


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", type=int, default=1000)
    parser.add_argument("--videos-per-product", type=int, default=20)
    parser.add_argument("--history-days", type=int, choices=(30, 90), default=30)
    parser.add_argument("--refreshes-per-day", type=int, default=4)
    parser.add_argument("--refresh-waves", type=int, default=2)
    parser.add_argument("--seed-batch-rows", type=int, default=5000)
    parser.add_argument("--shared-products-per-import", type=int, default=1)
    parser.add_argument("--hot-products", type=int, default=0)
    parser.add_argument("--hot-product-batch-rows", type=int, default=100)
    parser.add_argument("--analysis-product-batch-size", type=int, default=10)
    parser.add_argument("--cache-bytes", type=int, default=2_000_000_000)
    parser.add_argument("--max-rss-bytes", type=int, default=2 * GIB)
    parser.add_argument("--min-disk-free-bytes", type=int, default=2 * GIB)
    parser.add_argument("--max-owned-disk-bytes", type=int, default=24 * GIB)
    parser.add_argument(
        "--deadline-seconds",
        type=float,
        default=14400,
        help="Wall-clock guard for the entire case (default: 4 hours)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New report path; existing reports are never overwritten",
    )
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    for name, minimum, maximum in (
        ("products", 1, 10000),
        ("videos_per_product", 1, 100),
        ("refreshes_per_day", 1, 24),
        ("refresh_waves", 0, 24),
        ("seed_batch_rows", 100, 10000),
        ("shared_products_per_import", 1, 100),
        ("hot_products", 0, 10000),
        ("hot_product_batch_rows", 100, 10000),
        ("analysis_product_batch_size", 1, 100),
        ("cache_bytes", 1_000_000, 4 * GIB),
        ("max_rss_bytes", 100_000_000, 8 * GIB),
        ("min_disk_free_bytes", 100_000_000, 100 * GIB),
        ("max_owned_disk_bytes", 100_000_000, 100 * GIB),
        ("deadline_seconds", 1, 14400),
    ):
        if not minimum <= getattr(args, name) <= maximum:
            parser.error(f"--{name.replace('_', '-')} must be between {minimum} and {maximum}")
    if 24 % args.refreshes_per_day:
        parser.error("--refreshes-per-day must divide 24")
    if args.hot_products > args.products:
        parser.error("--hot-products cannot exceed --products")
    return args


def fact(product, video, point, cutoff, samples, hours):
    observed = cutoff + timedelta(hours=hours * (point - samples + 1))
    return {
        "observation_id": hashlib.sha256(
            f"retained:{product}:{video}:{point}".encode()
        ).hexdigest(),
        "source": "retained_fixture",
        "entity_type": "video",
        "entity_id": f"p{product}-v{video}",
        "observed_at": observed.isoformat(),
        "available_at": observed.isoformat(),
        "published_at": (cutoff - timedelta(days=100)).isoformat(),
        "synthetic": True,
        "metrics": {"view_count": {"lower": 1000 + point * 100, "upper": 1000 + point * 100}},
        "attributes": {"creator_id": f"creator-{product}-{video % 7}"},
    }


def adapter_budget(args):
    """Reject known disk-spool FD overload before allocating any test state."""
    samples = args.history_days * args.refreshes_per_day
    rows = samples * args.videos_per_product
    hot_datasets = args.hot_products * (
        (rows + args.hot_product_batch_rows - 1) // args.hot_product_batch_rows
    )
    groups, remainder = divmod(args.products - args.hot_products, args.shared_products_per_import)
    grouped_datasets = groups * (
        (rows * args.shared_products_per_import + args.seed_batch_rows - 1) // args.seed_batch_rows
    )
    remainder_datasets = (remainder * rows + args.seed_batch_rows - 1) // args.seed_batch_rows
    datasets = hot_datasets + grouped_datasets + remainder_datasets
    # Upper bound: every source raw, normalized object and manifest spills.
    # Provenance's day leaves fit in RAM for the ordinary 20-video/4-daily shape;
    # larger leaves conservatively reserve an FD for every retained leaf/version.
    leaf_bytes = 2048 + args.videos_per_product * args.refreshes_per_day * 68
    leaves = (
        0 if leaf_bytes <= MOTO_KEY_BUFFER_BYTES else args.products * 31 * (args.refresh_waves + 1)
    )
    expected = 3 * datasets + leaves + 1024
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft != resource.RLIM_INFINITY and expected > soft:
        raise ValueError(
            f"Requested fixture may need {expected} open descriptors; soft limit is {soft}. "
            "Use a separately provisioned native object store for this workload. "
            "The harness does not raise system limits or reduce requested data."
        )
    return {
        "key_buffer_bytes": MOTO_KEY_BUFFER_BYTES,
        "estimated_source_datasets": datasets,
        "estimated_spill_fd_upper_bound": expected,
        "soft_fd_limit": soft,
        "hard_fd_limit": hard,
    }


def inventory(pipeline):
    from sm_trending_products.contracts import canonical

    result = {
        "datasets": pipeline.catalog.one(
            "SELECT count(*) AS count,COALESCE(sum(row_count),0) AS observations "
            "FROM datasets WHERE state='committed'"
        ),
        "normalized": pipeline.catalog.one(
            "SELECT count(*) AS objects,COALESCE(sum((descriptor->>'bytes')::bigint),0) AS bytes "
            "FROM normalized_objects"
        ),
        "raw": pipeline.catalog.one(
            "SELECT count(*) AS objects,COALESCE(sum((manifest->'raw'->>'bytes')::bigint),0) "
            "AS bytes FROM datasets WHERE state='committed'"
        ),
        "derivations": pipeline.catalog.one(
            "SELECT count(*) AS objects,COALESCE(sum(object_bytes),0) AS bytes "
            "FROM derived_publications WHERE state='committed'"
        ),
        "sql": pipeline.catalog.one(
            "SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0) AS total_bytes "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=current_schema() AND c.relkind='r'"
        ),
        "wal_lsn": pipeline.catalog.one("SELECT pg_current_wal_lsn()::text AS value")["value"],
    }
    for value in result.values():
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, Decimal):
                    value[key] = int(item)
    manifest_bytes = 0
    manifest_objects = 0
    with pipeline.catalog.transaction() as connection:
        for row in connection.execute("SELECT manifest FROM datasets WHERE state='committed'"):
            manifest_bytes += len(canonical(row["manifest"]).encode())
            manifest_objects += 1
    result["manifests"] = {"objects": manifest_objects, "canonical_bytes": manifest_bytes}
    return result


def cache_files(directory):
    return sum(path.stat().st_size for path in directory.iterdir() if path.is_file())


def evict_files(directory):
    for path in directory.iterdir():
        if path.is_file() and not path.name.startswith("."):
            path.unlink()


def open_spool_snapshot(pid, directory):
    """Account for Moto's unlinked disk spool files, absent from directory walks."""
    total = 0
    try:
        descriptors = list((Path("/proc") / str(pid) / "fd").iterdir())
    except OSError:
        return {"bytes": None, "open_file_descriptors": None}
    for descriptor in descriptors:
        try:
            target = os.readlink(descriptor)
            if target.startswith(str(directory)) and target.endswith(" (deleted)"):
                total += descriptor.stat().st_blocks * 512
        except OSError:
            continue
    return {"bytes": total, "open_file_descriptors": len(descriptors)}


class DirectoryAccountant:
    """Scan at most 2,500 entries per poll; free-space/deadline checks stay cheap."""

    def __init__(self, directory):
        self.directory = directory
        self.paths = None
        self.last_complete = 0.0
        self.complete_bytes = 0
        self.scanned_bytes = 0
        self.seen = set()

    def sample(self):
        if self.paths is None and time.monotonic() - self.last_complete >= 30:
            self.paths = self.directory.rglob("*")
            self.scanned_bytes = 0
            self.seen = set()
        if self.paths is not None:
            for _ in range(2500):
                try:
                    path = next(self.paths)
                except StopIteration:
                    self.paths = None
                    self.last_complete = time.monotonic()
                    self.complete_bytes = self.scanned_bytes
                    self.seen.clear()
                    break
                try:
                    info = path.lstat()
                except OSError:
                    continue
                identity = (info.st_dev, info.st_ino)
                if identity not in self.seen:
                    self.seen.add(identity)
                    self.scanned_bytes += info.st_blocks * 512
        return {
            "bytes": max(self.complete_bytes, self.scanned_bytes),
            "complete_pass_age_seconds": (
                time.monotonic() - self.last_complete if self.last_complete else None
            ),
            "scan_in_progress": self.paths is not None,
            "scope": "rolling allocated blocks; not an atomic current snapshot",
        }


def worker(config_path):
    # Moto keeps small nodes in RAM and spills larger objects to owned disk files.
    # Both the FD budget and actual process RSS are guarded; neither is S3 sizing.
    os.environ["MOTO_S3_DEFAULT_KEY_BUFFER_SIZE"] = str(MOTO_KEY_BUFFER_BYTES)
    config = json.loads(config_path.read_text())
    if (
        hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        != config["report"]["harness_sha256"]
    ):
        raise ValueError("Harness source changed between supervision and worker startup")
    directory = Path(config["directory"])
    tempfile.tempdir = str(directory / "object-store")
    Path(tempfile.tempdir).mkdir()
    from moto.server import ThreadedMotoServer

    from sm_trending_products.contracts import canonical, now
    from sm_trending_products.pipeline import Pipeline
    from sm_trending_products.settings import Settings

    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    output = Path(config["output"])
    report = config["report"]
    settings = Settings(
        database_url=config["database_url"],
        s3_endpoint_url=f"http://127.0.0.1:{port}",
        s3_bucket="retained-" + uuid4().hex,
        cache_dir=directory / "cache",
        spool_dir=directory / "spool",
        cache_max_bytes=config["cache_bytes"],
        analysis_product_batch_size=config["analysis_product_batch_size"],
        history_target_rows_per_file=10000,
    )
    cutoff = now() + timedelta(hours=4)
    samples = config["history_days"] * config["refreshes_per_day"]
    hours = 24 // config["refreshes_per_day"]

    def publish():
        report["updated_at"] = now().isoformat()
        HELPER.write_report(output, report)

    try:
        with Pipeline(settings) as pipeline:
            pipeline.initialize(create_bucket=True)
            report["postgresql"] = pipeline.catalog.one("SELECT version() AS version")["version"]
            report["cutoff"] = cutoff.isoformat()
            seed_start = time.perf_counter()
            product_start = 0
            while product_start < config["products"]:
                hot = product_start < config["hot_products"]
                group_size = 1 if hot else config["shared_products_per_import"]
                group = range(product_start, min(config["products"], product_start + group_size))
                batch_limit = config["hot_product_batch_rows"] if hot else config["seed_batch_rows"]
                for product in group:
                    pipeline.create_product(f"p{product}", f"Product {product}", synthetic=True)
                chunk = []
                for product in group:
                    for point in range(samples):
                        for video in range(config["videos_per_product"]):
                            chunk.append(fact(product, video, point, cutoff, samples, hours))
                            if len(chunk) == batch_limit:
                                pipeline.import_records(chunk, synthetic=True)
                                chunk = []
                if chunk:
                    pipeline.import_records(chunk, synthetic=True)
                for product in group:
                    pipeline.identity.reassign_entities(
                        [
                            f"retained_fixture:video:p{product}-v{i}"
                            for i in range(config["videos_per_product"])
                        ],
                        f"p{product}",
                        method="retained-measurement-reviewed-v1",
                    )
                    report["seeded_products"] = product + 1
                    report["seeded_observations"] = (
                        (product + 1) * samples * config["videos_per_product"]
                    )
                product_start += len(group)
                if product_start % 10 == 0 or product_start == config["products"]:
                    report["seed_seconds"] = time.perf_counter() - seed_start
                    publish()
                    print(f"seeded {product_start}/{config['products']} products", flush=True)
            report["seed_inventory"] = inventory(pipeline)
            expected = config["products"] * samples * config["videos_per_product"]
            if report["seed_inventory"]["datasets"]["observations"] != expected:
                raise ValueError(
                    "Persisted retained observation count differs from requested workload"
                )
            report["seed_metrics"] = dict(pipeline.history.metrics)

            def phase(name, at):
                report["current_phase"] = name
                publish()
                before = dict(pipeline.history.metrics)
                analytical_before = dict(pipeline.history.analysis_metrics)
                start = time.perf_counter()
                cpu_start = time.process_time()
                profile = (
                    cProfile.Profile()
                    if config["profile"] and (name == "cold" or name.startswith("refresh_"))
                    else None
                )
                if profile:
                    profile.enable()
                payload = pipeline.assess(at, True)
                if profile:
                    profile.disable()
                    profile_path = output.with_suffix(f".{name}.profile")
                    profile.dump_stats(profile_path)
                    stats = pstats.Stats(profile)
                    report.setdefault("profiles", {})[name] = {
                        "path": str(profile_path),
                        "top_cumulative": [
                            {
                                "function": f"{key[0]}:{key[1]}:{key[2]}",
                                "calls": value[1],
                                "seconds": value[3],
                            }
                            for key, value in sorted(
                                stats.stats.items(), key=lambda item: item[1][3], reverse=True
                            )[:30]
                        ],
                    }
                rows = payload["rows"]
                if len(rows) != config["products"] or {row["product_id"] for row in rows} != {
                    f"p{index}" for index in range(config["products"])
                }:
                    raise ValueError("Assessment silently omitted requested products")
                normalized = sum(row["coverage"]["observations"] for row in rows)
                wave = int((at - cutoff).total_seconds() // (hours * 3600))
                expected_selected = (
                    min(samples + wave, 30 * config["refreshes_per_day"] + 1)
                    * config["products"]
                    * config["videos_per_product"]
                )
                if normalized != expected_selected:
                    raise ValueError(
                        "Selected analytical observation count differs from the declared window"
                    )
                seconds = time.perf_counter() - start
                cpu_seconds = time.process_time() - cpu_start
                # Hash outside the timed assessment, excluding the API-only run_id.
                body = {key: value for key, value in payload.items() if key != "run_id"}
                publication_sha256 = hashlib.sha256(canonical(body).encode()).hexdigest()
                if publication_sha256 != payload["run_id"]:
                    raise ValueError("Assessment run ID differs from its published body hash")
                report["phases"][name] = {
                    "seconds": seconds,
                    "cpu_process_seconds": cpu_seconds,
                    "publication_sha256": publication_sha256,
                    "metrics": {
                        key: pipeline.history.metrics[key] - value for key, value in before.items()
                    },
                    "analysis_work": {
                        key: pipeline.history.analysis_metrics[key] - value
                        for key, value in analytical_before.items()
                    },
                    "rows": len(rows),
                    "selected_observations": normalized,
                    "assessment_bytes": len(canonical(payload).encode()),
                    "cache_bytes": cache_files(settings.cache_dir),
                    "inventory": inventory(pipeline),
                }
                publish()
                print(f"phase {name}: {report['phases'][name]['seconds']:.3f}s", flush=True)
                return payload

            def sampled_oracle(payload, at):
                from sm_trending_products.analytics import assess
                from sm_trending_products.contracts import Product, ProductLink

                selected_ids = {"p0", f"p{config['products'] // 2}", f"p{config['products'] - 1}"}
                products = [
                    Product.model_validate(row)
                    for row in pipeline._all_products(True, at)
                    if row["product_id"] in selected_ids
                ]
                links = [
                    ProductLink.model_validate(row)
                    for row in pipeline.links(at, True)
                    if row["product_id"] in selected_ids
                ]
                observations, _ = pipeline.history.observations(
                    at,
                    True,
                    entity_keys={row.entity_key for row in links},
                    since=at - timedelta(days=30),
                    until=at,
                )
                expected = assess(observations, products, links, at, True)

                def expand(value):
                    if isinstance(value, dict):
                        if value.get("format") == "sharded-provenance-v1":
                            refs = []
                            for offset in range(0, value["count"], 200):
                                refs.extend(
                                    pipeline.provenance.page(value, limit=200, offset=offset)[
                                        "references"
                                    ]
                                )
                            return sorted(refs)
                        return {key: expand(child) for key, child in value.items()}
                    if isinstance(value, list):
                        return [expand(child) for child in value]
                    return value

                actual = [
                    expand(deepcopy(row))
                    for row in payload["rows"]
                    if row["product_id"] in selected_ids
                ]
                actual.sort(key=lambda row: row["product_id"])
                expected.sort(key=lambda row: row["product_id"])
                if canonical(actual) != canonical(expected):
                    raise ValueError(
                        "Sampled full scalar/rank/reference oracle differs from publication"
                    )
                report["checks"]["sampled_complete_oracle"] = True
                report["oracle"] = {
                    "products": sorted(selected_ids),
                    "scope": (
                        "complete rows and reference IDs; equal trajectory fixture "
                        "makes sampled cohort rank identical"
                    ),
                }
                publish()

            evict_files(settings.cache_dir)
            initial = phase("cold", cutoff)
            warm = phase("warm_same_cutoff", cutoff)
            if canonical(initial) != canonical(warm):
                raise ValueError("Warm result differs from exact cold publication")
            report["checks"]["cold_warm_exact_publication"] = True
            last_publication_sha256 = report["phases"]["cold"]["publication_sha256"]
            sampled_oracle(initial, cutoff)
            del initial, warm
            for wave in range(config["refresh_waves"]):
                wave_name = f"refresh_{wave + 1}"
                report["current_phase"] = f"{wave_name}_import"
                before_import = dict(pipeline.history.metrics)
                import_start = time.perf_counter()
                import_cpu_start = time.process_time()
                progress = {
                    "requested_products": config["products"],
                    "requested_observations": config["products"] * config["videos_per_product"],
                    "completed_products": 0,
                    "imported_observations": 0,
                }
                report.setdefault("refresh_imports", {})[wave_name] = progress
                publish()
                for product in range(config["products"]):
                    pipeline.import_records(
                        [
                            fact(product, video, samples + wave, cutoff, samples, hours)
                            for video in range(config["videos_per_product"])
                        ],
                        synthetic=True,
                    )
                    progress["completed_products"] = product + 1
                    progress["imported_observations"] = (product + 1) * config["videos_per_product"]
                    if (product + 1) % 100 == 0 or product + 1 == config["products"]:
                        progress["seconds"] = time.perf_counter() - import_start
                        progress["cpu_process_seconds"] = time.process_time() - import_cpu_start
                        progress["metrics"] = {
                            key: pipeline.history.metrics[key] - value
                            for key, value in before_import.items()
                        }
                        publish()
                at = cutoff + timedelta(hours=hours * (wave + 1))
                changed = phase(wave_name, at)
                last_publication_sha256 = report["phases"][wave_name]["publication_sha256"]
                sampled_oracle(changed, at)
                del changed
                report["refresh_waves_completed"] = wave + 1
                publish()
            evict_files(settings.cache_dir)
            pipeline.catalog.execute("DELETE FROM feature_product_projections")
            phase("evicted", cutoff + timedelta(hours=hours * config["refresh_waves"]))
            if report["phases"]["evicted"]["publication_sha256"] != last_publication_sha256:
                raise ValueError("Evicted result differs from the last publication at that cutoff")
            report["checks"]["evicted_exact_publication"] = True
            report["checks"]["cache_budget"] = (
                cache_files(settings.cache_dir) <= settings.cache_max_bytes
            )
            report["final_inventory"] = inventory(pipeline)
            report["total_storage_metrics"] = dict(pipeline.history.metrics)
            report["ok"] = all(report["checks"].values())
            report["status"] = "complete" if report["ok"] else "failed_checks"
            publish()
    except Exception as error:  # noqa: BLE001 - Keep a partial report on an isolated measurement failure.
        report["ok"] = False
        report["status"] = "worker_error"
        report["error_type"] = type(error).__name__
        publish()
        raise
    finally:
        server.stop()


def run(args):
    adapter = adapter_budget(args)
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        raise ValueError("Set TEST_DATABASE_URL to a task-owned native PostgreSQL test database")
    output = args.output.resolve()
    if output.exists():
        raise ValueError("Choose a new --output path; existing reports are never overwritten")
    schema = "retained_probe_" + uuid4().hex
    output.parent.mkdir(parents=True, exist_ok=True)
    harness_source = Path(__file__).read_bytes()
    with tempfile.TemporaryDirectory(prefix="retained-", dir=output.parent) as temporary:
        directory = Path(temporary)
        report = {
            "ok": False,
            "status": "starting",
            "synthetic": True,
            "test_adapter_budget": adapter,
            "requested": {
                key: value for key, value in vars(args).items() if key not in {"output", "worker"}
            },
            "scope": {
                "source": "synthetic explicit imports, not live HTTP collection",
                "object_store": "HTTP Moto, 32 KiB RAM/spool threshold; no provider latency claim",
                "layout": (
                    "bounded product-major retained timelines; configured groups share imports; "
                    "not one object per upstream request"
                ),
                "analytical_lookback_days": 30,
                "stack": (
                    "application and Moto same child; PostgreSQL resources separately reported; "
                    "Dagster/dashboard not running"
                ),
                "postgresql_resources": (
                    "schema relation bytes measured; database server CPU/RSS and per-schema WAL "
                    "attribution unavailable"
                ),
                "wal_lsn_scope": "server-global position; concurrent schemas may contribute",
                "observed_history_span_hours": (args.history_days * args.refreshes_per_day - 1)
                * (24 // args.refreshes_per_day),
            },
            "hardware": HELPER.hardware_facts(directory),
            "versions": {
                name: importlib.metadata.version(name)
                for name in ("moto", "polars", "pyarrow", "psycopg", "boto3", "pydantic")
            },
            "commit": HELPER.command(["git", "rev-parse", "HEAD"]),
            "harness_sha256": hashlib.sha256(harness_source).hexdigest(),
            "seeded_products": 0,
            "seeded_observations": 0,
            "refresh_waves_completed": 0,
            "phases": {},
            "checks": {},
        }
        HELPER.write_report(output, report)
        config = {
            **vars(args),
            "output": str(output),
            "directory": temporary,
            "database_url": HELPER.scoped_database_url(database_url, schema),
            "report": report,
        }
        config_path = directory / "configuration.json"
        environment = dict(os.environ)
        environment.update(
            AWS_ACCESS_KEY_ID="retained-fixture",
            AWS_SECRET_ACCESS_KEY="retained-fixture",
            AWS_EC2_METADATA_DISABLED="true",
            TMPDIR=str(directory),
        )
        process = None
        accountant = DirectoryAccountant(directory)
        started = time.monotonic()
        samples = []
        stop_reason = None
        schema_created = False
        pending_error = None
        try:
            with output.with_suffix(".harness.py").open("xb") as archive:
                archive.write(harness_source)
            config_path.write_text(json.dumps(config, default=str))
            config_path.chmod(0o600)
            with psycopg.connect(database_url, autocommit=True) as connection:
                connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                schema_created = True
            with output.with_suffix(".log").open("w") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        __file__,
                        "--worker",
                        str(config_path),
                        "--output",
                        str(output),
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                )
                while process.poll() is None:
                    resources = HELPER.session_resources(process.pid, {process.pid})
                    disk_observer = accountant.sample()
                    owned = disk_observer["bytes"]
                    database = HELPER.database_snapshot(config["database_url"])
                    spool = open_spool_snapshot(process.pid, directory)
                    spooled = spool["bytes"]
                    sample = {
                        "elapsed_seconds": time.monotonic() - started,
                        "process": resources,
                        "owned_disk_bytes": owned,
                        "owned_disk_observer": disk_observer,
                        "free_disk_bytes": shutil.disk_usage(directory).free,
                        "database": database,
                        "unlinked_object_spool_bytes": spooled,
                        "open_file_descriptors": spool["open_file_descriptors"],
                    }
                    samples.append(sample)
                    if sample["elapsed_seconds"] > args.deadline_seconds:
                        stop_reason = "deadline"
                    elif resources["discovered_rss_bytes"] > args.max_rss_bytes:
                        stop_reason = "process_rss_limit"
                    elif (
                        owned + (database.get("schema_bytes") or 0) + (spooled or 0)
                        > args.max_owned_disk_bytes
                    ):
                        stop_reason = "owned_disk_limit"
                    elif sample["free_disk_bytes"] < args.min_disk_free_bytes:
                        stop_reason = "disk_headroom_limit"
                    if stop_reason:
                        HELPER.stop_session(process)
                        break
                    time.sleep(1)
            report = HELPER.read_report(output) or report
            if stop_reason:
                report.update(ok=False, status=stop_reason)
            elif process.returncode != 0 and report.get("status") != "worker_error":
                report.update(ok=False, status="worker_exit")
            report["supervisor"] = {
                "exit_code": process.returncode,
                "elapsed_seconds": time.monotonic() - started,
                "peak_discovered_rss_bytes": max(
                    (s["process"]["discovered_rss_bytes"] for s in samples), default=0
                ),
                "peak_owned_disk_bytes": max((s["owned_disk_bytes"] for s in samples), default=0),
                "samples": samples,
            }
        except BaseException as error:  # Preserve interruption after owned-state cleanup.
            pending_error = error
            report = HELPER.read_report(output) or report
            report.update(
                ok=False,
                status="preparation_error" if process is None else "supervisor_error",
                error_type=type(error).__name__,
            )
        finally:
            if process is not None and process.poll() is None:
                HELPER.stop_session(process)
            if schema_created:
                with psycopg.connect(database_url, autocommit=True) as connection:
                    connection.execute(
                        sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                    )
            report["cleanup"] = {"schema_removed": True, "working_directory_removed": False}
            HELPER.write_report(output, report)
    report["cleanup"]["working_directory_removed"] = not directory.exists()
    HELPER.write_report(output, report)
    if pending_error is not None:
        raise pending_error
    return report


if __name__ == "__main__":
    options = arguments()
    if options.worker:
        worker(options.worker)
    else:
        result = run(options)
        print(
            json.dumps(
                {"ok": result["ok"], "status": result["status"], "output": str(options.output)}
            )
        )
        raise SystemExit(0 if result["ok"] else 1)
