"""Read-only SQL behavior over actual normalized Parquet files."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from sm_trending_products.settings import Settings

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def settings(tmp_path: Path) -> Settings:
    cache = tmp_path / "cache"
    cache.mkdir(exist_ok=True)
    return Settings(
        database_url="postgresql://unused",
        cache_dir=cache,
        spool_dir=tmp_path / "spool",
        duckdb_memory_limit="64MB",
    )


def normalized(path: Path, *, dataset_id: str = "first", views: int = 100) -> None:
    pl.DataFrame(
        [
            {
                "schema_version": "1",
                "observation_id": "o1",
                "source": "tiktok_display",
                "entity_type": "video",
                "entity_id": "v1",
                "entity_key": "tiktok_display:video:v1",
                "observed_at": BASE.isoformat(),
                "available_at": BASE.isoformat(),
                "published_at": None,
                "synthetic": True,
                "market": None,
                "account_id": "authorized-account",
                "raw_dataset_id": dataset_id,
                "metrics_json": '{"view_count":{"lower":'
                + str(views)
                + ',"upper":'
                + str(views)
                + ',"precision":"exact"}}',
                "attributes_json": '{"title":"Desk lamp"}',
                "missing_fields_json": "{}",
            }
        ]
    ).write_parquet(path)


def test_query_reads_metadata_and_json_without_mutating_evidence(tmp_path: Path) -> None:
    from sm_trending_products.exploration import ParquetInput, query_parquet

    config = settings(tmp_path)
    path = config.cache_dir / "record.parquet"
    normalized(path)
    result = query_parquet(
        [ParquetInput(path, "first", BASE)],
        "SELECT entity_id, CAST(json_extract(metrics_json,'$.view_count.lower') AS BIGINT) "
        "AS views FROM observations; -- trailing comment",
        config,
        max_rows=10,
    )
    assert result["columns"] == ["entity_id", "views"]
    assert result["rows"] == [["v1", 100]]
    assert result["truncated"] is False


def test_results_are_limited_in_native_sql_and_report_truncation(tmp_path: Path) -> None:
    from sm_trending_products.exploration import query_parquet

    result = query_parquet(
        [],
        "WITH values AS (SELECT * FROM range(1000000000)) SELECT * FROM values",
        settings(tmp_path),
        max_rows=3,
    )
    assert result["rows"] == [[0], [1], [2]]
    assert result["truncated"] is True
    assert result["row_count"] == 3


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1; SELECT 2",
        "CREATE TABLE forbidden(x INTEGER)",
        "COPY (SELECT 1) TO '/tmp/forbidden-exploration-output.csv'",
        "PRAGMA version",
        "SET enable_external_access=true",
        "DESCRIBE SELECT 1",
    ],
)
def test_only_one_select_or_cte_is_accepted(tmp_path: Path, query: str) -> None:
    from sm_trending_products.exploration import query_parquet

    with pytest.raises(ValueError):
        query_parquet([], query, settings(tmp_path))


def test_private_files_and_environment_are_not_readable(tmp_path: Path) -> None:
    import duckdb

    from sm_trending_products.exploration import ParquetInput, query_parquet

    config = settings(tmp_path)
    path = config.cache_dir / "selected.parquet"
    normalized(path)
    secret = config.cache_dir / "unselected-secret.txt"
    secret.write_text("access_token=must-not-leak")
    inputs = [ParquetInput(path, "first", BASE)]
    for query in [
        f"SELECT * FROM read_text('{secret}')",
        "SELECT * FROM read_text('/proc/self/environ')",
        "SELECT * FROM read_parquet('https://example.com/forbidden.parquet')",
    ]:
        with pytest.raises(duckdb.Error):
            query_parquet(inputs, query, config)
    result = query_parquet(
        inputs,
        "SELECT current_setting('enable_external_access'),"
        "current_setting('autoload_known_extensions'),current_setting('lock_configuration')",
        config,
    )
    assert result["rows"] == [[False, False, True]]


def test_replays_deduplicate_and_keep_oldest_dataset_provenance(tmp_path: Path) -> None:
    from sm_trending_products.exploration import ParquetInput, query_parquet

    config = settings(tmp_path)
    first = config.cache_dir / "first.parquet"
    replay = config.cache_dir / "replay.parquet"
    normalized(first, dataset_id="z-original")
    normalized(replay, dataset_id="a-replay")
    inputs = [
        ParquetInput(replay, "a-replay", BASE + timedelta(days=1)),
        ParquetInput(first, "z-original", BASE),
    ]
    result = query_parquet(
        inputs, "SELECT source_dataset_id,raw_dataset_id FROM observations", config
    )
    assert result["rows"] == [["z-original", "z-original"]]
    versions = query_parquet(inputs, "SELECT count(*) FROM observation_versions", config)
    assert versions["rows"] == [[2]]
    normalized(replay, dataset_id="a-replay", views=999)
    with pytest.raises(ValueError, match="Conflicting payloads"):
        query_parquet(inputs, "SELECT * FROM observations", config)


@pytest.mark.integration
def test_completed_history_query_and_cache_pin_survive_eviction(pipeline: object) -> None:
    from sm_trending_products.cache import pin_cached
    from sm_trending_products.exploration import explore

    pipeline.import_records(
        [
            {
                "observation_id": "fixture-o1",
                "source": "offer_import",
                "entity_type": "offer",
                "entity_id": "o1",
                "observed_at": BASE.isoformat(),
                "available_at": BASE.isoformat(),
                "synthetic": True,
                "metrics": {},
                "attributes": {"title": "Desk lamp"},
            }
        ],
        synthetic=True,
    )
    manifests = pipeline.catalog.manifests(synthetic=True)
    result = explore(pipeline.history, manifests, "SELECT entity_id,attrs_json FROM observations")
    assert result["rows"][0][0] == "o1"
    assert result["object_store_metrics"]["get_requests"] >= 1
    obj = manifests[0]["manifest"]["normalized"][0]
    with pin_cached(pipeline.history, obj) as pinned:
        original = pipeline.history.cached(obj)
        original.unlink()
        assert pl.read_parquet(pinned)["entity_id"].to_list() == ["o1"]
    assert not pinned.exists()
    with pytest.raises(ValueError, match="completed datasets"):
        explore(
            pipeline.history, [{**manifests[0], "state": "staged"}], "SELECT * FROM observations"
        )
