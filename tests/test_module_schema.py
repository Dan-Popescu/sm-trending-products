"""Optional application migrations remain atomic and independent of capture versions."""

from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from sm_trending_products.module_schema import initialize_module, require_module


def test_readiness_does_not_create_tables_and_init_is_repeatable(pipeline):
    catalog = pipeline.catalog
    capture_migrations = catalog.all("SELECT version FROM schema_migrations ORDER BY version")
    registry = catalog.one("SELECT to_regclass('application_module_migrations') AS name")["name"]
    other_modules = (
        catalog.all(
            "SELECT module,version,sha256 FROM application_module_migrations "
            "ORDER BY module,version"
        )
        if registry
        else []
    )
    with pytest.raises(ValueError, match="not initialized"):
        require_module(catalog, "example")
    assert (
        catalog.one("SELECT to_regclass('application_module_migrations') AS name")["name"]
        == registry
    )
    migrations = {1: "CREATE TABLE module_example (id integer PRIMARY KEY)"}
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(lambda _: initialize_module(catalog, "example", migrations), range(2))
        )
    assert results == [{"module": "example", "version": 1, "status": "initialized"}] * 2
    require_module(catalog, "example")
    assert (
        catalog.one(
            "SELECT count(*) AS n FROM application_module_migrations WHERE module='example'"
        )["n"]
        == 1
    )
    assert (
        catalog.all(
            "SELECT module,version,sha256 FROM application_module_migrations "
            "WHERE module<>'example' ORDER BY module,version"
        )
        == other_modules
    )
    assert (
        catalog.all("SELECT version FROM schema_migrations ORDER BY version") == capture_migrations
    )
    with pytest.raises(ValueError, match="not initialized"):
        require_module(catalog, "example", 2)


def test_migrations_reject_edits_and_roll_back_the_entire_upgrade(pipeline):
    catalog = pipeline.catalog
    initial = "CREATE TABLE module_example (id integer PRIMARY KEY)"
    initialize_module(catalog, "example", {1: initial})
    with pytest.raises(ValueError, match="changed"):
        initialize_module(catalog, "example", {1: initial + "; SELECT 1"})
    with pytest.raises(psycopg.Error):
        initialize_module(
            catalog,
            "example",
            {
                1: initial,
                2: "CREATE TABLE module_second (id integer)",
                3: "SELECT missing_module_relation FROM missing_module_relation",
            },
        )
    assert catalog.one("SELECT to_regclass('module_second') AS name")["name"] is None
    assert (
        catalog.one(
            "SELECT max(version) AS version FROM application_module_migrations "
            "WHERE module='example'"
        )["version"]
        == 1
    )
    initialize_module(
        catalog, "example", {1: initial, 2: "CREATE TABLE module_second (id integer)"}
    )
    with pytest.raises(ValueError, match="newer"):
        initialize_module(catalog, "example", {1: initial})


@pytest.mark.parametrize(
    "module,migrations",
    [
        ("invalid-name", {1: "SELECT 1"}),
        ("example", {}),
        ("example", {2: "SELECT 1"}),
        ("example", {True: "SELECT 1"}),
        ("example", {1: " "}),
    ],
)
def test_invalid_module_definitions_are_rejected_before_sql(module, migrations):
    with pytest.raises(ValueError):
        initialize_module(None, module, migrations)
