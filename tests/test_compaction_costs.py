"""Physical delivery stocks and write rates have separate marginal byte budgets."""

from datetime import timedelta
from decimal import Decimal

import pytest
from test_costs import scenario
from test_maintenance import record

from sm_trending_products.contracts import now
from sm_trending_products.costs import Coefficients, usage


def no_extra_deliveries():
    s = scenario()
    s.coefficients.retained_compaction_delivery_copies_per_observation = Decimal(0)
    s.coefficients.compaction_delivery_writes_per_new_observation = Decimal(0)
    return s


@pytest.mark.parametrize(
    "stock,rate", [("0", "0"), ("1", "0"), ("0", "1"), ("2", "0.5"), ("0", "3")]
)
def test_marginal_delivery_stock_and_daily_flow_are_independent(stock, rate):
    s = no_extra_deliveries()
    s.coefficients.wal_retention_days = 3
    original = usage(s.workload, s.coefficients)
    s.coefficients.retained_compaction_delivery_copies_per_observation = Decimal(stock)
    s.coefficients.compaction_delivery_writes_per_new_observation = Decimal(rate)
    u = usage(s.workload, s.coefficients)
    extra_rows = Decimal(72_000_000) * Decimal(stock)
    written_rows = Decimal(800_000) * Decimal(rate)
    assert u["retained_original_delivery_rows"] == 72_000_000
    assert u["retained_compaction_delivery_rows"] == extra_rows
    assert u["retained_total_delivery_rows"] == 72_000_000 + extra_rows
    assert u["compaction_delivery_rows_written_per_day"] == written_rows
    assert u["compaction_delivery_rows_written_per_month"] == written_rows * 30
    # Base SQL contains shared source identity/state once, not once per copy.
    assert u["sql_base_bytes"] == original["sql_bytes"]
    assert u["sql_bytes"] == original["sql_bytes"] + extra_rows * 800
    assert u["sql_dump_bytes"] == original["sql_dump_bytes"] + extra_rows * 128
    assert u["wal_bytes"] == original["wal_bytes"] + written_rows * 1000 * 3


def test_object_byte_copy_fraction_does_not_infer_delivery_rows_or_write_rates():
    s = scenario()
    before = usage(s.workload, s.coefficients)
    s.coefficients.retained_compacted_copy_fraction = Decimal(0)
    after = usage(s.workload, s.coefficients)
    assert after["compacted_copy_bytes"] == 0 < before["compacted_copy_bytes"]
    for name in (
        "retained_compaction_delivery_rows",
        "compaction_delivery_rows_written_per_day",
        "sql_bytes",
        "sql_dump_bytes",
        "wal_bytes",
    ):
        assert after[name] == before[name]


def test_zero_additional_rows_keep_original_budget_despite_unknown_marginal_densities():
    s = no_extra_deliveries()
    original = usage(s.workload, s.coefficients)
    s.coefficients.sql_bytes_per_compaction_delivery = None
    s.coefficients.sql_dump_bytes_per_compaction_delivery = None
    s.coefficients.wal_bytes_per_compaction_delivery = None
    u = usage(s.workload, s.coefficients)
    assert u["sql_compaction_delivery_bytes"] == 0
    assert u["sql_dump_compaction_delivery_component_bytes"] == 0
    assert u["wal_compaction_retained_bytes"] == 0
    for name in ("sql_bytes", "sql_dump_bytes", "wal_bytes"):
        assert u[name] == original[name]


@pytest.mark.parametrize(
    "missing,unknown",
    [
        ("sql_bytes_per_compaction_delivery", "sql_bytes"),
        ("sql_dump_bytes_per_compaction_delivery", "sql_dump_bytes"),
        ("wal_bytes_per_compaction_delivery", "wal_bytes"),
    ],
)
def test_missing_density_preserves_unknown_extra_costs(missing, unknown):
    s = scenario()
    setattr(s.coefficients, missing, None)
    u = usage(s.workload, s.coefficients)
    assert u[unknown] is None
    for name in {"sql_bytes", "sql_dump_bytes", "wal_bytes"} - {unknown}:
        assert u[name] is not None


def test_unknown_stock_and_rate_propagate_only_to_their_own_budgets():
    s = no_extra_deliveries()
    s.coefficients.retained_compaction_delivery_copies_per_observation = None
    u = usage(s.workload, s.coefficients)
    assert u["sql_bytes"] is u["sql_dump_bytes"] is None
    assert u["wal_bytes"] is not None
    s = no_extra_deliveries()
    s.coefficients.compaction_delivery_writes_per_new_observation = None
    u = usage(s.workload, s.coefficients)
    assert u["sql_bytes"] is not None and u["sql_dump_bytes"] is not None
    assert u["wal_bytes"] is None
    defaults = Coefficients(source="unmeasured compaction")
    assert defaults.retained_compaction_delivery_copies_per_observation is None
    assert defaults.compaction_delivery_writes_per_new_observation is None


def test_explicit_total_dump_ignores_even_unknown_fallback_components():
    s = scenario()
    s.coefficients.sql_dump_bytes = Decimal(1234567)
    s.coefficients.sql_dump_bytes_per_retained_observation = None
    s.coefficients.sql_dump_bytes_per_retired_marker = None
    s.coefficients.sql_dump_bytes_per_compaction_delivery = None
    u = usage(s.workload, s.coefficients)
    assert u["sql_dump_compaction_delivery_component_bytes"] is None
    assert u["sql_dump_bytes"] == u["restore_chain_target_sql_dump_bytes"] == 1234567


@pytest.mark.integration
def test_real_compaction_adds_copy_tuples_without_duplicating_logical_facts_or_entities(pipeline):
    at = now()
    parents = []
    for chunk in range(3):
        parents.extend(
            pipeline.import_records(
                [
                    record(
                        "same-video",
                        at - timedelta(minutes=6 - (chunk * 2 + index)),
                        100 + chunk * 2 + index,
                    )
                    for index in range(2)
                ],
                synthetic=True,
            )
        )

    def counts():
        return pipeline.catalog.one(
            "SELECT (SELECT count(*) FROM observation_deliveries) AS deliveries,"
            "(SELECT count(DISTINCT observation_id) FROM observation_deliveries) AS facts,"
            "(SELECT count(*) FROM normalized_objects) AS objects,"
            "(SELECT count(*) FROM normalized_object_entities) AS memberships,"
            "(SELECT count(*) FROM entities) AS entities"
        )

    assert counts() == {"deliveries": 6, "facts": 6, "objects": 3, "memberships": 3, "entities": 1}
    result = pipeline.compact(synthetic=True, max_datasets=10)
    assert result["status"] == "compacted" and result["rows"] == 6
    assert set(result["parent_dataset_ids"]) == set(parents)
    assert counts() == {"deliveries": 12, "facts": 6, "objects": 4, "memberships": 4, "entities": 1}
    assert (
        pipeline.catalog.one(
            "SELECT count(*) AS n FROM datasets WHERE dataset_id=ANY(%s) "
            "AND state='committed' AND compacted_into=%s",
            (parents, result["dataset_id"]),
        )["n"]
        == 3
    )
    with pipeline.catalog.transaction() as connection:
        with connection.cursor().copy("COPY observation_deliveries TO STDOUT") as stream:
            assert len(b"".join(bytes(block) for block in stream).splitlines()) == 12
    selected, _ = pipeline.history.observations(
        at + timedelta(seconds=1), True, entity_keys={"maintenance_fixture:video:same-video"}
    )
    assert len(selected) == 6
