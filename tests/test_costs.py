"""Operator pricing contracts exercise independent monetary and quantity arithmetic."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from sm_trending_products.cli import main
from sm_trending_products.costs import (
    Coefficients,
    Machine,
    Scenario,
    Store,
    Workload,
    estimate,
    usage,
)

ROOT = Path(__file__).resolve().parents[1]


def scenario(store=0, machine=1, **updates):
    catalog = json.loads((ROOT / "docs/operations/costs/prices-2026-10-24.json").read_text())
    coefficients = Coefficients.model_validate_json(
        (ROOT / "docs/operations/costs/illustrative-coefficients.json").read_text()
    )
    data = dict(
        workload=Workload(
            products=10000, videos_per_product=20, refreshes_per_day=4, history_days=90
        ),
        coefficients=coefficients,
        machine=Machine.model_validate(catalog["machines"][machine]),
        store=Store.model_validate(catalog["stores"][store]),
        backup_store=Store.model_validate(catalog["stores"][store]),
    )
    data.update(updates)
    return Scenario.model_validate(data)


def test_daily_observations_requests_history_and_each_physical_copy_are_counted():
    s = scenario()
    u = usage(s.workload, s.coefficients)
    assert u["observations_per_day"] == 800000
    assert u["observations_per_month"] == 24000000
    assert u["retained_observations"] == 72000000
    assert u["source_refresh_requests_per_month_lower_bound"] == 1200000
    assert u["raw_retained_bytes"] == 10800000000
    assert u["normalized_retained_bytes"] == 4320000000
    assert u["compacted_copy_bytes"] == 15120000000
    assert u["assessment_retained_bytes"] == Decimal("345380544000")
    assert u["sql_dump_bytes"] == (
        9216000000
        + 96 * u["retired_marker_rows"]
        + u["sql_dump_compaction_delivery_component_bytes"]
    )
    assert u["source_store_bytes"] == sum(
        u[key]
        for key in (
            "raw_retained_bytes",
            "normalized_retained_bytes",
            "compacted_copy_bytes",
            "manifest_retained_bytes",
            "provenance_retained_bytes",
            "assessment_retained_bytes",
        )
    )
    assert u["backup_store_bytes"] > u["source_store_bytes"] + 30 * u["sql_dump_bytes"]


def test_r2_account_quotas_and_rounding_are_applied_once_across_both_buckets():
    overrides = {
        "source_store_bytes": Decimal(6_000_000_000),
        "backup_store_bytes": Decimal(6_000_000_000),
        "source_write_requests": Decimal(600000),
        "backup_write_requests": Decimal(600001),
        "source_read_requests": Decimal(10000000),
        "backup_read_requests": Decimal(1),
    }
    report = estimate(scenario(account_free_tier_available=True, usage_overrides=overrides))
    charges = report["monthly_charges_excluding_tax"]
    assert Decimal(charges["object_account_storage"]) == Decimal("0.03")
    assert Decimal(charges["object_account_writes_and_lists"]) == Decimal("4.50")
    assert Decimal(charges["object_account_reads_and_heads"]) == Decimal("0.36")


def test_b2_account_egress_allowance_is_shared_and_api_prices_are_really_zero():
    overrides = {
        "source_store_bytes": Decimal(100_000_000_000),
        "backup_store_bytes": Decimal(100_000_000_000),
        "source_store_egress_bytes": Decimal(500_000_000_000),
        "backup_store_egress_bytes": Decimal(200_000_000_000),
        "source_write_requests": None,
    }
    report = estimate(
        scenario(store=1, account_free_tier_available=True, usage_overrides=overrides)
    )
    charges = report["monthly_charges_excluding_tax"]
    assert Decimal(charges["object_account_storage"]) == Decimal("1.3205")
    assert Decimal(charges["object_account_egress"]) == Decimal("1")
    assert Decimal(charges["object_account_writes_and_lists"]) == 0
    assert "source_write_requests" in report["unmeasured_quantities"]


def test_unknowns_do_not_turn_into_zero_or_an_invented_complete_bill():
    s = scenario()
    s.coefficients.sql_dump_bytes_per_retained_observation = None
    report = estimate(s)
    assert report["quantities"]["backup_store_bytes"] is None
    assert report["modeled_infrastructure_excluding_tax"] is None
    assert report["monthly_total_excluding_tax"] is None
    assert report["monthly_total_including_tax"] is None
    assert "source_api" in report["unknown_costs"]
    assert "object_account_storage" in report["unknown_costs"]
    assert Decimal(report["known_subtotal_excluding_tax"]) >= s.machine.monthly_price


def test_lightsail_inbound_consumes_allowance_but_only_outbound_can_be_billed():
    s = scenario(
        machine=4,
        usage_overrides={"vps_outbound_bytes": Decimal(0), "vps_inbound_bytes": Decimal(10**15)},
    )
    report = estimate(s)
    assert report["monthly_charges_excluding_tax"]["vps_transfer_overage"] == "0"
    s.usage_overrides["vps_outbound_bytes"] = Decimal(10**9)
    report = estimate(s)
    assert report["monthly_charges_excluding_tax"]["vps_transfer_overage"] is None


def test_provider_price_currency_and_tax_are_not_implicitly_converted():
    s = scenario()
    s.coefficients.source_api_monthly_cost = Decimal(7)
    s.coefficients.other_monthly_cost = Decimal(3)
    s.tax_rate = Decimal("0.2")
    r = estimate(s)
    assert Decimal(r["monthly_total_including_tax"]) == Decimal(
        r["monthly_total_excluding_tax"]
    ) * Decimal("1.2")
    bad = s.model_dump()
    bad["backup_store"]["currency"] = "EUR"
    with pytest.raises(ValidationError, match="currency"):
        Scenario.model_validate(bad)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1"])
def test_bad_money_never_produces_a_cheap_offer(value):
    data = scenario().store.model_dump()
    data["storage_price_per_gb_month"] = value
    with pytest.raises(ValidationError):
        Store.model_validate(data)


def test_credential_free_cli_roundtrips_the_same_explicit_configuration(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    path = tmp_path / "scenario.json"
    s = scenario()
    path.write_text(s.model_dump_json())
    assert main(["cost", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out) == estimate(s)


def test_override_typo_is_rejected_instead_of_silently_underpricing():
    s = scenario(usage_overrides={"soruce_store_bytes": Decimal(0)})
    with pytest.raises(ValueError, match="Unknown usage override"):
        estimate(s)


def test_local_backup_family_and_recovery_copy_are_not_free_disk_space():
    s = scenario()
    u = usage(s.workload, s.coefficients)
    assert u["vps_local_backup_family_bytes"] == u["backup_store_bytes"]
    assert u["vps_restore_copy_bytes"] == u["source_store_bytes"] + u["sql_bytes"]
    assert u["vps_working_disk_bytes"] == (
        u["vps_live_disk_bytes"] + u["backup_store_bytes"] + u["vps_restore_copy_bytes"]
    ) * Decimal("1.2")
    r = estimate(s)
    assert r["working_disk_fits_advertised_capacity"] is False
    assert Decimal(r["monthly_charges_excluding_tax"]["extra_disk"]) > 0
    assert r["capacity_verified"] is False


def test_allocated_volume_rounding_and_minimum_are_applied_to_excess_only():
    s = scenario(machine=4, usage_overrides={"vps_working_disk_bytes": Decimal(160_000_000_001)})
    r = estimate(s)
    assert Decimal(r["monthly_charges_excluding_tax"]["extra_disk"]) == Decimal("0.8")
    s.usage_overrides["vps_working_disk_bytes"] = Decimal(160_000_000_000)
    assert estimate(s)["monthly_charges_excluding_tax"]["extra_disk"] == "0"


def test_restore_rehearsal_reads_and_transfer_are_explicit_in_billing():
    s = scenario()
    u = usage(s.workload, s.coefficients)
    assert u["backup_read_requests"] == (
        u["restore_chain_owned_objects"] + u["restore_chain_metadata_requests"]
    )
    assert u["backup_store_egress_bytes"] > u["source_store_bytes"]
    s.coefficients.restore_rehearsals_per_month = Decimal(0)
    no_restore = usage(s.workload, s.coefficients)
    assert no_restore["backup_read_requests"] == 0
    assert no_restore["backup_store_egress_bytes"] == 0


def test_cold_restore_fetches_one_required_chain_and_only_the_target_sql_dump():
    s = scenario()
    s.workload = Workload(
        products=1,
        videos_per_product=1,
        refreshes_per_day=1,
        history_days=2,
        assessment_runs_per_day=1,
        assessment_retention_days=2,
    )
    c = s.coefficients
    c.raw_bytes_per_observation = Decimal(10)
    c.normalized_bytes_per_observation = Decimal(20)
    c.manifest_bytes_per_dataset = Decimal(1)
    c.observations_per_dataset = Decimal(1)
    c.retained_compacted_copy_fraction = Decimal(0)
    c.compacted_output_datasets_per_input = Decimal(0)
    c.provenance_bytes_per_product_refresh = Decimal(0)
    c.provenance_nodes_per_product_refresh = Decimal(0)
    c.assessment_bytes_per_product_run = Decimal(0)
    c.backup_full_every_days = 3
    c.backup_inventory_bytes_per_object = Decimal(2)
    c.restore_ancestor_snapshot_metadata_bytes = Decimal(8)
    c.sql_dump_bytes = Decimal(100)
    u = usage(s.workload, c)
    # Two retained observations: 62 payload bytes, eight live objects. Each
    # additional day creates 31 bytes and four objects. One base + two deltas
    # needs 124 bytes/16 objects even when old keys leave the latest inventory.
    assert u["source_store_bytes"] == 62
    assert u["source_objects"] == 8
    assert u["restore_chain_captures"] == 3
    assert u["restore_chain_payload_bytes"] == 124
    assert u["restore_chain_owned_objects"] == 16
    assert u["restore_chain_inventory_bytes"] == 48
    assert u["restore_chain_ancestor_snapshot_metadata_bytes"] == 16
    assert u["restore_chain_target_sql_dump_bytes"] == 100
    assert u["backup_read_requests"] == 16 + 5 + 2 * 2
    assert u["backup_store_egress_bytes"] == 124 + 48 + 16 + 100
    # Unrelated retained families increase storage, not the selected chain fetch.
    c.backup_retention_days = 365
    longer_retention = usage(s.workload, c)
    assert longer_retention["backup_store_bytes"] > u["backup_store_bytes"]
    assert longer_retention["backup_read_requests"] == u["backup_read_requests"]
    assert longer_retention["backup_store_egress_bytes"] == u["backup_store_egress_bytes"]


def test_full_restore_keeps_existing_budget_without_unknown_ancestor_bytes():
    s = scenario()
    s.coefficients.backup_full_every_days = 1
    s.coefficients.restore_ancestor_snapshot_metadata_bytes = None
    u = usage(s.workload, s.coefficients)
    assert u["restore_chain_captures"] == 1
    assert u["backup_read_requests"] == u["source_objects"] + 5
    assert u["backup_store_egress_bytes"] == (
        u["source_store_bytes"]
        + u["sql_dump_bytes"]
        + u["source_objects"] * s.coefficients.backup_inventory_bytes_per_object
    )
    s.coefficients.backup_full_every_days = 2
    incremental = usage(s.workload, s.coefficients)
    assert incremental["restore_chain_payload_bytes"] is not None
    assert incremental["backup_store_egress_bytes"] is None
    s.coefficients.restore_rehearsals_per_month = Decimal(0)
    disabled = usage(s.workload, s.coefficients)
    assert disabled["restore_chain_ancestor_snapshot_metadata_bytes"] is None
    assert disabled["backup_read_requests"] == disabled["backup_store_egress_bytes"] == 0


def test_fewer_backup_full_bases_reduce_remote_reads_without_removing_dumps():
    s = scenario()
    baseline = usage(s.workload, s.coefficients)
    s.coefficients.backup_full_every_days = 15
    twice = usage(s.workload, s.coefficients)
    assert baseline["backup_full_captures_per_month"] == 1
    assert twice["backup_full_captures_per_month"] == 2
    assert twice["backup_reserved_families"] == 3
    assert baseline["backup_reserved_families"] == 2
    assert twice["source_store_egress_bytes"] > baseline["source_store_egress_bytes"]


def test_source_and_backup_request_classes_have_distinct_provider_pricing():
    s = scenario()
    s.backup_store = scenario(store=1).store
    s.usage_overrides = {"source_write_requests": Decimal(1), "backup_write_requests": Decimal(1)}
    charges = estimate(s)["monthly_charges_excluding_tax"]
    assert Decimal(charges["source_writes_and_lists"]) == Decimal("4.50")
    assert Decimal(charges["backup_writes_and_lists"]) == 0


def test_official_r2_standard_example_reconciles_without_double_free_tier():
    s = scenario(
        account_free_tier_available=True,
        usage_overrides={
            "source_store_bytes": Decimal(1000 * 10**9),
            "backup_store_bytes": Decimal(0),
            "source_write_requests": Decimal(1000),
            "backup_write_requests": Decimal(0),
            "source_read_requests": Decimal(1000000),
            "backup_read_requests": Decimal(0),
        },
    )
    c = estimate(s)["monthly_charges_excluding_tax"]
    assert Decimal(c["object_account_storage"]) == Decimal("14.85")
    assert Decimal(c["object_account_writes_and_lists"]) == 0
    assert Decimal(c["object_account_reads_and_heads"]) == 0


def test_zero_metadata_heads_on_deltas_still_counts_full_and_new_object_heads():
    s = scenario()
    original = usage(s.workload, s.coefficients)
    s.coefficients.backup_heads_per_object_per_capture = Decimal(0)
    s.coefficients.backup_new_object_heads_per_object = Decimal(1)
    optimized = usage(s.workload, s.coefficients)
    assert (
        0 < optimized["backup_inventory_head_requests"] < original["backup_inventory_head_requests"]
    )
    assert optimized["backup_inventory_head_requests"] > optimized["source_objects"]
    assert optimized["backup_inventory_list_requests"] == original["backup_inventory_list_requests"]


def test_older_deployment_keeps_more_sql_markers_after_same_payload_retention():
    s = scenario()
    initial = usage(s.workload, s.coefficients)
    s.workload.operating_age_days = 730
    later = usage(s.workload, s.coefficients)
    assert later["source_store_bytes"] == initial["source_store_bytes"]
    assert later["retired_marker_rows"] > initial["retired_marker_rows"]
    assert later["sql_bytes"] > initial["sql_bytes"]
    assert later["backup_store_bytes"] > initial["backup_store_bytes"]


def test_full_period_cannot_create_more_backups_than_capture_schedule():
    data = scenario().coefficients.model_dump()
    data.update(backup_every_days=7, backup_full_every_days=1)
    with pytest.raises(ValidationError, match="shorter than its capture interval"):
        Coefficients.model_validate(data)
    data.update(backup_full_every_days=90)
    c = Coefficients.model_validate(data)
    u = usage(scenario().workload, c)
    assert u["backup_captures_per_month"] == 5
    assert u["backup_full_captures_per_month"] == 1
    assert u["backup_effective_full_period_days"] == 91
    assert u["backup_max_family_snapshots"] == 13
    data.update(backup_full_every_days=225)
    with pytest.raises(ValidationError, match="32-snapshot"):
        Coefficients.model_validate(data)


def test_backup_reservation_keeps_cross_boundary_full_bases_and_parent_delta_dumps():
    s = scenario()
    u = usage(s.workload, s.coefficients)
    assert u["backup_reserved_families"] == 2
    assert u["backup_reserved_captures"] == 59
    # Enumerate the actual dependency closure at every daily phase: each retained
    # snapshot needs its base and intervening parents, including pre-window ones.
    for age in range(330, 390):
        retained = range(age - 29, age + 1)
        required = {
            ancestor for target in retained for ancestor in range(target // 30 * 30, target + 1)
        }
        full = {snapshot for snapshot in required if snapshot % 30 == 0}
        assert len(full) <= u["backup_reserved_families"]
        assert len(required) <= u["backup_reserved_captures"]
    assert (
        max(
            len(
                {
                    ancestor
                    for target in range(age - 29, age + 1)
                    for ancestor in range(target // 30 * 30, target + 1)
                }
            )
            for age in range(330, 390)
        )
        == 59
    )


def test_daily_fulls_reserve_only_actual_full_snapshots_without_phantom_deltas():
    s = scenario()
    s.coefficients.backup_full_every_days = 1
    u = usage(s.workload, s.coefficients)
    assert u["backup_captures_per_month"] == u["backup_full_captures_per_month"] == 30
    assert u["backup_reserved_families"] == u["backup_reserved_captures"] == 30
    assert u["backup_store_bytes"] == 30 * (
        u["source_store_bytes"]
        + u["sql_dump_bytes"]
        + u["source_objects"] * s.coefficients.backup_inventory_bytes_per_object
    )


@pytest.mark.parametrize(
    "retained,interval,full", [(1, 1, 30), (30, 1, 30), (31, 1, 30), (35, 7, 90), (5, 1, 1)]
)
def test_reservation_bounds_the_dependency_closure_at_every_calendar_phase(
    retained, interval, full
):
    s = scenario()
    s.coefficients.backup_retention_days = retained
    s.coefficients.backup_every_days = interval
    s.coefficients.backup_full_every_days = full
    u = usage(s.workload, s.coefficients)
    required_count = (retained + interval - 1) // interval
    full_spacing = (full + interval - 1) // interval
    closures = []
    for phase in range(full_spacing):
        last = 100 * full_spacing + phase
        retained_targets = range(last - required_count + 1, last + 1)
        ancestors = {
            node
            for target in retained_targets
            for node in range((target // full_spacing) * full_spacing, target + 1)
        }
        bases = sum(node % full_spacing == 0 for node in ancestors)
        closures.append((len(ancestors), bases, len(ancestors) - bases))
    assert max(count for count, _, _ in closures) == u["backup_reserved_captures"]
    assert max(bases for _, bases, _ in closures) == u["backup_reserved_families"]
    assert max(deltas for _, _, deltas in closures) == (
        u["backup_reserved_captures"] - u["backup_reserved_families"]
    )


def test_unknown_http_wire_ingress_changes_only_providers_that_bill_inbound_allowance():
    do = scenario()
    assert do.coefficients.upstream_response_bytes_per_observation is None
    r = estimate(do)
    assert r["quantities"]["vps_inbound_bytes"] is None
    assert r["modeled_infrastructure_excluding_tax"] is not None
    lightsail = scenario(machine=4)
    r = estimate(lightsail)
    assert "vps_transfer_overage" in r["unknown_costs"]
    assert r["modeled_infrastructure_excluding_tax"] is None
    lightsail.coefficients.upstream_response_bytes_per_observation = Decimal(1024)
    r = estimate(lightsail)
    assert r["quantities"]["vps_inbound_bytes"] is not None
    assert r["modeled_infrastructure_excluding_tax"] is not None


def test_free_provider_egress_price_does_not_hide_unknown_quantities_or_invent_charges():
    r2 = scenario()
    r2.coefficients.source_download_bytes_per_product_refresh = None
    r = estimate(r2)
    assert r["quantities"]["source_store_egress_bytes"] is None
    assert r["monthly_charges_excluding_tax"]["object_account_egress"] == "0"
    assert r["modeled_infrastructure_excluding_tax"] is not None
    b2 = scenario(store=1)
    b2.coefficients.source_download_bytes_per_product_refresh = None
    r = estimate(b2)
    assert r["monthly_charges_excluding_tax"]["object_account_egress"] is None
    assert r["modeled_infrastructure_excluding_tax"] is None
