"""Dated infrastructure scenarios with explicit unknowns and provider billing units.

This module performs arithmetic, not capacity admission. Prices and workload
coefficients are supplied data; measured usage can replace projected quantities.
"""

from __future__ import annotations

from datetime import date
from decimal import ROUND_CEILING, Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Nonnegative = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
Positive = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
Quantity = Nonnegative | None
GB = Decimal(1_000_000_000)
GIB = Decimal(1_073_741_824)


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Workload(Contract):
    products: int = Field(gt=0, le=10_000_000)
    videos_per_product: int = Field(gt=0, le=10_000)
    refreshes_per_day: Positive
    history_days: int = Field(gt=0, le=3650)
    month_days: int = Field(default=30, gt=0, le=31)
    videos_per_request: int = Field(default=20, gt=0, le=20)
    assessment_runs_per_day: Positive = Decimal(96)
    assessment_retention_days: int = Field(default=90, gt=0, le=3650)
    operating_age_days: int = Field(default=365, gt=0, le=36500)


class CoefficientEvidence(Contract):
    classification: Literal["measured_ratio", "assumed", "unknown", "contract"]
    reference: str
    scope: str


class Coefficients(Contract):
    """Scenario assumptions; None means unmeasured, never a free quantity."""

    source: str
    evidence: dict[str, CoefficientEvidence] = Field(default_factory=dict)
    raw_bytes_per_observation: Quantity = None
    normalized_bytes_per_observation: Quantity = None
    manifest_bytes_per_dataset: Quantity = None
    observations_per_dataset: Positive = Decimal(20)
    retained_compacted_copy_fraction: Quantity = None
    compacted_output_datasets_per_input: Quantity = None
    compacted_manifest_bytes_per_dataset: Quantity = None
    compaction_remote_read_fraction: Quantity = None
    provenance_bytes_per_product_refresh: Quantity = None
    provenance_nodes_per_product_refresh: Quantity = None
    assessment_bytes_per_product_run: Quantity = None
    sql_bytes_per_observation: Quantity = None
    sql_bytes_per_provenance_node: Quantity = None
    sql_bytes_per_assessment_run: Quantity = None
    sql_bytes_per_retired_artifact: Quantity = None
    sql_bytes_per_retired_dataset: Quantity = None
    sql_dump_bytes_per_retired_marker: Quantity = None
    source_gets_per_product_refresh: Quantity = None
    source_download_bytes_per_product_refresh: Quantity = None
    upstream_response_bytes_per_observation: Quantity = None
    wal_bytes_per_new_observation: Quantity = None
    wal_retention_days: int = Field(default=1, gt=0, le=365)
    cache_bytes: Nonnegative = Decimal(2_000_000_000)
    operating_system_spool_bytes: Quantity = None
    sql_dump_bytes: Quantity = None
    sql_dump_bytes_per_retained_observation: Quantity = None
    backup_retention_days: int = Field(default=30, gt=0, le=3650)
    backup_every_days: int = Field(default=1, gt=0, le=365)
    backup_full_every_days: int = Field(default=30, gt=0, le=11680)
    backup_inventory_bytes_per_object: Quantity = None
    backup_heads_per_object_per_capture: Quantity = None
    backup_full_base_heads_per_object: Quantity = None
    backup_new_object_heads_per_object: Quantity = None
    backup_metadata_objects_per_capture: Nonnegative = Decimal(5)
    backup_list_page_size: int = Field(default=1000, gt=0, le=1000)
    offsite_backup_egress_fraction: Nonnegative = Decimal(1)
    local_backup_family_fraction: Quantity = None
    restore_rehearsals_per_month: Quantity = None
    restore_local_copy_fraction: Quantity = None
    disk_headroom_fraction: Nonnegative = Decimal("0.2")
    source_api_monthly_cost: Quantity = None
    other_monthly_cost: Quantity = None

    def _backup_schedule(self) -> tuple[int, int]:
        if self.backup_full_every_days < self.backup_every_days:
            raise ValueError("A full-backup period cannot be shorter than its capture interval")
        snapshots = (
            self.backup_full_every_days + self.backup_every_days - 1
        ) // self.backup_every_days
        if snapshots > 32:
            raise ValueError("The aligned schedule exceeds the supported 32-snapshot chain")
        return snapshots, snapshots * self.backup_every_days

    @model_validator(mode="after")
    def valid_backup_schedule(self):
        self._backup_schedule()
        return self


class Store(Contract):
    name: str
    currency: str = Field(pattern="^[A-Z]{3}$")
    observed_on: date
    source_url: str
    region: str
    storage_price_per_gb_month: Nonnegative
    storage_free_gb: Nonnegative = Decimal(0)
    storage_billing_grain_gb: Nonnegative = Decimal(0)
    write_price_per_unit: Nonnegative
    write_unit: Positive
    write_free_requests: Nonnegative = Decimal(0)
    read_price_per_unit: Nonnegative
    read_unit: Positive
    read_free_requests: Nonnegative = Decimal(0)
    round_request_units_up: bool = False
    egress_price_per_gb: Nonnegative
    egress_free_gb: Nonnegative = Decimal(0)
    egress_free_storage_multiple: Nonnegative = Decimal(0)
    notes: str


class Machine(Contract):
    name: str
    currency: str = Field(pattern="^[A-Z]{3}$")
    observed_on: date
    source_url: str
    region: str
    monthly_price: Nonnegative
    vcpus: int = Field(gt=0)
    memory_bytes: Positive
    included_disk_bytes: Positive
    included_transfer_bytes: Nonnegative
    inbound_counts_toward_transfer: bool = False
    extra_transfer_price_per_gb: Quantity = None
    extra_disk_price_per_gb_month: Quantity = None
    extra_disk_billing_grain_bytes: Positive = GB
    extra_disk_minimum_bytes: Nonnegative = Decimal(0)
    notes: str


class Scenario(Contract):
    workload: Workload
    coefficients: Coefficients
    machine: Machine
    store: Store
    backup_store: Store
    account_free_tier_available: bool = False
    tax_rate: Quantity = None
    usage_overrides: dict[str, Quantity] = Field(default_factory=dict)

    @model_validator(mode="after")
    def compatible_currencies(self):
        if len({self.machine.currency, self.store.currency, self.backup_store.currency}) != 1:
            raise ValueError("Use one currency; currency conversion requires explicit prices")
        return self


def _sum(*values: Quantity) -> Quantity:
    total = Decimal(0)
    for value in values:
        if value is None:
            return None
        total += value
    return total


def _multiply(*values: Quantity) -> Quantity:
    result = Decimal(1)
    for value in values:
        if value is None:
            return None
        result *= value
    return result


def _ceiling(value: Decimal) -> Decimal:
    return value.to_integral_value(rounding=ROUND_CEILING)


def usage(workload: Workload, coefficients: Coefficients) -> dict[str, Quantity]:
    """Project a steady retained workload, retaining every original physical copy."""
    w, c = workload, coefficients
    chain_snapshots, effective_full_days = c._backup_schedule()
    products, days = Decimal(w.products), Decimal(w.month_days)
    daily = products * w.videos_per_product * w.refreshes_per_day
    monthly, retained = daily * days, daily * w.history_days
    datasets_day = _ceiling(daily / c.observations_per_dataset)
    product_refreshes_day = products * w.refreshes_per_day
    normalized = _multiply(retained, c.normalized_bytes_per_observation)
    raw = _multiply(retained, c.raw_bytes_per_observation)
    compacted = _multiply(_sum(raw, normalized), c.retained_compacted_copy_fraction)
    compacted_datasets_day = _multiply(datasets_day, c.compacted_output_datasets_per_input)
    if compacted_datasets_day is not None:
        compacted_datasets_day = _ceiling(compacted_datasets_day)
    manifests = _multiply(datasets_day, Decimal(w.history_days), c.manifest_bytes_per_dataset)
    manifests = _sum(
        manifests,
        _multiply(
            compacted_datasets_day, Decimal(w.history_days), c.compacted_manifest_bytes_per_dataset
        ),
    )
    provenance_day = _multiply(product_refreshes_day, c.provenance_bytes_per_product_refresh)
    nodes_day = _multiply(product_refreshes_day, c.provenance_nodes_per_product_refresh)
    provenance = _multiply(provenance_day, Decimal(w.history_days))
    assessment_day = _multiply(
        products, w.assessment_runs_per_day, c.assessment_bytes_per_product_run
    )
    assessments = _multiply(assessment_day, Decimal(w.assessment_retention_days))
    objects = _sum(
        _multiply(_sum(datasets_day, compacted_datasets_day), Decimal(w.history_days), Decimal(3)),
        _multiply(nodes_day, Decimal(w.history_days)),
        w.assessment_runs_per_day * w.assessment_retention_days,
    )
    stored = _sum(raw, normalized, compacted, manifests, provenance, assessments)
    daily_new = _sum(
        _multiply(daily, c.raw_bytes_per_observation),
        _multiply(daily, c.normalized_bytes_per_observation),
        _multiply(datasets_day, c.manifest_bytes_per_dataset),
        provenance_day,
        assessment_day,
        _multiply(
            daily,
            _sum(c.raw_bytes_per_observation, c.normalized_bytes_per_observation),
            c.retained_compacted_copy_fraction,
        ),
        _multiply(compacted_datasets_day, c.compacted_manifest_bytes_per_dataset),
    )
    # Active payload retention does not remove immutable key tombstones. Model
    # accumulated marker rows beyond the retained window at the declared age.
    retired_days = Decimal(max(0, w.operating_age_days - w.history_days))
    retired_nodes = _multiply(nodes_day, retired_days)
    retired_datasets = _multiply(_sum(datasets_day, compacted_datasets_day), retired_days)
    markers = _sum(retired_nodes, retired_datasets)
    marker_sql = _sum(
        _multiply(retired_nodes, c.sql_bytes_per_retired_artifact),
        _multiply(retired_datasets, c.sql_bytes_per_retired_dataset),
    )
    # Full logical dumps and full inventories are repeated even for payload deltas.
    dump = c.sql_dump_bytes
    if dump is None:
        dump = _sum(
            _multiply(retained, c.sql_dump_bytes_per_retained_observation),
            _multiply(markers, c.sql_dump_bytes_per_retired_marker),
        )
    # A retention cutoff can split an old family: reserve the preceding complete
    # family as well as the requested interval. This is disk reservation, not
    # additional captures or remote transfers. Parent dumps/inventories stay too.
    retained_captures = _ceiling(Decimal(c.backup_retention_days) / c.backup_every_days)
    families = _ceiling((retained_captures - 1) / chain_snapshots) + 1
    captures = retained_captures + chain_snapshots - 1
    retained_delta_days = (captures - families) * c.backup_every_days
    inventories = _multiply(objects, c.backup_inventory_bytes_per_object)
    backup_bytes = _sum(
        _multiply(stored, families),
        _multiply(daily_new, retained_delta_days),
        _multiply(dump, captures),
        _multiply(inventories, captures),
    )
    captures_month = _ceiling(days / c.backup_every_days)
    list_requests = (
        None if objects is None else _ceiling(objects / c.backup_list_page_size) * captures_month
    )
    source_gets = _multiply(product_refreshes_day, days, c.source_gets_per_product_refresh)
    source_gets = _sum(
        source_gets, _multiply(datasets_day, days, c.compaction_remote_read_fraction)
    )
    source_bytes = _multiply(
        product_refreshes_day, days, c.source_download_bytes_per_product_refresh
    )
    source_bytes = _sum(
        source_bytes,
        _multiply(monthly, c.normalized_bytes_per_observation, c.compaction_remote_read_fraction),
    )
    # No extra payload GET for unchanged deltas, but one full family reads a base.
    # Recurring planning month is aligned to a full capture. All full captures
    # are actual members of this bounded capture count, not phantom calendar jobs.
    full_captures_month = _ceiling(captures_month / chain_snapshots)
    delta_capture_days = (captures_month - full_captures_month) * c.backup_every_days
    new_objects_month = _multiply(
        _sum(
            _multiply(_sum(datasets_day, compacted_datasets_day), Decimal(3)),
            nodes_day,
            w.assessment_runs_per_day,
        ),
        delta_capture_days,
    )
    heads = _sum(
        _multiply(
            objects,
            c.backup_heads_per_object_per_capture,
            max(Decimal(0), captures_month - full_captures_month),
        ),
        _multiply(objects, c.backup_full_base_heads_per_object, full_captures_month),
        _multiply(new_objects_month, c.backup_new_object_heads_per_object),
    )
    backup_gets = _sum(_multiply(objects, full_captures_month), new_objects_month)
    backup_downloads = _sum(
        _multiply(stored, full_captures_month), _multiply(daily_new, delta_capture_days)
    )
    offsite_bytes = _sum(
        backup_downloads,
        _multiply(dump, captures_month),
        _multiply(inventories, captures_month),
    )
    upload_bytes = _sum(
        _multiply(daily_new, days), _multiply(offsite_bytes, c.offsite_backup_egress_fraction)
    )
    reads = _sum(source_gets, heads, backup_gets)
    writes = _sum(
        _multiply(_sum(datasets_day, compacted_datasets_day), days, Decimal(3)),
        _multiply(nodes_day, days),
        w.assessment_runs_per_day * days,
        list_requests,
    )
    sql = _sum(
        _multiply(retained, c.sql_bytes_per_observation),
        _multiply(nodes_day, Decimal(w.history_days), c.sql_bytes_per_provenance_node),
        _multiply(
            w.assessment_runs_per_day,
            Decimal(w.assessment_retention_days),
            c.sql_bytes_per_assessment_run,
        ),
        marker_sql,
    )
    wal = _multiply(daily, c.wal_bytes_per_new_observation, Decimal(c.wal_retention_days))
    live_disk = _sum(sql, wal, c.cache_bytes, c.operating_system_spool_bytes, dump)
    local_backups = _multiply(backup_bytes, c.local_backup_family_fraction)
    restore_disk = _multiply(_sum(stored, sql), c.restore_local_copy_fraction)
    disk = _multiply(_sum(live_disk, local_backups, restore_disk), 1 + c.disk_headroom_fraction)
    restore_reads = _multiply(
        _sum(objects, c.backup_metadata_objects_per_capture), c.restore_rehearsals_per_month
    )
    restore_bytes = _multiply(_sum(stored, dump, inventories), c.restore_rehearsals_per_month)
    return {
        "observations_per_day": daily,
        "observations_per_month": monthly,
        "retained_observations": retained,
        "source_refresh_requests_per_month_lower_bound": _ceiling(
            products * w.videos_per_product / w.videos_per_request
        )
        * w.refreshes_per_day
        * days,
        "raw_retained_bytes": raw,
        "normalized_retained_bytes": normalized,
        "compacted_copy_bytes": compacted,
        "manifest_retained_bytes": manifests,
        "provenance_retained_bytes": provenance,
        "assessment_retained_bytes": assessments,
        "source_store_bytes": stored,
        "source_objects": objects,
        "source_write_requests": writes,
        "source_read_requests": reads,
        "backup_inventory_head_requests": heads,
        "backup_inventory_list_requests": list_requests,
        "backup_store_bytes": backup_bytes,
        "backup_write_requests": _sum(
            backup_gets, captures_month * c.backup_metadata_objects_per_capture
        ),
        "backup_read_requests": restore_reads,
        "source_store_egress_bytes": _sum(source_bytes, backup_downloads),
        "backup_store_egress_bytes": restore_bytes,
        "vps_outbound_bytes": upload_bytes,
        "vps_inbound_bytes": _sum(
            source_bytes,
            backup_downloads,
            restore_bytes,
            _multiply(monthly, c.upstream_response_bytes_per_observation),
        ),
        "sql_bytes": sql,
        "retired_marker_rows": markers,
        "retired_marker_sql_bytes": marker_sql,
        "backup_captures_per_month": captures_month,
        "backup_full_captures_per_month": full_captures_month,
        "backup_effective_full_period_days": Decimal(effective_full_days),
        "backup_max_family_snapshots": Decimal(chain_snapshots),
        "backup_reserved_families": families,
        "backup_reserved_captures": captures,
        "sql_dump_bytes": dump,
        "wal_bytes": wal,
        "vps_working_disk_bytes": disk,
        "vps_live_disk_bytes": live_disk,
        "vps_local_backup_family_bytes": local_backups,
        "vps_restore_copy_bytes": restore_disk,
    }


def _charge(
    amount: Quantity,
    price: Decimal,
    unit: Decimal,
    free: Decimal = Decimal(0),
    rounded: bool = False,
) -> Quantity:
    if price == 0:
        return Decimal(0)
    if amount is None:
        return None
    billed = max(Decimal(0), amount - free) / unit
    return (_ceiling(billed) if rounded else billed) * price


def _store_charges(
    store: Store, quantities: dict[str, Quantity], prefix: str, free: bool
) -> dict[str, Quantity]:
    retained, outgoing = (
        quantities[prefix + "_store_bytes"],
        quantities[prefix + "_store_egress_bytes"],
    )
    gb = None if retained is None else retained / GB
    free_storage = store.storage_free_gb if free else Decimal(0)
    grain = store.storage_billing_grain_gb
    storage = _charge(
        gb,
        store.storage_price_per_gb_month * (grain or 1),
        grain or Decimal(1),
        free_storage,
        bool(grain),
    )
    free_egress = _sum(store.egress_free_gb, _multiply(gb, store.egress_free_storage_multiple))
    egress = (
        Decimal(0)
        if store.egress_price_per_gb == 0
        else None
        if outgoing is None or free_egress is None
        else _charge(outgoing / GB, store.egress_price_per_gb, Decimal(1), free_egress)
    )
    return {
        prefix + "_storage": storage,
        prefix + "_writes_and_lists": _charge(
            quantities[prefix + "_write_requests"],
            store.write_price_per_unit,
            store.write_unit,
            store.write_free_requests if free else Decimal(0),
            store.round_request_units_up,
        ),
        prefix + "_reads_and_heads": _charge(
            quantities[prefix + "_read_requests"],
            store.read_price_per_unit,
            store.read_unit,
            store.read_free_requests if free else Decimal(0),
            store.round_request_units_up,
        ),
        prefix + "_egress": egress,
    }


def estimate(scenario: Scenario) -> dict:
    """Return transparent subtotals; an unknown amount never becomes a total of zero."""
    quantities = usage(scenario.workload, scenario.coefficients)
    if set(scenario.usage_overrides).difference(quantities):
        raise ValueError("Unknown usage override; use a named quantity from the report")
    quantities.update(scenario.usage_overrides)
    m = scenario.machine
    charges: dict[str, Quantity] = {"machine": m.monthly_price}
    if scenario.store == scenario.backup_store:
        account = dict(quantities)
        for suffix in ("store_bytes", "store_egress_bytes", "write_requests", "read_requests"):
            account["source_" + suffix] = _sum(
                quantities["source_" + suffix], quantities["backup_" + suffix]
            )
        charges.update(
            {
                key.replace("source_", "object_account_", 1): value
                for key, value in _store_charges(
                    scenario.store, account, "source", scenario.account_free_tier_available
                ).items()
            }
        )
    else:
        charges.update(
            _store_charges(
                scenario.store, quantities, "source", scenario.account_free_tier_available
            )
        )
        charges.update(
            _store_charges(
                scenario.backup_store, quantities, "backup", scenario.account_free_tier_available
            )
        )
    disk = quantities["vps_working_disk_bytes"]
    extra_disk = None if disk is None else max(Decimal(0), disk - m.included_disk_bytes)
    if extra_disk is not None and extra_disk > 0:
        extra_disk = (
            _ceiling(max(extra_disk, m.extra_disk_minimum_bytes) / m.extra_disk_billing_grain_bytes)
            * m.extra_disk_billing_grain_bytes
        )
    charges["extra_disk"] = (
        Decimal(0)
        if extra_disk == 0
        else _multiply(
            extra_disk,
            None
            if m.extra_disk_price_per_gb_month is None
            else m.extra_disk_price_per_gb_month / GB,
        )
    )
    traffic = quantities["vps_outbound_bytes"]
    outbound = traffic
    if m.inbound_counts_toward_transfer:
        traffic = _sum(traffic, quantities["vps_inbound_bytes"])
    excess = (
        Decimal(0)
        if outbound == 0
        else None
        if traffic is None
        else max(Decimal(0), traffic - m.included_transfer_bytes)
    )
    if m.inbound_counts_toward_transfer and excess is not None and outbound is not None:
        excess = min(excess, outbound)
    charges["vps_transfer_overage"] = (
        Decimal(0)
        if excess == 0
        else _multiply(
            excess,
            None if m.extra_transfer_price_per_gb is None else m.extra_transfer_price_per_gb / GB,
        )
    )
    charges["source_api"] = scenario.coefficients.source_api_monthly_cost
    charges["other"] = scenario.coefficients.other_monthly_cost
    unknown = sorted(key for key, value in charges.items() if value is None)
    known = sum((value for value in charges.values() if value is not None), Decimal(0))
    total = None if unknown else known
    infrastructure_unknown = [key for key in unknown if key not in {"source_api", "other"}]
    infrastructure = (
        None
        if infrastructure_unknown
        else sum(
            (
                value
                for key, value in charges.items()
                if key not in {"source_api", "other"} and value is not None
            ),
            Decimal(0),
        )
    )
    taxed = _multiply(total, None if scenario.tax_rate is None else 1 + scenario.tax_rate)
    return {
        "schema_version": "1",
        "currency": m.currency,
        "price_dates": {
            "machine": str(m.observed_on),
            "source_store": str(scenario.store.observed_on),
            "backup_store": str(scenario.backup_store.observed_on),
        },
        "workload": scenario.workload.model_dump(mode="json"),
        "coefficient_source": scenario.coefficients.source,
        "coefficients": scenario.coefficients.model_dump(mode="json"),
        "quantities": {
            key: None if value is None else str(value) for key, value in quantities.items()
        },
        "monthly_charges_excluding_tax": {
            key: None if value is None else str(value) for key, value in charges.items()
        },
        "known_subtotal_excluding_tax": str(known),
        "modeled_infrastructure_excluding_tax": None
        if infrastructure is None
        else str(infrastructure),
        "working_disk_fits_advertised_capacity": None
        if disk is None
        else disk <= m.included_disk_bytes,
        "monthly_total_excluding_tax": None if total is None else str(total),
        "monthly_total_including_tax": None if taxed is None else str(taxed),
        "unknown_costs": unknown,
        "tax_rate": None if scenario.tax_rate is None else str(scenario.tax_rate),
        "unmeasured_quantities": sorted(key for key, value in quantities.items() if value is None),
        "capacity_verified": False,
        "limitations": [
            "Steady-state scenario arithmetic, not a VPS throughput or memory certificate.",
            "Published storage quotas are account-wide; existing unrelated usage must be supplied.",
            "Provider conditional/version semantics require separate acceptance.",
            "CPU, RAM, backup verification and retained-history coefficients require measurement.",
        ],
    }
