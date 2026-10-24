"""Disposable exact product features between source and clock transitions.

History and the normal assessor remain authoritative. A projection contains an
unnormalized compact product row, its immutable audit root and a conservative
clock interval. Reuse advances the clock analytically; global cohort/rank
normalization still runs over the complete current universe.
"""

from __future__ import annotations

import io
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from .analysis_batches import RowBudgetExceeded
from .cache import local_cache_descriptor, pin_local_cached, store_local_cached
from .catalog import Catalog
from .contracts import AssessmentPolicy, Observation, Product, canonical, digest, utc
from .history import History

PROJECTION_VERSION = "exact-product-features-v3"
MAX_PROJECTION_BYTES = 1_000_000
MAX_WINDOW_BYTES = 64 * 1024 * 1024
WINDOW_ARROW_BATCH_ROWS = 1000


@dataclass(frozen=True)
class ProjectionScope:
    """Resolved ownership supplied by the caller's authoritative identity context."""

    product: Product
    entity_keys: frozenset[str]
    identity_version: str

    def context(self, policy: AssessmentPolicy) -> str:
        return digest(
            {
                "version": PROJECTION_VERSION,
                "product": self.product.model_dump(mode="json"),
                "entities": sorted(self.entity_keys),
                "identity": self.identity_version,
                "policy": policy.model_dump(mode="json"),
            }
        )

    def window_context(self, policy: AssessmentPolicy) -> str:
        """Raw facts are independent of feature weights, identity history and gates."""
        return digest(
            {
                "format": "exact-raw-window-v1",
                "product_id": self.product.product_id,
                "synthetic": self.product.synthetic,
                "entities": sorted(self.entity_keys),
                "lookback_days": policy.lookback_days,
            }
        )


@dataclass(frozen=True)
class SourceFrontier:
    fingerprint: str
    latest_available_at: datetime | None
    latest_observed_at: datetime | None
    unknown_bounds: bool

    def fully_visible(self, cutoff: datetime) -> bool:
        return not self.unknown_bounds and all(
            value is None or value <= cutoff
            for value in (self.latest_available_at, self.latest_observed_at)
        )


@dataclass
class ProjectionRead:
    """One bulk read's hits and source tokens, also used to fence cache writes."""

    rows: dict[str, dict[str, Any]]
    frontiers: dict[str, SourceFrontier]
    audit_roots: dict[str, str]
    reasons: dict[str, str]
    input_provenance: dict[str, Any] = field(default_factory=dict)
    observation_counts: dict[str, dict[str, int]] = field(default_factory=dict)


def _freshness_interval(hours: float) -> timedelta:
    """Conservatively satisfy both authoritative floating-point age checks.

    Analytics compares seconds in some dimensions and hours in attention.
    timedelta rounds fractional microseconds; rounding upward can otherwise
    permit a cached row one microsecond after the actual oracle turns stale.
    Ordinary integral durations retain their inclusive equality boundary.
    """
    interval = timedelta(hours=hours)
    while interval.total_seconds() > hours * 3600 or interval.total_seconds() / 3600 > hours:
        interval -= timedelta(microseconds=1)
    return interval


def projection_valid_through(
    observations: Sequence[Observation], cutoff: datetime, policy: AssessmentPolicy
) -> datetime | None:
    """First inclusive timestamp at which an input can expire or become stale.

    Deadlines cover all evidence, deliberately including records that currently
    contribute no metric. A source correction can make such a record useful.
    Existing feature code uses inclusive windows/freshness; equality is safe and
    any later instant rebuilds. Malformed period declarations never add a fact.
    """
    cutoff = utc(cutoff)
    try:
        freshness = _freshness_interval(policy.max_age_hours)
        lookback = timedelta(days=policy.lookback_days)
    except OverflowError:
        return cutoff
    deadlines: list[datetime] = []
    for row in observations:
        for interval in (lookback, freshness):
            try:
                deadline = utc(row.observed_at) + interval
            except OverflowError:
                return cutoff
            if deadline >= cutoff:
                deadlines.append(deadline)
        for name in ("period_end", "source_period_end"):
            try:
                period_end = utc(row.attributes[name])
                deadline = period_end + freshness
            except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
                continue
            if deadline >= cutoff:
                deadlines.append(deadline)
    return min(deadlines) if deadlines else None


def advance_projection(
    record: Mapping[str, Any], cutoff: datetime, context: str, frontier: SourceFrontier
) -> dict[str, Any] | None:
    """Advance only the continuous age feature; all discrete changes rebuild."""
    cutoff = utc(cutoff)
    computed_at = utc(record["computed_at"])
    if (
        record["projection_version"] != PROJECTION_VERSION
        or record["context_sha256"] != context
        or record["source_sha256"] != frontier.fingerprint
        or cutoff < computed_at
        or not frontier.fully_visible(computed_at)
        or (record["valid_through"] is not None and cutoff > utc(record["valid_through"]))
    ):
        return None
    row = deepcopy(record["payload"])
    row["as_of"] = cutoff.isoformat()
    age_anchor = record["age_anchor"]
    if age_anchor is not None:
        row["features"]["age_hours"] = max(0.0, (cutoff - utc(age_anchor)).total_seconds() / 3600)
    elif row["features"].get("age_hours") is not None:
        return None
    return row


@dataclass
class WindowRead:
    """Exact current inputs plus a metadata proof usable for a local rolling state."""

    observations: list[Observation]
    manifests: list[dict[str, Any]]
    object_ids: tuple[int, ...]
    objects_sha256: str
    frontier: SourceFrontier
    mode: str
    source_objects_read: int
    state_bytes: int = 0


def _encode_window(observations: Sequence[Observation], max_bytes: int) -> bytes | None:
    """Encode fixed-contract Arrow batches without materializing all row dictionaries."""
    names = (
        "schema_version",
        "observation_id",
        "source",
        "entity_type",
        "entity_id",
        "observed_at",
        "available_at",
        "synthetic",
        "published_at",
        "market",
        "account_id",
        "metrics_json",
        "attributes_json",
        "missing_fields_json",
        "raw_dataset_id",
        "entity_key",
    )
    schema = pa.schema(
        [(name, pa.bool_() if name == "synthetic" else pa.string()) for name in names]
    )
    stream = io.BytesIO()
    with pq.ParquetWriter(stream, schema, compression="zstd") as writer:
        for start in range(0, len(observations), WINDOW_ARROW_BATCH_ROWS):
            rows = []
            for observation in observations[start : start + WINDOW_ARROW_BATCH_ROWS]:
                row = observation.model_dump(mode="json")
                row["entity_key"] = observation.entity_key
                for name in ("metrics", "attributes", "missing_fields"):
                    row[name + "_json"] = canonical(row.pop(name))
                rows.append(row)
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
            if stream.tell() > max_bytes:
                return None
    return stream.getvalue() if stream.tell() <= max_bytes else None


class FeatureProjection:
    """Bulk, rebuildable cache for exact unnormalized product feature rows.

    Callers assess a miss through History and the normal analytics engine, then
    compact its audit references before store(). A hit's audit root must join the
    normal publication dependency tree; it never substitutes for publication's
    authoritative retention/input guards. Invalidation has no business effect:
    it merely makes the next request rebuild the affected product.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    @staticmethod
    def _frontiers(connection: Any, scopes: Sequence[ProjectionScope]) -> dict[str, SourceFrontier]:
        keys = sorted({key for scope in scopes for key in scope.entity_keys})
        namespaces = sorted({scope.product.synthetic for scope in scopes})
        rows = connection.execute(
            "SELECT entity_key,synthetic,generation,max_available_at,max_observed_at,"
            "unknown_bounds FROM feature_source_frontiers "
            "WHERE entity_key=ANY(%s) AND synthetic=ANY(%s)",
            (keys, namespaces),
        ).fetchall()
        by_entity = {(row["synthetic"], row["entity_key"]): row for row in rows}
        result: dict[str, SourceFrontier] = {}
        for scope in scopes:
            versions = []
            available: list[datetime] = []
            observed: list[datetime] = []
            unknown = False
            for key in sorted(scope.entity_keys):
                record = by_entity.get((scope.product.synthetic, key))
                versions.append((key, record["generation"] if record else 0))
                if record is not None:
                    unknown |= record["unknown_bounds"]
                    if record["max_available_at"] is not None:
                        available.append(record["max_available_at"])
                    if record["max_observed_at"] is not None:
                        observed.append(record["max_observed_at"])
            result[scope.product.product_id] = SourceFrontier(
                digest(versions), max(available, default=None), max(observed, default=None), unknown
            )
        return result

    def read_many(
        self, scopes: Sequence[ProjectionScope], cutoff: datetime, policy: AssessmentPolicy
    ) -> ProjectionRead:
        cutoff = utc(cutoff)
        if len({scope.product.product_id for scope in scopes}) != len(scopes):
            raise ValueError("Projection scopes require unique products")
        if not scopes:
            return ProjectionRead({}, {}, {}, {})
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            frontiers = self._frontiers(connection, scopes)
            records = connection.execute(
                "SELECT p.*,d.state AS audit_state,i.state AS input_audit_state "
                "FROM feature_product_projections p "
                "LEFT JOIN derived_publications d ON d.object_key=p.audit_root "
                "LEFT JOIN derived_publications i ON i.object_key=p.input_audit_root "
                "WHERE product_id=ANY(%s)",
                ([scope.product.product_id for scope in scopes],),
            ).fetchall()
        by_product = {record["product_id"]: record for record in records}
        result = ProjectionRead({}, frontiers, {}, {})
        for scope in scopes:
            product_id = scope.product.product_id
            record = by_product.get(product_id)
            if record is None:
                result.reasons[product_id] = "missing_projection"
                continue
            if record["audit_root"] is not None and record["audit_state"] != "committed":
                result.reasons[product_id] = "unavailable_audit_root"
                continue
            if (
                record["input_audit_root"] is not None
                and record["input_audit_state"] != "committed"
            ):
                result.reasons[product_id] = "unavailable_input_audit_root"
                continue
            row = advance_projection(record, cutoff, scope.context(policy), frontiers[product_id])
            if row is None:
                result.reasons[product_id] = "source_identity_policy_or_clock_changed"
                continue
            result.rows[product_id] = row
            result.input_provenance[product_id] = record["input_provenance"]
            result.observation_counts[product_id] = record["observation_counts"]
            if record["audit_root"] is not None:
                result.audit_roots[product_id] = record["audit_root"]
        return result

    def store(
        self,
        scope: ProjectionScope,
        cutoff: datetime,
        policy: AssessmentPolicy,
        frontier: SourceFrontier,
        row: dict[str, Any],
        observations: Sequence[Observation],
        *,
        age_anchor: datetime | None,
        audit_root: str | None,
        input_provenance: Any = None,
        observation_counts: Mapping[str, int] | None = None,
    ) -> bool:
        """Save a compact oracle result only if the read source token is still current."""
        cutoff = utc(cutoff)
        if row.get("product_id") != scope.product.product_id or row.get("score") is not None:
            raise ValueError("Projection requires this product's unnormalized feature row")
        if len(canonical(row).encode()) > MAX_PROJECTION_BYTES:
            return False
        if observations and audit_root is None:
            return False
        if age_anchor is None and row["features"].get("age_hours") is not None:
            return False
        if age_anchor is not None and row["features"].get("age_hours") != max(
            0.0, (cutoff - utc(age_anchor)).total_seconds() / 3600
        ):
            return False
        if not frontier.fully_visible(cutoff):
            return False
        valid_through = projection_valid_through(observations, cutoff, policy)
        input_root = input_provenance["root"]["key"] if input_provenance else None
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            current = self._frontiers(connection, [scope])[scope.product.product_id]
            if current != frontier:
                return False
            for root in {audit_root, input_root} - {None}:
                audit = connection.execute(
                    "SELECT 1 FROM derived_publications WHERE object_key=%s AND state='committed'",
                    (root,),
                ).fetchone()
                if audit is None:
                    return False
            saved = connection.execute(
                "INSERT INTO feature_product_projections(product_id,synthetic,projection_version,"
                "context_sha256,source_sha256,computed_at,valid_through,"
                "age_anchor,payload,audit_root,input_audit_root,input_provenance,"
                "observation_counts) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(product_id) DO UPDATE SET "
                "synthetic=EXCLUDED.synthetic,projection_version=EXCLUDED.projection_version,"
                "context_sha256=EXCLUDED.context_sha256,source_sha256=EXCLUDED.source_sha256,"
                "computed_at=EXCLUDED.computed_at,valid_through=EXCLUDED.valid_through,"
                "age_anchor=EXCLUDED.age_anchor,payload=EXCLUDED.payload,"
                "audit_root=EXCLUDED.audit_root,input_audit_root=EXCLUDED.input_audit_root,"
                "input_provenance=EXCLUDED.input_provenance,"
                "observation_counts=EXCLUDED.observation_counts "
                "WHERE feature_product_projections.computed_at<=EXCLUDED.computed_at",
                (
                    scope.product.product_id,
                    scope.product.synthetic,
                    PROJECTION_VERSION,
                    scope.context(policy),
                    frontier.fingerprint,
                    cutoff,
                    valid_through,
                    utc(age_anchor) if age_anchor is not None else None,
                    Jsonb(row),
                    audit_root,
                    input_root,
                    Jsonb(input_provenance or []),
                    Jsonb(dict(observation_counts or {})),
                ),
            )
        return saved.rowcount == 1

    @staticmethod
    def _object_fingerprint(records: Sequence[dict[str, Any]]) -> str:
        return digest(sorted((row["object_id"], row["descriptor"]["sha256"]) for row in records))

    def load_window(
        self,
        history: History,
        scope: ProjectionScope,
        cutoff: datetime,
        policy: AssessmentPolicy,
    ) -> WindowRead:
        """Read one exact window; batch callers should share reads with load_windows."""
        return self.load_windows(history, [scope], cutoff, policy)[0]

    def load_windows(
        self,
        history: History,
        scopes: Sequence[ProjectionScope],
        cutoff: datetime,
        policy: AssessmentPolicy,
    ) -> list[WindowRead]:
        """Share each new/cold source parse across a bounded product partition.

        Local states remain optional. Missing, evicted, corrupt, corrected or
        compacted state contributes its full source selection to the same shared
        read, while intact states contribute only new source objects. Metadata
        and decoded observations are bounded by the caller's adaptive partition.
        """
        if not scopes:
            return []
        synthetic = scopes[0].product.synthetic
        if any(scope.product.synthetic != synthetic for scope in scopes):
            raise ValueError("Window partitions must share their namespace")
        owners: dict[str, str] = {}
        for scope in scopes:
            for key in scope.entity_keys:
                if key in owners:
                    raise ValueError("Window partitions require exclusive entity ownership")
                owners[key] = scope.product.product_id
        cutoff = utc(cutoff)
        since = cutoff - timedelta(days=policy.lookback_days)
        limit = history.settings.analysis_max_rows_per_batch
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            frontiers = self._frontiers(connection, scopes)
            manifests = list(
                self.catalog.iter_manifests(
                    synthetic=synthetic,
                    as_of=cutoff,
                    since=since,
                    until=cutoff,
                    entity_keys=set(owners),
                )
            )
            requested = {
                (manifest["dataset_id"], obj["key"])
                for manifest in manifests
                for obj in manifest["manifest"]["normalized"]
            }
            current = (
                connection.execute(
                    "SELECT object_id,dataset_id,object_key,descriptor FROM normalized_objects "
                    "WHERE dataset_id=ANY(%s) AND object_key=ANY(%s)",
                    (sorted({key[0] for key in requested}), sorted({key[1] for key in requested})),
                ).fetchall()
                if requested
                else []
            )
            by_pair = {
                (row["dataset_id"], row["object_key"]): row
                for row in current
                if (row["dataset_id"], row["object_key"]) in requested
            }
            records = connection.execute(
                "SELECT w.* FROM feature_product_windows w JOIN derived_publications p "
                "ON p.object_key=w.audit_root WHERE w.product_id=ANY(%s) AND w.state='ready' "
                "AND p.state='committed' AND NOT EXISTS(SELECT 1 FROM retention_objects r "
                "WHERE r.object_key=w.descriptor->>'key')",
                ([scope.product.product_id for scope in scopes],),
            ).fetchall()
            by_product = {record["product_id"]: record for record in records}
            prior_ids = sorted(
                {identifier for record in records for identifier in record["covered_object_ids"]}
            )
            prior = (
                connection.execute(
                    "SELECT o.object_id,o.descriptor,d.state,d.compacted_into "
                    "FROM normalized_objects o JOIN datasets d USING(dataset_id) "
                    "WHERE o.object_id=ANY(%s)",
                    (prior_ids,),
                ).fetchall()
                if prior_ids
                else []
            )
            by_id = {row["object_id"]: row for row in prior}
            # Object metadata is partitioned once; no payload is decoded here.
            pairs_by_product: dict[str, set[tuple[str, str]]] = {
                scope.product.product_id: set() for scope in scopes
            }
            for manifest in manifests:
                for obj in manifest["manifest"]["normalized"]:
                    products = {owners[key] for key in obj["entity_keys"] if key in owners}
                    for product_id in products:
                        pairs_by_product[product_id].add((manifest["dataset_id"], obj["key"]))

            def selected_manifests(pairs: set[tuple[str, str]]) -> list[dict[str, Any]]:
                return [
                    {
                        **manifest,
                        "manifest": {
                            **manifest["manifest"],
                            "normalized": [
                                obj
                                for obj in manifest["manifest"]["normalized"]
                                if (manifest["dataset_id"], obj["key"]) in pairs
                            ],
                        },
                    }
                    for manifest in manifests
                    if any(
                        (manifest["dataset_id"], obj["key"]) in pairs
                        for obj in manifest["manifest"]["normalized"]
                    )
                ]

            plans: dict[str, dict[str, Any]] = {}
            union_pairs: set[tuple[str, str]] = set()
            retained_count = 0
            for scope in scopes:
                product_id = scope.product.product_id
                pairs = pairs_by_product[product_id]
                objects = [by_pair[pair] for pair in pairs if pair in by_pair]
                object_ids = (
                    tuple(sorted(row["object_id"] for row in objects))
                    if len(objects) == len(pairs)
                    else ()
                )
                record = by_product.get(product_id)
                retained = None
                state_bytes = 0
                covered: set[int] = set()
                if (
                    record is not None
                    and record["context_sha256"] == scope.window_context(policy)
                    and utc(record["computed_at"]) <= cutoff
                    and frontiers[product_id].fully_visible(cutoff)
                    and len(objects) == len(pairs)
                ):
                    covered = set(record["covered_object_ids"])
                    previous = [by_id[key] for key in covered if key in by_id]
                    removed = [row for row in previous if row["object_id"] not in object_ids]
                    safe = (
                        len(previous) == len(covered)
                        and self._object_fingerprint(previous) == record["covered_sha256"]
                        and all(
                            row["state"] == "committed" and row["compacted_into"] is None
                            for row in previous
                        )
                        and all(
                            row["descriptor"].get("max_observed_at") is not None
                            and utc(row["descriptor"]["max_observed_at"]) < since
                            for row in removed
                        )
                    )
                    if safe:
                        try:
                            with pin_local_cached(history, record["descriptor"]) as path:
                                if path is not None:
                                    frame = (
                                        pl.scan_parquet(path)
                                        .filter(
                                            (
                                                pl.col("observed_at").str.to_datetime(
                                                    time_zone="UTC"
                                                )
                                                >= since
                                            )
                                            & (
                                                pl.col("observed_at").str.to_datetime(
                                                    time_zone="UTC"
                                                )
                                                <= cutoff
                                            )
                                            & (
                                                pl.col("available_at").str.to_datetime(
                                                    time_zone="UTC"
                                                )
                                                <= cutoff
                                            )
                                            & pl.col("entity_key").is_in(sorted(scope.entity_keys))
                                        )
                                        .head(limit + 1)
                                        .collect(engine="streaming")
                                    )
                                    if frame.height > limit:
                                        raise RowBudgetExceeded(limit)
                                    retained = [
                                        history._decode(row) for row in frame.iter_rows(named=True)
                                    ]
                                    state_bytes = record["descriptor"]["bytes"]
                        except RowBudgetExceeded:
                            raise
                        except (OSError, ValueError, pl.exceptions.PolarsError):
                            retained = None
                selected_pairs = (
                    pairs
                    if retained is None
                    else {
                        (row["dataset_id"], row["object_key"])
                        for row in objects
                        if row["object_id"] not in covered
                    }
                )
                retained_count += len(retained or ())
                if retained_count > limit:
                    raise RowBudgetExceeded(limit)
                union_pairs.update(selected_pairs)
                plans[product_id] = {
                    "pairs": pairs,
                    "objects": objects,
                    "object_ids": object_ids,
                    "retained": retained,
                    "state_bytes": state_bytes,
                    "source_objects_read": len(selected_pairs),
                }
            # A cold shared file is parsed once even when hundreds of products
            # lack local state. Adaptive callers split if the combined rows exceed
            # the same existing analytical budget.
            source_rows = history.read(
                selected_manifests(union_pairs),
                as_of=cutoff,
                since=since,
                until=cutoff,
                entity_keys=set(owners),
            )
            loaded: dict[str, list[Observation]] = {
                scope.product.product_id: [] for scope in scopes
            }
            for row in source_rows:
                loaded[owners[row.entity_key]].append(row)
            results = []
            total = 0
            for scope in scopes:
                product_id = scope.product.product_id
                plan = plans[product_id]
                retained = plan["retained"]
                observations = loaded[product_id]
                source_count = plan["source_objects_read"]
                mode = "full" if retained is None else "delta"
                if retained is not None:
                    previous_rows = {row.observation_id: row for row in retained}
                    changed_delivery = any(
                        row.observation_id in previous_rows
                        and row != previous_rows[row.observation_id]
                        for row in observations
                    )
                    if changed_delivery:
                        observations = history.read(
                            selected_manifests(plan["pairs"]),
                            as_of=cutoff,
                            since=since,
                            until=cutoff,
                            entity_keys=set(scope.entity_keys),
                        )
                        mode = "duplicate_rebuild"
                        source_count += len(plan["pairs"])
                    else:
                        observations = sorted(
                            [
                                *retained,
                                *(
                                    row
                                    for row in observations
                                    if row.observation_id not in previous_rows
                                ),
                            ],
                            key=lambda row: (row.observed_at, row.observation_id),
                        )
                total += len(observations)
                if total > limit:
                    raise RowBudgetExceeded(limit)
                results.append(
                    WindowRead(
                        observations,
                        selected_manifests(plan["pairs"]),
                        plan["object_ids"],
                        self._object_fingerprint(plan["objects"]),
                        frontiers[product_id],
                        mode,
                        source_count,
                        plan["state_bytes"],
                    )
                )
            return results

    def store_window(
        self,
        history: History,
        scope: ProjectionScope,
        cutoff: datetime,
        policy: AssessmentPolicy,
        window: WindowRead,
        *,
        audit_root: str,
    ) -> bool:
        """Stage ownership before local admission; all cached inputs require an audit root."""
        cutoff = utc(cutoff)
        if not window.object_ids or not window.frontier.fully_visible(cutoff):
            return False
        body = _encode_window(
            window.observations, min(MAX_WINDOW_BYTES, history.settings.cache_max_bytes)
        )
        if body is None:
            return False
        descriptor = local_cache_descriptor(
            body,
            digest(
                {
                    "context": scope.window_context(policy),
                    "cutoff": cutoff,
                    "covered": window.objects_sha256,
                    "audit_root": audit_root,
                }
            ),
        )
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            if self._frontiers(connection, [scope])[scope.product.product_id] != window.frontier:
                return False
            root = connection.execute(
                "SELECT 1 FROM derived_publications WHERE object_key=%s AND state='committed'",
                (audit_root,),
            ).fetchone()
            if root is None:
                return False
            staged = connection.execute(
                "INSERT INTO feature_product_windows(product_id,context_sha256,computed_at,"
                "covered_object_ids,covered_sha256,descriptor,audit_root,state) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,'staged') ON CONFLICT(product_id) DO UPDATE SET "
                "context_sha256=EXCLUDED.context_sha256,computed_at=EXCLUDED.computed_at,"
                "covered_object_ids=EXCLUDED.covered_object_ids,covered_sha256=EXCLUDED.covered_sha256,"
                "descriptor=EXCLUDED.descriptor,audit_root=EXCLUDED.audit_root,state='staged' "
                "WHERE feature_product_windows.computed_at<=EXCLUDED.computed_at",
                (
                    scope.product.product_id,
                    scope.window_context(policy),
                    cutoff,
                    list(window.object_ids),
                    window.objects_sha256,
                    Jsonb(descriptor),
                    audit_root,
                ),
            )
            if staged.rowcount != 1:
                return False
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            current = connection.execute(
                "SELECT 1 FROM feature_product_windows w JOIN derived_publications p "
                "ON p.object_key=w.audit_root WHERE w.product_id=%s AND w.descriptor=%s "
                "AND p.state='committed' AND w.state='staged' FOR UPDATE OF w",
                (scope.product.product_id, Jsonb(descriptor)),
            ).fetchone()
            if current is None:
                return False
            admitted = store_local_cached(
                history, body, descriptor=descriptor, connection=connection
            )
            if admitted is None:
                connection.execute(
                    "DELETE FROM feature_product_windows WHERE product_id=%s AND descriptor=%s",
                    (scope.product.product_id, Jsonb(descriptor)),
                )
                return False
            connection.execute(
                "UPDATE feature_product_windows SET state='ready' "
                "WHERE product_id=%s AND descriptor=%s",
                (scope.product.product_id, Jsonb(descriptor)),
            )
        return True

    def rebuild_frontiers(self, scopes: Sequence[ProjectionScope]) -> dict[str, SourceFrontier]:
        """Reconcile conservative bounds after retirement, using metadata only.

        Source writers serialize on the same ordered frontier rows before commit.
        A concurrent unpublished input therefore either appears in these bounds
        or advances the generation after this transaction releases those rows.
        No projection result is trusted merely because its bounds were rebuilt.
        """
        identities = sorted(
            {(scope.product.synthetic, key) for scope in scopes for key in scope.entity_keys}
        )
        if not identities:
            return {}
        keys = [key for _synthetic, key in identities]
        namespaces = [synthetic for synthetic, _key in identities]
        with self.catalog.transaction() as connection:
            self.catalog.guard_retention(connection)
            connection.execute(
                "INSERT INTO feature_source_frontiers(entity_key,synthetic,generation) "
                "SELECT entity_key,synthetic,nextval('feature_source_generation') "
                "FROM unnest(%s::text[],%s::boolean[]) r(entity_key,synthetic) "
                "ORDER BY synthetic,entity_key ON CONFLICT DO NOTHING",
                (keys, namespaces),
            )
            connection.execute(
                "SELECT entity_key FROM feature_source_frontiers "
                "WHERE entity_key=ANY(%s) AND synthetic=ANY(%s) "
                "ORDER BY synthetic,entity_key FOR UPDATE",
                (keys, namespaces),
            ).fetchall()
            connection.execute(
                "WITH bounded AS (SELECT r.entity_key,r.synthetic,b.* FROM "
                "unnest(%s::text[],%s::boolean[]) r(entity_key,synthetic) "
                "LEFT JOIN LATERAL (SELECT max(o.max_available_at) AS available,"
                "max(o.max_observed_at) AS observed,COALESCE(bool_or("
                "o.max_available_at IS NULL OR o.max_observed_at IS NULL),false) AS unknown "
                "FROM normalized_object_entities e JOIN normalized_objects o "
                "ON o.object_id=e.object_id JOIN datasets d USING(dataset_id) "
                "WHERE e.entity_key=r.entity_key AND d.synthetic=r.synthetic "
                "AND d.state='committed' AND d.compacted_into IS NULL) b ON true) "
                "UPDATE feature_source_frontiers f SET "
                "generation=nextval('feature_source_generation'),"
                "max_available_at=b.available,max_observed_at=b.observed,unknown_bounds=b.unknown "
                "FROM bounded b WHERE f.entity_key=b.entity_key AND f.synthetic=b.synthetic",
                (keys, namespaces),
            )
            return self._frontiers(connection, scopes)
