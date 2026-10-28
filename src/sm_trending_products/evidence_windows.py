"""Complete, bounded Evidence windows and their coherent feature publication.

Callers provide analytical scope and policy. Source proofs, all-input lineage,
projection reuse and both guarded cache writes remain inside this module.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .analysis_batches import MetadataBudgetExceeded, RowBudgetExceeded
from .analytics import AssessmentIdentity, assess
from .assessment_eligibility import AssessmentEligibility
from .contracts import AssessmentPolicy, ProductLink
from .feature_projection import FeatureProjection, ProjectionScope, WindowRead
from .history import History
from .provenance import Provenance


@dataclass
class PreparedEvidence:
    """Complete unnormalized features and the exact facts behind them."""

    rows: list[dict[str, Any]]
    input_scopes: list[Any]
    observation_counts: Counter[str]
    hits: int
    rebuilds: int
    delta_reads: int


class EvidenceWindows:
    def __init__(self, history: History, provenance: Provenance):
        self.history, self.provenance = history, provenance
        self._projection = FeatureProjection(history.catalog)

    def _windows(
        self, scopes: Sequence[ProjectionScope], cutoff: datetime, policy: AssessmentPolicy
    ) -> Iterator[tuple[ProjectionScope, WindowRead]]:
        if not scopes:
            return
        try:
            windows = self._projection.load_windows(self.history, scopes, cutoff, policy)
        except (RowBudgetExceeded, MetadataBudgetExceeded):
            if len(scopes) == 1:
                raise
            middle = len(scopes) // 2
            yield from self._windows(scopes[:middle], cutoff, policy)
            yield from self._windows(scopes[middle:], cutoff, policy)
        else:
            yield from zip(scopes, windows, strict=True)

    def prepare(
        self,
        scopes: Sequence[ProjectionScope],
        cutoff: datetime,
        policy: AssessmentPolicy,
        *,
        links: Sequence[ProductLink],
        identity: AssessmentIdentity,
        eligibility: AssessmentEligibility | None = None,
    ) -> PreparedEvidence:
        """Own every hit/rebuild path; callers never assemble coverage proofs."""
        if not scopes:
            return PreparedEvidence([], [], Counter(), 0, 0, 0)
        synthetic = scopes[0].product.synthetic
        if any(scope.product.synthetic != synthetic for scope in scopes):
            raise ValueError("Evidence partitions must share their namespace")
        identity.validate(cutoff, synthetic, policy.min_link_confidence)
        since = cutoff - timedelta(days=policy.lookback_days)
        rows: list[dict[str, Any]] = []
        input_scopes: list[Any] = []
        counts: Counter[str] = Counter()
        hits = rebuilds = delta_reads = 0
        with self.history.read_session():
            reusable = self._projection.read_many(scopes, cutoff, policy)
            self.history.analysis_metrics["projection_hits"] += len(reusable.rows)
            self.history.analysis_metrics["projection_misses"] += len(scopes) - len(reusable.rows)
            for scope in scopes:
                product_id = scope.product.product_id
                if product_id in reusable.rows:
                    rows.append(reusable.rows[product_id])
                    input_scopes.append(reusable.input_provenance[product_id])
                    counts.update(reusable.observation_counts[product_id])
                    hits += 1
            misses = [scope for scope in scopes if scope.product.product_id not in reusable.rows]
            for scope, window in self._windows(misses, cutoff, policy):
                mode_counter = {
                    "delta": "delta_windows",
                    "full": "full_windows",
                    "duplicate_rebuild": "duplicate_rebuild_windows",
                }[window.mode]
                self.history.analysis_metrics[mode_counter] += 1
                product_id = scope.product.product_id
                observations = (
                    eligibility.select(
                        window.observations, cutoff=cutoff, synthetic=synthetic, since=since
                    )
                    if eligibility is not None
                    else window.observations
                )
                product_counts = Counter(
                    key for key, _at in {(row.entity_key, row.observed_at) for row in observations}
                )
                counts.update(product_counts)
                anchors: dict[str, datetime | None] = {}
                raw = assess(
                    observations,
                    [scope.product],
                    links,
                    cutoff,
                    synthetic,
                    policy,
                    normalize=False,
                    identity=identity,
                    age_anchors=anchors,
                )
                row = self.provenance.compact(raw, observations, synthetic)[0]
                # Retain even market/capability-excluded facts for both window
                # retirement and the ordinary result's complete input lineage.
                input_root = self.provenance.audit(window.observations, synthetic)
                feature_root = row["evidence_refs"] or input_root
                audit_key = feature_root["root"]["key"] if feature_root else None
                self._projection.store(
                    scope,
                    cutoff,
                    policy,
                    window.frontier,
                    row,
                    window.observations,
                    age_anchor=anchors.get(product_id),
                    audit_root=audit_key,
                    input_provenance=input_root,
                    observation_counts=product_counts,
                )
                if input_root:
                    self._projection.store_window(
                        self.history,
                        scope,
                        cutoff,
                        policy,
                        window,
                        audit_root=input_root["root"]["key"],
                    )
                rows.append(row)
                input_scopes.append(input_root)
                rebuilds += 1
                delta_reads += window.mode == "delta"
        return PreparedEvidence(rows, input_scopes, counts, hits, rebuilds, delta_reads)
