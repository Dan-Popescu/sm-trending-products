"""One exact feature computation path for published and frozen assessments."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .analysis_batches import RowBudgetExceeded
from .analytics import AssessmentIdentity, assess, normalize_assessments
from .assessment_eligibility import AssessmentEligibility
from .contracts import AssessmentPolicy, Product, ProductLink, digest
from .feature_projection import FeatureProjection, ProjectionScope, WindowRead
from .history import History
from .provenance import Provenance


@dataclass
class PreparedAssessments:
    rows: list[dict[str, Any]]
    input_scopes: list[Any]
    observation_counts: Counter[str]
    hits: int
    rebuilds: int
    delta_reads: int


class AssessmentService:
    """Reuse exact compact rows, or rebuild dirty products from bounded inputs.

    Callers supply resolved ownership and captured eligibility facts. Eligibility
    owns both filtering and its stable projection identity; the reader supplies
    the same authoritative clock and lookback to both. Every input root remains
    attached to the ordinary publication, including a full feature-cache hit.
    """

    def __init__(self, history: History, provenance: Provenance):
        self.history, self.provenance = history, provenance
        self.projection = FeatureProjection(history.catalog)

    def _windows(
        self, scopes: Sequence[ProjectionScope], cutoff: datetime, policy: AssessmentPolicy
    ) -> Iterator[tuple[ProjectionScope, WindowRead]]:
        if not scopes:
            return
        try:
            windows = self.projection.load_windows(self.history, scopes, cutoff, policy)
        except RowBudgetExceeded:
            if len(scopes) == 1:
                raise
            middle = len(scopes) // 2
            yield from self._windows(scopes[:middle], cutoff, policy)
            yield from self._windows(scopes[middle:], cutoff, policy)
        else:
            yield from zip(scopes, windows, strict=True)

    def prepare(
        self,
        products: Sequence[Product],
        links: Sequence[ProductLink],
        identity: AssessmentIdentity,
        cutoff: datetime,
        synthetic: bool,
        policy: AssessmentPolicy,
        *,
        eligibility: AssessmentEligibility | None = None,
    ) -> PreparedAssessments:
        eligibility_identity = (
            eligibility.cache_identity(cutoff)
            if eligibility is not None
            else "all-history-inputs-v1"
        )
        since = cutoff - timedelta(days=policy.lookback_days)
        with self.history.read_session():
            rows: list[dict[str, Any]] = []
            input_scopes: list[Any] = []
            counts: Counter[str] = Counter()
            hits = rebuilds = delta_reads = 0
            batch_size = self.history.settings.analysis_product_batch_size
            for start in range(0, len(products), batch_size):
                batch = products[start : start + batch_size]
                scopes = [
                    ProjectionScope(
                        product,
                        frozenset(
                            link.entity_key
                            for link in identity.by_product.get(product.product_id, ())
                        ),
                        digest(
                            {
                                "links": [
                                    link.model_dump(mode="json")
                                    for link in identity.by_product.get(product.product_id, ())
                                ],
                                "eligibility": eligibility_identity,
                            }
                        ),
                    )
                    for product in batch
                ]
                reusable = self.projection.read_many(scopes, cutoff, policy)
                self.history.analysis_metrics["projection_hits"] += len(reusable.rows)
                self.history.analysis_metrics["projection_misses"] += len(scopes) - len(
                    reusable.rows
                )
                for scope in scopes:
                    product_id = scope.product.product_id
                    if product_id in reusable.rows:
                        rows.append(reusable.rows[product_id])
                        input_scopes.append(reusable.input_provenance[product_id])
                        counts.update(reusable.observation_counts[product_id])
                        hits += 1
                misses = [
                    scope for scope in scopes if scope.product.product_id not in reusable.rows
                ]
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
                        key
                        for key, _at in {(row.entity_key, row.observed_at) for row in observations}
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
                    self.projection.store(
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
                        self.projection.store_window(
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
            return PreparedAssessments(
                normalize_assessments(rows, policy),
                input_scopes,
                counts,
                hits,
                rebuilds,
                delta_reads,
            )
