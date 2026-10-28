"""One exact feature computation path for published and frozen assessments."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .analytics import AssessmentIdentity, normalize_assessments
from .assessment_eligibility import AssessmentEligibility
from .contracts import AssessmentPolicy, Product, ProductLink, digest
from .evidence_windows import EvidenceWindows
from .feature_projection import ProjectionScope
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
        self._windows = EvidenceWindows(history, provenance)

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
        identity.validate(cutoff, synthetic, policy.min_link_confidence)
        eligibility_identity = (
            eligibility.cache_identity(cutoff)
            if eligibility is not None
            else "all-history-inputs-v1"
        )
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
                prepared = self._windows.prepare(
                    scopes, cutoff, policy, links=links, identity=identity, eligibility=eligibility
                )
                rows.extend(prepared.rows)
                input_scopes.extend(prepared.input_scopes)
                counts.update(prepared.observation_counts)
                hits += prepared.hits
                rebuilds += prepared.rebuilds
                delta_reads += prepared.delta_reads
            return PreparedAssessments(
                normalize_assessments(rows, policy),
                input_scopes,
                counts,
                hits,
                rebuilds,
                delta_reads,
            )
