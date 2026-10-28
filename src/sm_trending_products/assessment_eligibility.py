"""Captured evaluation gates own both evidence selection and projection identity."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from .contracts import Capability, Observation, digest, utc
from .evaluation import eligible_evidence


@dataclass(frozen=True)
class AssessmentEligibility:
    """Keep a filter and its reusable context derived from the same captured facts.

    Clock/lookback expiry and workspace identity already belong to the projection
    scope. The stable gate identity includes complete source capability history,
    including future scoped sources, and the grants visible at this cutoff.
    """

    market: str | None
    capabilities: Sequence[Capability]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "capabilities",
            tuple(capability.model_copy(deep=True) for capability in self.capabilities),
        )

    def cache_identity(self, cutoff: datetime) -> str:
        cutoff = utc(cutoff)
        return digest(
            {
                "format": "evaluation-eligibility-v1",
                "market": self.market,
                "capabilities": self.capabilities,
                "visible_capabilities": [
                    row for row in self.capabilities if row.available_at <= cutoff
                ],
            }
        )

    def select(
        self,
        observations: Sequence[Observation],
        *,
        cutoff: datetime,
        synthetic: bool,
        since: datetime | None = None,
    ) -> list[Observation]:
        return eligible_evidence(
            observations,
            cutoff,
            synthetic,
            self.market,
            self.capabilities,
            since=since,
        )
