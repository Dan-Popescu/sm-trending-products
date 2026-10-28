"""Selected-video enrichment through versioned text evidence and atomic budgets."""

from .contracts import (
    EnrichmentBudget,
    EnrichmentDenied,
    InsightOutput,
    ProviderSpec,
    TextSegment,
    TranscriptAdapter,
    TranscriptContext,
    TranscriptInput,
)
from .service import TranscriptInsights

__all__ = [
    "EnrichmentBudget",
    "EnrichmentDenied",
    "InsightOutput",
    "ProviderSpec",
    "TextSegment",
    "TranscriptAdapter",
    "TranscriptContext",
    "TranscriptInput",
    "TranscriptInsights",
]
