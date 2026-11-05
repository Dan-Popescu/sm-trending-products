"""Bounded selected-video enrichment; all external effects require explicit admission."""

from .contracts import (
    ExecutionConfig,
    ResearchApproval,
    VideoRequest,
    VideoResearchDenied,
    VideoSelection,
)
from .service import VideoResearch

__all__ = [
    "ExecutionConfig",
    "ResearchApproval",
    "VideoRequest",
    "VideoResearch",
    "VideoResearchDenied",
    "VideoSelection",
]
