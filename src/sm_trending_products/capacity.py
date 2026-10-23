"""Explicit workload arithmetic; no inference of granted source or VPS capacity."""

import math

from pydantic import Field

from .contracts import Contract
from .settings import PlanningSettings


class WorkloadProfile(Contract):
    products: int = Field(ge=1)
    videos_per_product: int = Field(ge=1)
    refreshes_per_day: float = Field(gt=0, le=1440)
    videos_per_request: int = Field(default=20, ge=1, le=20)
    history_days: int = Field(default=90, ge=1)
    source_requests_per_day: int | None = Field(default=None, ge=0)

    def report(self, settings: PlanningSettings) -> dict:
        unique_videos = self.products * self.videos_per_product
        observations = math.ceil(unique_videos * self.refreshes_per_day)
        requests = math.ceil(unique_videos / self.videos_per_request) * self.refreshes_per_day
        required = math.ceil(requests)
        planned = settings.scheduler_request_budget * 1440 // settings.collection_schedule_minutes
        available = (
            min(planned, self.source_requests_per_day)
            if self.source_requests_per_day is not None
            else planned
        )
        return {
            "profile": self.model_dump(mode="json"),
            "unique_videos_assumed": unique_videos,
            "observations_per_day": observations,
            "retained_observations": observations * self.history_days,
            "minimum_refresh_requests_per_day": required,
            "maximum_new_tasks_planned_per_day": planned,
            "declared_request_upper_bound_per_day": available,
            "planning_or_declared_budget_shortfall_per_day": max(0, required - available),
            "planning_sufficient": available >= required,
            "source_budget_supplied": self.source_requests_per_day is not None,
            "runtime_capacity_verified": False,
            "collection_schedule": settings.cron(settings.collection_schedule_minutes),
            "assessment_schedule": settings.cron(settings.assessment_schedule_minutes),
            "assumptions": [
                "Videos are distinct across products; shared entities lower demand.",
                "Requests are fully grouped within compatible accounts, tiers and contexts.",
                "Discovery, pagination, retries and scheduling fragmentation add work.",
                "Planning is an upper bound; quotas, worker latency and access remain separate.",
            ],
        }
