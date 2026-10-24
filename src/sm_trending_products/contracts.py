"""Versioned evidence contracts shared by collection, analysis, and evaluation."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def utc(value: str | datetime) -> datetime:
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must contain an explicit timezone")
    return parsed.astimezone(UTC)


def now() -> datetime:
    return datetime.now(UTC)


def observation_period(row: Observation) -> tuple[datetime, datetime] | None:
    """Return a valid explicit source period without inventing missing bounds."""
    start = row.attributes.get("period_start", row.attributes.get("source_period_start"))
    end = row.attributes.get("period_end", row.attributes.get("source_period_end"))
    if start is None or end is None:
        return None
    try:
        bounds = utc(start), utc(end)
        return bounds if bounds[0] < bounds[1] else None
    except (ValueError, TypeError, AttributeError):
        return None


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item),
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Measurement(Contract):
    lower: int = Field(ge=0, strict=True)
    upper: int = Field(ge=0, strict=True)
    precision: Literal["exact", "bounded", "estimated"] = "exact"
    semantics: Literal["cumulative", "interval", "unknown"] = "cumulative"
    unit: str = "count"
    raw: str | None = None
    model_version: str | None = None

    @model_validator(mode="after")
    def valid_bounds(self) -> Measurement:
        if self.upper < self.lower:
            raise ValueError("Measurement bounds are reversed")
        if self.precision == "exact" and self.lower != self.upper:
            raise ValueError("Exact measurements require equal bounds")
        if self.precision == "estimated" and not self.model_version:
            raise ValueError("Estimated measurements require a model version")
        return self


class Observation(Contract):
    schema_version: str = "1"
    observation_id: str
    source: str
    entity_type: Literal["video", "ad", "commercial_content", "offer", "cost"]
    entity_id: str
    observed_at: datetime
    available_at: datetime
    synthetic: bool = False
    published_at: datetime | None = None
    market: str | None = None
    account_id: str | None = None
    metrics: dict[str, Measurement] = Field(default_factory=dict)
    attributes: dict[str, Any] = Field(default_factory=dict)
    missing_fields: dict[str, str] = Field(default_factory=dict)
    raw_dataset_id: str | None = None

    @property
    def entity_key(self) -> str:
        return f"{self.source}:{self.entity_type}:{self.entity_id}"

    @model_validator(mode="after")
    def valid_times(self) -> Observation:
        for value in (self.observed_at, self.available_at, self.published_at):
            if value is not None:
                utc(value)
        if self.available_at < self.observed_at:
            raise ValueError("Availability cannot precede collection")
        if self.published_at and self.published_at > self.observed_at:
            raise ValueError("Publication cannot follow collection")
        if not self.entity_id or not self.source or not self.observation_id:
            raise ValueError("Evidence identifiers cannot be empty")
        if self.schema_version != "1":
            raise ValueError("Unsupported observation schema")
        return self


class Product(Contract):
    product_id: str
    name: str
    created_at: datetime
    synthetic: bool = False
    market: str | None = None


class ProductLink(Contract):
    product_id: str
    entity_key: str
    available_at: datetime
    status: Literal["confirmed", "proposed", "rejected"] = "confirmed"
    confidence: float = Field(default=1.0, ge=0, le=1)
    method: str = "manual"
    evidence_refs: list[str] = Field(default_factory=list)
    decision_id: str = ""


class FamilyMembership(Contract):
    product_id: str
    family_id: str
    available_at: datetime


class Capability(Contract):
    source: str
    account_id: str
    capability: str
    state: Literal["unverified", "available", "denied", "degraded"]
    available_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)


class AssessmentPolicy(Contract):
    version: str = "attention-v1"
    min_observations: int = Field(default=2, ge=2)
    max_age_hours: float = Field(default=72, gt=0)
    lookback_days: int = Field(default=30, ge=1)
    min_link_confidence: float = Field(default=0.9, ge=0, le=1)
    alert_threshold: float = Field(default=70, ge=0, le=100)
    cooldown_hours: float = Field(default=24, ge=0)
    weights: dict[str, float] = Field(default_factory=lambda: {"attention": 1.0})
    missing_data: Literal["exclude", "renormalize"] = "exclude"

    @model_validator(mode="after")
    def valid_weights(self) -> AssessmentPolicy:
        allowed = {"attention", "demand", "competition", "feasibility"}
        if (
            set(self.weights) - allowed
            or not self.weights
            or any(v < 0 for v in self.weights.values())
        ):
            raise ValueError("Invalid assessment weights")
        if sum(self.weights.values()) <= 0:
            raise ValueError("At least one weight must be positive")
        return self
