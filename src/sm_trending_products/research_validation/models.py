"""Preregistered, bounded research contracts; no fitted model is implied."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from pydantic import Field, field_validator, model_validator

from ..contracts import AssessmentPolicy, Contract, utc
from ..evaluation import EvaluationConfig, ProfitContract

PartitionName = Literal["development", "test", "holdout"]
PARTITIONS: tuple[PartitionName, ...] = ("development", "test", "holdout")


class ResearchError(ValueError):
    """Stable public failure without evidence or credential payloads."""


class ValidationPartition(Contract):
    product_ids: tuple[str, ...] = Field(min_length=1, max_length=500)
    decision_times: tuple[datetime, ...] = Field(min_length=1, max_length=64)
    profit_contract: ProfitContract | None = None

    @field_validator("decision_times")
    @classmethod
    def dates(cls, value: tuple[datetime, ...]) -> tuple[datetime, ...]:
        return tuple(utc(item) for item in value)

    @model_validator(mode="after")
    def unique_scope(self) -> ValidationPartition:
        if len(set(self.product_ids)) != len(self.product_ids) or any(
            not item.strip() or item != item.strip() or len(item) > 256 for item in self.product_ids
        ):
            raise ValueError("Partition products must be unique bounded identifiers")
        if tuple(sorted(set(self.decision_times))) != self.decision_times:
            raise ValueError("Decision dates must be unique and strictly chronological")
        return self


class ValidationProtocol(Contract):
    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][\w.:-]*$")
    revision: int = Field(default=1, strict=True, ge=1, le=10000)
    previous_protocol_id: str | None = Field(default=None, min_length=1, max_length=128)
    config: EvaluationConfig = Field(default_factory=EvaluationConfig)
    policies: dict[str, AssessmentPolicy] = Field(min_length=1, max_length=8)
    partitions: dict[PartitionName, ValidationPartition]
    measurement_sample_size: int = Field(default=0, strict=True, ge=0, le=25)
    seed: int = Field(default=42, strict=True)
    max_attempts: int = Field(default=32, strict=True, ge=1, le=128)

    @model_validator(mode="after")
    def protocol(self) -> ValidationProtocol:
        if set(self.partitions) != set(PARTITIONS):
            raise ValueError("Declare exactly development, test and holdout partitions")
        if any(not key.strip() or len(key) > 128 for key in self.policies):
            raise ValueError("Policies need bounded nonblank identifiers")
        if (self.revision == 1) != (self.previous_protocol_id is None):
            raise ValueError("Revisions after one require the previous protocol identifier")
        config = EvaluationConfig.for_creation(self.config.model_dump(mode="json"))
        config.validate_creation()
        if self.measurement_sample_size and config.target != "attention":
            raise ValueError("Measurement-only sampling requires the attention target")
        products: set[str] = set()
        dates: list[datetime] = []
        for name in PARTITIONS:
            partition = self.partitions[name]
            if products.intersection(partition.product_ids):
                raise ValueError("Products must be isolated between partitions")
            products.update(partition.product_ids)
            dates.extend(partition.decision_times)
            if (config.target == "profit") != (partition.profit_contract is not None):
                raise ValueError("Profit partitions require their own declared ledger contract")
        if len(products) > 500 or len(dates) > 64:
            raise ValueError("Protocol capacity is 500 products and 64 total decision dates")
        if dates != sorted(set(dates)):
            raise ValueError("Development, test and holdout must be strictly chronological")
        # A boundary observation may arrive through the declared grace window.
        # It must not also enter a later partition's development decision.
        if (
            len(config.horizon_days) > 8
            or max(config.horizon_days) > 366
            or len(config.launch_delays) > 8
            or config.max_boundary_gap_hours > 168
            or any(policy.lookback_days > 366 for policy in self.policies.values())
            or any(policy.max_age_hours > 8760 for policy in self.policies.values())
        ):
            raise ValueError("Protocol horizon, delay, grace or history budget exceeded")
        purge = timedelta(days=max(config.horizon_days), hours=config.max_boundary_gap_hours)
        if any(right < left + purge for left, right in zip(dates, dates[1:], strict=False)):
            raise ValueError("Outcome horizons and boundary grace must not overlap decisions")
        object.__setattr__(self, "config", config)
        return self
