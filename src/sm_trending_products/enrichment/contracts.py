"""Bounded transcript evidence and explicitly reserved enrichment costs."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..contracts import Contract

MAX_TEXT_CHARACTERS = 16_000
IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
type SegmentLabel = Literal[
    "hook", "problem", "benefit", "claimed_proof", "objection", "call_to_action", "offer"
]


class EnrichmentDenied(ValueError):
    """A safe reason code; remote diagnostics never enter persistent state."""


class _EnrichmentContract(Contract):
    # Pydantic can accept containers other than the lists/tuples unwrapped below.
    # Revalidate every nested instance at its own boundary, regardless of container.
    model_config = ConfigDict(revalidate_instances="always")


def _plain(value: object) -> Any:
    """Preserve raw scalar types while unwrapping nested, possibly bypassed models."""
    if isinstance(value, BaseModel):
        value = dict(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def revalidate[T: Contract](contract: type[T], value: object) -> T:
    # model_validate(existing_instance) skips field validation by default. JSON
    # serialization can conceal invalid scalar types such as monetary floats.
    return contract.model_validate(_plain(value))


class TranscriptInput(_EnrichmentContract):
    product_id: str = Field(pattern=IDENTIFIER)
    entity_key: str = Field(pattern=r"^[A-Za-z0-9_.-]+:video:\S{1,384}$", max_length=512)
    dataset_id: str = Field(min_length=1, max_length=128)
    observation_id: str = Field(min_length=1, max_length=256)
    synthetic: bool = Field(default=False, strict=True)
    text: str = Field(min_length=1, max_length=MAX_TEXT_CHARACTERS)
    source_kind: Literal["subtitles", "human_transcript", "description"]
    language: str | None = Field(default=None, pattern=r"^[a-z]{2,3}(?:-[A-Za-z0-9]{2,8})?$")
    permission_reference: str = Field(min_length=1, max_length=512)

    @field_validator("permission_reference")
    @classmethod
    def require_permission_reference(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("An explicit permission reference is required")
        return value

    @field_validator("text")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        value = unicodedata.normalize(
            "NFC", value.replace("\r\n", "\n").replace("\r", "\n")
        ).strip()
        if (
            not value
            or len(value) > MAX_TEXT_CHARACTERS
            or any(unicodedata.category(char) == "Cc" and char not in "\n\t" for char in value)
        ):
            raise ValueError("Transcript must contain bounded printable text")
        return value


def exact_money(value):
    if isinstance(value, (float, bool)):
        raise ValueError("Costs require exact decimal strings or integers")
    return value


class EnrichmentBudget(_EnrichmentContract):
    daily_requests: int = Field(default=100, ge=1, le=10_000, strict=True)
    daily_cost_limit: Decimal = Field(default=Decimal(0), ge=0, le=1_000_000)
    maximum_request_cost: Decimal = Field(default=Decimal(0), ge=0, le=1_000_000)
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")
    max_text_characters: int = Field(default=MAX_TEXT_CHARACTERS, ge=1, le=MAX_TEXT_CHARACTERS)
    allow_external: bool = Field(default=False, strict=True)

    _costs = field_validator("daily_cost_limit", "maximum_request_cost", mode="before")(exact_money)


class ProviderSpec(_EnrichmentContract):
    provider_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    enabled: bool = Field(default=False, strict=True)
    reserved_cost: Decimal = Field(default=Decimal(0), ge=0, le=1_000_000)
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")

    _cost = field_validator("reserved_cost", mode="before")(exact_money)


class TextSegment(_EnrichmentContract):
    start: int = Field(ge=0, strict=True)
    end: int = Field(gt=0, strict=True)
    label: SegmentLabel

    @model_validator(mode="after")
    def bounded(self):
        if not self.start < self.end <= self.start + 500:
            raise ValueError("A segment must reference between 1 and 500 source characters")
        return self


class InsightOutput(_EnrichmentContract):
    method: Literal["lexical", "model"]
    segments: tuple[TextSegment, ...] = Field(max_length=24)

    @field_validator("segments", mode="before")
    @classmethod
    def ordered_bounded_segments(cls, value):
        # Tuple/list are the ordered in-memory and JSON forms. Do not consume
        # arbitrary iterables: invalid infinite streams can bypass max_length's
        # valid-item count and make the underlying validator run indefinitely.
        if not isinstance(value, list | tuple) or len(value) > 24:
            raise ValueError("Segments require an ordered tuple/list of at most 24 records")
        return value


class TranscriptAdapter(Protocol):
    spec: ProviderSpec

    def analyze(self, transcript: dict) -> InsightOutput: ...
