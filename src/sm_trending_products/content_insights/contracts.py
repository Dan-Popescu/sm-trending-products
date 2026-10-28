"""Small, reviewed taxonomies for inspectable supplied-text interpretations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import Field, field_validator, model_validator

from ..contracts import utc
from ..enrichment.contracts import IDENTIFIER, _EnrichmentContract


class ThemePhrase(_EnrichmentContract):
    language: Literal["en", "fr"]
    text: str = Field(min_length=2, max_length=80)
    category: Literal["problem", "benefit", "objection", "angle"]

    @field_validator("text")
    @classmethod
    def printable(cls, value: str) -> str:
        if not value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError("A phrase must contain bounded printable text")
        return value.strip()


class ThemeRule(_EnrichmentContract):
    theme_id: str = Field(pattern=IDENTIFIER)
    label: str = Field(min_length=1, max_length=128)
    phrases: tuple[ThemePhrase, ...] = Field(min_length=1, max_length=16)


class ContentTaxonomy(_EnrichmentContract):
    version: str = Field(pattern=IDENTIFIER)
    reviewed_at: datetime
    review_reference: str = Field(min_length=1, max_length=512)
    themes: tuple[ThemeRule, ...] = Field(min_length=1, max_length=16)

    @field_validator("reviewed_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def unique_themes(self):
        if len({theme.theme_id for theme in self.themes}) != len(self.themes):
            raise ValueError("Theme identifiers must be unique within a taxonomy")
        if not self.review_reference.strip():
            raise ValueError("A taxonomy requires an explicit review reference")
        return self


def _theme(theme_id: str, label: str, rows: tuple[tuple[str, str, str], ...]) -> ThemeRule:
    return ThemeRule.model_validate(
        {
            "theme_id": theme_id,
            "label": label,
            "phrases": [
                {"language": language, "text": text, "category": category}
                for language, text, category in rows
            ],
        }
    )


DEFAULT_TAXONOMY = ContentTaxonomy(
    version="content-themes-en-fr-v1",
    reviewed_at=datetime(2026, 10, 28, 20, 54, 33, tzinfo=UTC),
    review_reference="decision-research:R06:initial-bounded-taxonomy",
    themes=(
        _theme(
            "time",
            "Time and effort",
            (
                ("en", "save time", "benefit"),
                ("en", "takes too long", "problem"),
                ("fr", "gagner du temps", "benefit"),
                ("fr", "perdre du temps", "problem"),
            ),
        ),
        _theme(
            "ease",
            "Ease of use",
            (
                ("en", "easier", "benefit"),
                ("en", "easy to use", "benefit"),
                ("en", "hard to use", "objection"),
                ("fr", "plus facile", "benefit"),
                ("fr", "facile à utiliser", "benefit"),
                ("fr", "difficile à utiliser", "objection"),
            ),
        ),
        _theme(
            "price",
            "Price and value",
            (
                ("en", "expensive", "objection"),
                ("en", "affordable", "benefit"),
                ("fr", "trop cher", "objection"),
                ("fr", "abordable", "benefit"),
            ),
        ),
        _theme(
            "reliability",
            "Reliability",
            (
                ("en", "doesn't work", "objection"),
                ("en", "stopped working", "problem"),
                ("fr", "ne marche pas", "objection"),
                ("fr", "ne fonctionne plus", "problem"),
            ),
        ),
        _theme(
            "portability",
            "Portability",
            (
                ("en", "portable", "benefit"),
                ("en", "lightweight", "benefit"),
                ("fr", "portable", "benefit"),
                ("fr", "léger", "benefit"),
            ),
        ),
        _theme(
            "demonstration",
            "Demonstration angle",
            (
                ("en", "before and after", "angle"),
                ("en", "watch this", "angle"),
                ("fr", "avant et après", "angle"),
                ("fr", "regardez", "angle"),
            ),
        ),
        _theme(
            "comparison",
            "Comparison angle",
            (
                ("en", "compared to", "angle"),
                ("en", "versus", "angle"),
                ("fr", "comparé à", "angle"),
                ("fr", "par rapport à", "angle"),
            ),
        ),
    ),
)
