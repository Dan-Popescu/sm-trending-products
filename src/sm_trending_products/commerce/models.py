"""Bounded operator-authored launch facts and explicit scenario amounts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, StrictBool, field_validator, model_validator

from ..contracts import Contract, utc


class LaunchError(ValueError):
    """A stable operator-facing failure without private draft contents."""

    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code.replace("_", " "))


def _exact_amount(value: object) -> object:
    if isinstance(value, bool) or not isinstance(value, str | int | Decimal):
        raise ValueError("Money requires an exact decimal string or Decimal, never a float")
    return value


Money = Annotated[
    Decimal, BeforeValidator(_exact_amount), Field(ge=0, max_digits=18, decimal_places=6)
]
CostName = Literal[
    "goods", "shipping", "payment_fees", "nonrecoverable_taxes", "returns_allowance", "fulfillment"
]
REQUIRED_COSTS: tuple[CostName, ...] = (
    "goods",
    "shipping",
    "payment_fees",
    "nonrecoverable_taxes",
    "returns_allowance",
    "fulfillment",
)

# Explicit countries, including non-European ones; a continent is not a launch country.
_COUNTRIES = frozenset(
    "AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO "
    "BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ "
    "DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP "
    "GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG "
    "KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML "
    "MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE "
    "PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL "
    "SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM "
    "US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW".split()
)


class LaunchDraft(Contract):
    """One country and currency scenario, containing no inferred commercial facts.

    For an inclusive retail price, tax_per_order is deducted explicitly. For an
    exclusive retail price the declared price is net revenue. Input taxes which
    cannot be recovered belong in nonrecoverable_taxes, not in tax_per_order.
    desired_profit is an operator policy, defaulting explicitly to break-even.
    """

    dossier_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][\w.:-]*$")
    product_id: str = Field(min_length=1, max_length=256)
    country: str = Field(min_length=2, max_length=2, pattern=r"^[A-Z]{2}$")
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    synthetic: StrictBool = False
    fulfillment_mode: Literal["dropship", "stock", "external"] = "dropship"
    title: str = Field(default="", max_length=240)
    description: str = Field(default="", max_length=20000)
    supplier_name: str = Field(default="", max_length=240)
    supplier_reference: str = Field(default="", max_length=1000)
    quote_expires_at: datetime | None = None
    stock_status: Literal["unknown", "available", "unavailable"] = "unknown"
    delivery_min_days: int | None = Field(default=None, ge=0, le=365, strict=True)
    delivery_max_days: int | None = Field(default=None, ge=0, le=365, strict=True)
    return_terms: str = Field(default="", max_length=4000)
    decision: Literal["watch", "test", "hold", "reject"] = "watch"
    note: str = Field(default="", max_length=4000)
    retail_price: Money | None = None
    prices_include_tax: StrictBool | None = None
    tax_per_order: Money | None = None
    costs: dict[CostName, Money] = Field(default_factory=dict)
    desired_profit: Money = Decimal("0")

    @field_validator("country")
    @classmethod
    def explicit_country(cls, value: str) -> str:
        if value not in _COUNTRIES:
            raise ValueError("Use an explicit ISO 3166-1 country, not a continent or region")
        return value

    @field_validator("product_id")
    @classmethod
    def product_identifier(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError("Product identifier must be nonempty printable text")
        return value

    @field_validator("quote_expires_at")
    @classmethod
    def explicit_expiry(cls, value: datetime | None) -> datetime | None:
        return utc(value) if value is not None else None

    @model_validator(mode="after")
    def consistent_facts(self) -> LaunchDraft:
        if (
            self.delivery_min_days is not None
            and self.delivery_max_days is not None
            and self.delivery_min_days > self.delivery_max_days
        ):
            raise ValueError("Delivery bounds are reversed")
        if (
            self.prices_include_tax is True
            and self.tax_per_order is not None
            and self.retail_price is not None
            and self.tax_per_order > self.retail_price
        ):
            raise ValueError("Included tax cannot exceed the retail price")
        return self
