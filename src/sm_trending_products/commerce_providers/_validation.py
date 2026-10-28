"""Strict response selection: customer data and remote diagnostics never escape."""

import base64
import hashlib
import hmac
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import parse_qs, urlsplit

from ._http import parse_json, require_enabled
from .contracts import ConnectionConfig, DraftProduct, ProviderError


def obj(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProviderError("invalid_provider_schema")
    return value


def seq(value: Any, maximum: int = 100) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ProviderError("unsupported_provider_volume")
    return value


def text(value: Any, maximum: int = 200) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ProviderError("invalid_provider_schema")
    if any(ord(c) < 32 for c in value):
        raise ProviderError("invalid_provider_schema")
    return value


def numeric_id(value: Any) -> str:
    if isinstance(value, bool):
        raise ProviderError("invalid_resource_id")
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", value):
        raise ProviderError("invalid_resource_id")
    return value


def gid(value: Any, resource: str) -> str:
    value = text(value)
    if not re.fullmatch(rf"gid://shopify/{resource}/[1-9][0-9]{{0,19}}", value):
        raise ProviderError("invalid_resource_id")
    return value


def currency(value: Any) -> str:
    value = text(value, 3)
    if not re.fullmatch(r"[A-Z]{3}", value):
        raise ProviderError("invalid_currency")
    return value


def amount(value: Any) -> str:
    # Strings only: neither binary floats nor exponents are money contracts.
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,16}(?:\.[0-9]{1,6})?", value):
        raise ProviderError("invalid_amount")
    try:
        if not Decimal(value).is_finite():
            raise InvalidOperation()
    except InvalidOperation:
        raise ProviderError("invalid_amount") from None
    return value


def timestamp(value: Any) -> str:
    value = text(value, 40)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            # Woo date_modified_gmt explicitly expresses GMT without a suffix.
            return value + "Z"
    except ValueError:
        raise ProviderError("invalid_timestamp") from None
    return value


def quantity(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1_000_000:
        raise ProviderError("invalid_quantity")
    return value


def optional_text(value: Any, maximum: int = 200) -> str | None:
    return None if value is None or value == "" else text(value, maximum)


def validate_draft(draft: DraftProduct) -> None:
    text(draft.launch_id)
    text(draft.sku, 100)
    text(draft.title, 255)
    amount(draft.price)
    currency(draft.currency)
    if not isinstance(draft.price_includes_tax, bool):
        raise ProviderError("price_tax_basis_required")
    if draft.variant_count != 1:
        raise ProviderError("unsupported_multiple_variants")
    if not isinstance(draft.description_html, str) or len(draft.description_html) > 100_000:
        raise ProviderError("invalid_draft_description")


def owned_marker(launch_id: str) -> str:
    return "sm-launch-" + hashlib.sha256(text(launch_id).encode()).hexdigest()


def hosted_url(config: ConnectionConfig, candidate: Any) -> str | None:
    if candidate is None or candidate == "":
        return None
    candidate = text(candidate, 2048)
    expected = urlsplit(config.base_url)
    supplied = urlsplit(candidate)
    if (
        supplied.scheme != expected.scheme
        or supplied.netloc != expected.netloc
        or supplied.username
        or supplied.password
        or supplied.fragment
        or "\\" in candidate
    ):
        raise ProviderError("unexpected_product_url")
    if supplied.query:
        # WordPress draft permalinks commonly use this non-secret product path.
        try:
            query = parse_qs(supplied.query, strict_parsing=True)
        except ValueError:
            raise ProviderError("unexpected_product_url") from None
        if (
            config.provider != "woocommerce"
            or set(query) != {"post_type", "p"}
            or query["post_type"] != ["product"]
            or len(query["p"]) != 1
        ):
            raise ProviderError("unexpected_product_url")
        numeric_id(query["p"][0])
    return candidate


def verified_body(
    config: ConnectionConfig,
    raw_body: bytes,
    headers: dict[str, str],
    signature_header: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    require_enabled(config)
    if not isinstance(raw_body, bytes) or len(raw_body) > config.max_webhook_bytes:
        raise ProviderError("webhook_too_large")
    lowered: dict[str, str] = {}
    for key, value in headers.items():
        lower = key.lower()
        if lower in lowered:
            raise ProviderError("duplicate_webhook_header")
        lowered[lower] = value
    actual = lowered.get(signature_header, "")
    if len(actual) > 100:
        raise ProviderError("invalid_webhook_signature")
    expected = base64.b64encode(
        hmac.digest(config.webhook_secret.encode(), raw_body, "sha256")
    ).decode("ascii")
    try:
        valid = hmac.compare_digest(actual, expected)
    except TypeError:
        valid = False
    if not valid:
        raise ProviderError("invalid_webhook_signature")
    return obj(parse_json(raw_body)), lowered


def header_hint(headers: dict[str, str], name: str) -> str | None:
    # Host, topic and delivery metadata are unsigned and never financial facts.
    value = headers.get(name)
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        return None
    return value
