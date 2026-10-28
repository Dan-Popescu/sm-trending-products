"""Private connection files reference environment secrets; no platform is selected."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictBool

from .commerce_providers import CommerceProvider, ConnectionConfig, create_provider
from .contracts import Contract


class StoreConnection(Contract):
    provider: Literal["woocommerce", "shopify"]
    connection_id: str = Field(min_length=1, max_length=128)
    base_url: str = Field(min_length=1, max_length=2048)
    enabled: StrictBool = False
    test_mode: StrictBool = False
    api_version: Literal["2026-10"] = "2026-10"
    consumer_key_env: str = Field(
        default="COMMERCE_CONSUMER_KEY", pattern=r"^[A-Z][A-Z0-9_]{0,127}$"
    )
    consumer_secret_env: str = Field(
        default="COMMERCE_CONSUMER_SECRET", pattern=r"^[A-Z][A-Z0-9_]{0,127}$"
    )
    access_token_env: str = Field(
        default="COMMERCE_ACCESS_TOKEN", pattern=r"^[A-Z][A-Z0-9_]{0,127}$"
    )
    webhook_secret_env: str = Field(
        default="COMMERCE_WEBHOOK_SECRET", pattern=r"^[A-Z][A-Z0-9_]{0,127}$"
    )
    timeout_seconds: float = Field(default=10, gt=0, le=60)

    def provider_instance(self) -> CommerceProvider:
        """Construct locally; the adapter validates the origin without network I/O."""
        return create_provider(
            ConnectionConfig(
                provider=self.provider,
                connection_id=self.connection_id,
                base_url=self.base_url,
                enabled=self.enabled,
                test_mode=self.test_mode,
                api_version=self.api_version,
                consumer_key=os.environ.get(self.consumer_key_env, ""),
                consumer_secret=os.environ.get(self.consumer_secret_env, ""),
                access_token=os.environ.get(self.access_token_env, ""),
                webhook_secret=os.environ.get(self.webhook_secret_env, ""),
                timeout_seconds=self.timeout_seconds,
            )
        )


def load_connection(path: Path) -> CommerceProvider:
    with path.open("rb") as file:
        content = file.read(16_385)
    if len(content) > 16_384:
        raise ValueError("Connection file exceeds its size limit")
    return StoreConnection.model_validate_json(content).provider_instance()


def connection_template(provider: Literal["woocommerce", "shopify"]) -> dict:
    return StoreConnection(
        provider=provider,
        connection_id="CHOOSE-STORE-ID",
        base_url="https://CHOOSE-STORE.myshopify.com"
        if provider == "shopify"
        else "https://CHOOSE-STORE.example",
    ).model_dump(mode="json")
