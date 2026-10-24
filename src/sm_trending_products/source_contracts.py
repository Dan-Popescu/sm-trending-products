"""Canonical source operations and bounded physical refresh requests.

The scheduler budgets physical requests. Each grouped request retains every
entity intention so priorities, due times and provenance remain inspectable.
"""

from __future__ import annotations

import hashlib
import os
from collections import defaultdict
from copy import deepcopy
from typing import Any

from .contracts import digest

SOURCES = {"tiktok_display", "tiktok_ads", "tiktok_commercial"}
DISPLAY_QUERY_LIMIT = 20


def raw_capture_identity(task: dict) -> str:
    """Keep first-attempt identity stable; retry only an archived business error."""
    return digest(
        {
            "capture_of": task["task_id"],
            "page_index": task["page_index"],
            "protocol": "1",
            **({"capture_attempt": task["capture_attempt"]} if task.get("capture_attempt") else {}),
        }
    )


def credential_keys(synthetic: bool) -> dict[str, str]:
    """Hint only current source credentials; Rust still reserves immediately before HTTP.

    Historical bindings cannot identify a rotated token, and synthetic requests
    deliberately use account budgets without a credential-wide limiter.
    """
    if synthetic:
        return {}
    keys = {}
    for source, variable in (
        ("tiktok_display", "TIKTOK_DISPLAY_ACCESS_TOKEN"),
        ("tiktok_ads", "TIKTOK_CCL_ACCESS_TOKEN"),
        ("tiktok_commercial", "TIKTOK_CCL_ACCESS_TOKEN"),
    ):
        token = os.environ.get(variable, "")
        if token:
            keys[source] = hashlib.sha256(token.encode()).hexdigest()
    return keys


def canonical_operation(source: str, operation: str | None = None) -> str:
    """Normalize the historical commercial ``list`` alias at every input boundary."""
    if source not in SOURCES:
        raise ValueError("Unsupported source")
    if operation is None:
        operation = "list" if source == "tiktok_display" else "query"
    if operation not in {"list", "query"}:
        raise ValueError("Unsupported source operation")
    return operation if source == "tiktok_display" else "query"


def refresh_signature(source: str, account_id: str, operation: str, payload: dict) -> str:
    """Identify compatible request context, excluding per-page continuation and IDs."""
    context = deepcopy(payload)
    context.pop("cursor", None)
    context.pop("search_id", None)
    if source in {"tiktok_ads", "tiktok_commercial"}:
        # Continuation payloads include the adapter's default page size. It is
        # the same physical query as an origin that omitted that default.
        context.setdefault("max_count", 10 if source == "tiktok_ads" else 50)
    if source == "tiktok_display" and operation == "query":
        context.get("filters", {}).pop("video_ids", None)
    return digest(
        {
            "source": source,
            "account_id": account_id,
            "operation": canonical_operation(source, operation),
            "payload": context,
        }
    )


def group_refreshes(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Coalesce query refreshes without mixing Display tiers or fixture contexts."""
    groups: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        entity = candidate["entity"]
        signature = refresh_signature(
            entity["source"], entity["account_id"], candidate["operation"], candidate["payload"]
        )
        # A hot Display batch must not consume the reserved low-tier exploration
        # allocation. Commercial queries necessarily refresh the whole result set.
        tier = candidate["tier"] if entity["source"] == "tiktok_display" else None
        groups[(signature, tier)].append(candidate)
    requests = []
    for (signature, _), members in groups.items():
        members.sort(
            key=lambda item: (
                -item["priority"],
                item["entity"]["next_due_at"],
                item["entity"]["entity_key"],
            )
        )
        limit = (
            DISPLAY_QUERY_LIMIT
            if members[0]["entity"]["source"] == "tiktok_display"
            else len(members)
        )
        for start in range(0, len(members), limit):
            chunk = members[start : start + limit]
            request = {**chunk[0], "members": chunk, "request_signature": signature}
            payload = deepcopy(chunk[0]["payload"])
            if chunk[0]["entity"]["source"] == "tiktok_display":
                payload["filters"]["video_ids"] = [
                    member["entity"]["entity_key"].split(":", 2)[-1] for member in chunk
                ]
            request["payload"] = payload
            requests.append(request)
    return requests
