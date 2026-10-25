"""One validated reconstruction path for authenticated columnar observations."""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError
from pydantic_core import to_json

from .contracts import Observation


def decode_columnar_observation(row: dict[str, Any]) -> Observation:
    """Consume a columnar row without building three temporary Python graphs.

    Scalar values are escaped by the JSON encoder. The nested columns contain
    complete JSON values emitted by the canonical writer and authenticated with
    their containing object. One native validation pass preserves the complete
    model, including large integers, offsets, unknown attributes and missingness.
    """
    row.pop("entity_key", None)
    nested = {
        name: row.pop(name + "_json").encode()
        for name in ("metrics", "attributes", "missing_fields")
    }
    body = to_json(row)[:-1]
    for name, value in nested.items():
        body += b',"' + name.encode() + b'":' + value
    try:
        return Observation.model_validate_json(body + b"}")
    except ValidationError as error:
        # The native JSON parser has a smaller nesting limit than the previous
        # source reader. Existing authenticated unknown attributes may be deeper.
        # Keep their original fully validated path; reject ordinary bad inputs.
        errors = error.errors(include_input=False, include_url=False)
        if not any(
            item["type"] == "json_invalid"
            and "recursion limit exceeded" in item.get("ctx", {}).get("error", "")
            for item in errors
        ):
            raise
        return Observation.model_validate(
            {**row, **{name: json.loads(value) for name, value in nested.items()}}
        )
