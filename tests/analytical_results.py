"""Explicit reconstruction oracles for small fixtures, never application read APIs.

Production reads remain paged. These helpers intentionally visit every fixture
page so the former inline behavioral assertions retain their independent meaning.
"""

from copy import deepcopy


def reference_ids(pipeline, value):
    if isinstance(value, list):
        return value
    result, offset = [], 0
    while True:
        page = pipeline.provenance.page(value, limit=100, offset=offset)
        result.extend(page["references"])
        if page["next_offset"] is None:
            return sorted(result)
        offset = page["next_offset"]


def record_items(pipeline, value):
    if isinstance(value, list):
        return [
            {"id": str(index), "value": row, "position": index} for index, row in enumerate(value)
        ]
    result, offset = [], 0
    while True:
        page = pipeline.provenance.page(value, limit=100, offset=offset)
        result.extend(page["records"])
        if page["next_offset"] is None:
            return sorted(result, key=lambda item: item["position"])
        offset = page["next_offset"]


def record_values(pipeline, value):
    return [item["value"] for item in record_items(pipeline, value)]


def record_mapping(pipeline, value):
    return {item["id"]: item["value"] for item in record_items(pipeline, value)}


def _config(pipeline, value):
    result = deepcopy(value)
    contract = result.get("profit_contract")
    if contract and isinstance(contract["ledgers"], dict) and "format" in contract["ledgers"]:
        contract["ledgers"] = record_mapping(pipeline, contract["ledgers"])
    return result


def backtest_snapshots(pipeline, value):
    result = []
    for header in record_values(pipeline, value["snapshots"]):
        snapshot = deepcopy(header)
        snapshot.pop("representation", None)
        for key in ("universe", "identity_decisions", "selected", "capabilities"):
            snapshot[key] = record_values(pipeline, snapshot[key])
        for key in ("entity_bindings", "family_memberships"):
            snapshot[key] = record_mapping(pipeline, snapshot[key])
        snapshot["baselines"] = {
            key: record_values(pipeline, child) for key, child in snapshot["baselines"].items()
        }
        snapshot["config"] = _config(pipeline, snapshot["config"])
        result.append(snapshot)
    return result


def backtest_reports(pipeline, value):
    def expand(item):
        if isinstance(item, dict):
            if item.get("format") == "sharded-records-v1":
                return [expand(child) for child in record_values(pipeline, item)]
            if item.get("format") == "sharded-provenance-v1":
                return item
            return {
                key: _config(pipeline, child) if key == "config" else expand(child)
                for key, child in item.items()
            }
        if isinstance(item, list):
            return [expand(child) for child in item]
        return item

    return [expand(header) for header in record_values(pipeline, value["reports"])]


def snapshot_semantics(pipeline, value):
    def expand(item):
        if isinstance(item, dict):
            if item.get("format") == "sharded-provenance-v1":
                return reference_ids(pipeline, item)
            return {key: expand(child) for key, child in item.items()}
        if isinstance(item, list):
            return [expand(child) for child in item]
        return item

    result = expand(value)
    result.pop("schema_version", None)
    result.pop("snapshot_id", None)
    result.pop("input_provenance", None)
    return result
