"""Public Rust CLI contract exercised through a controlled HTTP boundary."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "collector/target/debug/sm-collector"


@pytest.fixture(scope="module", autouse=True)
def collector_binary():
    executable = shutil.which(
        "cargo", path=os.pathsep.join([os.environ.get("PATH", ""), str(Path.home() / ".cargo/bin")])
    )
    if executable is None:
        pytest.fail("Rust Cargo is required; install the toolchain or add cargo to PATH")
    subprocess.run(
        [executable, "build", "--locked", "--manifest-path", str(ROOT / "collector/Cargo.toml")],
        check=True,
        timeout=300,
        capture_output=True,
    )


@contextmanager
def source_server(responses):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(
                {
                    "path": self.path,
                    "body": json.loads(self.rfile.read(int(self.headers["Content-Length"]))),
                }
            )
            status, body, headers = responses[min(len(requests) - 1, len(responses) - 1)]
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            try:
                self.wfile.write(body if isinstance(body, bytes) else json.dumps(body).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def collect(
    endpoint,
    *,
    source="tiktok_display",
    operation="list",
    payload=None,
    synthetic=True,
    token="fixture-secret",
    observed_at="2026-10-01T12:00:00Z",
):
    request = {
        "source": source,
        "operation": operation,
        "account_id": "authorized-account",
        "synthetic": synthetic,
        "payload": payload or {},
        "endpoint_override": endpoint,
    }
    if observed_at:
        request["observed_at"] = observed_at
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env["TIKTOK_DISPLAY_ACCESS_TOKEN"] = token
    env["TIKTOK_CCL_ACCESS_TOKEN"] = token
    result = subprocess.run(
        [str(BINARY)],
        input=json.dumps(request),
        capture_output=True,
        text=True,
        env=env,
        timeout=15,
    )
    return result, json.loads(result.stdout)


def test_video_missing_views_remains_missing_and_cursor_is_preserved():
    response = {
        "data": {
            "videos": [
                {
                    "id": "video-1",
                    "create_time": 1790812800,
                    "title": "Physical product",
                    "like_count": 7,
                }
            ],
            "cursor": 1790812800000,
            "has_more": True,
        },
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, requests):
        result, output = collect(endpoint)
    assert result.returncode == 0, output
    record = output["records"][0]
    assert record["metrics"]["like_count"]["lower"] == 7
    assert "view_count" not in record["metrics"]
    assert record["missing_fields"]["view_count"] == "not_returned"
    assert record["attributes"]["creator_id"] == "authorized-account"
    assert output["next_payload"]["cursor"] == 1790812800000
    assert requests[0]["body"]["max_count"] == 20
    assert record["synthetic"] is True


def test_source_failure_inside_http_success_is_not_evidence():
    response = {"error": {"code": "access_token_invalid", "message": "fixture-secret invalid"}}
    with source_server([(200, response, {})]) as (endpoint, requests):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["category"] == "authorization"
    assert "fixture-secret" not in result.stdout + result.stderr
    assert len(requests) == 1


def test_live_override_is_rejected_before_request():
    with source_server([(200, {}, {})]) as (endpoint, requests):
        result, output = collect(endpoint, synthetic=False, observed_at=None)
    assert result.returncode != 0
    assert output["error"]["category"] == "configuration"
    assert requests == []


def test_ad_explicit_bounds_are_preserved_without_creating_video_views():
    response = {
        "data": {
            "ads": [
                {
                    "ad": {"id": 1, "reach": {"unique_users_seen": "10000-19999"}},
                    "advertiser": {"business_id": 42},
                }
            ],
            "has_more": "false",
            "search_id": "cached-search",
        },
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(
            endpoint, source="tiktok_ads", operation="query", payload={"filters": {}}
        )
    assert result.returncode == 0, output
    record = output["records"][0]
    assert record["metrics"]["reach"] == {
        "lower": 10000,
        "upper": 19999,
        "precision": "bounded",
        "semantics": "cumulative",
        "unit": "count",
        "raw": "10000-19999",
    }
    assert "view_count" not in record["metrics"]
    assert record["attributes"]["advertiser_id"] == "42"
    assert output["next_payload"] is None


def test_rounded_ad_reach_is_not_misrepresented_as_an_exact_count():
    response = {
        "data": {
            "ads": [{"ad": {"id": "ad-1", "reach": {"unique_user_seen": "11K"}}}],
            "has_more": True,
            "search_id": "cached-search",
        },
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(endpoint, source="tiktok_ads", payload={"filters": {}})
    assert result.returncode == 0, output
    record = output["records"][0]
    assert record["attributes"]["reach_raw"] == "11K"
    assert "reach" not in record["metrics"]
    assert record["missing_fields"]["reach"] == "rounding_bounds_not_documented"
    assert output["next_payload"]["search_id"] == "cached-search"


def test_source_rate_limit_returns_retry_hint_without_waiting():
    with source_server([(429, {"message": "fixture-secret"}, {"Retry-After": "120"})]) as (
        endpoint,
        requests,
    ):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["category"] == "rate_limit"
    assert output["error"]["retry_after_seconds"] == 120
    assert len(requests) == 1


def test_rate_limit_inside_http_success_preserves_the_retry_category():
    response = {"error": {"code": "upstream_error", "http_status_code": 429}}
    with source_server([(200, response, {})]) as (endpoint, requests):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["category"] == "rate_limit"
    assert output["error"]["retry_after_seconds"] == 60
    assert len(requests) == 1


def test_raw_secrets_are_redacted_and_bad_records_are_quarantined():
    response = {
        "data": {
            "videos": [
                {"id": "good", "view_count": 3, "title": "fixture-secret"},
                {"id": "bad", "view_count": -1},
            ],
            "has_more": False,
        },
        "secret": "irrelevant-sensitive-value",
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(endpoint)
    assert result.returncode == 0, output
    assert len(output["records"]) == 1
    assert output["records"][0]["entity_id"] == "good"
    assert output["quarantine"][0]["reason"] == "invalid_counter"
    assert "fixture-secret" not in result.stdout + result.stderr
    assert "irrelevant-sensitive-value" not in result.stdout + result.stderr


def test_empty_access_token_is_a_visible_authorization_failure():
    with source_server([(200, {}, {})]) as (endpoint, requests):
        result, output = collect(endpoint, token="")
    assert result.returncode != 0
    assert output["error"]["code"] == "missing_access_token"
    assert requests == []


def test_replay_preserves_observation_identity_without_source_requests():
    response = {
        "data": {"videos": [{"id": "video-1", "view_count": 15}], "has_more": False},
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, requests):
        result, output = collect(endpoint)
    assert result.returncode == 0
    archived = {
        "request": {
            "source": "tiktok_display",
            "operation": "list",
            "account_id": "authorized-account",
            "synthetic": True,
            "payload": {},
        },
        "raw": output["raw"],
        "observed_at": output["observed_at"],
    }
    replay = subprocess.run(
        [str(BINARY), "normalize"],
        input=json.dumps(archived),
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert replay.returncode == 0, replay.stdout
    assert json.loads(replay.stdout)["records"] == output["records"]
    assert json.loads(replay.stdout)["request_count"] == 0
    assert len(requests) == 1


def test_collector_records_validate_against_shared_contract():
    from sm_trending_products.contracts import Observation

    response = {
        "data": {
            "commercial_contents": [
                {
                    "id": "commercial-1",
                    "create_timestamp": 1790812800,
                    "creator": {"username": "creator"},
                    "brand_names": ["Product maker"],
                }
            ],
            "has_more": "false",
        },
        "error": {"code": "ok"},
    }
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(endpoint, source="tiktok_commercial", payload={"filters": {}})
    assert result.returncode == 0, output
    record = Observation.model_validate(output["records"][0])
    assert record.entity_type == "commercial_content"
    assert record.metrics == {}
    assert record.attributes["creator_id"] == "creator"


def test_transient_server_failure_retries_the_same_page_with_a_bounded_attempt_count():
    response = {
        "data": {"videos": [{"id": "video-1", "view_count": 15}], "has_more": False},
        "error": {"code": "ok"},
    }
    with source_server([(503, {"error": "upstream failure"}, {}), (200, response, {})]) as (
        endpoint,
        requests,
    ):
        result, output = collect(endpoint)
    assert result.returncode == 0, output
    assert len(requests) == 2
    assert requests[0] == requests[1]
    assert output["request_count"] == 2


def test_display_query_keeps_owned_video_filters_and_needs_no_pagination_cursor():
    response = {"data": {"videos": [{"id": "video-1", "view_count": 15}]}, "error": {"code": "ok"}}
    with source_server([(200, response, {})]) as (endpoint, requests):
        result, output = collect(
            endpoint, operation="query", payload={"filters": {"video_ids": ["video-1"]}}
        )
    assert result.returncode == 0, output
    assert requests[0]["body"] == {"filters": {"video_ids": ["video-1"]}}
    assert requests[0]["path"].startswith("/v2/video/query/?fields=")
    assert output["next_payload"] is None


def test_claimed_pagination_without_a_cursor_is_a_schema_failure():
    response = {"data": {"videos": [], "has_more": True}, "error": {"code": "ok"}}
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["code"] == "pagination_cursor_missing"
    assert output["error"]["artifact"]["raw"] == response


def test_source_response_size_is_bounded():
    response = {"unexpected": "x" * (8 * 1024 * 1024 + 1)}
    with source_server([(200, response, {})]) as (endpoint, _):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["code"] == "response_too_large"
    assert "artifact" not in output["error"]


def test_invalid_json_schema_artifact_is_bounded_and_redacted_before_truncation():
    malformed = b'{"partial":"fixture-secret", "data": ' + b"x" * 70_000
    with source_server([(200, malformed, {})]) as (endpoint, _):
        result, output = collect(endpoint)
    assert result.returncode != 0
    assert output["error"]["code"] == "invalid_json_response"
    artifact = output["error"]["artifact"]
    assert artifact["source"] == "tiktok_display"
    assert artifact["synthetic"] is True
    assert artifact["raw"]["truncated"] is True
    assert len(artifact["raw"]["invalid_json_text"]) <= 65_536
    assert "fixture-secret" not in result.stdout + result.stderr
    assert "[REDACTED]" in artifact["raw"]["invalid_json_text"]


_REPLAY_DEFAULT = object()
_OBSERVED = "2026-10-01T12:00:00Z"
_AVAILABLE = "2026-10-22T12:00:00Z"


def replay_raw(
    raw,
    *,
    source="tiktok_display",
    version=_REPLAY_DEFAULT,
    available_at=_REPLAY_DEFAULT,
    source_dataset_id="fixture-source-dataset",
    endpoint=None,
    token=None,
):
    request = {
        "source": source,
        "operation": "query",
        "account_id": "authorized-account",
        "synthetic": True,
        "payload": {"filters": {"video_ids": ["stable"]}}
        if source == "tiktok_display"
        else {"filters": {}},
    }
    if endpoint is not None:
        request["endpoint_override"] = endpoint
    replay = {"request": request, "raw": raw, "observed_at": _OBSERVED}
    if version is not _REPLAY_DEFAULT:
        replay["transform_version"] = version
    if available_at is not _REPLAY_DEFAULT:
        replay["available_at"] = available_at
    if version == "2" and source_dataset_id is not _REPLAY_DEFAULT:
        replay["source_dataset_id"] = source_dataset_id
    environment = dict(os.environ)
    environment.pop("TIKTOK_DISPLAY_ACCESS_TOKEN", None)
    environment.pop("TIKTOK_CCL_ACCESS_TOKEN", None)
    # Offline normalization must not connect even with an unusable configured DB.
    environment["DATABASE_URL"] = "postgresql://offline:offline@127.0.0.1:1/unused"
    if token:
        environment["TIKTOK_DISPLAY_ACCESS_TOKEN"] = token
        environment["TIKTOK_CCL_ACCESS_TOKEN"] = token
    result = subprocess.run(
        [str(BINARY), "normalize"],
        input=json.dumps(replay),
        capture_output=True,
        text=True,
        env=environment,
        timeout=15,
    )
    return result, json.loads(result.stdout)


@pytest.mark.parametrize(
    ("source", "key", "rows"),
    [
        (
            "tiktok_display",
            "videos",
            [
                {"id": "stable", "view_count": 7, "ignored": {"country": "FR"}},
                {"id": "invalid", "view_count": -1},
                {"id": "stable", "view_count": 8},
            ],
        ),
        (
            "tiktok_ads",
            "ads",
            [
                {"ad": {"id": "bounded", "reach": {"unique_users_seen": "10000-19999"}}},
                {"ad": {"id": "rounded", "reach": {"unique_users_seen": "11K"}}},
                {"ad": "invalid"},
            ],
        ),
        (
            "tiktok_commercial",
            "commercial_contents",
            [{"id": "stable", "creator": "historically-ignored-shape", "unknown": 8}],
        ),
    ],
)
def test_explicit_v1_replay_preserves_legacy_ids_counters_and_quarantine(source, key, rows):
    raw = {"data": {key: rows, "has_more": False}}
    default_result, default = replay_raw(raw, source=source)
    explicit_result, explicit = replay_raw(raw, source=source, version="1", available_at=_AVAILABLE)
    assert default_result.returncode == explicit_result.returncode == 0
    assert default == explicit
    assert default["normalizer_version"] == "1"
    for record in default["records"]:
        assert record["available_at"] == record["observed_at"]
        assert "normalization" not in record["attributes"]
        assert "unmapped_source_fields" not in record["attributes"]


def test_v2_has_salted_stable_ids_explicit_availability_and_field_level_quarantine():
    import hashlib

    row = {
        "id": "stable",
        "view_count": 123,
        "creator": {"country": "FR", "geo": {"region": "EU"}},
        "new_signal": {"curve": [{"purchases": "private-unknown-value"}, {"purchases": 9}]},
        "new_empty": {},
    }
    raw = {"data": {"videos": [row]}}
    _, legacy = replay_raw(raw)
    first_result, first = replay_raw(raw, version="2", available_at=_AVAILABLE)
    repeat_result, repeat = replay_raw(raw, version="2", available_at=_AVAILABLE)
    assert first_result.returncode == repeat_result.returncode == 0
    assert first_result.stdout == repeat_result.stdout
    assert first == repeat
    record, previous = first["records"][0], legacy["records"][0]
    assert (
        record["observation_id"]
        == hashlib.sha256(
            f"normalizer:2:fixture-source-dataset:{previous['observation_id']}".encode()
        ).hexdigest()
    )
    assert record["observation_id"] != previous["observation_id"]
    assert record["observed_at"] == previous["observed_at"]
    assert record["entity_id"] == previous["entity_id"]
    assert record["available_at"] == "2026-10-22T12:00:00Z"
    assert record["metrics"] == previous["metrics"]
    assert record["market"] is None
    assert record["attributes"]["creator_id"] == "authorized-account"
    assert record["attributes"]["normalization"] == {
        "version": "2",
        "replaces_observation_id": previous["observation_id"],
        "source_dataset_id": "fixture-source-dataset",
    }
    fields = ["creator.country", "creator.geo.region", "new_empty", "new_signal.curve[].purchases"]
    assert record["attributes"]["unmapped_source_fields"] == fields
    assert first["quarantine"] == [
        {
            "reason": "unmapped_source_fields",
            "observation_id": record["observation_id"],
            "fields": fields,
        }
    ]
    assert "private-unknown-value" not in json.dumps(record)
    assert first["raw"] == raw
    assert first["normalizer_version"] == "2"
    assert first["request_count"] == 0


@pytest.mark.parametrize(
    ("source", "key", "row", "expected_paths"),
    [
        (
            "tiktok_ads",
            "ads",
            {
                "ad": {
                    "id": "ad",
                    "reach": {"unique_users_seen": "10000-19999", "country": "FR"},
                    "new_format": "opaque",
                },
                "advertiser": {
                    "business_id": "business",
                    "business_name": "Known",
                    "country": "FR",
                },
                "unexpected": {"score": 5},
            },
            ["ad.new_format", "ad.reach.country", "advertiser.country", "unexpected.score"],
        ),
        (
            "tiktok_commercial",
            "commercial_contents",
            {
                "id": "commercial",
                "creator": {"username": "creator", "country": "FR", "display_name": "Opaque"},
            },
            ["creator.country", "creator.display_name"],
        ),
    ],
)
def test_v2_reports_nested_source_paths_without_inventing_market_or_copying_values(
    source, key, row, expected_paths
):
    raw = {"data": {key: [row], "has_more": False}}
    result, output = replay_raw(raw, source=source, version="2", available_at=_AVAILABLE)
    assert result.returncode == 0, output
    record = output["records"][0]
    assert record["attributes"]["unmapped_source_fields"] == expected_paths
    assert record["market"] is None
    assert "FR" not in json.dumps(record)
    assert output["quarantine"][0]["fields"] == expected_paths
    assert "record" not in output["quarantine"][0]
    _, legacy = replay_raw(raw, source=source)
    assert record["metrics"] == legacy["records"][0]["metrics"]


@pytest.mark.parametrize(
    ("source", "key", "bad", "good", "reason"),
    [
        ("tiktok_ads", "ads", {"ad": "bad"}, {"ad": {"id": "good"}}, "invalid_ad_object"),
        (
            "tiktok_ads",
            "ads",
            {"ad": {"id": "bad"}, "advertiser": []},
            {"ad": {"id": "good"}},
            "invalid_advertiser_object",
        ),
        (
            "tiktok_ads",
            "ads",
            {"ad": {"id": "bad", "reach": "11K"}},
            {"ad": {"id": "good"}},
            "invalid_reach_object",
        ),
        (
            "tiktok_commercial",
            "commercial_contents",
            {"id": "bad", "creator": []},
            {"id": "good", "creator": None},
            "invalid_creator_object",
        ),
        (
            "tiktok_commercial",
            "commercial_contents",
            {"id": "bad", "creator": {"username": 42}},
            {"id": "good", "creator": {"username": "valid"}},
            "invalid_creator_username",
        ),
    ],
)
def test_v2_quarantines_incompatible_known_nested_shapes_per_row(source, key, bad, good, reason):
    result, output = replay_raw(
        {"data": {key: [bad, good], "has_more": False}},
        source=source,
        version="2",
        available_at=_AVAILABLE,
    )
    assert result.returncode == 0, output
    assert len(output["records"]) == 1
    assert output["records"][0]["entity_id"] == "good"
    assert output["quarantine"] == [{"record": bad, "reason": reason}]
    assert output["normalizer_version"] == "2"


@pytest.mark.parametrize("version", ["3", "v2", 2, None, {"2": None}])
def test_replay_rejects_unknown_or_non_string_transformation_versions(version):
    result, output = replay_raw(
        {"data": {"videos": [{"id": "stable"}]}}, version=version, available_at=_AVAILABLE
    )
    assert result.returncode != 0
    assert output["error"]["category"] == "configuration"


@pytest.mark.parametrize("available", [_REPLAY_DEFAULT, None, "invalid", "2026-10-01T11:59:59Z"])
def test_v2_requires_valid_availability_at_or_after_the_observation(available):
    result, output = replay_raw(
        {"data": {"videos": [{"id": "stable"}]}}, version="2", available_at=available
    )
    assert result.returncode != 0
    assert output["error"]["category"] == "configuration"


def test_v2_fully_mapped_rounded_reach_has_no_field_quarantine():
    raw = {
        "data": {
            "ads": [{"ad": {"id": "rounded", "reach": {"unique_users_seen": "11K"}}}],
            "has_more": False,
        }
    }
    result, output = replay_raw(raw, source="tiktok_ads", version="2", available_at=_OBSERVED)
    assert result.returncode == 0, output
    assert output["quarantine"] == []
    record = output["records"][0]
    assert record["attributes"]["unmapped_source_fields"] == []
    assert record["attributes"]["reach_raw"] == "11K"
    assert record["missing_fields"]["reach"] == "rounding_bounds_not_documented"
    assert "reach" not in record["metrics"]


def test_v2_normalization_is_offline_and_redacts_unmapped_and_quarantined_values():
    raw = {
        "data": {
            "commercial_contents": [
                {
                    "id": "good",
                    "new_secret_value": "contract-secret",
                    "access_token": "contract-secret",
                },
                {"id": "bad", "creator": "contract-secret"},
            ],
            "has_more": False,
        }
    }
    with source_server([(500, {}, {})]) as (endpoint, requests):
        result, output = replay_raw(
            raw,
            source="tiktok_commercial",
            version="2",
            available_at=_AVAILABLE,
            endpoint=endpoint,
            token="contract-secret",
        )
    assert result.returncode == 0, output
    assert requests == []
    assert "contract-secret" not in result.stdout + result.stderr
    assert output["raw"]["data"]["commercial_contents"][0]["new_secret_value"] == "[REDACTED]"
    assert output["quarantine"][0]["reason"] == "unmapped_source_fields"
    assert output["quarantine"][1]["record"]["creator"] == "[REDACTED]"


def test_v2_schema_path_metadata_is_bounded_and_overflow_retains_raw_quarantine():
    bad = {"id": "bad", **{f"new_field_{index}": index for index in range(1025)}}
    result, output = replay_raw(
        {"data": {"videos": [bad, {"id": "good"}]}}, version="2", available_at=_AVAILABLE
    )
    assert result.returncode == 0, output
    assert [record["entity_id"] for record in output["records"]] == ["good"]
    assert output["quarantine"] == [{"record": bad, "reason": "unmapped_source_fields_limit"}]


def test_v2_bounds_total_schema_metadata_without_dropping_accepted_or_quarantined_rows():
    # Shared deep prefixes make path reporting larger than the original JSON;
    # the normalizer must bound the page total as well as each individual row.
    prefix = "nested_" + "x" * 1024
    rows = [
        {
            "id": f"bounded-{index}",
            "view_count": index,
            prefix: {f"field_{field}": field for field in range(8)},
        }
        for index in range(300)
    ]
    result, output = replay_raw({"data": {"videos": rows}}, version="2", available_at=_AVAILABLE)
    assert result.returncode == 0, output
    overflow = [
        item for item in output["quarantine"] if item["reason"] == "unmapped_source_fields_limit"
    ]
    reports = [item for item in output["quarantine"] if item["reason"] == "unmapped_source_fields"]
    assert overflow and output["records"]
    assert len(output["records"]) + len(overflow) == len(rows)
    assert len(reports) == len(output["records"])
    assert (
        sum(len(json.dumps(item["fields"], separators=(",", ":")).encode()) for item in reports)
        <= 2 * 1024 * 1024
    )
    assert output["raw"]["data"]["videos"] == rows


@pytest.mark.parametrize(
    "parent",
    [_REPLAY_DEFAULT, None, "", "   ", "x" * 257, "bad\nparent", 17, {"id": "parent"}],
)
def test_v2_requires_bounded_nonempty_canonical_source_delivery_context(parent):
    result, output = replay_raw(
        {"data": {"videos": [{"id": "stable"}]}},
        version="2",
        available_at=_AVAILABLE,
        source_dataset_id=parent,
    )
    assert result.returncode != 0
    assert output["error"]["category"] == "configuration"


def test_v2_namespaces_independent_deliveries_while_fixed_parent_replays_remain_stable():
    raw = {"data": {"videos": [{"id": "stable", "view_count": 123}]}}
    _, first = replay_raw(
        raw, version="2", available_at=_AVAILABLE, source_dataset_id="original-delivery-one"
    )
    _, second = replay_raw(
        raw,
        version="2",
        available_at="2026-10-23T12:00:00Z",
        source_dataset_id="original-delivery-two",
    )
    _, repeated = replay_raw(
        raw, version="2", available_at=_AVAILABLE, source_dataset_id="original-delivery-one"
    )
    first_row, second_row = first["records"][0], second["records"][0]
    assert first_row["observation_id"] != second_row["observation_id"]
    assert first == repeated
    assert first_row["metrics"] == second_row["metrics"]
    assert first_row["observed_at"] == second_row["observed_at"]
    assert first_row["entity_id"] == second_row["entity_id"]
    assert (
        first_row["attributes"]["normalization"]["replaces_observation_id"]
        == (second_row["attributes"]["normalization"]["replaces_observation_id"])
    )
    assert first_row["attributes"]["normalization"]["source_dataset_id"] == "original-delivery-one"
    assert second_row["attributes"]["normalization"]["source_dataset_id"] == "original-delivery-two"
