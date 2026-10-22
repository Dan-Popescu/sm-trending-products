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
