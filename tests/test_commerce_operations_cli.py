"""Native operator commands share the reviewed order and actual-cost boundaries."""

import json
from dataclasses import replace

import pytest
from test_commerce_learning import period, plan
from test_commerce_orders import context as context
from test_commerce_orders import signed
from test_evaluation_integration import seed_product
from test_launch_dossiers import draft

from sm_trending_products import commerce_connection
from sm_trending_products.catalog import Catalog
from sm_trending_products.cli import main
from sm_trending_products.commerce import LaunchService
from sm_trending_products.commerce_connection import connection_template
from sm_trending_products.contracts import now, utc
from sm_trending_products.history import History


def invoke(capsys, *arguments, code=0):
    assert main(["commerce", "operations", *map(str, arguments)]) == code
    streams = capsys.readouterr()
    if code == 2:
        assert streams.out == ""
        return json.loads(streams.err)
    assert streams.err == ""
    return json.loads(streams.out)


def write(tmp_path, name, value):
    file = tmp_path / name
    file.write_text(json.dumps(value))
    file.chmod(0o600)
    return file


@pytest.mark.parametrize(
    "action",
    [
        None,
        "order-init",
        "learning-init",
        "plan-template",
        "period-template",
        "register-listing",
        "ingest-event",
        "reconcile-order",
        "reconcile-page",
        "orders",
        "order",
        "inbox",
        "handoff",
        "tracking",
        "notification-intent",
        "plan-test",
        "review-period",
        "publish-period",
        "test",
        "tests",
        "periods",
    ],
)
def test_every_operations_help_requires_no_database_or_object_store(action, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("Help must not construct database or object-store runtime")

    monkeypatch.setattr(Catalog, "__init__", forbidden)
    monkeypatch.setattr(History, "__init__", forbidden)
    command = ["commerce", "operations", *([action] if action else []), "--help"]
    with pytest.raises(SystemExit) as exit_status:
        main(command)
    assert exit_status.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_templates_are_safe_and_work_without_database(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    plan_template = invoke(capsys, "plan-template")
    actual_template = invoke(capsys, "period-template")
    assert plan_template["selection"] == "watch"
    assert plan_template["reason"] == plan_template["selection_policy"] == ""
    assert actual_template["realized_revenue"] is None
    assert all(value is None for value in actual_template["realized_costs"].values())
    assert not any(actual_template["coverage"].values())
    assert actual_template["evidence_refs"] == []


@pytest.fixture
def order_cli(context, monkeypatch, tmp_path):
    service, provider, operation = context
    # Order commands must never construct History, even with unusable S3 settings.
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://unreachable.invalid")

    def forbidden(*args, **kwargs):
        pytest.fail("Order commands must remain independent of S3")

    monkeypatch.setattr(History, "__init__", forbidden)
    connection = write(tmp_path, "private-connection.json", {"provider": "controlled"})
    monkeypatch.setattr(commerce_connection, "load_connection", lambda _path: provider)
    return service, provider, operation, connection


@pytest.mark.integration
def test_cli_order_init_registration_signed_dedup_and_no_effect_manual_records(
    order_cli,
    pipeline,
    tmp_path,
    capsys,
):
    _service, provider, operation, connection = order_cli
    assert invoke(capsys, "order-init")["module"] == "commerce_orders"
    registered = invoke(
        capsys, "register-listing", "--operation-id", operation["operation_id"], "--synthetic"
    )
    assert registered["canonical_product_id"] == "p1"
    raw, headers = signed(provider, {"id": 71, "billing": {"email": "private@example.test"}})
    body_file = tmp_path / "event.json"
    body_file.write_bytes(raw)
    headers_file = write(tmp_path, "headers.json", headers)
    command = (
        "ingest-event",
        "--connection",
        connection,
        "--body-file",
        body_file,
        "--headers-file",
        headers_file,
        "--synthetic",
    )
    first = invoke(capsys, *command)
    assert first["revision"] == 1 and first["cash_complete"] is False
    assert "private@example.test" not in json.dumps(first)
    assert first["payload"]["payments"] == []
    assert invoke(capsys, *command)["state"] == "duplicate"
    assert provider.calls == 1
    assert (
        invoke(capsys, "orders", "--connection-id", "local-store", "--synthetic")["items"][0][
            "order_id"
        ]
        == "71"
    )
    assert invoke(capsys, "inbox", "--connection-id", "local-store", "--synthetic")["items"] == []
    assert (
        invoke(
            capsys, "inbox", "--connection-id", "local-store", "--synthetic", "--include-processed"
        )["items"][0]["state"]
        == "completed"
    )
    handoff_file = write(
        tmp_path,
        "handoff.json",
        {
            "supplier_reference": "supplier-quote-123",
            "line_ids": ["81"],
        },
    )
    handoff_command = (
        "handoff",
        "--connection-id",
        "local-store",
        "--order-id",
        "71",
        "--request-id",
        "handoff",
        "--expected-revision",
        "0",
        "--file",
        handoff_file,
        "--synthetic",
    )
    handoff = invoke(capsys, *handoff_command)
    assert handoff["payload"]["external_action"] == "none"
    assert invoke(capsys, *handoff_command) == handoff
    tracking_file = write(
        tmp_path,
        "tracking.json",
        {
            "handoff_revision": 1,
            "carrier": "manual-carrier",
            "tracking_reference": "TRACK-123",
        },
    )
    tracked = invoke(
        capsys,
        "tracking",
        "--connection-id",
        "local-store",
        "--order-id",
        "71",
        "--request-id",
        "tracking",
        "--expected-revision",
        "0",
        "--file",
        tracking_file,
        "--synthetic",
    )
    assert tracked["payload"]["carrier_verified"] is False
    notification_file = write(
        tmp_path, "notification.json", {"notification_kind": "shipping_update"}
    )
    notified = invoke(
        capsys,
        "notification-intent",
        "--connection-id",
        "local-store",
        "--order-id",
        "71",
        "--request-id",
        "notification",
        "--expected-revision",
        "0",
        "--file",
        notification_file,
        "--synthetic",
    )
    assert notified["payload"]["external_action"] == "none"
    assert notified["payload"]["confirmed_delivered"] is False
    detail = invoke(
        capsys, "order", "--connection-id", "local-store", "--order-id", "71", "--synthetic"
    )
    assert {item["kind"] for item in detail["events"]} == {"handoff", "tracking", "notification"}
    assert provider.calls == 1  # Manual records never execute provider effects.
    assert pipeline.catalog.one("SELECT count(*) AS count FROM commerce_order_events")["count"] == 3
    assert (
        invoke(capsys, "order", "--connection-id", "local-store", "--order-id", "71", code=2)[
            "error"
        ]
        == "order_not_found"
    )
    assert invoke(capsys, "orders", "--connection-id", "local-store")["items"] == []


@pytest.mark.integration
def test_cli_provider_failure_keeps_retryable_inbox_and_reconciles_page(
    order_cli, tmp_path, capsys
):
    _service, provider, _operation, connection = order_cli

    def failed():
        raise RuntimeError("PRIVATE-provider-token customer@example.test")

    provider.callback = failed
    raw, headers = signed(provider, {"id": 71})
    body = tmp_path / "event.json"
    body.write_bytes(raw)
    header_file = write(tmp_path, "headers.json", headers)
    error = invoke(
        capsys,
        "ingest-event",
        "--connection",
        connection,
        "--body-file",
        body,
        "--headers-file",
        header_file,
        "--synthetic",
        code=2,
    )
    assert error["error"] == "order_provider_failed"
    assert "PRIVATE" not in json.dumps(error) and "customer@" not in json.dumps(error)
    pending = invoke(capsys, "inbox", "--connection-id", "local-store", "--synthetic")
    assert pending["items"][0]["state"] == "pending"
    provider.callback = None
    reconciled = invoke(
        capsys, "reconcile-order", "--connection", connection, "--order-id", "71", "--synthetic"
    )
    assert reconciled["revision"] == 1
    page = invoke(
        capsys,
        "reconcile-page",
        "--connection",
        connection,
        "--updated-after",
        "2026-10-01T00:00:00Z",
        "--synthetic",
    )
    assert page["coverage_complete"] is False
    assert page["orders"][0]["order_id"] == "71"


@pytest.mark.integration
def test_cli_database_guard_loss_rejects_commit_keeps_inbox_and_allows_retry(
    order_cli,
    pipeline,
    tmp_path,
    capsys,
):
    _service, provider, _operation, connection = order_cli

    def lose_database_session():
        key = pipeline.catalog.one(
            "SELECT hashtextextended(current_schema()||':commerce-order:'||%s,492371253) AS key",
            ("local-store:71",),
        )["key"]
        pid = pipeline.catalog.one(
            "SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid::bigint=%s "
            "AND objid::bigint=%s AND granted",
            ((key >> 32) & 4294967295, key & 4294967295),
        )["pid"]
        assert pipeline.catalog.one("SELECT pg_terminate_backend(%s) AS killed", (pid,))["killed"]

    raw, headers = signed(provider, {"id": 71})
    body = tmp_path / "event.json"
    body.write_bytes(raw)
    header_file = write(tmp_path, "headers.json", headers)
    command = (
        "ingest-event",
        "--connection",
        connection,
        "--body-file",
        body,
        "--headers-file",
        header_file,
        "--synthetic",
    )
    provider.callback = lose_database_session
    error = invoke(capsys, *command, code=2)
    assert error["error"] == "order_database_guard_unavailable"
    assert pipeline.catalog.all("SELECT * FROM commerce_order_snapshots") == []
    assert (
        invoke(capsys, "inbox", "--connection-id", "local-store", "--synthetic")["items"][0][
            "state"
        ]
        == "pending"
    )
    provider.callback = None
    assert invoke(capsys, *command)["revision"] == 1


@pytest.mark.integration
def test_cli_conflicts_require_explicit_current_revision_resolution(order_cli, capsys):
    _service, provider, _operation, connection = order_cli
    command = ("reconcile-order", "--connection", connection, "--order-id", "71", "--synthetic")
    first = invoke(capsys, *command)
    provider.snapshot = replace(provider.snapshot, total="1.00")
    assert invoke(capsys, *command)["state"] == "conflict"
    assert (
        invoke(capsys, *command, "--resolve-conflict", code=2)["error"]
        == "conflict_resolution_requires_revision"
    )
    resolved = invoke(capsys, *command, "--resolve-conflict", "--expected-revision", "1")
    assert resolved["state"] == "current" and resolved["revision"] == 2
    historical = invoke(
        capsys,
        "order",
        "--connection-id",
        "local-store",
        "--order-id",
        "71",
        "--revision",
        "1",
        "--synthetic",
    )
    assert historical["payload"] == first["payload"] and historical["state"] == "historical"


@pytest.mark.integration
@pytest.mark.parametrize(
    "invalid", ["large_headers", "header_values", "many_headers", "large_body"]
)
def test_cli_bounds_event_inputs_without_echoing_private_values(
    order_cli,
    tmp_path,
    capsys,
    invalid,
):
    _service, provider, _operation, connection = order_cli
    raw, headers = signed(provider, {"id": 71})
    body = tmp_path / "event.json"
    body.write_bytes(raw if invalid != "large_body" else b"x" * 262145)
    if invalid == "header_values":
        headers["Private"] = ["PRIVATE-header-token"]
    elif invalid == "many_headers":
        headers.update({f"Extra-{value}": "private" for value in range(101)})
    elif invalid == "large_headers":
        headers["Private"] = "PRIVATE-header-token" * 1000
    header_file = write(tmp_path, "headers.json", headers)
    error = invoke(
        capsys,
        "ingest-event",
        "--connection",
        connection,
        "--body-file",
        body,
        "--headers-file",
        header_file,
        "--synthetic",
        code=2,
    )
    assert "PRIVATE" not in json.dumps(error)
    assert provider.calls == 0


@pytest.mark.integration
def test_private_connection_errors_do_not_echo_environment_secrets(
    pipeline, tmp_path, capsys, monkeypatch
):
    config = connection_template("woocommerce")
    config.update(
        base_url="http://127.0.0.1:9", connection_id="private-store", enabled=True, test_mode=True
    )
    connection = write(tmp_path, "private-store.json", config)
    monkeypatch.setenv("COMMERCE_CONSUMER_KEY", "PRIVATE-CONSUMER-KEY")
    monkeypatch.setenv("COMMERCE_CONSUMER_SECRET", "PRIVATE-CONSUMER-SECRET")
    monkeypatch.setenv("COMMERCE_WEBHOOK_SECRET", "PRIVATE-WEBHOOK-SECRET")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://unreachable.invalid")
    invoke(capsys, "order-init")
    body = write(tmp_path, "event.json", {"id": 71})
    headers = write(tmp_path, "headers.json", {"X-WC-Webhook-Signature": "wrong"})
    error = invoke(
        capsys,
        "ingest-event",
        "--connection",
        connection,
        "--body-file",
        body,
        "--headers-file",
        headers,
        "--synthetic",
        code=2,
    )
    assert error["error"] == "invalid_webhook_signature"
    assert "PRIVATE" not in json.dumps(error)
    connection.write_text("PRIVATE-CONNECTION-SECRET" * 1000)
    error = invoke(
        capsys,
        "reconcile-order",
        "--connection",
        connection,
        "--order-id",
        "71",
        "--synthetic",
        code=2,
    )
    assert "PRIVATE" not in json.dumps(error)


@pytest.fixture
def learning_cli(pipeline, monkeypatch):
    seed_product(pipeline, "p1")
    launch = LaunchService(pipeline.catalog)
    launch.initialize()
    launch.save(draft(), 0, "dossier")
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("CACHE_DIR", str(pipeline.settings.cache_dir))
    return pipeline


def plan_command(file):
    return (
        "plan-test",
        "--dossier-id",
        "launch-p1-fr",
        "--expected-revision",
        "1",
        "--request-id",
        "plan",
        "--file",
        file,
        "--synthetic",
    )


def review_command(test, file, expected_revision, request_id):
    return (
        "review-period",
        "--test-id",
        test["test_id"],
        "--expected-revision",
        expected_revision,
        "--request-id",
        request_id,
        "--file",
        file,
        "--synthetic",
    )


@pytest.mark.integration
def test_cli_learning_incomplete_unknown_complete_publication_replay_and_isolation(
    learning_cli,
    tmp_path,
    capsys,
):
    invoke(capsys, "learning-init")
    plan_file = write(tmp_path, "plan.json", plan().model_dump(mode="json"))
    command = plan_command(plan_file)
    test = invoke(capsys, *command)
    assert test["state"] == "ready" and test["profit_contract"]["ledgers"][test["ledger_key"]]
    assert invoke(capsys, *command) == test
    assert invoke(capsys, "test", "--test-id", test["test_id"], code=2)["error"] == "test_not_found"
    assert invoke(capsys, "tests") == []
    assert invoke(capsys, "tests", "--synthetic")[0]["test_id"] == test["test_id"]
    start, end = utc(test["planned_at"]), now()
    complete = period(start, end).model_dump(mode="json")
    missing = json.loads(json.dumps(complete))
    del missing["realized_costs"]["advertising"]
    file = write(tmp_path, "actuals.json", missing)
    first = invoke(capsys, *review_command(test, file, 0, "incomplete"))
    assert first["realized_profit"] is None and first["complete"] is False
    assert "unknown_actual_cost:advertising" in first["reasons"]
    publish = (
        "publish-period",
        "--test-id",
        test["test_id"],
        "--period-id",
        first["period_id"],
        "--revision",
    )
    assert invoke(capsys, *publish, "1", "--synthetic", code=2)["error"] == "incomplete_period"
    file.write_text(json.dumps(complete))
    second = invoke(capsys, *review_command(test, file, 1, "complete"), code=1)
    assert second["complete"] is True and second["realized_profit"] == "40.00"
    assert second["publication_state"] == "pending"
    assert invoke(capsys, *review_command(test, file, 1, "complete"), code=1) == second
    published = invoke(capsys, *publish, "2", "--synthetic")
    assert published["publication_state"] == "published"
    assert invoke(capsys, *publish, "2", "--synthetic") == published
    assert invoke(capsys, "periods", "--test-id", test["test_id"], "--synthetic") == [published]
    assert invoke(capsys, *publish, "2", code=2)["error"] == "test_not_found"


@pytest.mark.integration
def test_cli_historical_pending_withdrawal_is_recoverable_after_a_newer_complete_period(
    learning_cli,
    tmp_path,
    capsys,
    monkeypatch,
):
    invoke(capsys, "learning-init")
    plan_file = write(tmp_path, "plan.json", plan().model_dump(mode="json"))
    test = invoke(capsys, *plan_command(plan_file))
    payload = period(utc(test["planned_at"]), now()).model_dump(mode="json")
    file = write(tmp_path, "period.json", payload)
    original = invoke(capsys, *review_command(test, file, 0, "original"), code=1)
    publish = (
        "publish-period",
        "--test-id",
        test["test_id"],
        "--period-id",
        original["period_id"],
        "--revision",
    )
    invoke(capsys, *publish, "1", "--synthetic")
    payload["coverage"]["refunds_complete"] = False
    file.write_text(json.dumps(payload))
    with monkeypatch.context() as fault:
        real_publish = History.publish

        def unavailable(self, observations, *args, **kwargs):
            if observations[0].attributes.get("publication_kind") == "withdrawal":
                raise RuntimeError("PRIVATE-object-store-error")
            return real_publish(self, observations, *args, **kwargs)

        fault.setattr(History, "publish", unavailable)
        error = invoke(capsys, *review_command(test, file, 1, "withdraw"), code=2)
        assert "PRIVATE" not in json.dumps(error)
    pending = invoke(capsys, "periods", "--test-id", test["test_id"], "--synthetic")[0]
    assert pending["publication_kind"] == "withdrawal" and pending["publication_state"] == "pending"
    payload["coverage"]["refunds_complete"] = True
    file.write_text(json.dumps(payload))
    invoke(capsys, *review_command(test, file, 2, "newer"), code=1)
    invoke(capsys, *publish, "3", "--synthetic")
    history = invoke(
        capsys, "periods", "--test-id", test["test_id"], "--synthetic", "--include-history"
    )
    assert history[1]["superseded"] is True and history[1]["publication_state"] == "pending"
    recovered = invoke(capsys, *publish, "2", "--synthetic")
    assert recovered["available_at"] == pending["available_at"]
    assert recovered["publication_kind"] == "withdrawal" and recovered["realized_profit"] is None
    assert invoke(capsys, *publish, "2", "--synthetic") == recovered


@pytest.mark.integration
def test_cli_operator_files_are_bounded_and_errors_exclude_sensitive_input(
    learning_cli,
    tmp_path,
    capsys,
):
    invoke(capsys, "learning-init")
    file = tmp_path / "oversized-plan.json"
    file.write_text("PRIVATE-INPUT" * 22000)
    error = invoke(capsys, *plan_command(file), code=2)
    assert "PRIVATE" not in json.dumps(error)
    file.write_text(json.dumps({**plan().model_dump(mode="json"), "reason": ""}))
    error = invoke(capsys, *plan_command(file), code=2)
    assert error["error"] == "invalid_commercial_record"
    assert ["reason"] in error["fields"]


@pytest.mark.integration
@pytest.mark.parametrize("location", ["root", "cost_contract"])
def test_cli_unknown_field_names_do_not_echo_private_source_data(
    learning_cli,
    tmp_path,
    capsys,
    location,
):
    invoke(capsys, "learning-init")
    payload = plan().model_dump(mode="json")
    target = payload if location == "root" else payload["cost_contract"]
    target["PRIVATE-customer@example.test"] = "unexpected"
    file = write(
        tmp_path,
        "private-plan.json",
        payload,
    )
    error = invoke(capsys, *plan_command(file), code=2)
    assert error["error"] == "invalid_commercial_record"
    assert "PRIVATE" not in json.dumps(error)
    assert "customer@example.test" not in json.dumps(error)
