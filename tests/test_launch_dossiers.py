"""Commercial preparation through the same boundary as the private operator UI."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Barrier

import pytest
from pydantic import ValidationError

from sm_trending_products.commerce import LaunchDraft, LaunchError, LaunchService


def draft(**changes) -> LaunchDraft:
    values = {
        "dossier_id": "launch-p1-fr",
        "product_id": "p1",
        "country": "FR",
        "currency": "EUR",
        "synthetic": True,
        "title": "Useful product",
        "description": "Operator-authored description",
        "supplier_name": "Fixture supplier",
        "supplier_reference": "quote-123",
        "quote_expires_at": datetime.now(UTC) + timedelta(days=7),
        "stock_status": "available",
        "delivery_min_days": 3,
        "delivery_max_days": 8,
        "return_terms": "Operator supplied return terms",
        "decision": "test",
        "retail_price": "36.00",
        "prices_include_tax": True,
        "tax_per_order": "6.00",
        "costs": {
            "goods": "8.25",
            "shipping": "3.50",
            "payment_fees": "1.25",
            "nonrecoverable_taxes": "0",
            "returns_allowance": "2.00",
            "fulfillment": "0",
        },
        "desired_profit": "5.00",
    }
    values.update(changes)
    return LaunchDraft.model_validate(values)


@pytest.fixture
def launches(pipeline):
    pipeline.create_product("p1", "Fixture product", synthetic=True)
    service = LaunchService(pipeline.catalog)
    service.initialize()
    return service


def test_revision_scenarios_replay_and_private_escaped_page(launches):
    original = draft(title='<script>alert("title")</script>', description="A < B & C\nnext")
    saved = launches.save(original, expected_revision=0, request_id="create")
    assert saved["revision"] == 1
    assert saved["readiness"]["status"] == "ready"
    assert saved["readiness"]["scenario_only"] is True
    assert saved["economics"] == {
        "currency": "EUR",
        "net_revenue": "30.00",
        "total_costs": "15.00",
        "pre_ad_contribution": "15.00",
        "maximum_acquisition_cost": "10.00",
        "desired_profit": "5.00",
        "unknown_reasons": [],
        "scenario_only": True,
    }
    html = launches.draft_page(original.dossier_id, synthetic=True)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "A &lt; B &amp; C<br>next" in html
    assert "noindex,nofollow" in html
    assert "Fixture supplier" not in html
    assert "quote-123" not in html

    revised = original.model_copy(
        update={"note": "A later decision", "retail_price": Decimal("42")}
    )
    second = launches.save(revised, expected_revision=1, request_id="revise")
    replay = launches.save(original, expected_revision=0, request_id="create")
    assert replay["revision"] == 1
    assert replay["draft"] == saved["draft"]
    assert replay["updated_at"] == saved["updated_at"]
    assert launches.dossier(original.dossier_id, synthetic=True)["revision"] == 2
    assert second["created_at"] == saved["created_at"]
    assert second["economics"]["net_revenue"] == "36.00"
    assert launches.catalog.one("SELECT count(*) AS n FROM commerce_dossier_revisions")["n"] == 2


def test_request_conflict_and_stale_revision_leave_history_unchanged(launches):
    original = draft()
    launches.save(original, 0, "one")
    with pytest.raises(LaunchError, match="request conflict") as error:
        launches.save(original.model_copy(update={"note": "changed"}), 0, "one")
    assert error.value.code == "request_conflict"
    with pytest.raises(LaunchError) as error:
        launches.save(original, 0, "two")
    assert error.value.code == "revision_conflict"
    assert launches.dossier(original.dossier_id, True)["revision"] == 1
    assert launches.catalog.one("SELECT count(*) AS n FROM commerce_dossier_revisions")["n"] == 1


def test_missing_costs_and_tax_basis_remain_unknown(launches):
    incomplete = draft(costs={"goods": "0"}, prices_include_tax=None)
    saved = launches.save(incomplete, 0, "one")
    economics = saved["economics"]
    assert saved["readiness"]["status"] == "incomplete"
    assert economics["net_revenue"] is None
    assert economics["total_costs"] is None
    assert economics["pre_ad_contribution"] is None
    assert economics["maximum_acquisition_cost"] is None
    assert "costs.goods" not in economics["unknown_reasons"]
    assert "costs.payment_fees" in economics["unknown_reasons"]
    assert "prices_include_tax" in economics["unknown_reasons"]

    exclusive = draft(prices_include_tax=False, tax_per_order=None)
    net = launches.save(exclusive, 1, "two")["economics"]
    assert net["net_revenue"] == "36.00"
    assert net["maximum_acquisition_cost"] == "16.00"
    tax_unknown = draft(tax_per_order=None)
    net = launches.save(tax_unknown, 2, "three")["economics"]
    assert net["net_revenue"] is None
    assert net["unknown_reasons"] == ["tax_per_order"]


def test_explicit_zero_costs_and_default_break_even_policy_are_valid(launches):
    values = draft().model_dump(mode="python")
    values.pop("desired_profit")
    values["costs"] = {name: "0" for name in values["costs"]}
    saved = launches.save(LaunchDraft.model_validate(values), 0, "zero-costs")
    assert saved["readiness"]["status"] == "ready"
    assert saved["economics"]["total_costs"] == "0"
    assert saved["economics"]["maximum_acquisition_cost"] == "30.00"
    assert saved["economics"]["desired_profit"] == "0"


def test_blank_commercial_facts_do_not_imply_readiness(launches):
    blank = LaunchDraft(
        dossier_id="blank", product_id="p1", country="FR", currency="EUR", synthetic=True
    )
    saved = launches.save(blank, 0, "empty")
    assert saved["readiness"]["status"] == "incomplete"
    assert saved["economics"]["maximum_acquisition_cost"] is None
    fields = {reason["field"] for reason in saved["readiness"]["reasons"]}
    assert {
        "supplier_name",
        "supplier_reference",
        "quote_expires_at",
        "return_terms",
        "stock_status",
        "delivery_min_days",
        "delivery_max_days",
        "decision",
    } <= fields


def test_stale_supplier_unavailability_and_unaffordable_scenario_block(launches):
    stale = draft(
        quote_expires_at=datetime(2020, 1, 1, tzinfo=UTC),
        stock_status="unavailable",
        retail_price="10",
        tax_per_order="1",
    )
    saved = launches.save(stale, 0, "one")
    assert saved["readiness"]["status"] == "blocked"
    assert {reason["code"] for reason in saved["readiness"]["reasons"]} == {
        "stale_quote",
        "unavailable_stock",
        "negative_acquisition_budget",
    }
    assert saved["economics"]["maximum_acquisition_cost"] == "-11.00"
    assert launches.dossier(stale.dossier_id, True)["readiness"]["status"] == "blocked"


def test_real_synthetic_isolation_unknown_product_and_fixed_identity(launches, pipeline):
    pipeline.create_product("real", "Real canonical product", synthetic=False)
    pipeline.create_product("other", "Other fixture product", synthetic=True)
    original = draft()
    saved = launches.save(original, 0, "one")
    assert saved["synthetic"] is True
    assert launches.dossiers() == []
    assert len(launches.dossiers(synthetic=True)) == 1
    with pytest.raises(LaunchError) as error:
        launches.dossier(original.dossier_id)
    assert error.value.code == "not_found"
    for change, expected in (
        ({"product_id": "missing"}, "unknown_product"),
        ({"product_id": "real"}, "workspace_mismatch"),
        ({"country": "DE"}, "immutable_identity"),
        ({"currency": "USD"}, "immutable_identity"),
        ({"product_id": "other"}, "immutable_identity"),
        ({"synthetic": False}, "workspace_mismatch"),
    ):
        with pytest.raises(LaunchError) as error:
            launches.save(original.model_copy(update=change), 1, "changed")
        assert error.value.code == expected
    assert launches.dossier(original.dossier_id, True)["revision"] == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("retail_price", 1.1),
        ("retail_price", True),
        ("desired_profit", "NaN"),
        ("retail_price", "-1"),
        ("retail_price", "0.0000001"),
        ("prices_include_tax", "false"),
        ("country", "EU"),
        ("country", "fr"),
        ("delivery_min_days", True),
        ("quote_expires_at", "2026-01-01T00:00:00"),
        ("costs", {"goods": 1.5}),
        ("costs", {"arbitrary": "1"}),
    ],
)
def test_inputs_are_explicit_bounded_and_exact(field, value):
    with pytest.raises(ValidationError):
        draft(**{field: value})


def test_authoritative_save_revalidates_nested_and_bypassed_models(launches):
    invalid = draft().model_copy(update={"country": "EU"})
    with pytest.raises(ValidationError):
        launches.save(invalid, 0, "invalid")
    mutable = draft()
    mutable.costs["goods"] = 1.1
    with pytest.raises(ValidationError):
        launches.save(mutable, 0, "invalid")
    assert launches.dossiers(True) == []


def test_competing_creates_and_revisions_have_one_winner(launches):
    original = draft()

    def compete(expected_revision):
        barrier = Barrier(2)

        def attempt(index):
            barrier.wait(timeout=5)
            try:
                result = launches.save(
                    original.model_copy(update={"note": f"worker {index}"}),
                    expected_revision,
                    f"write-{expected_revision}-{index}",
                )
                return result["revision"]
            except LaunchError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(attempt, range(2)))

    assert sorted(map(str, compete(0))) == ["1", "revision_conflict"]
    assert sorted(map(str, compete(1))) == ["2", "revision_conflict"]
    assert launches.catalog.one("SELECT count(*) AS n FROM commerce_dossier_revisions")["n"] == 2


def test_competing_identical_replays_append_only_once(launches):
    original = draft()
    with ThreadPoolExecutor(max_workers=4) as pool:
        revisions = list(
            pool.map(lambda _: launches.save(original, 0, "same")["revision"], range(4))
        )
    assert revisions == [1, 1, 1, 1]
    assert launches.catalog.one("SELECT count(*) AS n FROM commerce_dossier_revisions")["n"] == 1


def test_bounded_deterministic_pages(launches):
    launches.save(draft(dossier_id="first"), 0, "one")
    launches.save(draft(dossier_id="second"), 0, "two")
    assert [row["dossier_id"] for row in launches.dossiers(True, 1, 0)] == ["second"]
    assert [row["dossier_id"] for row in launches.dossiers(True, 1, 1)] == ["first"]
    for limit, offset in ((0, 0), (1001, 0), (1, -1), (1, 1_000_001), (True, 0)):
        with pytest.raises(LaunchError) as error:
            launches.dossiers(True, limit, offset)
        assert error.value.code == "invalid_page"
    with pytest.raises(LaunchError):
        launches.dossiers(synthetic="true")


def test_reads_never_initialize_optional_module_or_change_capture_schema(pipeline):
    service = LaunchService(pipeline.catalog)
    before = pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version")
    for read in (
        lambda: service.dossier("missing"),
        service.dossiers,
        lambda: service.draft_page("missing"),
    ):
        with pytest.raises(LaunchError) as error:
            read()
        assert error.value.code == "not_initialized"
    assert pipeline.catalog.one("SELECT to_regclass('commerce_dossiers') AS table")["table"] is None
    assert service.initialize() == {"module": "commerce", "version": 1, "status": "initialized"}
    assert service.initialize()["version"] == 1
    assert pipeline.catalog.all("SELECT * FROM schema_migrations ORDER BY version") == before
