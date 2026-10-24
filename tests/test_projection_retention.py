"""Retirement physically removes disposable SQL evidence, including old payloads."""

from importlib.resources import files

import pytest
from test_feature_projection import seeded


@pytest.mark.parametrize("root_column", ["audit_root", "input_audit_root"])
@pytest.mark.parametrize("state", ["invalidated", "deleted"])
def test_retiring_either_root_removes_sql_price_payload_and_preserves_valid_product(
    pipeline, root_column, state
):
    _cutoff, _product, _rows, _datasets, _links, _scope, _cache, _row, root = seeded(pipeline)
    pipeline.create_product("valid", "Valid", synthetic=True)
    pipeline.catalog.stage_derivation("valid-root", [])
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='committed' WHERE object_key='valid-root'"
    )
    pipeline.catalog.execute(
        "INSERT INTO feature_product_projections SELECT 'valid',synthetic,projection_version,"
        "context_sha256,source_sha256,computed_at,valid_through,age_anchor,payload,"
        "'valid-root',NULL,'[]','{}' FROM feature_product_projections WHERE product_id='p'"
    )
    if root_column == "input_audit_root":
        pipeline.catalog.execute(
            "UPDATE feature_product_projections SET audit_root='valid-root',input_audit_root=%s "
            "WHERE product_id='p'",
            (root,),
        )
    pipeline.catalog.execute(
        "UPDATE feature_product_projections SET payload=payload || "
        "'{\"private_offer_prices\":[199.99,149.99]}'::jsonb WHERE product_id='p'"
    )
    assert pipeline.catalog.one(
        "SELECT payload FROM feature_product_projections WHERE product_id='p'"
    )["payload"]["private_offer_prices"] == [199.99, 149.99]
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state=%s WHERE object_key=%s",
        (state, root),
    )
    assert (
        pipeline.catalog.one("SELECT payload FROM feature_product_projections WHERE product_id='p'")
        is None
    )
    assert pipeline.catalog.one(
        "SELECT product_id FROM feature_product_projections WHERE product_id='valid'"
    )


@pytest.mark.parametrize("root_column", ["audit_root", "input_audit_root"])
def test_migration_removes_preexisting_retired_payload_without_object_storage(
    pipeline, monkeypatch, root_column
):
    _cutoff, _product, _rows, _datasets, _links, _scope, _cache, _row, root = seeded(pipeline)
    if root_column == "input_audit_root":
        pipeline.catalog.execute(
            "UPDATE feature_product_projections SET input_audit_root=audit_root,audit_root=NULL"
        )
    original = files("sm_trending_products").joinpath("sql/021_feature_projection.sql").read_text()
    legacy_body = original.split("CREATE FUNCTION retire_feature_audit_windows()", 1)[1].split(
        "END $$;", 1
    )[0]
    legacy_function = (
        "CREATE OR REPLACE FUNCTION retire_feature_audit_windows()" + legacy_body + "END $$;"
    )
    pipeline.catalog.execute(legacy_function)
    pipeline.catalog.execute(
        "UPDATE derived_publications SET state='invalidated' WHERE object_key=%s",
        (root,),
    )
    assert (
        pipeline.catalog.one("SELECT count(*) AS count FROM feature_product_projections")["count"]
        == 1
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("SQL payload cleanup must not access object storage")

    monkeypatch.setattr(pipeline.history, "_make_client", forbidden)
    migration = files("sm_trending_products").joinpath("sql/027_retire_feature_payloads.sql")
    pipeline.catalog.execute(migration.read_text())
    assert (
        pipeline.catalog.one("SELECT count(*) AS count FROM feature_product_projections")["count"]
        == 0
    )
