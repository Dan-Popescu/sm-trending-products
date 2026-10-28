"""Matching operator interfaces explain evidence without changing its owner."""

import json
from datetime import UTC, datetime

from streamlit.testing.v1 import AppTest
from test_matching import add_candidate

from sm_trending_products.cli import main


def test_candidate_command_needs_explicit_review(pipeline, tmp_path, capsys):
    pipeline.create_product("brush", "Electric toothbrush", synthetic=True)
    key = add_candidate(
        pipeline.identity,
        "brush-fr",
        "Brosse à dents électrique Bearer fixture-secret",
        synthetic=True,
    )
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({"aliases": ["brosse à dents électrique"]}))
    assert (
        main(["match-products", "--product-id", "brush", "--synthetic", "--profile", str(profile)])
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["suggestions"][0]["entity_key"] == key
    assert result["suggestions"][0]["requires_review"] is True
    assert "fixture-secret" not in str(result)
    assert pipeline.product_links("brush", datetime.now(UTC), True) == []


def test_match_desk_reads_only_after_operator_requests_candidates(pipeline):
    pipeline.create_product("brush", "Electric toothbrush", synthetic=True)
    add_candidate(
        pipeline.identity,
        "brush-fr",
        "Brosse à dents électrique Bearer fixture-secret",
        synthetic=True,
    )
    app = AppTest.from_string("""
from datetime import UTC, datetime
from sm_trending_products.catalog import Catalog
from sm_trending_products.matching import ProductMatcher
from sm_trending_products.matching_dashboard import render_matching
from sm_trending_products.settings import Settings
catalog = Catalog(Settings().database_url.get_secret_value())
try:
    render_matching(ProductMatcher(catalog), [{"product_id":"brush"}], datetime.now(UTC), True)
finally:
    catalog.close()
""").run(timeout=20)
    assert not app.exception
    assert not app.json
    app.text_area[0].set_value("brosse à dents électrique")
    app.button[0].click().run()
    assert not app.exception
    result = json.loads(app.json[0].value)
    assert result["suggestions"][0]["requires_review"] is True
    assert "fixture-secret" not in str(result)
    assert pipeline.product_links("brush", datetime.now(UTC), True) == []


def test_explicit_index_setup_unlocks_identifier_only_operator_search(pipeline, tmp_path, capsys):
    pipeline.create_product("bottle", "Travel bottle", synthetic=True)
    key = add_candidate(
        pipeline.identity,
        "fr-bottle",
        "Gourde de voyage",
        synthetic=True,
        attributes={"ean": "4006381333931"},
    )
    profile = tmp_path / "ean.json"
    profile.write_text(json.dumps({"identifiers": {"ean": "4006381333931"}}))
    command = ["match-products", "--product-id", "bottle", "--synthetic", "--profile", str(profile)]
    assert main(command) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "matching_indexes_not_ready"
    assert main(["match-products", "--initialize-indexes"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert main(command) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["suggestions"][0]["entity_key"] == key
    assert report["suggestions"][0]["requires_review"] is True


def test_profile_file_is_bounded_before_database_access(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    profile = tmp_path / "oversize.json"
    profile.write_bytes(b" " * 65_537)
    assert main(["match-products", "--product-id", "unused", "--profile", str(profile)]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "profile_too_large"
