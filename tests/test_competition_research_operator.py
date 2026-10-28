"""Selected-product operator paths share the guarded competition computation."""

import json
from datetime import timedelta

from streamlit.testing.v1 import AppTest
from test_competition_insights import observation, seed

from sm_trending_products.competition_research_cli import main
from sm_trending_products.contracts import now


def _field(app, kind, label):
    return next(field for field in getattr(app, kind) if field.label == label)


def _app(pipeline, cutoff):
    return AppTest.from_string(f"""
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.settings import Settings
from sm_trending_products.contracts import utc
from sm_trending_products.competition_research_dashboard import render_competition
settings = Settings(s3_bucket={pipeline.settings.s3_bucket!r},
    s3_endpoint_url={pipeline.settings.s3_endpoint_url!r},
    cache_dir={str(pipeline.settings.cache_dir)!r}, history_partition_buckets=4)
with Pipeline(settings) as pipeline:
    render_competition(pipeline, [{{"product_id": "competitive"}}],
        utc({cutoff.isoformat()!r}), True)
""").run(timeout=20)


def test_competition_cli_reuses_guarded_evidence_and_rejects_budget_namespace(
    pipeline, monkeypatch, capsys
):
    insights, cutoff, _ = seed(pipeline)
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    monkeypatch.setenv("CACHE_DIR", str(pipeline.settings.cache_dir))
    monkeypatch.setenv("HISTORY_PARTITION_BUCKETS", "4")
    args = [
        "--product-id",
        "competitive",
        "--as-of",
        cutoff.isoformat(),
        "--synthetic",
        "--country",
        "FR",
        "--source",
        "competition_fixture",
        "--query-scope",
        "tools-fr-v1",
        "--max-rows",
        "50000",
    ]
    assert main(args) == 0
    report = json.loads(capsys.readouterr().out)
    expected = insights.describe(
        "competitive",
        cutoff,
        True,
        country="FR",
        source="competition_fixture",
        query_scope="tools-fr-v1",
    )
    assert report["report_id"] == expected["report_id"]
    assert report["competition_growth"] is report["market_saturation_percent"] is None
    assert sum(row["price_interval_count"] for row in report["scopes"]) == 3
    assert main([*args, "--max-rows", "2"]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "competition_inspection_failed"
    assert main([item for item in args if item != "--synthetic"]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "competition_inspection_failed"


def test_competition_cli_preflight_and_help_need_no_pipeline(capsys, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid input and help must not construct a pipeline")

    monkeypatch.setattr("sm_trending_products.pipeline.Pipeline.from_env", forbidden)
    assert main(["--help"]) == 0
    assert "Example:" in capsys.readouterr().out
    secret = "do-not-display-this-secret"
    assert main(["--product-id", secret, "--max-rows", secret]) == 2
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert json.loads(captured.err)["error"] == "invalid_competition_arguments"
    for flags in (
        ["--max-age-hours", "nan"],
        ["--max-rows", "100001"],
        ["--min-link-confidence", "inf"],
        ["--window-days", "0"],
        ["--country", "fr"],
        ["--as-of", secret],
    ):
        assert main(["--product-id", "competitive", *flags]) == 2
        captured = capsys.readouterr()
        assert secret not in captured.out + captured.err
        assert json.loads(captured.err)["error"] == "invalid_competition_input"


def test_competition_dashboard_is_explicit_and_refreshes_source_withdrawal(pipeline):
    _, cutoff, _ = seed(pipeline)
    app = _app(pipeline, cutoff)
    assert not app.exception and not app.dataframe and not app.json
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception and app.json
    scopes = app.dataframe[0].value
    assert set(scopes["Observed country"]) == {"FR"}
    assert set(scopes["Source account"]) == {"Unknown"}
    assert scopes["Compatible price intervals"].sum() == 3
    app.run()
    assert not app.json and not app.dataframe  # No retained source-derived report on reruns.
    pipeline.catalog.execute(
        "INSERT INTO source_capabilities(source,account_id,capability,state,available_at,details) "
        "VALUES('competition_fixture','','collect','denied',%s,'{}')",
        (cutoff,),
    )
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception and app.warning
    refreshed = json.loads(app.json[0].value)
    assert refreshed["eligible_observation_count"] == 0 and refreshed["scopes"] == []
    assert "offer-a" not in app.json[0].value


def test_competition_dashboard_preserves_missing_scope_and_incompatible_prices(pipeline):
    base = now()
    _, cutoff, _ = seed(
        pipeline,
        [
            observation("unknown", base, query=None, coverage=None, distribution=None),
            observation(
                "unknown",
                base + timedelta(hours=1),
                query=None,
                coverage=None,
                distribution=None,
                price="10",
            ),
        ],
    )
    app = _app(pipeline, cutoff)
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception and app.warning
    scopes = app.dataframe[0].value
    assert set(scopes["Distribution"]) == {"Unknown"}
    assert set(scopes["Query scope"]) == {"Unknown"}
    assert scopes["Compatible price intervals"].sum() == 0
    assert json.loads(app.json[0].value)["status"] == "not_comparable"
    assert any("No compatible retail-price intervals" in row.value for row in app.info)
    _field(app, "text_input", "Observed-country filter (blank = all, for example FR)").set_value(
        "DE"
    ).run()
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception
    assert any("Missing evidence is not zero competition" in row.value for row in app.info)


def test_competition_dashboard_sanitizes_every_scope_timeline_and_price_frame(
    pipeline, monkeypatch
):
    secret = "FAKE-OPERATOR-SCOPE-SECRET"
    url = "https://reader:pass@example.test/x?access_token=shape-value&signature=signed-value"
    base = now()
    insights, cutoff, _ = seed(
        pipeline,
        [
            observation(
                "offer-" + secret,
                base + timedelta(hours=hour),
                query=secret,
                coverage=url,
                seller="seller-" + secret,
                price=price,
            )
            for hour, price in ((0, "20"), (1, "18"))
        ],
    )
    original = insights.describe("competitive", cutoff, True)
    assert all(value in json.dumps(original) for value in (secret, "reader:pass", "signed-value"))
    monkeypatch.setenv("REVIEW_API_SECRET", secret)
    app = _app(pipeline, cutoff)
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception and len(app.dataframe) == 4
    visible = "".join(str(frame.value.to_dict()) for frame in app.dataframe) + app.json[0].value
    for value in (secret, "reader:pass", "shape-value", "signed-value"):
        assert value not in visible
    assert app.dataframe[0].value["Compatible price intervals"].sum() == 1
    assert app.dataframe[2].value["Observed decreases"].sum() == 1
    assert insights.describe("competitive", cutoff, True) == original


def test_competition_dashboard_labels_each_price_interval_with_its_own_context(pipeline):
    base = now()
    _, cutoff, _ = seed(
        pipeline,
        [
            observation(
                "quote",
                base + timedelta(hours=index),
                price=price,
                currency=currency,
                variant_id=variant,
                tax_basis=tax,
                shipping_basis=shipping,
            )
            for index, (price, currency, variant, tax, shipping) in enumerate(
                (
                    ("20", "EUR", "blue", "tax_included", "shipping_excluded"),
                    ("18", "EUR", "blue", "tax_included", "shipping_excluded"),
                    ("30", "USD", "red", "tax_excluded", "shipping_included"),
                    ("25", "USD", "red", "tax_excluded", "shipping_included"),
                    ("20", "EUR", "blue", "tax_included", "shipping_excluded"),
                    ("19", "EUR", "blue", "tax_included", "shipping_excluded"),
                )
            )
        ],
    )
    app = _app(pipeline, cutoff)
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception
    intervals = app.dataframe[3].value
    assert list(intervals["Currency"]) == ["EUR", "USD", "EUR"]
    assert list(intervals["Variant"]) == ["blue", "red", "blue"]
    assert list(intervals["Tax basis"]) == ["tax_included", "tax_excluded", "tax_included"]
    assert list(intervals["Shipping basis"]) == [
        "shipping_excluded",
        "shipping_included",
        "shipping_excluded",
    ]
    assert list(intervals["Measured change (lower)"]) == ["-2", "-5", "-1"]
    assert list(intervals["Measured interval (hours)"]) == [1, 1, 1]


def test_competition_dashboard_bounds_the_interval_preview_across_multiple_offers(pipeline):
    base = now()
    _, cutoff, _ = seed(
        pipeline,
        [
            observation(entity, base + timedelta(minutes=index), price=str(1000 - index))
            for entity in ("first", "second")
            for index in range(103)
        ],
    )
    app = _app(pipeline, cutoff)
    _field(app, "button", "Inspect observed competition").click().run()
    assert not app.exception
    assert len(app.dataframe[3].value) == 200
    report = json.loads(app.json[0].value)
    assert report["scopes"][0]["price_interval_count"] == 204
    assert all(
        not offer["intervals_preview_truncated"]
        for offer in report["scopes"][0]["price_offers_preview"]
    )
    assert any("Price-interval table is a bounded preview" in row.value for row in app.warning)
