"""Privacy redaction never changes the meaning of authoritative brief structure."""

import pytest

from sm_trending_products.contracts import canonical, now
from sm_trending_products.decision_brief_presentation import present_brief, render_text


@pytest.mark.parametrize(
    "secret",
    [
        "title",
        "state",
        "context",
        "groups",
        "null",
        "contrary",
        "sections",
        "windows",
        "true",
        "watch",
    ],
)
def test_primary_meaning_uses_original_structure_before_private_value_redaction(secret):
    report = {
        "name": "probe",
        "product_id": "p",
        "status": "watch",
        "synthetic": True,
        "sections": {
            "trends": {
                "report": {
                    "windows": [
                        {
                            "window_hours": 24,
                            "observed_growth_lower": 10,
                            "observed_growth_upper": 20,
                            "unit": "count",
                            "measured_entity_count": 1,
                            "known_entity_count": 1,
                            "comparable_interval_count": 2,
                        }
                    ]
                }
            },
            "economics": {"report": None},
        },
        "contrary": [
            {
                "dimension": "trends",
                "code": "dominant_creator_contribution",
                "window_hours": 24,
                "share_lower": 0.75,
            }
        ],
    }
    original = canonical(report)
    primary = render_text(present_brief(report, secret_values=(secret,))).split(
        "Détails techniques", 1
    )[0]
    assert "À surveiller" in primary and "Données synthétiques : simulation" in primary
    assert "croissance observée de 10 à 20" in primary
    assert "concentre au moins la moitié" in primary
    assert secret not in primary
    assert canonical(report) == original


def private_app(pipeline, monkeypatch, cutoff):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_brief_dashboard import render_brief
with Pipeline.from_env() as pipeline:
    render_brief(pipeline, pipeline.products(True), utc(CUTOFF), True)
""".replace("CUTOFF", repr(cutoff.isoformat()))
    app = AppTest.from_string(source).run(timeout=20)
    app.text_input[0].set_value("FR").run()
    app.button[0].click().run(timeout=30)
    return app


@pytest.mark.parametrize("secret", ["title", "null"])
def test_private_partial_brief_has_no_structural_exception_or_credential_echo(
    pipeline, monkeypatch, secret
):
    pipeline.create_product("p", "Partial product", synthetic=True)
    cutoff = now()
    original = pipeline.decision_brief().describe("p", "FR", cutoff, True)
    assert original["sections"]["economics"]["report"] is None
    monkeypatch.setenv("REVIEW_API_KEY", secret)
    app = private_app(pipeline, monkeypatch, cutoff)
    assert not app.exception and not app.error and app.json
    assert all(secret not in block.value for block in app.json)
    assert pipeline.decision_brief().describe("p", "FR", cutoff, True) == original


def test_private_brief_keeps_actual_contrary_evidence_when_credential_matches_its_key(
    pipeline, monkeypatch
):
    from test_trend_insights import seed

    _, cutoff = seed(
        pipeline,
        entities=("one", "two"),
        gaps=1,
        attributes=[{"creator_id": "a"}, {"creator_id": "b"}],
    )
    original = pipeline.decision_brief().describe("trend", "FR", cutoff, True)
    assert any(note["code"] == "dominant_creator_contribution" for note in original["contrary"])
    monkeypatch.setenv("REVIEW_API_KEY", "contrary")
    app = private_app(pipeline, monkeypatch, cutoff)
    assert not app.exception and not app.error and app.json
    assert any("concentre au moins la moitié" in block.value for block in app.text)
    assert all("contrary" not in block.value for block in app.json)
    assert pipeline.decision_brief().describe("trend", "FR", cutoff, True) == original


def test_private_watch_state_is_not_erased_by_secret_preprocessing(pipeline, monkeypatch, capsys):
    from sm_trending_products.cli import main
    from sm_trending_products.collector import redact

    pipeline.create_product("p", "Partial product", synthetic=True)
    cutoff = now()
    original = pipeline.decision_brief().describe("p", "FR", cutoff, True)
    monkeypatch.setenv("REVIEW_API_SECRET", "watch")
    app = private_app(pipeline, monkeypatch, cutoff)
    assert not app.exception and not app.error and app.json
    assert any("À surveiller" in block.value for block in app.markdown)
    args = [
        "research",
        "brief",
        "--product-id",
        "p",
        "--target-country",
        "FR",
        "--synthetic",
        "--as-of",
        cutoff.isoformat(),
    ]
    assert main(args) == 0
    assert capsys.readouterr().out == canonical(redact(original)) + "\n"
    assert main([*args, "--format", "text"]) == 0
    assert "À surveiller" in capsys.readouterr().out


def test_pure_presentation_preserves_legacy_bare_credential_assignment_redaction():
    report = {"name": "api_key=unconfigured-private-value", "status": "watch"}
    output = render_text(present_brief(report))
    assert "unconfigured-private-value" not in output
