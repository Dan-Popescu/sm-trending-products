"""Real window-scoped gaps and selected-cutoff explanations across public adapters."""

import json
from datetime import timedelta

import pytest
from streamlit.testing.v1 import AppTest
from test_evaluation_integration import record

from sm_trending_products.contracts import now
from sm_trending_products.decision_brief import DecisionBrief
from sm_trending_products.decision_changes import DecisionChanges, explain_changes
from sm_trending_products.decision_changes_cli import main

pytestmark = pytest.mark.integration


def compare(pipeline, previous, current):
    return DecisionChanges(pipeline.catalog, pipeline.history, pipeline.provenance).compare(
        "p1", "FR", previous, current, True
    )


def assert_adapters(pipeline, monkeypatch, capsys, previous, current, expected):
    monkeypatch.setenv("S3_BUCKET", pipeline.settings.s3_bucket)
    monkeypatch.setenv("S3_ENDPOINT_URL", pipeline.settings.s3_endpoint_url)
    args = [
        "--product-id",
        "p1",
        "--target-country",
        "FR",
        "--synthetic",
        "--previous-as-of",
        previous.isoformat(),
        "--as-of",
        current.isoformat(),
    ]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out) == expected
    assert main([*args, "--format", "text"]) == 0
    text = capsys.readouterr().out
    assert text == "\n".join(explain_changes(expected)) + "\n"
    source = """
from sm_trending_products.pipeline import Pipeline
from sm_trending_products.contracts import utc
from sm_trending_products.decision_changes_dashboard import render_changes
with Pipeline.from_env() as application:
    render_changes(application, [{"product_id":"p1"}], utc(CUTOFF), True)
""".replace("CUTOFF", repr(current.isoformat()))
    app = AppTest.from_string(source).run(timeout=30)
    next(widget for widget in app.text_input if widget.key == "changes_previous").set_value(
        previous.isoformat()
    ).run()
    next(widget for widget in app.text_input if widget.key == "changes_country").set_value(
        "FR"
    ).run()
    next(widget for widget in app.button if widget.key == "changes_build").click().run(timeout=30)
    assert not app.exception and not app.error
    assert json.loads(app.json[-1].value) == expected
    shown = [block.value for block in app.markdown]
    assert all(line in shown for line in explain_changes(expected))
    return text


def test_resolved_24_hour_gap_stays_distinct_from_unresolved_longer_windows(
    pipeline, monkeypatch, capsys
):
    pipeline.create_product("p1", "Windowed product", synthetic=True)
    previous = now() + timedelta(days=2)
    current = previous + timedelta(hours=24)
    rows = [
        record("windows", previous + timedelta(hours=hour), 100 + hour)
        for hour in (0, 6, 12, 18, 24)
    ]
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("p1", "fixture_import:video:windows")
    briefs = DecisionBrief(pipeline.catalog, pipeline.history, pipeline.provenance)
    before = briefs.describe("p1", "FR", previous, True)
    after = briefs.describe("p1", "FR", current, True)

    def incomplete(report):
        return [
            row["window_hours"]
            for row in report["missing"]
            if row["code"] == "incomplete_observed_window"
        ]

    assert incomplete(before) == [24, 72, 168]
    assert incomplete(after) == [72, 168]
    report = compare(pipeline, previous, current)
    assert report["evidence_changes"]["missing"] == {
        "added": [],
        "removed": [
            {"dimension": "trends", "code": "incomplete_observed_window", "window_hours": 24}
        ],
        "added_count": 0,
        "removed_count": 1,
        "preview_truncated": False,
    }
    text = assert_adapters(pipeline, monkeypatch, capsys, previous, current, report)
    assert "1 point(s) qui ne sont plus signalé(s)" in text


@pytest.mark.parametrize("hours", [72, 168])
@pytest.mark.parametrize(
    "problem,expected_reason,explanation",
    [
        ("context", "source_context_unknown_or_incomplete", "Le contexte de diffusion est inconnu"),
        ("correction", "counter_corrections_limit_comparison", "Des corrections ou remises à zéro"),
        ("gap", "insufficient_comparable_panel", "Les mêmes vidéos n’ont pas assez d’intervalles"),
    ],
)
def test_unavailable_reason_uses_the_window_matching_the_selected_cutoffs(
    pipeline, monkeypatch, capsys, hours, problem, expected_reason, explanation
):
    pipeline.create_product("p1", "Windowed product", synthetic=True)
    current = now() + timedelta(days=21)
    previous = current - timedelta(hours=hours)
    rows = []
    for elapsed in range(0, 2 * hours + 1, 48 if problem == "gap" else 6):
        count = 1 if problem == "correction" and elapsed == 2 * hours - 6 else 100 + elapsed
        row = record("windows", current - timedelta(hours=2 * hours - elapsed), count)
        row["attributes"]["distribution"] = "unknown" if problem == "context" else "public_video"
        rows.append(row)
    pipeline.import_records(rows, synthetic=True)
    pipeline.review_link("p1", "fixture_import:video:windows")
    report = compare(pipeline, previous, current)
    windows = report["trajectory"]["windows"]
    assert windows[0]["reason"] == "requested_cutoffs_do_not_match_adjacent_kernel_windows"
    matching = next(row for row in windows if row["window_hours"] == hours)
    assert matching["comparison_status"] == "unavailable" and matching["reason"] == expected_reason
    assert report["evidence_changes"]["support"]["added_count"] == 0
    assert report["evidence_changes"]["support"]["removed_count"] == 0
    text = assert_adapters(pipeline, monkeypatch, capsys, previous, current, report)
    assert explanation in text
    assert "Ces dates ne délimitent pas" not in text
    assert "multiplié par" not in text
