"""Actual Streamlit traverses bounded published pages without analytical work."""

import pytest
from streamlit.testing.v1 import AppTest

APP = """
import streamlit as st
from sm_trending_products.dashboard import _analytical_details
def descriptor(format,count):
    return {"format":format,"root":{"key":"controlled","sha256":"0"*64,"bytes":1},"count":count}
class Published:
    def analytical_page(self,kind,identifier,synthetic=False,*,path,limit=100,offset=0):
        st.session_state.setdefault("calls",[]).append((kind,identifier,synthetic,list(path),limit,offset))
        assert limit == 100
        if path == ("snapshots",):
            st.session_state["selected_parent"] = offset
            return {"count":101,"next_offset":100 if offset==0 else None,"records":[
                {"id":f"decision-{offset}","value":{
                    "evidence_refs":descriptor("sharded-provenance-v1",201),
                    "authorization":"Bearer hidden"}}
            ]}
        parent = st.session_state['selected_parent']
        assert path == ("snapshots",f"decision-{parent}","evidence_refs")
        return {"count":201,"references":[f"obs-{offset}"],"next_offset":offset+100}
_analytical_details(Published(),"evaluation","evaluation-1",True,{"snapshots":descriptor("sharded-records-v1",101)})
"""


def test_selected_record_and_references_use_separate_bounded_pages():
    app = AppTest.from_string(APP).run(timeout=20)
    assert not app.exception
    assert list(app.session_state["calls"]) == [
        ("evaluation", "evaluation-1", True, ["snapshots"], 100, 0),
        ("evaluation", "evaluation-1", True, ["snapshots", "decision-0", "evidence_refs"], 100, 0),
    ]
    assert "hidden" not in " ".join(item.value for item in app.json)
    assert "[redacted]" in " ".join(item.value for item in app.json)
    next(item for item in app.number_input if item.max == 3).set_value(2).run(timeout=20)
    assert not app.exception
    assert app.session_state["calls"][-1] == (
        "evaluation",
        "evaluation-1",
        True,
        ["snapshots", "decision-0", "evidence_refs"],
        100,
        100,
    )
    assert app.dataframe[-1].value.iloc[0]["Observation ID"] == "obs-100"


def test_empty_and_legacy_results_do_not_make_new_detail_reads():
    app = AppTest.from_string("""
from sm_trending_products.dashboard import _analytical_details
class Legacy:
    def analytical_page(self,*args,**kwargs):
        raise AssertionError("Legacy results must not load remote detail")
_analytical_details(Legacy(),"evaluation","old",False,{"evidence_refs":["obs-1"],"universe":[]})
""").run(timeout=20)
    assert not app.exception
    assert not app.selectbox


def test_changing_parent_page_does_not_reuse_the_previous_reference_page():
    app = AppTest.from_string(APP).run(timeout=20)
    next(item for item in app.number_input if item.max == 3).set_value(3).run(timeout=20)
    assert app.session_state["calls"][-1][-1] == 200
    next(item for item in app.number_input if item.max == 2).set_value(2).run(timeout=20)
    assert not app.exception
    assert app.session_state["calls"][-1] == (
        "evaluation",
        "evaluation-1",
        True,
        ["snapshots", "decision-100", "evidence_refs"],
        100,
        0,
    )


def test_failed_detail_read_shows_a_redacted_error():
    app = AppTest.from_string("""
from sm_trending_products.dashboard import _analytical_details
class Broken:
    def analytical_page(self,*args,**kwargs):
        raise RuntimeError("postgresql://secret-password")
_analytical_details(Broken(),"evaluation","new",False,{"evidence_refs":{"format":"sharded-provenance-v1","count":2}})
""").run(timeout=20)
    assert not app.exception
    assert len(app.error) == 1
    assert "secret-password" not in app.error[0].value


def test_new_published_root_resets_the_previously_selected_detail_page():
    app = AppTest.from_string("""
import streamlit as st
from sm_trending_products.dashboard import _analytical_details
new = st.checkbox("New publication")
class Published:
    def analytical_page(self,kind,identifier,synthetic=False,*,path,limit=100,offset=0):
        st.session_state["offset"] = offset
        assert offset == 0 or not new
        return {"count":1 if new else 201,"references":["obs-new" if new else "obs-old"]}
_analytical_details(Published(),"assessment","same-product",False,{
    "evidence_refs":{"format":"sharded-provenance-v1",
    "root":{"key":"new" if new else "old"},"count":1 if new else 201}})
""").run(timeout=20)
    app.number_input[0].set_value(3).run(timeout=20)
    assert app.session_state["offset"] == 200
    app.checkbox[0].check().run(timeout=20)
    assert not app.exception
    assert app.session_state["offset"] == 0
    assert app.dataframe[0].value.iloc[0]["Observation ID"] == "obs-new"


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["assessment", "evaluation"])
def test_dashboard_traverses_real_published_artifacts_without_recomputation(
    pipeline, monkeypatch, kind
):
    from test_sharded_results import seed

    cutoff, _, _ = seed(pipeline, videos=1, points=120)
    if kind == "assessment":
        row = pipeline.assess(cutoff, True)["rows"][0]
        identifier = "p0"
        value = {"evidence_refs": row["evidence_refs"]}
        reference_path = ("evidence_refs",)
    else:
        frozen = pipeline.freeze(cutoff, True)
        tested = pipeline.backtest([cutoff], cutoff, True)
        identifier = tested["evaluation_id"]
        value = {"snapshots": tested["result"]["snapshots"]}
        reference_path = (
            "snapshots",
            frozen["result"]["snapshot_id"],
            "universe",
            "p0",
            "evidence_refs",
        )
    expected = pipeline.analytical_page(
        kind, identifier, True, path=reference_path, limit=100, offset=100
    )["references"]
    calls = []
    original = pipeline.analytical_page

    def page(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)

    def unexpected(*args, **kwargs):
        raise AssertionError("Published review must not recompute analytical history")

    monkeypatch.setattr(pipeline, "analytical_page", page)
    monkeypatch.setattr(pipeline, "assess", unexpected)
    monkeypatch.setattr(pipeline.history, "observations", unexpected)
    app = AppTest.from_string("""
import streamlit as st
from sm_trending_products.dashboard import _analytical_details
_analytical_details(st.session_state.pipeline,st.session_state.kind,
                   st.session_state.identifier,True,st.session_state.publication)
""")
    for key, item in {
        "pipeline": pipeline,
        "kind": kind,
        "identifier": identifier,
        "publication": value,
    }.items():
        app.session_state[key] = item
    before = dict(pipeline.history.metrics)
    app.run(timeout=20)
    assert not app.exception
    if kind == "evaluation":
        candidates = next(item for item in app.selectbox if "Candidate products" in item.options)
        candidates.select_index(candidates.options.index("Candidate products")).run(timeout=20)
        references = next(
            item for item in reversed(list(app.selectbox)) if "Evidence references" in item.options
        )
        references.select_index(references.options.index("Evidence references")).run(timeout=20)
        assert not app.exception
    next(item for item in app.number_input if item.max == 2).set_value(2).run(timeout=20)
    assert not app.exception
    assert app.dataframe[-1].value["Observation ID"].tolist() == expected
    assert all(call["limit"] == 100 for call in calls)
    assert calls[-1]["path"] == reference_path and calls[-1]["offset"] == 100
    assert pipeline.history.metrics["put_requests"] == before["put_requests"]
