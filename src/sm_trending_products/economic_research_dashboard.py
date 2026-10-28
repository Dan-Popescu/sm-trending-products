"""On-demand private economic scenarios; no source report is cached in session state."""

from __future__ import annotations

from decimal import Decimal

import streamlit as st

from .collector import redact
from .commerce import LaunchError, LaunchService
from .commercial_sensitivity import EconomicInsights
from .economic_research_cli import PROFILE_MAX_BYTES, ProfileInputError, profile_from_bytes


def render_economics(pipeline, products: list[dict], synthetic: bool) -> None:
    st.subheader("Economic breakpoints and cash exposure")
    st.caption(
        "Declared operator scenarios, not measured profit or a sales forecast. "
        "Missing costs and timing stay unknown. No launch or payment action is performed."
    )
    if not products:
        st.info("Create a product and save a launch dossier first.")
        return
    product = st.selectbox(
        "Product for economic scenarios",
        [row["product_id"] for row in products[:1000]],
        key=f"economic-product:{synthetic}",
    )
    if len(products) > 1000:
        st.caption("The product selector shows the first 1000 supplied products.")
    page = int(
        st.number_input(
            "Economic dossier page",
            min_value=1,
            max_value=10001,
            value=1,
            step=1,
            key=f"economic-page:{synthetic}",
        )
    )
    launches = LaunchService(pipeline.catalog)
    try:
        page_rows = launches.dossiers(synthetic=synthetic, limit=100, offset=(page - 1) * 100)
    except Exception:
        st.error("Launch dossiers are unavailable. Run sm-trends commerce init explicitly.")
        return
    dossiers = [row for row in page_rows if row["product_id"] == product]
    st.caption("Each page contains at most 100 workspace dossiers, filtered to this product.")
    if not dossiers:
        st.info("No dossier for this product on this page. Change the page or save a dossier.")
        return
    identifier = st.selectbox(
        "Economic launch dossier",
        [row["dossier_id"] for row in dossiers],
        key=f"economic-dossier:{synthetic}:{product}",
    )
    dossier = next(row for row in dossiers if row["dossier_id"] == identifier)
    st.write(
        f"Current revision {dossier['revision']} · {dossier['country']} · {dossier['currency']}"
    )
    st.write(f"Quote and launch readiness: {dossier['readiness']['status']}")
    for reason in dossier["readiness"]["reasons"]:
        st.warning(reason["message"])
    unknown = dossier["economics"]["unknown_reasons"]
    if unknown:
        st.warning("Unknown economic facts: " + ", ".join(unknown))
    expected = int(
        st.number_input(
            "Expected dossier revision",
            min_value=1,
            max_value=2_147_483_647,
            value=dossier["revision"],
            step=1,
            key=f"economic-revision:{synthetic}:{identifier}",
        )
    )
    if expected != dossier["revision"]:
        st.warning("The dossier changed. Select its current revision before inspecting again.")
    upload = st.file_uploader(
        "Economic scenario profile (JSON, at most 128 KiB)",
        type=["json"],
        key=f"economic-profile:{synthetic}:{identifier}",
    )
    chart_point = int(
        st.number_input(
            "Cash chart point",
            min_value=1,
            max_value=128,
            value=1,
            step=1,
            key=f"economic-chart:{synthetic}:{identifier}",
        )
    )
    if not st.button("Inspect economics", key=f"economic-inspect:{synthetic}:{identifier}"):
        return
    if upload is None:
        st.error("Choose a scenario profile before inspecting economics.")
        return
    try:
        if getattr(upload, "size", 0) > PROFILE_MAX_BYTES:
            raise ProfileInputError("economic_profile_too_large")
        upload.seek(0)
        profile = profile_from_bytes(upload.read(PROFILE_MAX_BYTES + 1))
        report = EconomicInsights(launches).describe(
            identifier,
            expected,
            profile,
            synthetic=synthetic,
        )
    except LaunchError as error:
        if error.code == "revision_conflict":
            st.error("The dossier changed. Select its current revision and inspect again.")
        else:
            st.error("Economic insights are unavailable for this dossier and workspace.")
        return
    except ProfileInputError:
        st.error(
            "Invalid scenario profile. Use a JSON object of at most 128 KiB with exact amounts."
        )
        return
    except Exception:
        st.error(
            "Economic insights are unavailable. Check the dossier and declared scenario facts."
        )
        return
    _report(report, chart_point)


def _report(report: dict, chart_point: int) -> None:
    st.write(f"Scenario result for revision {report['revision']} · {report['currency']}")
    st.caption(f"Evaluated {report['evaluated_at']} · Profile {report['profile_hash']}")
    st.dataframe(
        redact(
            [
                {
                    "Scenario": row["scenario_id"],
                    "Status": row["status"],
                    "Post-ad contribution": row["economics"]["post_ad_contribution"],
                    "Surplus above profit target": row["economics"]["target_surplus"],
                    "Maximum ad cost": row["breakpoints"]["maximum_ad_cost"],
                    "Maximum goods cost": row["breakpoints"]["maximum_goods_cost"],
                    "Return tolerance": row["breakpoints"]["return_tolerance"].get("maximum_rate"),
                    "Cash status": row["cash"]["status"],
                    "Additional cash required": row["cash"].get("additional_cash_required"),
                    "Unknown facts": ", ".join(row["unknown_reasons"]),
                }
                for row in report["scenarios"]
            ]
        ),
        hide_index=True,
    )
    if chart_point > len(report["scenarios"]):
        st.info(
            "The selected cash chart point is outside this profile. "
            "Choose a point and inspect again."
        )
        return
    point = report["scenarios"][chart_point - 1]
    cash = point["cash"]
    st.write(f"Cash exposure for {point['scenario_id']}")
    if cash["status"] != "complete":
        st.warning("Cash exposure is unknown: " + ", ".join(cash["unknown_reasons"]))
    else:
        st.write(
            f"Additional cash required: {cash['additional_cash_required']} {report['currency']} · "
            f"Closing cash: {cash['closing_cash']} {report['currency']}"
        )
        if cash["daily_flows"]:
            st.line_chart(
                [
                    {"Day": row["day"], "Closing cash": float(Decimal(row["closing_cash"]))}
                    for row in cash["daily_flows"]
                ],
                x="Day",
                y="Closing cash",
            )
            st.dataframe(cash["daily_flows"], hide_index=True)
        st.write({"Beyond horizon": cash["beyond_horizon"]})
        st.caption(
            "Daily net cash after output-tax reserve; intraday payment ordering remains unknown."
        )
    with st.expander("Declared assumptions and limits"):
        st.json(redact({"profile": report["profile"], "limitations": report["limitations"]}))
