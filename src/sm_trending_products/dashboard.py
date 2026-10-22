"""Private operator review over published results and bounded evidence queries.

The dashboard never collects from TikTok or runs analytical scans. Deployment binds
it to loopback; reach it through an authenticated SSH tunnel or a private proxy.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import streamlit as st

from sm_trending_products.contracts import utc

Row = dict[str, Any]
DIMENSIONS = ("attention", "demand", "competition", "feasibility")
SECRET_KEY = re.compile(r"token|password|secret|authorization|credential|database_url|dsn", re.I)
TOKEN_VALUE = re.compile(r"\b(?:act|clt)\.[A-Za-z0-9._~-]+|Bearer\s+\S+", re.I)
MAX_REVIEW_ROWS = 200
EVIDENCE_PAGE_SIZE = 100


class DashboardPipeline(Protocol):
    """Application boundary consumed by the private review surface."""

    def status(self) -> Row: ...

    def products(self, synthetic: bool = False) -> list[Row]: ...

    def links(self, as_of: datetime, synthetic: bool = False) -> list[Row]: ...

    def latest_assessments(self, synthetic: bool = False) -> list[Row]: ...

    def alerts(self, synthetic: bool = False) -> list[Row]: ...

    def datasets(self, limit: int = 100) -> list[Row]: ...

    def capabilities(self) -> list[Row]: ...

    def evaluations(self, synthetic: bool = False) -> list[Row]: ...

    def create_product(
        self, product_id: str, name: str, synthetic: bool = False, market: str | None = None
    ) -> Row: ...

    def review_link(
        self,
        product_id: str,
        entity_key: str,
        status: str = "confirmed",
        confidence: float = 1.0,
        method: str = "manual",
        evidence_refs: list[str] | None = None,
    ) -> Row: ...

    def set_family(self, product_id: str, family_id: str) -> Row: ...

    def suggest_links(self, query: str, synthetic: bool = False, limit: int = 20) -> list[Row]: ...

    def evidence(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Row]: ...


def display_safe(value: Any) -> Any:
    """Remove secret-bearing metadata before rendering a public facade result."""
    if isinstance(value, Mapping):
        return {
            str(key): "[redacted]" if SECRET_KEY.search(str(key)) else display_safe(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [display_safe(item) for item in value]
    if isinstance(value, str):
        if value.startswith(("postgres://", "postgresql://")):
            return "[connection details redacted]"
        result = TOKEN_VALUE.sub("[redacted]", value)
        if result.startswith(("https://", "http://")):
            try:
                parsed = urlsplit(result)
                netloc = parsed.netloc.rsplit("@", 1)[-1]
                query = urlencode(
                    [
                        (
                            key,
                            "[redacted]"
                            if SECRET_KEY.search(key) or "signature" in key.lower()
                            else item,
                        )
                        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
                    ]
                )
                return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))
            except ValueError:
                return "[invalid URL]"
        return result
    return value


def _read[T](label: str, operation: Callable[[], T], fallback: T) -> T:
    try:
        return operation()
    except Exception:  # noqa: BLE001 - UI boundary must not expose connection or source secrets.
        st.error(f"Could not load {label}. Check the private operational logs for details.")
        return fallback


def _mutate(operation: Callable[[], Any], success: str) -> None:
    try:
        operation()
    except Exception:  # noqa: BLE001 - Failed operator actions show a redacted error.
        st.error("The change was not saved. Check the inputs and private operational logs.")
    else:
        st.success(success)


def _dimension_label(row: Row, dimension: str) -> str:
    evidence = row.get(dimension)
    if not isinstance(evidence, Mapping):
        return "Unknown"
    if evidence.get("score") is None:
        status = str(evidence.get("status", "unknown"))
        return "Unknown" if status == "unknown" else f"Unscored ({status})"
    score = evidence["score"]
    status = str(evidence.get("status", "unclassified"))
    return f"{score:.1f} ({status})" if isinstance(score, (int, float)) else str(score)


def opportunity_rows(assessments: Sequence[Row]) -> list[Row]:
    """Render four separate assessments without replacing missing scores by zero."""
    return [
        {
            "Product": row.get("name", row.get("product_id", "Unknown")),
            "Product ID": row.get("product_id"),
            "Market": row.get("market") or "Unknown",
            "Rank score": row.get("score"),
            **{dimension.title(): _dimension_label(row, dimension) for dimension in DIMENSIONS},
            "Evidence cutoff": row.get("as_of"),
            "Policy": row.get("policy_version"),
        }
        for row in assessments[:MAX_REVIEW_ROWS]
    ]


def _records(label: str, records: Sequence[Row]) -> None:
    if records:
        st.dataframe(
            display_safe(list(records)[:MAX_REVIEW_ROWS]), hide_index=True, width="stretch"
        )
        if len(records) > MAX_REVIEW_ROWS:
            st.caption(f"Showing the first {MAX_REVIEW_ROWS} {label}; narrow the review scope.")
    else:
        st.info(f"No {label} recorded in this workspace.")


def _opportunities(
    pipeline: DashboardPipeline, products: list[Row], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Opportunity review")
    st.warning("Attention does not establish purchasing demand or profitability.")
    st.caption(
        "These are published results. Refreshing this page does not query TikTok "
        "or recompute history."
    )
    assessments: list[Row] = _read(
        "published assessments", lambda: pipeline.latest_assessments(synthetic), []
    )
    if assessments:
        st.dataframe(opportunity_rows(assessments), hide_index=True, width="stretch")
        assessment_ids = [str(row["product_id"]) for row in assessments if row.get("product_id")]
        selected = st.selectbox("Inspect assessment", assessment_ids, key="assessment_product")
        selected_row = next(row for row in assessments if row.get("product_id") == selected)
        st.json(display_safe(selected_row), expanded=False)
    else:
        st.info(
            "No published assessments. Run the analytical batch after collecting "
            "and reviewing evidence."
        )

    st.subheader("Evidence at the selected cutoff")
    st.caption(
        "Only evidence and product links available by the cutoff are eligible. "
        "Each page is bounded."
    )
    if products:
        product_ids = [str(row["product_id"]) for row in products if row.get("product_id")]
        selected_product = st.selectbox("Product", product_ids, key="evidence_product")
        page = st.number_input("Evidence page", min_value=1, value=1, step=1, key="evidence_page")
        evidence: list[Row] = _read(
            "evidence",
            lambda: pipeline.evidence(
                selected_product,
                as_of,
                synthetic=synthetic,
                limit=EVIDENCE_PAGE_SIZE,
                offset=(int(page) - 1) * EVIDENCE_PAGE_SIZE,
            ),
            [],
        )
        _records("observations", evidence)
        if evidence:
            with st.expander("Inspect observation provenance, precision, and missing fields"):
                st.json(display_safe(evidence), expanded=False)
    else:
        st.info("Create a product and review evidence links to inspect its history.")

    st.subheader("Frozen alerts")
    alerts: list[Row] = _read("alerts", lambda: pipeline.alerts(synthetic), [])
    _records("alerts", alerts)
    if alerts:
        with st.expander("Alert snapshots and original reasons"):
            st.json(display_safe(alerts[:MAX_REVIEW_ROWS]), expanded=False)


def _catalog(
    pipeline: DashboardPipeline, products: list[Row], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Reviewed product catalog")
    _records("products", products)
    with st.form("create_product"):
        st.markdown("**Create a canonical product**")
        product_id = st.text_input(
            "Product ID", help="Stable internal identifier, distinct from a seller SKU."
        )
        name = st.text_input("Product name")
        market = st.text_input("Target market", help="Optional market code; unknown is left blank.")
        if st.form_submit_button("Create product"):
            if not product_id.strip() or not name.strip():
                st.error("Product ID and name are required.")
            else:
                _mutate(
                    lambda: pipeline.create_product(
                        product_id.strip(), name.strip(), synthetic, market.strip() or None
                    ),
                    "Product created. Refresh the workspace to review its evidence.",
                )

    if not products:
        return
    product_ids = [str(row["product_id"]) for row in products if row.get("product_id")]
    with st.form("review_link"):
        st.markdown("**Review or transfer an evidence link**")
        target = st.selectbox("Target product", product_ids, key="link_product")
        entity_key = st.text_input(
            "Entity key", help="Source-qualified identifier, for example tiktok_display:video:123."
        )
        decision = st.selectbox("Decision", ["confirmed", "proposed", "rejected"])
        confidence = st.slider("Matching confidence", 0.0, 1.0, 1.0, 0.05)
        refs = st.text_area("Supporting observation IDs", help="One observation ID per line.")
        st.caption(
            "New decisions take effect now and preserve earlier history. "
            "Confirming a transfer must not merge distinct products."
        )
        if st.form_submit_button("Save link decision"):
            if not entity_key.strip():
                st.error("An entity key is required.")
            else:
                _mutate(
                    lambda: pipeline.review_link(
                        target,
                        entity_key.strip(),
                        status=decision,
                        confidence=confidence,
                        method="manual",
                        evidence_refs=[line.strip() for line in refs.splitlines() if line.strip()],
                    ),
                    "Link decision saved with its current availability time.",
                )

    with st.form("set_family"):
        st.markdown("**Set product-family membership**")
        family_product = st.selectbox("Product for family review", product_ids)
        family_id = st.text_input(
            "Family ID", help="A family groups related items; it does not assert exact identity."
        )
        if st.form_submit_button("Save family membership"):
            if not family_id.strip():
                st.error("A family ID is required.")
            else:
                _mutate(
                    lambda: pipeline.set_family(family_product, family_id.strip()),
                    "Family membership saved. Earlier membership history is preserved.",
                )

    st.subheader("Product links at cutoff")
    links: list[Row] = _read("product links", lambda: pipeline.links(as_of, synthetic), [])
    _records("product links", links)
    with st.form("suggest_links"):
        query = st.text_input("Find text-based link candidates")
        submitted = st.form_submit_button("Find candidates")
    if submitted:
        if query.strip():
            suggestions: list[Row] = _read(
                "link candidates",
                lambda: pipeline.suggest_links(query.strip(), synthetic, limit=20),
                [],
            )
            st.caption(
                "Text similarity proposes candidates only. No links are automatically confirmed."
            )
            _records("link candidates", suggestions)
        else:
            st.info("Enter product terms to search the recorded candidate catalog.")


def _history(pipeline: DashboardPipeline, synthetic: bool) -> None:
    st.subheader("Dataset publication and replay")
    st.caption(
        "Operational metadata can cover both workspaces; each dataset retains "
        "its synthetic classification."
    )
    datasets: list[Row] = _read("dataset metadata", lambda: pipeline.datasets(limit=100), [])
    _records("datasets", datasets)
    st.caption(
        "Only completed, valid manifests are eligible for analysis. "
        "Run replay and recovery through the operational CLI."
    )

    st.subheader("Evaluation snapshots")
    st.caption(
        "Backtests replay past knowledge. Forward tests freeze decisions before "
        "outcomes mature. Neither establishes business profit without actual costs and outcomes."
    )
    evaluations: list[Row] = _read("evaluations", lambda: pipeline.evaluations(synthetic), [])
    _records("evaluations", evaluations)
    if evaluations:
        selected_index = st.selectbox(
            "Inspect evaluation",
            list(range(min(len(evaluations), MAX_REVIEW_ROWS))),
            format_func=lambda index: str(
                evaluations[index].get("evaluation_id", evaluations[index].get("id", index))
            ),
        )
        if selected_index is not None:
            st.json(display_safe(evaluations[selected_index]), expanded=False)


def _health(status: Row, capabilities: list[Row], synthetic: bool = False) -> None:
    st.subheader("Source capabilities and measured coverage")
    if not any(row.get("state") == "available" for row in capabilities):
        st.warning(
            "No verified live source capability is available. "
            "Fixture success does not grant API access."
        )
    _records("capabilities", capabilities)
    st.caption(
        "Display access is scoped to authorized accounts. Discovery, sales, retention, "
        "and comment access must each be verified; a key alone does not establish them."
    )
    st.subheader("Operational status")
    queues = [row for row in status.get("queues", []) if row.get("synthetic") is synthetic]
    sources = [row for row in status.get("sources", []) if row.get("synthetic") is synthetic]
    queued = sum(row["count"] for row in queues if row["state"] in {"pending", "leased"})
    quarantined = sum(row["count"] for row in queues if row["state"] == "quarantined")
    observed = [utc(row["last_observed_at"]) for row in sources if row.get("last_observed_at")]
    columns = st.columns(3)
    columns[0].metric("Queued tasks", queued if "queues" in status else "Unknown")
    columns[1].metric("Quarantined tasks", quarantined if "queues" in status else "Unknown")
    columns[2].metric(
        "Latest observation", max(observed).isoformat() if observed else "No observation"
    )
    st.caption("Health cards describe the selected live or synthetic workspace.")
    st.json(display_safe(status), expanded=False)
    st.caption(
        "Healthy processes do not establish fresh evidence, complete market coverage, "
        "or usable source permissions."
    )


def render_dashboard(pipeline: DashboardPipeline) -> None:
    """Render a review workspace using only the public application boundary."""
    st.set_page_config(page_title="Product trend intelligence", page_icon="📈", layout="wide")
    st.title("Product trend intelligence")
    with st.sidebar:
        st.header("Review workspace")
        synthetic = st.toggle("Include synthetic workspace", value=False, key="synthetic_workspace")
        st.caption(
            "The switch selects an isolated workspace; synthetic evidence is never "
            "mixed with live evidence."
        )
        if st.button("Refresh published results"):
            st.rerun()
        cutoff = st.text_input(
            "Evidence and link cutoff (UTC)",
            value=datetime.now(UTC).isoformat(timespec="seconds"),
            key="review_cutoff",
        )
        st.caption(
            "Private operator surface. Use the configured private network or authenticated tunnel."
        )
    try:
        as_of = utc(cutoff)
    except (ValueError, TypeError):
        st.error("Enter an ISO timestamp with a timezone, such as 2026-10-22T10:00:00Z.")
        return
    if synthetic:
        st.warning(
            "Synthetic workspace: simulated evidence and evaluations. "
            "These are not live market observations."
        )
    else:
        st.caption("Live workspace: synthetic records are excluded.")
    status: Row = _read("operational status", pipeline.status, {})
    capabilities: list[Row] = _read("source capabilities", pipeline.capabilities, [])
    products: list[Row] = _read("products", lambda: pipeline.products(synthetic), [])
    products = products[:MAX_REVIEW_ROWS]
    opportunities, catalog, history, health = st.tabs(
        ["Opportunities", "Catalog", "Datasets and evaluations", "Health and coverage"]
    )
    with opportunities:
        _opportunities(pipeline, products, as_of, synthetic)
    with catalog:
        _catalog(pipeline, products, as_of, synthetic)
    with history:
        _history(pipeline, synthetic)
    with health:
        _health(status, capabilities, synthetic)


def main() -> None:
    if not os.environ.get("DATABASE_URL"):
        st.set_page_config(page_title="Product trend intelligence", layout="wide")
        st.title("Product trend intelligence")
        st.info(
            "Configure DATABASE_URL and the object-store settings, then initialize "
            "the pipeline with sm-trends init."
        )
        return
    try:
        from sm_trending_products.pipeline import Pipeline

        pipeline = Pipeline.from_env()
    except Exception:  # noqa: BLE001 - Initialization failures may contain a secret DSN.
        st.error(
            "The pipeline is unavailable. Verify private database/object-store "
            "configuration and initialization; connection details are not displayed here."
        )
        return
    try:
        render_dashboard(pipeline)
    finally:
        close = getattr(pipeline, "close", None)
        if callable(close):
            _read("pipeline cleanup", close, None)


if __name__ == "__main__":
    main()
