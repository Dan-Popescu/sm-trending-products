"""Private operator review over published results and bounded evidence queries.

The dashboard never collects from TikTok or runs analytical scans. Deployment binds
it to loopback; reach it through an authenticated SSH tunnel or a private proxy.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import streamlit as st

from sm_trending_products.contracts import canonical, utc

Row = dict[str, Any]
DIMENSIONS = ("attention", "demand", "competition", "feasibility")
SECRET_KEY = re.compile(r"token|password|secret|authorization|credential|database_url|dsn", re.I)
TOKEN_VALUE = re.compile(r"\b(?:act|clt)\.[A-Za-z0-9._~-]+|Bearer\s+\S+", re.I)
MAX_REVIEW_ROWS = 200
EVIDENCE_PAGE_SIZE = 100


class DashboardPipeline(Protocol):
    """Application boundary consumed by the private review surface."""

    def status(self) -> Row: ...

    def launch_service(self) -> Any: ...

    def commerce_delivery(self) -> Any: ...

    def products(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[Row]: ...

    def product_links(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list[Row]: ...

    def assessment_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[Row]: ...

    def assessment(self, product_id: str, synthetic: bool = False) -> Row | None: ...

    def alert_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[Row]: ...

    def alert(self, alert_id: str, synthetic: bool = False) -> Row | None: ...

    def datasets(self, limit: int = 100) -> list[Row]: ...

    def capabilities(self) -> list[Row]: ...

    def evaluation_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[Row]: ...

    def evaluation(self, evaluation_id: str, synthetic: bool = False) -> Row | None: ...

    def collection_summaries(
        self, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> list[Row]: ...

    def collection_decision(
        self, task_id: str, synthetic: bool = False, *, limit: int = 200, offset: int = 0
    ) -> Row | None: ...

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

    def evidence_page(
        self,
        product_id: str,
        as_of: datetime,
        synthetic: bool = False,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Row: ...

    def analytical_page(
        self,
        kind: str,
        identifier: str,
        synthetic: bool = False,
        *,
        path: Sequence[str | int],
        limit: int = 100,
        offset: int = 0,
    ) -> Row: ...


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
        st.session_state["_review_notice"] = success
        st.session_state["_review_refresh_cutoff"] = True
        st.rerun()


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


def _analytical_details(
    pipeline: DashboardPipeline,
    kind: str,
    identifier: str,
    synthetic: bool,
    value: Any,
    *,
    prefix: tuple[str | int, ...] = (),
    depth: int = 0,
) -> None:
    """Read one published collection page and one selected record per level."""
    if depth >= 6:
        return
    found: list[tuple[tuple[str | int, ...], Row]] = []
    pending: list[tuple[tuple[str | int, ...], Any]] = [((), value)]
    visited = 0
    while pending and visited < 1000:
        path, item = pending.pop()
        visited += 1
        if isinstance(item, dict):
            if item.get("format") in {"sharded-provenance-v1", "sharded-records-v1"}:
                found.append((path, item))
            elif len(path) < 8:
                pending.extend(
                    (path + (key,), child)
                    for key, child in reversed(list(item.items()))
                    if isinstance(key, str) and not SECRET_KEY.search(key)
                )
        elif isinstance(item, list) and len(path) < 8:
            pending.extend((path + (index,), child) for index, child in enumerate(item[:20]))
    if not found:
        return
    scope = hashlib.sha256(
        canonical(
            [
                kind,
                identifier,
                synthetic,
                prefix,
                [(path, descriptor.get("root")) for path, descriptor in found],
            ]
        ).encode()
    ).hexdigest()[:20]
    labels = {
        "snapshots": "Frozen decisions",
        "reports": "Outcome reports",
        "universe": "Candidate products",
        "evidence_refs": "Evidence references",
    }

    def label(index: int) -> str:
        return (
            " / ".join(
                labels.get(str(part), str(part).replace("_", " ").title())
                for part in found[index][0]
            )
            or "Published records"
        )

    selected = st.selectbox(
        "Inspect published details",
        list(range(len(found))),
        format_func=label,
        key=f"analytical_collection_{scope}",
    )
    if selected is None:
        return
    path, descriptor = found[selected]
    full_path = prefix + path
    count = descriptor.get("count", 0)
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        st.error("This published collection has invalid metadata.")
        return
    st.caption(f"{count:,} published items. Details load one page at a time.")
    page_scope = scope + "_" + str(selected)
    page = st.number_input(
        "Detail page",
        min_value=1,
        max_value=max(1, (count + EVIDENCE_PAGE_SIZE - 1) // EVIDENCE_PAGE_SIZE),
        value=1,
        step=1,
        key=f"analytical_page_{page_scope}",
    )
    result: Row = _read(
        "published details",
        lambda: pipeline.analytical_page(
            kind,
            identifier,
            synthetic,
            path=full_path,
            limit=EVIDENCE_PAGE_SIZE,
            offset=(int(page) - 1) * EVIDENCE_PAGE_SIZE,
        ),
        {},
    )
    if "references" in result:
        references = result["references"][:EVIDENCE_PAGE_SIZE]
        if references:
            st.dataframe(
                {"Observation ID": [display_safe(item) for item in references]},
                hide_index=True,
                width="stretch",
            )
        return
    records = result.get("records", [])[:EVIDENCE_PAGE_SIZE]
    if not records:
        return
    chosen = st.selectbox(
        "Inspect published item",
        list(range(len(records))),
        format_func=lambda index: str(display_safe(records[index]["id"])),
        key=f"analytical_record_{page_scope}_{int(page)}",
    )
    if chosen is not None:
        record = records[chosen]
        st.json(display_safe(record["value"]), expanded=False)
        _analytical_details(
            pipeline,
            kind,
            identifier,
            synthetic,
            record["value"],
            prefix=full_path + (record["id"],),
            depth=depth + 1,
        )


def _evidence(
    pipeline: DashboardPipeline, product_id: str, as_of: datetime, synthetic: bool
) -> None:
    scope = canonical([product_id, as_of.isoformat(), synthetic])
    if st.session_state.get("_evidence_scope") != scope:
        st.session_state["_evidence_scope"] = scope
        st.session_state["_evidence_cursors"] = [None]
        st.session_state["_evidence_position"] = 0
    position = st.session_state["_evidence_position"]
    cursors = st.session_state["_evidence_cursors"]
    if st.button("Previous evidence page", disabled=position == 0, key="evidence_previous"):
        st.session_state["_evidence_position"] = position - 1
        st.rerun()
    page = _read(
        "evidence",
        lambda: pipeline.evidence_page(
            product_id,
            as_of,
            synthetic=synthetic,
            limit=EVIDENCE_PAGE_SIZE,
            cursor=cursors[position],
        ),
        None,
    )
    if page is None:
        return
    if page["status"] == "indexing":
        st.info("Historical evidence is being prepared. Continue loading to inspect it.")
        if st.button("Continue loading evidence", key="evidence_continue"):
            st.rerun()
        return
    evidence = page["records"]
    st.caption(f"Evidence page {position + 1}")
    _records("observations", evidence)
    if evidence:
        with st.expander("Inspect observation provenance, precision, and missing fields"):
            st.json(display_safe(evidence), expanded=False)
    next_cursor = page.get("next_cursor")
    if st.button("Next evidence page", disabled=not next_cursor, key="evidence_next"):
        st.session_state["_evidence_cursors"] = cursors[: position + 1] + [next_cursor]
        st.session_state["_evidence_position"] = position + 1
        st.rerun()


def _opportunities(
    pipeline: DashboardPipeline, products: list[Row], as_of: datetime, synthetic: bool
) -> None:
    st.subheader("Opportunity review")
    st.warning("Attention does not establish purchasing demand or profitability.")
    st.caption(
        "These are published results. Refreshing this page does not query TikTok "
        "or recompute history."
    )
    page = st.number_input("Assessment page", min_value=1, value=1, step=1, key="assessment_page")
    assessments: list[Row] = _read(
        "published assessments",
        lambda: pipeline.assessment_summaries(
            synthetic, limit=MAX_REVIEW_ROWS, offset=(int(page) - 1) * MAX_REVIEW_ROWS
        ),
        [],
    )
    if assessments:
        st.dataframe(opportunity_rows(assessments), hide_index=True, width="stretch")
        assessment_ids = [str(row["product_id"]) for row in assessments if row.get("product_id")]
        selected = st.selectbox("Inspect assessment", assessment_ids, key="assessment_product")
        selected_row = _read(
            "assessment detail", lambda: pipeline.assessment(selected, synthetic), None
        )
        if selected_row is not None:
            st.json(display_safe(selected_row), expanded=False)
            _analytical_details(pipeline, "assessment", selected, synthetic, selected_row)
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
        _evidence(pipeline, selected_product, as_of, synthetic)
    else:
        st.info("Create a product and review evidence links to inspect its history.")

    st.subheader("Frozen alerts")
    alert_page = st.number_input("Alert page", min_value=1, value=1, step=1, key="alert_page")
    alerts: list[Row] = _read(
        "alerts",
        lambda: pipeline.alert_summaries(
            synthetic, limit=MAX_REVIEW_ROWS, offset=(int(alert_page) - 1) * MAX_REVIEW_ROWS
        ),
        [],
    )
    _records("alerts", alerts)
    if alerts:
        selected_alert = st.selectbox(
            "Inspect frozen alert", [str(row["alert_id"]) for row in alerts], key="alert_id"
        )
        detail = _read("alert detail", lambda: pipeline.alert(selected_alert, synthetic), None)
        if detail is not None:
            st.json(display_safe(detail), expanded=False)


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
    reviewed_product = st.selectbox("Inspect product links", product_ids, key="links_product")
    link_page = st.number_input("Link page", min_value=1, value=1, step=1, key="links_page")
    links: list[Row] = _read(
        "product links",
        lambda: pipeline.product_links(
            reviewed_product,
            as_of,
            synthetic,
            limit=MAX_REVIEW_ROWS,
            offset=(int(link_page) - 1) * MAX_REVIEW_ROWS,
        ),
        [],
    )
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
    page = st.number_input("Evaluation page", min_value=1, value=1, step=1, key="evaluation_page")
    evaluations: list[Row] = _read(
        "evaluations",
        lambda: pipeline.evaluation_summaries(
            synthetic, limit=MAX_REVIEW_ROWS, offset=(int(page) - 1) * MAX_REVIEW_ROWS
        ),
        [],
    )
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
            detail = _read(
                "evaluation detail",
                lambda: pipeline.evaluation(
                    evaluations[selected_index]["evaluation_id"], synthetic
                ),
                None,
            )
            if detail is not None:
                st.json(display_safe(detail), expanded=False)
                _analytical_details(
                    pipeline,
                    "evaluation",
                    evaluations[selected_index]["evaluation_id"],
                    synthetic,
                    detail.get("payload", {}),
                )


def _collection_plan(pipeline: DashboardPipeline, synthetic: bool) -> None:
    st.subheader("Recorded collection plan")
    st.caption(
        "Inspect the recorded policy inputs, request scope and planned intervals. "
        "Candidate timestamps describe the current catalog state. "
        "Coverage is limited to sampled candidates and permitted sources."
    )
    page = st.number_input("Plan page", min_value=1, value=1, step=1, key="collection_page")
    tasks: list[Row] = _read(
        "collection plan",
        lambda: pipeline.collection_summaries(
            synthetic, limit=MAX_REVIEW_ROWS, offset=(int(page) - 1) * MAX_REVIEW_ROWS
        ),
        [],
    )
    _records("planned tasks", tasks)
    if not tasks:
        return
    selected = st.selectbox(
        "Inspect planned request",
        [str(row["task_id"]) for row in tasks],
        key="collection_task",
    )
    scope = (selected, synthetic)
    if st.session_state.get("_collection_member_scope") != scope:
        st.session_state["_collection_member_scope"] = scope
        st.session_state["collection_member_page"] = 1
    member_page = st.number_input(
        "Candidate page", min_value=1, value=1, step=1, key="collection_member_page"
    )
    detail = _read(
        "collection decision",
        lambda: pipeline.collection_decision(
            selected,
            synthetic,
            limit=MAX_REVIEW_ROWS,
            offset=(int(member_page) - 1) * MAX_REVIEW_ROWS,
        ),
        None,
    )
    if detail is not None:
        st.caption(f"{detail['member_count']} candidate intentions in this recorded request.")
        st.json(display_safe(detail), expanded=False)


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
    visible_status = dict(status)
    if "budgets" in status:
        visible_status["budgets"] = []
        for budget in status["budgets"]:
            units = budget.get("tokens")
            # Quota units are not authentication tokens. Rename only the known
            # numeric counter at this facade boundary; keep secret filtering broad.
            numeric = (
                isinstance(units, (int, float))
                and not isinstance(units, bool)
                and math.isfinite(units)
            )
            visible_status["budgets"].append(
                {
                    **{key: value for key, value in budget.items() if key != "tokens"},
                    "remaining_request_units": units if numeric else None,
                }
            )
    st.json(display_safe(visible_status), expanded=False)
    st.caption(
        "Healthy processes do not establish fresh evidence, complete market coverage, "
        "or usable source permissions."
    )


def render_dashboard(pipeline: DashboardPipeline) -> None:
    """Render a review workspace using only the public application boundary."""
    st.set_page_config(page_title="Product trend intelligence", page_icon="📈", layout="wide")
    st.title("Product trend intelligence")
    if st.session_state.pop("_review_refresh_cutoff", False):
        st.session_state["review_cutoff"] = datetime.now(UTC).isoformat()
    with st.sidebar:
        st.header("Review workspace")
        synthetic = st.toggle("Include synthetic workspace", value=False, key="synthetic_workspace")
        st.caption(
            "The switch selects an isolated workspace; synthetic evidence is never "
            "mixed with live evidence."
        )
        if st.button("Refresh published results"):
            st.session_state["_review_refresh_cutoff"] = True
            st.rerun()
        cutoff = st.text_input(
            "Evidence and link cutoff (UTC)",
            value=datetime.now(UTC).isoformat(),
            key="review_cutoff",
        )
        view = st.radio(
            "Review view",
            [
                "Opportunities",
                "Catalog",
                "Launch preparation",
                "Collection plan",
                "Datasets and evaluations",
                "Health and coverage",
            ],
            key="review_view",
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
    notice = st.session_state.pop("_review_notice", None)
    if notice:
        st.success(notice)
    # Read results on every interaction, but only for the selected view. No result
    # cache can retain pre-mutation identity or source-capability state.
    if view in {"Opportunities", "Catalog"}:
        product_page = st.number_input(
            "Product page", min_value=1, value=1, step=1, key="product_page"
        )
        products: list[Row] = _read(
            "products",
            lambda: pipeline.products(
                synthetic, limit=MAX_REVIEW_ROWS, offset=(int(product_page) - 1) * MAX_REVIEW_ROWS
            ),
            [],
        )
        if view == "Opportunities":
            _opportunities(pipeline, products, as_of, synthetic)
        else:
            _catalog(pipeline, products, as_of, synthetic)
    elif view == "Launch preparation":
        from sm_trending_products.commerce_dashboard import render_launches

        render_launches(pipeline.launch_service(), synthetic, pipeline.commerce_delivery())
    elif view == "Datasets and evaluations":
        _history(pipeline, synthetic)
    elif view == "Collection plan":
        _collection_plan(pipeline, synthetic)
    else:
        status: Row = _read("operational status", pipeline.status, {})
        _health(status, status.get("capabilities", []), synthetic)


def _release_pipeline(pipeline: Any) -> None:
    try:
        pipeline.close()
    except Exception:
        # Cleanup callbacks can run after session teardown; never expose a DSN.
        pass


@st.cache_resource(max_entries=1, scope="session", show_spinner=False, on_release=_release_pipeline)
def _review_pipeline(configuration_fingerprint: str):
    from sm_trending_products.pipeline import Pipeline

    return Pipeline.from_env()


def _provider_file_identity(selector: str | None) -> Row | None:
    """Observe provider-file identity without reading credential contents."""
    if selector is None:
        return None
    path = Path(os.path.expandvars(selector)).expanduser()
    try:
        metadata = path.stat()
    except OSError as error:
        return {"path": str(path), "error": error.errno}
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "ctime_ns": metadata.st_ctime_ns,
    }


def _configuration_fingerprint() -> str:
    """Invalidate resources on configuration/credential rotation without caching secrets."""
    from sm_trending_products.settings import Settings

    settings = Settings()  # type: ignore[call-arg]
    configuration = settings.model_dump(mode="json")
    configuration["database_url"] = settings.database_url.get_secret_value()
    for key in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    ):
        configuration[key] = os.environ.get(key)
    for key, default in (
        ("AWS_CONFIG_FILE", "~/.aws/config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials"),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", None),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", None),
    ):
        configuration[key] = _provider_file_identity(os.environ.get(key, default))
    return hashlib.sha256(canonical(configuration).encode()).hexdigest()


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
        pipeline = _review_pipeline(_configuration_fingerprint())
    except Exception:  # noqa: BLE001 - Initialization failures may contain a secret DSN.
        st.error(
            "The pipeline is unavailable. Verify private database/object-store "
            "configuration and initialization; connection details are not displayed here."
        )
        return
    render_dashboard(pipeline)


if __name__ == "__main__":
    main()
