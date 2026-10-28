"""Bounded point-in-time evaluation and a durable forward-outcome frontier."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from .analytics import AssessmentIdentity
from .assessment_eligibility import AssessmentEligibility
from .assessment_service import AssessmentService
from .catalog import Catalog, InvalidationPending
from .collector import redact
from .contracts import (
    AssessmentPolicy,
    Capability,
    FamilyMembership,
    Observation,
    Product,
    ProductLink,
    digest,
    now,
    utc,
)
from .evaluation import (
    EvaluationConfig,
    freeze_decision,
    mature_decision,
    outcome_entity_keys,
    reviewed_links,
    summarize_backtest,
)
from .history import History
from .identity import LINK_FIELDS, Identity
from .provenance import Provenance


class EvaluationService:
    def __init__(self, catalog: Catalog, history: History, identity: Identity):
        self.catalog, self.history, self.identity = catalog, history, identity
        self.provenance = Provenance(catalog, history)

    def _evaluation_inputs(
        self, as_of: datetime, synthetic: bool, product_ids: Sequence[str] | None = None
    ) -> dict[str, Any]:
        """Read compact catalog histories, never the entire observation lake."""
        if product_ids is not None:
            return self._scoped_inputs(as_of, synthetic, product_ids)
        caps = self.catalog.all(
            "SELECT source,account_id,capability,state,available_at,details "
            "FROM source_capabilities WHERE available_at<=%s ORDER BY available_at,id",
            (as_of,),
        )
        return {
            "products": [
                Product.model_validate(item)
                for item in self.identity.all_products(
                    as_of,
                    synthetic=synthetic,
                )
            ],
            "links": [
                ProductLink.model_validate(item)
                for item in self.identity.link_history(
                    as_of,
                    synthetic,
                )
            ],
            "families": [
                FamilyMembership.model_validate(item)
                for item in self.identity.family_history(as_of, synthetic=synthetic)
            ],
            "capabilities": [Capability.model_validate(item) for item in caps],
        }

    def _scoped_inputs(
        self, as_of: datetime, synthetic: bool, product_ids: Sequence[str]
    ) -> dict[str, Any]:
        """Bound research metadata while retaining all competing entity owners."""
        products = self.catalog.all(
            "SELECT * FROM products WHERE product_id=ANY(%s) AND synthetic=%s "
            "AND created_at<=%s ORDER BY product_id",
            (list(product_ids), synthetic, as_of),
        )
        keys = self.catalog.all(
            "SELECT DISTINCT entity_key FROM product_links WHERE product_id=ANY(%s) "
            "AND available_at<=%s ORDER BY entity_key LIMIT 10001",
            (list(product_ids), as_of),
        )
        if len(keys) > 10000:
            raise ValueError("Research entity budget exceeded")
        link_fields = ",".join(f"l.{field}" for field in LINK_FIELDS.split(","))
        links = self.catalog.all(
            f"SELECT {link_fields} FROM product_links l JOIN products p USING(product_id) "
            "WHERE l.entity_key=ANY(%s) AND p.synthetic=%s AND p.created_at<=%s "
            "AND l.available_at<=%s ORDER BY l.available_at,l.id LIMIT 50001",
            ([row["entity_key"] for row in keys], synthetic, as_of, as_of),
        )
        families = self.catalog.all(
            "SELECT product_id,family_id,available_at FROM family_memberships "
            "WHERE product_id=ANY(%s) "
            "AND available_at<=%s ORDER BY available_at,id LIMIT 10001",
            (list(product_ids), as_of),
        )
        caps = self.catalog.all(
            "SELECT source,account_id,capability,state,available_at,details "
            "FROM source_capabilities WHERE available_at<=%s ORDER BY available_at,id "
            "LIMIT 10001",
            (as_of,),
        )
        if len(links) > 50000 or len(families) > 10000 or len(caps) > 10000:
            raise ValueError("Research metadata budget exceeded")
        sources = {row["entity_key"].split(":", 1)[0] for row in keys}
        unscoped = sorted(sources.difference(row["source"] for row in caps))
        if unscoped:
            # A future declaration establishes control, never an earlier grant.
            # Preserve one actual declaration for each otherwise unseen source.
            caps.extend(
                self.catalog.all(
                    "SELECT c.* FROM unnest(%s::text[]) AS selected(source) "
                    "CROSS JOIN LATERAL (SELECT source,account_id,capability,state,"
                    "available_at,details FROM source_capabilities WHERE source=selected.source "
                    "ORDER BY account_id,capability,available_at DESC,id DESC LIMIT 1) c",
                    (unscoped,),
                )
            )
        if len(caps) > 10000:
            raise ValueError("Research capability budget exceeded")
        return {
            "products": [Product.model_validate(row) for row in products],
            "links": [ProductLink.model_validate(row) for row in links],
            "families": [FamilyMembership.model_validate(row) for row in families],
            "capabilities": [Capability.model_validate(row) for row in caps],
            "declared_product_ids": tuple(product_ids),
            "controlled_sources": sorted(sources.intersection(row["source"] for row in caps)),
        }

    def _read_scope(
        self,
        as_of: datetime,
        synthetic: bool,
        entity_keys: set[str],
        since: datetime,
        until: datetime,
        *,
        period_window: tuple[datetime, datetime] | None = None,
    ) -> tuple[list[Observation], set[str]]:
        if not entity_keys:
            return [], set()
        observations, manifests = self.history.observations(
            as_of,
            synthetic,
            entity_keys=entity_keys,
            since=since,
            until=until,
            period_window=period_window,
        )
        dataset_ids = {
            manifest["dataset_id"]
            for manifest in manifests
            if any(
                entity_keys.intersection(obj["entity_keys"])
                for obj in manifest["manifest"]["normalized"]
            )
        }
        return observations, dataset_ids

    def _prepare_decision(
        self,
        metadata: dict[str, Any],
        as_of: datetime,
        synthetic: bool,
        config: EvaluationConfig,
        policy: AssessmentPolicy | None = None,
        *,
        selection_audit: bool = False,
    ) -> tuple[dict[str, Any], set[str]]:
        policy = policy or AssessmentPolicy()
        cutoff = utc(as_of)
        products = [
            product
            for product in metadata["products"]
            if product.created_at <= cutoff
            and product.synthetic == synthetic
            and (config.market is None or product.market == config.market)
        ]
        product_ids = {product.product_id for product in products}
        links = [
            link
            for link in reviewed_links(
                metadata["links"],
                cutoff,
                max(config.min_link_confidence, policy.min_link_confidence),
            )
            if link.product_id in product_ids
        ]
        config.validate_ledger_scope(links)
        identity = AssessmentIdentity.resolve(
            products, links, cutoff, synthetic, policy.min_link_confidence
        )
        prepared = AssessmentService(self.history, self.provenance).prepare(
            products,
            links,
            identity,
            cutoff,
            synthetic,
            policy,
            eligibility=AssessmentEligibility(config.market, metadata["capabilities"]),
        )
        normalized = prepared.rows
        counts, input_scopes = prepared.observation_counts, prepared.input_scopes
        snapshot = freeze_decision(
            [],
            products,
            metadata["links"],
            cutoff,
            config,
            synthetic=synthetic,
            policy=policy,
            families=metadata["families"],
            capabilities=metadata["capabilities"],
            precomputed_assessments=normalized,
            observation_counts=counts,
            evidence_refs_by_entity={},
        )
        snapshot["schema_version"] = "2"
        snapshot["evidence_refs"] = self.provenance.combine(
            [row["evidence_refs"] for row in snapshot["universe"]], synthetic
        )
        snapshot["input_provenance"] = self.provenance.combine(input_scopes, synthetic)
        snapshot["coverage"]["observations"] = sum(
            row["coverage"]["observations"] for row in snapshot["universe"]
        )
        if selection_audit:
            from .research_validation._audit import selection_funnel

            snapshot["selection_audit"] = selection_funnel(
                self.catalog,
                metadata["products"],
                links,
                normalized,
                counts,
                snapshot,
                config,
                cutoff,
                synthetic,
                declared_product_ids=metadata.get("declared_product_ids"),
            )
            snapshot["controlled_sources"] = metadata.get("controlled_sources", [])
        snapshot.pop("snapshot_id")
        snapshot["snapshot_id"] = digest(snapshot)
        return snapshot, set()

    @staticmethod
    def _initial_due(payload: dict[str, Any]) -> datetime:
        decision_time = utc(payload["decision_time"])
        config = EvaluationConfig.model_validate(payload["config"])
        actionable = [
            day
            for day in config.horizon_days
            if any(
                config.delay_duration(delay) < timedelta(days=day) for delay in config.launch_delays
            )
        ]
        if not actionable or payload["status"] == "insufficient_history":
            return decision_time
        return decision_time + timedelta(days=min(actionable))

    def _save_evaluation(
        self,
        payload: dict,
        kind: str,
        synthetic: bool,
        decision_time: datetime | None = None,
        parent_id: str | None = None,
        dataset_ids: Sequence[str] = (),
        *,
        frontier_token: str | None = None,
    ) -> str:
        evaluation_id = digest({"kind": kind, "synthetic": synthetic, "payload": payload})
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, dataset_ids)
            artifacts = self.provenance.roots(payload)
            artifact_ids = self.provenance.guard(connection, artifacts)
            if parent_id:
                parent = connection.execute(
                    "SELECT invalidated FROM evaluation_runs WHERE evaluation_id=%s",
                    (parent_id,),
                ).fetchone()
                if not parent or parent["invalidated"]:
                    raise ValueError("Frozen decision was invalidated during computation")
            if frontier_token:
                lease = connection.execute(
                    "SELECT 1 FROM forward_frontier WHERE evaluation_id=%s AND lease_token=%s "
                    "AND state='running' AND lease_until>clock_timestamp() FOR UPDATE",
                    (parent_id, frontier_token),
                ).fetchone()
                if not lease:
                    raise ValueError("Forward outcome lease was lost")
            connection.execute(
                "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,decision_time,payload,"
                "parent_id) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (evaluation_id, kind, synthetic, decision_time, Jsonb(redact(payload)), parent_id),
            )
            for dataset_id in sorted(set(dataset_ids)):
                connection.execute(
                    "INSERT INTO evaluation_dependencies(evaluation_id,dataset_id) "
                    "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                    (evaluation_id, dataset_id),
                )
            for artifact in artifacts:
                connection.execute(
                    "INSERT INTO evaluation_artifacts(evaluation_id,object_id) "
                    "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                    (evaluation_id, artifact_ids[artifact["key"]]),
                )
            if kind == "forward":
                connection.execute(
                    "INSERT INTO forward_frontier(evaluation_id,synthetic,next_due_at) "
                    "VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                    (evaluation_id, synthetic, self._initial_due(payload)),
                )
        return evaluation_id

    def backtest(
        self,
        decision_times: list[datetime],
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
        *,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        return self._backtest(decision_times, as_of, synthetic, config, policy=policy)

    def scoped_backtest(
        self,
        decision_times: list[datetime],
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
        *,
        policy: AssessmentPolicy | None = None,
        product_ids: Sequence[str],
        measurement_sample_size: int = 0,
        seed: int = 42,
    ) -> dict:
        """Execute the ordinary kernel on an explicit bounded research population.

        Scope alone does not establish preregistration: ResearchValidation owns
        protocol locks. Existing unrestricted backtests keep their exact contract.
        """
        if (
            type(synthetic) is not bool
            or isinstance(product_ids, str | bytes)
            or not 1 <= len(product_ids) <= 500
            or len(set(product_ids)) != len(product_ids)
            or any(not isinstance(value, str) or not value.strip() for value in product_ids)
            or not 1 <= len(decision_times) <= 64
            or type(measurement_sample_size) is not int
            or not 0 <= measurement_sample_size <= 25
            or type(seed) is not int
        ):
            raise ValueError("Invalid bounded research scope")
        configuration = EvaluationConfig.for_creation(config)
        if measurement_sample_size and configuration.target != "attention":
            raise ValueError("Measurement-only sampling requires the attention target")
        policy = AssessmentPolicy.model_validate(
            (policy or AssessmentPolicy()).model_dump(mode="json")
        )
        return self._backtest(
            decision_times,
            as_of,
            synthetic,
            config,
            policy=policy,
            product_ids=tuple(product_ids),
            measurement_sample_size=measurement_sample_size,
            seed=seed,
        )

    def _backtest(
        self,
        decision_times: list[datetime],
        as_of: datetime,
        synthetic: bool,
        config: dict | None,
        *,
        policy: AssessmentPolicy | None,
        product_ids: Sequence[str] | None = None,
        measurement_sample_size: int = 0,
        seed: int = 42,
    ) -> dict:
        cutoff = utc(as_of)
        configuration = EvaluationConfig.for_creation(config)
        configuration.validate_creation()
        dates = sorted({utc(timestamp) for timestamp in decision_times})
        if any(timestamp > cutoff for timestamp in dates):
            raise ValueError("A backtest cannot contain a future decision")
        metadata = self._evaluation_inputs(cutoff, synthetic, product_ids)
        snapshots, reports = [], []
        snapshot_headers: dict[str, Any] = {}
        report_headers: dict[str, Any] = {}
        for timestamp in dates:
            snapshot, dependencies = self._prepare_decision(
                metadata,
                timestamp,
                synthetic,
                configuration,
                policy=policy,
                selection_audit=product_ids is not None,
            )
            if product_ids is not None and measurement_sample_size:
                from .research_validation._audit import measurement_snapshot

                sample = measurement_snapshot(snapshot, metadata, measurement_sample_size, seed)
                sampled_report, _ = self._maturation_payload(sample, cutoff, dependencies)
                # This additive audit never changes policy/baseline selections.
                snapshot = deepcopy(snapshot)
                snapshot["selection_audit"]["measurement_sample"] = {
                    "product_ids": sample["selected"],
                    "measurement_only": True,
                    "methodology": deepcopy(sample["methodology"]),
                    "report": self._report_header(sampled_report, synthetic),
                }
                snapshot.pop("snapshot_id")
                snapshot["snapshot_id"] = digest(snapshot)
            report, outcome_dependencies = self._maturation_payload(snapshot, cutoff, dependencies)
            # Aggregation needs only selection/family/status fields. Full product
            # universes and outcomes are immutable paged records, never retained
            # across all dates or embedded into one PostgreSQL JSON value.
            snapshots.append(
                {
                    "snapshot_id": snapshot["snapshot_id"],
                    "selected": snapshot["selected"],
                    "universe": [
                        {"product_id": row["product_id"], "family_id": row["family_id"]}
                        for row in snapshot["universe"]
                        if row["product_id"] in snapshot["selected"]
                    ],
                }
            )
            reports.append(
                {
                    "snapshot_id": report["snapshot_id"],
                    "scenarios": [
                        {
                            "horizon_days": scenario["horizon_days"],
                            configuration.delay_field: scenario[configuration.delay_field],
                            "methods": {
                                method: {
                                    "outcomes": [
                                        {
                                            key: value
                                            for key, value in outcome.items()
                                            if key
                                            in {
                                                "product_id",
                                                "status",
                                                "precision",
                                                "lead_time_hours",
                                            }
                                        }
                                        for outcome in values["outcomes"]
                                    ]
                                }
                                for method, values in scenario["methods"].items()
                            },
                        }
                        for scenario in report["scenarios"]
                    ],
                }
            )
            snapshot_headers[snapshot["snapshot_id"]] = self._snapshot_header(snapshot, synthetic)
            report_headers[report["report_id"]] = self._report_header(report, synthetic)
        payload = summarize_backtest(snapshots, reports, cutoff, configuration, synthetic=synthetic)
        payload["schema_version"] = "2"
        payload["snapshots"] = self.provenance.records(snapshot_headers, synthetic)
        payload["reports"] = self.provenance.records(report_headers, synthetic)
        payload["comparisons"] = self._report_header(payload["comparisons"], synthetic)
        payload["config"] = self._config_header(payload["config"], synthetic)
        payload.pop("evaluation_id")
        payload["evaluation_id"] = digest(payload)
        return {
            "evaluation_id": self._save_evaluation(
                payload,
                "backtest",
                synthetic,
            ),
            "result": payload,
        }

    def _config_header(self, value: dict[str, Any], synthetic: bool) -> dict[str, Any]:
        contract = value.get("profit_contract")
        if not contract:
            return value
        return {
            **value,
            "profit_contract": {
                **contract,
                "ledgers": self.provenance.records(contract["ledgers"], synthetic),
            },
        }

    def _snapshot_header(self, snapshot: dict[str, Any], synthetic: bool) -> dict[str, Any]:
        return {
            **snapshot,
            "representation": "sharded-backtest-partition-v1",
            "config": self._config_header(snapshot["config"], synthetic),
            "universe": self.provenance.records(
                {row["product_id"]: row for row in snapshot["universe"]}, synthetic
            ),
            "entity_bindings": self.provenance.records(snapshot["entity_bindings"], synthetic),
            "identity_decisions": self.provenance.records(
                {str(index): row for index, row in enumerate(snapshot["identity_decisions"])},
                synthetic,
            ),
            "family_memberships": self.provenance.records(
                snapshot["family_memberships"], synthetic
            ),
            "selected": self.provenance.records(
                {str(index): value for index, value in enumerate(snapshot["selected"])}, synthetic
            ),
            "baselines": {
                key: self.provenance.records(
                    {str(index): value for index, value in enumerate(values)}, synthetic
                )
                for key, values in snapshot["baselines"].items()
            },
            "capabilities": self.provenance.records(
                {str(index): row for index, row in enumerate(snapshot["capabilities"])}, synthetic
            ),
        }

    def _report_header(self, value: Any, synthetic: bool) -> Any:
        if isinstance(value, dict):
            if value.get("format") in {"sharded-provenance-v1", "sharded-records-v1"}:
                return value
            return {
                key: self.provenance.records(
                    {str(index): row for index, row in enumerate(child)}, synthetic
                )
                if key in {"outcomes", "values_hours"} and isinstance(child, list)
                else self._config_header(child, synthetic)
                if key == "config"
                else self._report_header(child, synthetic)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [self._report_header(child, synthetic) for child in value]
        return value

    def freeze(
        self,
        as_of: datetime,
        synthetic: bool = False,
        config: dict | None = None,
        *,
        policy: AssessmentPolicy | None = None,
    ) -> dict:
        cutoff = utc(as_of)
        if not synthetic and cutoff < now() - timedelta(minutes=5):
            raise ValueError("A live forward decision cannot be retrospectively backdated")
        if not synthetic and cutoff > now() + timedelta(seconds=30):
            raise ValueError("A live forward decision cannot be future dated")
        configuration = EvaluationConfig.for_creation(config)
        configuration.validate_creation()
        payload, datasets = self._prepare_decision(
            self._evaluation_inputs(cutoff, synthetic),
            cutoff,
            synthetic,
            configuration,
            policy=policy,
        )
        return {
            "evaluation_id": self._save_evaluation(
                payload,
                "forward",
                synthetic,
                cutoff,
                dataset_ids=sorted(datasets),
            ),
            "result": payload,
        }

    def _maturation_payload(
        self,
        snapshot: dict[str, Any],
        as_of: datetime,
        dependencies: set[str],
        *,
        frontier_token: str | None = None,
        forward_id: str | None = None,
    ) -> tuple[dict[str, Any], set[str]]:
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, sorted(dependencies))
            self.provenance.guard(connection, self.provenance.roots(snapshot))
        compact = snapshot.get("schema_version") == "2"
        configuration = EvaluationConfig.model_validate(snapshot["config"])
        decision_time, cutoff = utc(snapshot["decision_time"]), utc(as_of)
        gap = timedelta(hours=configuration.max_boundary_gap_hours)
        first_action = decision_time + configuration.delay_duration(
            min(configuration.launch_delays)
        )
        final_horizon = decision_time + timedelta(days=max(configuration.horizon_days))
        since, until = first_action - gap, min(cutoff, final_horizon + gap)
        datasets = set(dependencies)
        input_scopes: list[Any] = []
        keys_by_product: dict[str, set[str]] = defaultdict(set)
        for key in outcome_entity_keys(snapshot["entity_bindings"], configuration):
            keys_by_product[snapshot["entity_bindings"][key]].add(key)

        def load_product(product_id: str) -> list[Observation]:
            if frontier_token:
                renewed = self.catalog.one(
                    "UPDATE forward_frontier SET "
                    "lease_until=clock_timestamp()+interval '5 minutes' "
                    "WHERE evaluation_id=%s AND lease_token=%s AND lease_until>clock_timestamp() "
                    "RETURNING evaluation_id",
                    (forward_id, frontier_token),
                )
                if not renewed:
                    raise ValueError("Forward outcome lease was lost")
            keys = keys_by_product[product_id]
            observations, used = self._read_scope(
                cutoff,
                bool(snapshot["synthetic"]),
                keys,
                since,
                until,
                period_window=(first_action, final_horizon),
            )
            if compact:
                input_scopes.append(
                    self.provenance.audit(observations, bool(snapshot["synthetic"]))
                )
            else:
                datasets.update(used)
            return observations

        with self.history.read_session():
            payload = mature_decision(
                snapshot,
                [],
                cutoff,
                product_evidence_loader=load_product,
                verified_evidence_refs=None if compact else snapshot["evidence_refs"],
                provenance_verified=compact,
                outcome_compactor=(
                    lambda outcomes, observations: self.provenance.compact(
                        outcomes, observations, bool(snapshot["synthetic"])
                    )
                )
                if compact
                else None,
            )
        if compact:
            payload["input_provenance"] = self.provenance.combine(
                input_scopes, bool(snapshot["synthetic"])
            )
            payload.pop("report_id")
            payload["report_id"] = digest(payload)
        return payload, datasets

    def mature(
        self,
        evaluation_id: str,
        as_of: datetime,
        *,
        frontier_token: str | None = None,
    ) -> dict:
        row = self.catalog.one(
            "SELECT * FROM evaluation_runs WHERE evaluation_id=%s AND kind='forward' "
            "AND NOT invalidated",
            (evaluation_id,),
        )
        if not row:
            raise ValueError("No valid frozen decision found")
        dependencies = {
            item["dataset_id"]
            for item in self.catalog.all(
                "SELECT dataset_id FROM evaluation_dependencies WHERE evaluation_id=%s",
                (evaluation_id,),
            )
        }
        payload, datasets = self._maturation_payload(
            row["payload"],
            as_of,
            dependencies,
            frontier_token=frontier_token,
            forward_id=evaluation_id,
        )
        return {
            "evaluation_id": self._save_evaluation(
                payload,
                "outcome",
                row["synthetic"],
                parent_id=evaluation_id,
                dataset_ids=sorted(datasets),
                frontier_token=frontier_token,
            ),
            "result": payload,
        }

    def _claim_forward(self, cutoff: datetime, synthetic: bool) -> dict | None:
        token = uuid4().hex
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            connection.execute(
                "WITH cleanup AS (SELECT f.evaluation_id,r.invalidated "
                "FROM forward_frontier f JOIN evaluation_runs r USING(evaluation_id) "
                "WHERE f.synthetic=%s AND ("
                "(f.state='running' AND f.lease_until<clock_timestamp()) OR "
                "(r.invalidated AND f.state IN('pending','running'))) "
                "FOR UPDATE OF f SKIP LOCKED) "
                "UPDATE forward_frontier f SET state=CASE WHEN cleanup.invalidated "
                "THEN 'invalidated' ELSE 'pending' END,lease_token=NULL,lease_until=NULL "
                "FROM cleanup WHERE f.evaluation_id=cleanup.evaluation_id",
                (synthetic,),
            )
            return connection.execute(
                "WITH due AS (SELECT f.evaluation_id FROM forward_frontier f "
                "JOIN evaluation_runs r USING(evaluation_id) WHERE f.synthetic=%s "
                "AND f.state='pending' AND f.next_due_at<=%s AND NOT r.invalidated "
                "ORDER BY f.next_due_at,f.created_at,f.evaluation_id "
                "FOR UPDATE OF f SKIP LOCKED LIMIT 1) "
                "UPDATE forward_frontier f SET state='running',lease_token=%s,"
                "lease_until=clock_timestamp()+interval '5 minutes',attempts=attempts+1 "
                "FROM due WHERE f.evaluation_id=due.evaluation_id RETURNING f.*",
                (synthetic, cutoff, token),
            ).fetchone()

    @staticmethod
    def _next_due(report: dict[str, Any], cutoff: datetime) -> datetime | None:
        if report["status"] in {"insufficient_history", "invalidated"}:
            return None
        configuration = EvaluationConfig.model_validate(report["config"])
        opportunities = []
        for scenario in report["scenarios"]:
            if scenario["status"] == "not_actionable":
                continue
            end = utc(scenario["window_end"])
            if cutoff < end:
                opportunities.append(end)
                continue
            incomplete = any(
                method["status_counts"].get("censored", 0)
                for method in scenario["methods"].values()
            )
            grace = end + timedelta(hours=configuration.max_boundary_gap_hours)
            if incomplete and cutoff < grace:
                opportunities.append(min(grace, cutoff + timedelta(hours=1)))
        return min(opportunities) if opportunities else None

    def mature_due(
        self,
        limit: int = 100,
        synthetic: bool = False,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Advance oldest due decisions fairly; retry missing outcomes only through their grace."""
        if isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise ValueError("Maturation batch size must be between 1 and 1000")
        cutoff = utc(as_of or now())
        result: dict[str, Any] = {
            "processed": 0,
            "completed": 0,
            "pending": 0,
            "decisions": [],
            "evaluations": [],
            "errors": [],
        }
        for _ in range(limit):
            claim = self._claim_forward(cutoff, synthetic)
            if claim is None:
                break
            decision_id, token = claim["evaluation_id"], claim["lease_token"]
            try:
                evaluation = self.mature(decision_id, cutoff, frontier_token=token)
                due = self._next_due(evaluation["result"], cutoff)
                updated = self.catalog.one(
                    "UPDATE forward_frontier SET state=%s,next_due_at=%s,lease_token=NULL,"
                    "lease_until=NULL,last_report_id=%s,last_checked_at=%s,last_error=NULL "
                    "WHERE evaluation_id=%s AND lease_token=%s RETURNING evaluation_id",
                    (
                        "complete" if due is None else "pending",
                        due or cutoff,
                        evaluation["evaluation_id"],
                        cutoff,
                        decision_id,
                        token,
                    ),
                )
                if not updated:
                    raise ValueError("Forward outcome lease was lost")
                result["completed" if due is None else "pending"] += 1
                result["evaluations"].append(evaluation["evaluation_id"])
                result["decisions"].append(decision_id)
                result["processed"] += 1
            except InvalidationPending:
                # A late retirement fence is temporary backpressure, not an
                # outcome failure. Preserve its due time for the next attempt.
                self.catalog.execute(
                    "UPDATE forward_frontier f SET state=CASE WHEN EXISTS ("
                    "SELECT 1 FROM evaluation_runs r WHERE r.evaluation_id=f.evaluation_id "
                    "AND r.invalidated) THEN 'invalidated' ELSE 'pending' END,"
                    "lease_token=NULL,lease_until=NULL,attempts=GREATEST(0,attempts-1) "
                    "WHERE evaluation_id=%s AND lease_token=%s",
                    (decision_id, token),
                )
                raise
            except Exception:
                invalid = self.catalog.one(
                    "SELECT invalidated FROM evaluation_runs WHERE evaluation_id=%s",
                    (decision_id,),
                )
                category = (
                    "decision_invalidated"
                    if invalid and invalid["invalidated"]
                    else "maturation_failed"
                )
                self.catalog.execute(
                    "UPDATE forward_frontier SET state=%s,next_due_at=%s,lease_token=NULL,"
                    "lease_until=NULL,last_error=%s,last_checked_at=%s "
                    "WHERE evaluation_id=%s AND lease_token=%s",
                    (
                        "invalidated" if category == "decision_invalidated" else "pending",
                        cutoff + timedelta(hours=1),
                        category,
                        cutoff,
                        decision_id,
                        token,
                    ),
                )
                result["errors"].append({"evaluation_id": decision_id, "category": category})
        return result
