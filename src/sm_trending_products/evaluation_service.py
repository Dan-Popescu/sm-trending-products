"""Bounded point-in-time evaluation and a durable forward-outcome frontier."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from .analysis_batches import bounded_product_batches
from .analytics import AssessmentIdentity, assess, normalize_assessments
from .catalog import Catalog
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
    eligible_evidence,
    freeze_decision,
    mature_decision,
    outcome_entity_keys,
    reviewed_links,
    summarize_backtest,
)
from .history import History
from .identity import Identity
from .provenance import Provenance


class EvaluationService:
    def __init__(self, catalog: Catalog, history: History, identity: Identity):
        self.catalog, self.history, self.identity = catalog, history, identity
        self.provenance = Provenance(catalog, history)

    def _evaluation_inputs(self, as_of: datetime, synthetic: bool) -> dict[str, Any]:
        """Read compact catalog histories, never the entire observation lake."""
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
        counts: Counter[str] = Counter()
        rows: list[dict[str, Any]] = []
        input_scopes = []
        since = cutoff - timedelta(days=policy.lookback_days)

        def load(keys: set[str]) -> tuple[list[Observation], set[str]]:
            return self._read_scope(cutoff, synthetic, keys, since, cutoff)

        for batch, _keys, (observations, _dependencies) in bounded_product_batches(
            products, links, self.history.settings.analysis_product_batch_size, load
        ):
            input_scopes.extend(
                self.provenance.audit_products(observations, identity.owners, synthetic)
            )
            eligible = eligible_evidence(
                observations,
                cutoff,
                synthetic,
                config.market,
                metadata["capabilities"],
                since=since,
            )
            counts.update(
                entity_key
                for entity_key, _observed_at in {
                    (row.entity_key, utc(row.observed_at)) for row in eligible
                }
            )
            rows.extend(
                self.provenance.compact(
                    assess(
                        eligible,
                        batch,
                        links,
                        cutoff,
                        synthetic,
                        policy,
                        normalize=False,
                        identity=identity,
                    ),
                    eligible,
                    synthetic,
                )
            )

        normalized = normalize_assessments(rows, policy)
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
            if any(delay < day for delay in config.launch_delay_days)
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
        cutoff = utc(as_of)
        configuration = EvaluationConfig.model_validate(config or {})
        configuration.validate_creation()
        dates = sorted({utc(timestamp) for timestamp in decision_times})
        if any(timestamp > cutoff for timestamp in dates):
            raise ValueError("A backtest cannot contain a future decision")
        metadata = self._evaluation_inputs(cutoff, synthetic)
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
            )
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
                            "launch_delay_days": scenario["launch_delay_days"],
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
        configuration = EvaluationConfig.model_validate(config or {})
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
        first_action = decision_time + timedelta(days=min(configuration.launch_delay_days))
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
