"""Immutable commercial selection and manually reviewed actual-cost periods."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..commerce import LaunchError, LaunchService
from ..contracts import digest, utc
from ..history import History
from ..identity import Identity
from ..module_schema import initialize_module, require_module
from ._publication import SOURCE, Publisher, stage_intent
from ._schema import MIGRATIONS
from .models import ActualCostContract, LearningError, ReviewedPeriod, TestPlan


def _request(expected_revision: int, request_id: str, synthetic: bool) -> None:
    if not isinstance(synthetic, bool):
        raise LearningError("invalid_workspace")
    if (
        isinstance(expected_revision, bool)
        or not isinstance(expected_revision, int)
        or not 0 <= expected_revision < 2_147_483_647
    ):
        raise LearningError("invalid_revision")
    if (
        not isinstance(request_id, str)
        or not 1 <= len(request_id) <= 128
        or request_id != request_id.strip()
        or any(ord(char) < 32 for char in request_id)
    ):
        raise LearningError("invalid_request_id")


def _clock(connection, synthetic: bool, as_of: datetime | None) -> datetime:
    if as_of is not None and not synthetic:
        raise LearningError("real_clock_is_database_time")
    return (
        utc(as_of)
        if as_of is not None
        else connection.execute("SELECT clock_timestamp() AS timestamp").fetchone()["timestamp"]
    )


def _lock(connection, key: str) -> None:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended("
        "current_schema()||':commercial-learning:'||%s,492371259))",
        (key,),
    )


def _page(limit: int, offset: int, synthetic: bool) -> None:
    _request(0, "page", synthetic)
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= 1000
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or not 0 <= offset <= 1000000
    ):
        raise LearningError("invalid_page")


class CommercialLearning:
    """One private boundary for selection, actuals review and recoverable export."""

    def __init__(self, catalog: Catalog, history: History, identity: Identity):
        self.catalog, self.history, self.identity = catalog, history, identity
        self.publisher = Publisher(catalog, history, identity)

    def initialize(self) -> dict:
        require_module(self.catalog, "commerce")
        return initialize_module(self.catalog, "commerce_learning", MIGRATIONS)

    def _require(self) -> None:
        try:
            require_module(self.catalog, "commerce_learning")
        except ValueError as error:
            raise LearningError("not_initialized") from error

    @staticmethod
    def _policy_references(connection, plan: TestPlan, dossier: dict, timestamp: datetime) -> dict:
        references = {}
        if plan.assessment_run_id:
            assessment = connection.execute(
                "SELECT r.run_id,r.as_of,r.policy FROM assessment_runs r "
                "JOIN current_assessments c USING(run_id) WHERE r.run_id=%s AND c.product_id=%s "
                "AND r.synthetic=%s AND NOT r.invalidated AND r.as_of<=%s",
                (plan.assessment_run_id, dossier["product_id"], dossier["synthetic"], timestamp),
            ).fetchone()
            if assessment is None:
                raise LearningError("assessment_reference_unavailable")
            references["assessment"] = {
                "run_id": assessment["run_id"],
                "as_of": assessment["as_of"].isoformat(),
                "policy": assessment["policy"],
            }
        if plan.forward_evaluation_id:
            evaluation = connection.execute(
                "SELECT evaluation_id,payload FROM evaluation_runs WHERE evaluation_id=%s "
                "AND synthetic=%s AND kind='forward' AND NOT invalidated AND decision_time<=%s",
                (plan.forward_evaluation_id, dossier["synthetic"], timestamp),
            ).fetchone()
            if evaluation is None or dossier["product_id"] not in {
                row["product_id"] for row in evaluation["payload"]["universe"]
            }:
                raise LearningError("evaluation_reference_unavailable")
            snapshot = evaluation["payload"]
            references["forward_decision"] = {
                "evaluation_id": evaluation["evaluation_id"],
                "snapshot_id": snapshot["snapshot_id"],
                "decision_time": snapshot["decision_time"],
                "policy": snapshot["policy"],
                "evaluation_config_hash": digest(snapshot["config"]),
                "selected": dossier["product_id"] in snapshot["selected"],
            }
        return references

    def plan_test(
        self,
        dossier_id: str,
        expected_revision: int,
        request_id: str,
        synthetic: bool = False,
        *,
        plan: TestPlan,
        as_of: datetime | None = None,
    ) -> dict:
        _request(expected_revision, request_id, synthetic)
        plan = TestPlan.model_validate(plan.model_dump(mode="json"))
        self._require()
        try:
            dossier = LaunchService(self.catalog).dossier(dossier_id, synthetic)
        except LaunchError as error:
            raise LearningError(error.code) from error
        fingerprint = digest(
            {
                "plan": plan.model_dump(mode="json"),
                "expected_revision": expected_revision,
                "as_of": utc(as_of).isoformat() if as_of is not None else None,
            }
        )
        test_id = digest(
            {"dossier_id": dossier_id, "request_id": request_id, "synthetic": synthetic}
        )
        with self.catalog.transaction() as connection:
            _lock(connection, "plan:" + dossier_id)
            replay = connection.execute(
                "SELECT * FROM commercial_tests WHERE dossier_id=%s AND request_id=%s "
                "AND synthetic=%s",
                (dossier_id, request_id, synthetic),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != fingerprint:
                    raise LearningError("request_conflict")
            else:
                current = connection.execute(
                    "SELECT revision FROM commerce_dossiers WHERE dossier_id=%s AND synthetic=%s "
                    "FOR SHARE",
                    (dossier_id, synthetic),
                ).fetchone()
                if (
                    current is None
                    or current["revision"] != expected_revision
                    or (dossier["revision"] != expected_revision)
                ):
                    raise LearningError("dossier_revision_conflict")
                timestamp = _clock(connection, synthetic, as_of)
                if timestamp < utc(dossier["updated_at"]):
                    raise LearningError("plan_precedes_dossier")
                if plan.selection == "test" and (
                    dossier["readiness"]["status"] != "ready"
                    or utc(dossier["draft"]["quote_expires_at"]) <= timestamp
                ):
                    raise LearningError("dossier_not_ready")
                references = self._policy_references(connection, plan, dossier, timestamp)
                ledger_key = f"{SOURCE}:cost:{test_id}" if plan.selection == "test" else None
                replay = connection.execute(
                    "INSERT INTO commercial_tests(test_id,dossier_id,dossier_revision,product_id,"
                    "country,currency,synthetic,request_id,request_hash,plan,dossier_snapshot,"
                    "policy_references,planned_at,ledger_key,state) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        test_id,
                        dossier_id,
                        expected_revision,
                        dossier["product_id"],
                        dossier["country"],
                        dossier["currency"],
                        synthetic,
                        request_id,
                        fingerprint,
                        Jsonb(plan.model_dump(mode="json")),
                        Jsonb(dossier),
                        Jsonb(references),
                        timestamp,
                        ledger_key,
                        "pending_ledger" if ledger_key else "recorded",
                    ),
                ).fetchone()
                assert replay is not None
                if ledger_key:
                    stage_intent(connection, replay)
        if replay["state"] == "pending_ledger":
            intent = self.catalog.one(
                "SELECT dataset_id FROM commercial_publications WHERE test_id=%s "
                "AND kind='declaration'",
                (test_id,),
            )
            assert intent is not None
            self.publisher.deliver(intent["dataset_id"], synthetic)
        return self.test(test_id, synthetic)

    def test(self, test_id: str, synthetic: bool = False) -> dict:
        self._require()
        _request(0, "read", synthetic)
        row = self.catalog.one(
            "SELECT * FROM commercial_tests WHERE test_id=%s AND synthetic=%s", (test_id, synthetic)
        )
        if row is None:
            raise LearningError("test_not_found")
        return self._test_record(row)

    @staticmethod
    def _test_record(row: dict) -> dict:
        return {
            **{
                name: row[name]
                for name in (
                    "test_id",
                    "dossier_id",
                    "dossier_revision",
                    "product_id",
                    "country",
                    "currency",
                    "synthetic",
                    "plan",
                    "policy_references",
                    "ledger_key",
                    "state",
                    "ledger_decision_id",
                )
            },
            "planned_at": row["planned_at"].isoformat(),
            "revision": 1,
            "ledger_declared_at": (
                row["ledger_declared_at"].isoformat() if row["ledger_declared_at"] else None
            ),
            "profit_contract": (
                {
                    "version": "realized-profit-v1",
                    "ledgers": {row["ledger_key"]: row["plan"]["cost_contract"]["required_costs"]},
                }
                if row["ledger_key"]
                else None
            ),
            **({"dossier_snapshot": row["dossier_snapshot"]} if "dossier_snapshot" in row else {}),
            "learning": "manual_review_only_no_automatic_policy_training",
        }

    def tests(self, synthetic: bool = False, limit: int = 100, offset: int = 0) -> list[dict]:
        self._require()
        _page(limit, offset, synthetic)
        rows = self.catalog.all(
            "SELECT test_id,dossier_id,dossier_revision,product_id,country,currency,synthetic,"
            "plan,policy_references,ledger_key,state,ledger_declared_at,ledger_decision_id,"
            "planned_at "
            "FROM commercial_tests WHERE synthetic=%s ORDER BY planned_at DESC,test_id "
            "LIMIT %s OFFSET %s",
            (synthetic, limit, offset),
        )
        return [self._test_record(row) for row in rows]

    def review_period(
        self,
        test_id: str,
        period: ReviewedPeriod,
        expected_revision: int,
        request_id: str,
        synthetic: bool = False,
        *,
        as_of: datetime | None = None,
    ) -> dict:
        _request(expected_revision, request_id, synthetic)
        period = ReviewedPeriod.model_validate(period.model_dump(mode="json"))
        self._require()
        fingerprint = digest(
            {
                "period": period.model_dump(mode="json"),
                "expected_revision": expected_revision,
                "as_of": utc(as_of).isoformat() if as_of is not None else None,
            }
        )
        period_id = digest(
            {
                "test_id": test_id,
                "start": period.period_start.isoformat(),
                "end": period.period_end.isoformat(),
            }
        )
        withdrawal_id = None
        with self.catalog.transaction() as connection:
            _lock(connection, "periods:" + test_id)
            test = connection.execute(
                "SELECT * FROM commercial_tests WHERE test_id=%s AND synthetic=%s FOR SHARE",
                (test_id, synthetic),
            ).fetchone()
            if test is None:
                raise LearningError("test_not_found")
            replay = connection.execute(
                "SELECT * FROM commercial_period_reviews WHERE test_id=%s AND request_id=%s",
                (test_id, request_id),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != fingerprint:
                    raise LearningError("request_conflict")
            else:
                if test["state"] != "ready":
                    raise LearningError("test_has_no_ready_ledger")
                if (period.country, period.currency) != (test["country"], test["currency"]):
                    raise LearningError("immutable_country_currency")
                timestamp = _clock(connection, synthetic, as_of)
                if period.period_start < test["planned_at"] or period.period_end > timestamp:
                    raise LearningError("period_outside_known_test_time")
                current = connection.execute(
                    "SELECT * FROM commercial_periods WHERE test_id=%s AND period_id=%s FOR UPDATE",
                    (test_id, period_id),
                ).fetchone()
                if expected_revision != (current["revision"] if current else 0):
                    raise LearningError("period_revision_conflict")
                previous = connection.execute(
                    "SELECT max(available_at) AS available_at FROM commercial_period_reviews "
                    "WHERE test_id=%s",
                    (test_id,),
                ).fetchone()
                if previous and previous["available_at"] and timestamp <= previous["available_at"]:
                    raise LearningError("non_monotonic_review_time")
                if (
                    current is None
                    and connection.execute(
                        "SELECT 1 FROM commercial_periods WHERE test_id=%s AND period_start<%s "
                        "AND period_end>%s LIMIT 1",
                        (test_id, period.period_end, period.period_start),
                    ).fetchone()
                ):
                    raise LearningError("overlapping_period")
                revision = expected_revision + 1
                connection.execute(
                    "INSERT INTO commercial_periods("
                    "test_id,period_id,period_start,period_end,revision) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(test_id,period_id) "
                    "DO UPDATE SET revision=EXCLUDED.revision",
                    (test_id, period_id, period.period_start, period.period_end, revision),
                )
                contract = ActualCostContract.model_validate(test["plan"]["cost_contract"])
                reasons = period.missing(contract)
                replay = connection.execute(
                    "INSERT INTO commercial_period_reviews(test_id,period_id,revision,request_id,"
                    "request_hash,payload,available_at,complete,reasons) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        test_id,
                        period_id,
                        revision,
                        request_id,
                        fingerprint,
                        Jsonb(period.model_dump(mode="json")),
                        timestamp,
                        not reasons,
                        Jsonb(reasons),
                    ),
                ).fetchone()
                assert replay is not None
                prior_actual = connection.execute(
                    "SELECT 1 FROM commercial_publications WHERE test_id=%s AND period_id=%s "
                    "AND kind='actual' LIMIT 1",
                    (test_id, period_id),
                ).fetchone()
                if not reasons or prior_actual:
                    intent_id = stage_intent(connection, test, replay)
                    if reasons:
                        withdrawal_id = intent_id
            if not replay["complete"] and withdrawal_id is None:
                withdrawal = connection.execute(
                    "SELECT dataset_id FROM commercial_publications WHERE test_id=%s "
                    "AND period_id=%s AND revision=%s AND kind='withdrawal'",
                    (test_id, replay["period_id"], replay["revision"]),
                ).fetchone()
                withdrawal_id = withdrawal["dataset_id"] if withdrawal else None
        # Retraction is mandatory, not an optional financial export. A failed
        # delivery leaves its durable intent visible and can be resumed explicitly.
        if withdrawal_id:
            try:
                self.publisher.deliver(withdrawal_id, synthetic)
            except LearningError as error:
                if error.code != "superseded_review":
                    raise
        return self._period(test_id, replay["period_id"], replay["revision"], synthetic)

    def _period(self, test_id: str, period_id: str, revision: int, synthetic: bool) -> dict:
        row = self.catalog.one(
            "SELECT r.*,p.dataset_id,p.state AS publication_state,p.kind AS publication_kind,"
            "c.revision AS current_revision "
            "FROM commercial_period_reviews r JOIN commercial_tests t USING(test_id) "
            "JOIN commercial_periods c USING(test_id,period_id) "
            "LEFT JOIN commercial_publications p ON p.test_id=r.test_id "
            "AND p.period_id=r.period_id AND p.revision=r.revision "
            "WHERE r.test_id=%s AND r.period_id=%s AND r.revision=%s AND t.synthetic=%s",
            (test_id, period_id, revision, synthetic),
        )
        if row is None:
            raise LearningError("period_not_found")
        return self._period_record(row)

    @staticmethod
    def _period_record(row: dict) -> dict:
        payload = row["payload"]
        profit = (
            (
                Decimal(payload["realized_revenue"])
                - sum((Decimal(value) for value in payload["realized_costs"].values()), Decimal(0))
            )
            if row["complete"]
            else None
        )
        return {
            **{
                name: row[name]
                for name in (
                    "test_id",
                    "period_id",
                    "revision",
                    "complete",
                    "reasons",
                    "dataset_id",
                    "publication_kind",
                )
            },
            "period": payload,
            "available_at": row["available_at"].isoformat(),
            "realized_profit": format(profit, "f") if profit is not None else None,
            "superseded": row["revision"] != row["current_revision"],
            "publication_state": (
                "superseded"
                if row["revision"] != row["current_revision"]
                and row["publication_state"] == "pending"
                and row["publication_kind"] != "withdrawal"
                else row["publication_state"] or "not_publishable"
            ),
            "scope": "reviewed_period_only_not_entire_launch",
        }

    def publish(
        self,
        test_id: str,
        period_id: str,
        revision: int,
        synthetic: bool = False,
    ) -> dict:
        self.test(test_id, synthetic)
        _request(revision, "publish", synthetic)
        period = self._period(test_id, period_id, revision, synthetic)
        if period["dataset_id"] is None:
            raise LearningError("incomplete_period")
        self.publisher.deliver(period["dataset_id"], synthetic)
        return self._period(test_id, period_id, revision, synthetic)

    def periods(
        self,
        test_id: str,
        synthetic: bool = False,
        limit: int = 100,
        offset: int = 0,
        *,
        include_history: bool = False,
    ) -> list[dict]:
        self.test(test_id, synthetic)
        _page(limit, offset, synthetic)
        if not isinstance(include_history, bool):
            raise LearningError("invalid_history_selector")
        rows = self.catalog.all(
            "SELECT r.*,x.dataset_id,x.state AS publication_state,x.kind AS publication_kind,"
            "p.revision AS current_revision "
            "FROM commercial_period_reviews r JOIN commercial_periods p USING(test_id,period_id) "
            "LEFT JOIN commercial_publications x ON x.test_id=r.test_id "
            "AND x.period_id=r.period_id AND x.revision=r.revision "
            "WHERE r.test_id=%s AND (%s OR r.revision=p.revision) "
            "ORDER BY p.period_start DESC,r.revision DESC,p.period_id LIMIT %s OFFSET %s",
            (test_id, include_history, limit, offset),
        )
        return [self._period_record(row) for row in rows]
