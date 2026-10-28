"""Durable deterministic ledger delivery, outside learning SQL transactions."""

from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..contracts import Observation, digest
from ..history import History
from ..identity import Identity
from .models import LearningError

SOURCE = "commerce_actuals"


def stage_intent(connection, test: dict, review: dict | None = None) -> str:
    """Persist exact bytes' meaning before any publication call can fail."""
    kind = "declaration" if review is None else "actual" if review["complete"] else "withdrawal"
    identity = {
        "kind": kind,
        "test_id": test["test_id"],
        "period_id": review["period_id"] if review else None,
        "revision": review["revision"] if review else None,
    }
    attributes: dict[str, Any] = {
        "commercial_test_id": test["test_id"],
        "dossier_id": test["dossier_id"],
        "dossier_revision": test["dossier_revision"],
        "country": test["country"],
        "currency": test["currency"],
        "policy_references": test["policy_references"],
        "selection_policy_hash": digest(test["plan"]["selection_policy"]),
        "acquisition_method": "reviewed_own_commerce",
        "realized": kind == "actual",
        "costs_complete": kind == "actual",
        "publication_kind": kind,
    }
    if review:
        payload = review["payload"]
        attributes.update(
            {
                "period_start": payload["period_start"],
                "period_end": payload["period_end"],
                "review_revision": review["revision"],
                "actual_evidence_refs": payload["evidence_refs"],
            }
        )
        if kind == "actual":
            attributes.update(
                {
                    "realized_revenue": payload["realized_revenue"],
                    "realized_costs": payload["realized_costs"],
                }
            )
    observation = Observation(
        observation_id=digest(identity),
        source=SOURCE,
        entity_type="cost",
        entity_id=test["test_id"],
        synthetic=test["synthetic"],
        market=test["country"],
        observed_at=review["available_at"] if review else test["planned_at"],
        available_at=review["available_at"] if review else test["planned_at"],
        attributes=attributes,
    ).model_dump(mode="json")
    dataset_id = digest({"commercial_learning": observation})
    connection.execute(
        "INSERT INTO commercial_publications("
        "dataset_id,test_id,period_id,revision,kind,observation) "
        "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(dataset_id) DO NOTHING",
        (
            dataset_id,
            test["test_id"],
            identity["period_id"],
            identity["revision"],
            kind,
            Jsonb(observation),
        ),
    )
    return dataset_id


class Publisher:
    def __init__(self, catalog: Catalog, history: History, identity: Identity):
        self.catalog, self.history, self.identity = catalog, history, identity

    def deliver(self, dataset_id: str, synthetic: bool) -> dict:
        intent = self.catalog.one(
            "SELECT p.*,t.product_id,t.ledger_key,t.synthetic FROM commercial_publications p "
            "JOIN commercial_tests t USING(test_id) WHERE p.dataset_id=%s AND t.synthetic=%s",
            (dataset_id, synthetic),
        )
        if intent is None:
            raise LearningError("publication_not_found")
        if intent["state"] == "published":
            return {"dataset_id": dataset_id, "kind": intent["kind"], "state": "published"}

        def guard(connection):
            if intent["period_id"] is not None:
                row = connection.execute(
                    "SELECT revision FROM commercial_periods WHERE test_id=%s "
                    "AND period_id=%s FOR SHARE",
                    (intent["test_id"], intent["period_id"]),
                ).fetchone()
                # A missing historical retraction would leave a known-incomplete
                # interval profitable forever. Its immutable original availability
                # repairs that interval without overriding newer complete actuals.
                historical_withdrawal = (
                    intent["kind"] == "withdrawal"
                    and row is not None
                    and intent["revision"] < row["revision"]
                )
                if row is None or (
                    row["revision"] != intent["revision"] and not historical_withdrawal
                ):
                    raise LearningError("superseded_review")

        with self.catalog.transaction() as connection:
            guard(connection)
        observation = Observation.model_validate(intent["observation"])
        # History retains its authoritative source-retention fencing. No learning
        # transaction/row lock is held across these object-store operations.
        self.history.publish(
            [observation],
            {
                "schema_version": "1",
                "kind": "commercial_learning",
                "records": [intent["observation"]],
            },
            source=SOURCE,
            synthetic=synthetic,
            dataset_id=dataset_id,
            kind="commercial_learning",
            publication_guard=guard,
        )
        decision = None
        if intent["kind"] == "declaration":
            decision = self.identity.review_link(
                intent["product_id"],
                intent["ledger_key"],
                method="commercial-test-predeclared-v1",
                evidence_refs=[observation.observation_id],
            )
        with self.catalog.transaction() as connection:
            connection.execute(
                "UPDATE commercial_publications SET state='published',"
                "published_at=COALESCE(published_at,clock_timestamp()) WHERE dataset_id=%s",
                (dataset_id,),
            )
            if decision is not None:
                connection.execute(
                    "UPDATE commercial_tests SET state='ready',ledger_declared_at=%s,"
                    "ledger_decision_id=%s WHERE test_id=%s",
                    (decision["available_at"], decision["decision_id"], intent["test_id"]),
                )
        return {"dataset_id": dataset_id, "kind": intent["kind"], "state": "published"}
