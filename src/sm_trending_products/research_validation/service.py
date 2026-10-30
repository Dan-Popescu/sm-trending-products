"""Durable preregistration, immutable policy locks and actual scoped replay."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from psycopg.types.json import Jsonb

from ..catalog import Catalog
from ..contracts import digest, utc
from ..evaluation_service import EvaluationService
from ..history import History
from ..identity import Identity
from ..module_schema import initialize_module, require_module
from ._families import assert_separated, family_registry
from ._schema import MIGRATIONS
from .models import PARTITIONS, ResearchError, ValidationProtocol


def _request(request_id: str, synthetic: bool) -> None:
    if (
        type(synthetic) is not bool
        or not isinstance(request_id, str)
        or not request_id.strip()
        or request_id != request_id.strip()
        or len(request_id) > 128
    ):
        raise ResearchError("invalid_request")


def _clock(connection, synthetic: bool, as_of: datetime | None) -> datetime:
    if as_of is not None and not synthetic:
        raise ResearchError("real_clock_cannot_be_supplied")
    return (
        utc(as_of)
        if as_of is not None
        else connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]
    )


def _lineage_lock(connection, name: str, synthetic: bool) -> None:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("research-lineage:" + str(synthetic) + ":" + name,),
    )


def _locked_protocol(connection, protocol_id: str, synthetic: bool) -> dict:
    row = connection.execute(
        "SELECT name FROM research_protocols WHERE protocol_id=%s AND synthetic=%s",
        (protocol_id, synthetic),
    ).fetchone()
    if not row:
        raise ResearchError("protocol_not_found")
    _lineage_lock(connection, row["name"], synthetic)
    return connection.execute(
        "SELECT * FROM research_protocols WHERE protocol_id=%s AND synthetic=%s FOR UPDATE",
        (protocol_id, synthetic),
    ).fetchone()


class ResearchValidation:
    """Optional research boundary; never launches, collects or trains a model."""

    def __init__(self, catalog: Catalog, history: History, identity: Identity):
        self.catalog = catalog
        self.evaluations = EvaluationService(catalog, history, identity)

    def initialize(self) -> dict:
        return initialize_module(self.catalog, "research_validation", MIGRATIONS)

    def _require(self) -> None:
        require_module(self.catalog, "research_validation", 2)

    def register(
        self,
        protocol: ValidationProtocol,
        request_id: str,
        *,
        synthetic: bool = False,
        as_of: datetime | None = None,
    ) -> dict:
        _request(request_id, synthetic)
        # Nested Pydantic instances are mutable. Always reparse their JSON value.
        protocol = ValidationProtocol.model_validate(protocol.model_dump(mode="json"))
        self._require()
        payload = protocol.model_dump(mode="json")
        fingerprint = digest(
            {"payload": payload, "as_of": utc(as_of).isoformat() if as_of else None}
        )
        protocol_id = digest({"protocol": payload, "synthetic": synthetic})
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            self.catalog.guard_retention(connection)
            _lineage_lock(connection, protocol.name, synthetic)
            replay = connection.execute(
                "SELECT * FROM research_protocols WHERE synthetic=%s AND request_id=%s",
                (synthetic, request_id),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != fingerprint:
                    raise ResearchError("request_conflict")
                protocol_id = replay["protocol_id"]
            else:
                if connection.execute(
                    "SELECT 1 FROM research_lineage_freezes WHERE name=%s AND synthetic=%s",
                    (protocol.name, synthetic),
                ).fetchone():
                    raise ResearchError("opened_protocol_cannot_be_revised")
                existing = connection.execute(
                    "SELECT protocol_id FROM research_protocols WHERE name=%s AND revision=%s "
                    "AND synthetic=%s",
                    (protocol.name, protocol.revision, synthetic),
                ).fetchone()
                if existing:
                    raise ResearchError("protocol_revision_conflict")
                if protocol.previous_protocol_id:
                    previous = connection.execute(
                        "SELECT * FROM research_protocols WHERE protocol_id=%s AND synthetic=%s",
                        (protocol.previous_protocol_id, synthetic),
                    ).fetchone()
                    if (
                        not previous
                        or previous["name"] != protocol.name
                        or previous["revision"] != protocol.revision - 1
                    ):
                        raise ResearchError("invalid_previous_protocol")
                timestamp = _clock(connection, synthetic, as_of)
                registry = family_registry(self.catalog, protocol, timestamp, synthetic)
                connection.execute(
                    "INSERT INTO research_protocols(protocol_id,name,revision,synthetic,request_id,"
                    "request_hash,payload,registered_at,family_registry) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        protocol_id,
                        protocol.name,
                        protocol.revision,
                        synthetic,
                        request_id,
                        fingerprint,
                        Jsonb(payload),
                        timestamp,
                        Jsonb(registry),
                    ),
                )
        return self.protocol(protocol_id, synthetic)

    def protocol(self, protocol_id: str, synthetic: bool = False) -> dict:
        _request(protocol_id, synthetic)
        self._require()
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            row = connection.execute(
                "SELECT p.*,l.policy_id AS locked_policy_id,l.locked_at "
                "FROM research_protocols p LEFT JOIN research_policy_locks l USING(protocol_id) "
                "WHERE p.protocol_id=%s AND p.synthetic=%s",
                (protocol_id, synthetic),
            ).fetchone()
            if not row:
                raise ResearchError("protocol_not_found")
            return {
                "protocol_id": row["protocol_id"],
                "protocol": row["payload"],
                "synthetic": row["synthetic"],
                "registered_at": row["registered_at"].isoformat(),
                "locked_policy_id": row["locked_policy_id"],
                "locked_at": row["locked_at"].isoformat() if row["locked_at"] else None,
                "registration_timing": (
                    "prospective_predeclared"
                    if row["registered_at"]
                    <= min(
                        utc(date)
                        for partition in row["payload"]["partitions"].values()
                        for date in partition["decision_times"]
                    )
                    else "retrospective_governed"
                ),
                "heldout_timing": {
                    name: (
                        "prospective_predeclared"
                        if row["locked_at"] is not None
                        and max(row["registered_at"], row["locked_at"])
                        <= min(utc(date) for date in value["decision_times"])
                        else "retrospective_governed"
                    )
                    for name, value in row["payload"]["partitions"].items()
                    if name != "development"
                },
                "limits": [
                    "Access lock covers this interface, not prior operator knowledge",
                    "No fitting, causal estimate or commercial effectiveness is established",
                    "Population is the declared observed panel, not the global market",
                    "Unknown family fallback does not establish true family independence",
                ],
            }

    def lock(
        self,
        protocol_id: str,
        policy_id: str,
        request_id: str,
        synthetic: bool = False,
        *,
        as_of: datetime | None = None,
    ) -> dict:
        _request(request_id, synthetic)
        _request(protocol_id, synthetic)
        _request(policy_id, synthetic)
        self._require()
        fingerprint = digest(
            {"policy_id": policy_id, "as_of": utc(as_of).isoformat() if as_of else None}
        )
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            row = _locked_protocol(connection, protocol_id, synthetic)
            if policy_id not in row["payload"]["policies"]:
                raise ResearchError("policy_not_registered")
            prior = connection.execute(
                "SELECT * FROM research_policy_locks WHERE protocol_id=%s", (protocol_id,)
            ).fetchone()
            if prior:
                if (
                    prior["policy_id"] != policy_id
                    or prior["request_id"] != request_id
                    or prior["request_hash"] != fingerprint
                ):
                    raise ResearchError("policy_already_locked")
            else:
                freeze = connection.execute(
                    "SELECT protocol_id FROM research_lineage_freezes WHERE name=%s "
                    "AND synthetic=%s",
                    (row["name"], synthetic),
                ).fetchone()
                if freeze and freeze["protocol_id"] != protocol_id:
                    raise ResearchError("lineage_heldout_already_opened")
                timestamp = _clock(connection, synthetic, as_of)
                if timestamp < row["registered_at"]:
                    raise ResearchError("lock_precedes_registration")
                prior_cutoff = connection.execute(
                    "SELECT max(as_of) AS value FROM research_attempts WHERE protocol_id=%s "
                    "AND partition_name='development'",
                    (protocol_id,),
                ).fetchone()
                if prior_cutoff and prior_cutoff["value"] and timestamp < prior_cutoff["value"]:
                    raise ResearchError("lock_precedes_development_evidence")
                connection.execute(
                    "INSERT INTO research_policy_locks(protocol_id,policy_id,request_id,"
                    "request_hash,locked_at) VALUES(%s,%s,%s,%s,%s)",
                    (protocol_id, policy_id, request_id, fingerprint, timestamp),
                )
        return self.protocol(protocol_id, synthetic)

    def run(
        self,
        protocol_id: str,
        partition: str,
        policy_id: str,
        request_id: str,
        *,
        synthetic: bool = False,
        as_of: datetime | None = None,
    ) -> dict:
        for value in (protocol_id, partition, policy_id, request_id):
            _request(value, synthetic)
        self._require()
        fingerprint = digest(
            {
                "partition": partition,
                "policy_id": policy_id,
                "as_of": utc(as_of).isoformat() if as_of else None,
            }
        )
        attempt_id = digest({"protocol_id": protocol_id, "request_id": request_id})
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            row = _locked_protocol(connection, protocol_id, synthetic)
            prior = connection.execute(
                "SELECT * FROM research_attempts WHERE protocol_id=%s AND request_id=%s",
                (protocol_id, request_id),
            ).fetchone()
            if prior:
                if prior["request_hash"] != fingerprint:
                    raise ResearchError("request_conflict")
                replay = True
            else:
                protocol = ValidationProtocol.model_validate(row["payload"])
                count_row = connection.execute(
                    "SELECT count(*) AS value FROM research_attempts WHERE protocol_id=%s",
                    (protocol_id,),
                ).fetchone()
                assert count_row is not None
                count = count_row["value"]
                if count >= protocol.max_attempts:
                    raise ResearchError("attempt_capacity_exhausted")
                timestamp = _clock(connection, synthetic, as_of)
                connection.execute(
                    "INSERT INTO research_attempts(attempt_id,protocol_id,request_id,request_hash,"
                    "partition_name,policy_id,as_of,state) VALUES(%s,%s,%s,%s,%s,%s,%s,'running')",
                    (
                        attempt_id,
                        protocol_id,
                        request_id,
                        fingerprint,
                        partition,
                        policy_id,
                        timestamp,
                    ),
                )
                if partition in {"test", "holdout"}:
                    freeze = connection.execute(
                        "SELECT protocol_id FROM research_lineage_freezes WHERE name=%s "
                        "AND synthetic=%s",
                        (row["name"], synthetic),
                    ).fetchone()
                    if freeze and freeze["protocol_id"] != protocol_id:
                        connection.execute(
                            "UPDATE research_attempts SET state='failed',"
                            "error_code='lineage_heldout_already_opened',"
                            "completed_at=clock_timestamp() WHERE attempt_id=%s",
                            (attempt_id,),
                        )
                        replay = True
                    else:
                        lock = connection.execute(
                            "SELECT * FROM research_policy_locks WHERE protocol_id=%s",
                            (protocol_id,),
                        ).fetchone()
                        if (
                            lock
                            and lock["locked_at"] <= timestamp
                            and timestamp >= row["registered_at"]
                            and all(
                                date <= timestamp
                                for date in protocol.partitions[
                                    "test" if partition == "test" else "holdout"
                                ].decision_times
                            )
                        ):
                            connection.execute(
                                "INSERT INTO research_lineage_freezes(name,synthetic,protocol_id,"
                                "policy_id,opened_at) VALUES(%s,%s,%s,%s,clock_timestamp()) "
                                "ON CONFLICT DO NOTHING",
                                (row["name"], synthetic, protocol_id, lock["policy_id"]),
                            )
                        replay = False
                else:
                    replay = False
        if replay:
            return self.attempt(attempt_id, synthetic)
        try:
            protocol = ValidationProtocol.model_validate(row["payload"])
            if partition not in PARTITIONS:
                raise ResearchError("partition_not_registered")
            if policy_id not in protocol.policies:
                raise ResearchError("policy_not_registered")
            scope = protocol.partitions[partition]
            if timestamp < row["registered_at"]:
                raise ResearchError("attempt_precedes_registration")
            if any(date > timestamp for date in scope.decision_times):
                raise ResearchError("future_decision")
            lock = self.catalog.one(
                "SELECT * FROM research_policy_locks WHERE protocol_id=%s", (protocol_id,)
            )
            if partition != "development" and (
                not lock or lock["policy_id"] != policy_id or lock["locked_at"] > timestamp
            ):
                raise ResearchError("heldout_requires_prior_selected_policy_lock")
            registry = family_registry(self.catalog, protocol, timestamp, synthetic)
            merged = {
                key: sorted(set(values) | set(row["family_registry"][key]))
                for key, values in registry.items()
            }
            assert_separated(protocol, merged)
            config = protocol.config.model_dump(mode="json")
            config["split"] = "validation" if partition == "development" else "test"
            if scope.profit_contract is not None:
                config["profit_contract"] = scope.profit_contract.model_dump(mode="json")
            position = PARTITIONS.index(partition)
            evaluation_cutoff = timestamp
            if position + 1 < len(PARTITIONS):
                next_scope = protocol.partitions[PARTITIONS[position + 1]]
                evaluation_cutoff = min(
                    timestamp, next_scope.decision_times[0] - timedelta(microseconds=1)
                )
            execution = self.evaluations.scoped_backtest(
                list(scope.decision_times),
                evaluation_cutoff,
                synthetic,
                config,
                policy=protocol.policies[policy_id],
                product_ids=scope.product_ids,
                measurement_sample_size=protocol.measurement_sample_size,
                seed=protocol.seed,
            )
            with self.catalog.transaction() as connection:
                self.catalog.guard_namespace(connection, synthetic)
                evaluation = connection.execute(
                    "SELECT payload,invalidated FROM evaluation_runs WHERE evaluation_id=%s",
                    (execution["evaluation_id"],),
                ).fetchone()
                if not evaluation or evaluation["invalidated"]:
                    raise ResearchError("evaluation_invalidated")
                self.evaluations.provenance.guard(
                    connection, self.evaluations.provenance.roots(evaluation["payload"])
                )
                connection.execute(
                    "UPDATE research_attempts SET state='completed',evaluation_id=%s,"
                    "completed_at=clock_timestamp() WHERE attempt_id=%s AND state='running'",
                    (execution["evaluation_id"], attempt_id),
                )
        except Exception as error:
            code = str(error) if isinstance(error, ResearchError) else type(error).__name__
            self.catalog.execute(
                "UPDATE research_attempts SET state='failed',error_code=%s,"
                "completed_at=clock_timestamp() WHERE attempt_id=%s AND state='running'",
                (code, attempt_id),
            )
        return self.attempt(attempt_id, synthetic)

    def attempt(self, attempt_id: str, synthetic: bool = False) -> dict:
        _request(attempt_id, synthetic)
        self._require()
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            row = connection.execute(
                "SELECT a.*,p.synthetic FROM research_attempts a JOIN research_protocols p "
                "USING(protocol_id) WHERE a.attempt_id=%s AND p.synthetic=%s",
                (attempt_id, synthetic),
            ).fetchone()
            if not row:
                raise ResearchError("attempt_not_found")
            result: dict[str, Any] = {
                key: row[key]
                for key in (
                    "attempt_id",
                    "protocol_id",
                    "request_id",
                    "partition_name",
                    "policy_id",
                    "state",
                    "evaluation_id",
                    "error_code",
                    "synthetic",
                )
            }
            result["as_of"] = row["as_of"].isoformat()
            if row["evaluation_id"]:
                evaluation = connection.execute(
                    "SELECT payload,invalidated FROM evaluation_runs WHERE evaluation_id=%s",
                    (row["evaluation_id"],),
                ).fetchone()
                if evaluation and not evaluation["invalidated"]:
                    self.evaluations.provenance.guard(
                        connection, self.evaluations.provenance.roots(evaluation["payload"])
                    )
                    result["result"] = evaluation["payload"]
                    result["evaluation_cutoff"] = evaluation["payload"]["as_of"]
                else:
                    result["result_unavailable"] = "evidence_invalidated"
            return result

    def compare(self, attempt_ids: tuple[str, ...], synthetic: bool = False) -> dict:
        """Inspect 2–8 existing development attempts, without execution or selection."""
        from ._comparison import compare

        return compare(self, attempt_ids, synthetic)

    def attempts(
        self, protocol_id: str, synthetic: bool = False, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        _request(protocol_id, synthetic)
        if type(limit) is not int or not 1 <= limit <= 128 or type(offset) is not int or offset < 0:
            raise ResearchError("invalid_page")
        self.protocol(protocol_id, synthetic)
        rows = self.catalog.all(
            "SELECT attempt_id FROM research_attempts WHERE protocol_id=%s "
            "ORDER BY created_at,attempt_id LIMIT %s OFFSET %s",
            (protocol_id, limit, offset),
        )
        return [self.attempt(row["attempt_id"], synthetic) for row in rows]
