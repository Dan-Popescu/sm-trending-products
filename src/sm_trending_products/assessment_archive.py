"""Lossless assessment history on the existing immutable publication lifecycle.

Only the archive owns its physical representation. Logical schema-2 rows and
their ranking order stay unchanged; stable keyed parts can be shared across
clock-only publications. Source input roots remain attached independently.
"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from .analytical_json import compress_json, read_json
from .cache import pin_cached
from .catalog import Catalog
from .contracts import canonical, digest
from .history import History
from .provenance import MAX_NODE_BYTES, Provenance, PublicationScope

RANK_BLOCK_SIZE = 200
REPRESENTATION = "shared-assessment-v1"
_MOBILE_PATHS = (
    ("as_of",),
    ("score",),
    ("features", "age_hours"),
    ("features", "cohort_growth_percentile"),
    ("attention", "score"),
    ("attention", "components", "cohort_growth_percentile", "value"),
)
_MOBILE_KEYS = frozenset(str(index) for index in range(len(_MOBILE_PATHS)))


@dataclass(frozen=True)
class AssessmentPublication:
    run_id: str
    body: dict[str, Any]
    descriptor: dict[str, Any]
    roots: tuple[dict[str, Any], ...]


def _split(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    stable = deepcopy(row)
    mobile = {}
    for index, path in enumerate(_MOBILE_PATHS):
        parent = stable
        for part in path[:-1]:
            if not isinstance(parent, dict) or part not in parent:
                break
            parent = parent[part]
        else:
            if isinstance(parent, dict) and path[-1] in parent:
                mobile[str(index)] = parent.pop(path[-1])
    return stable, mobile


def _join(stable: dict[str, Any], mobile: dict[str, Any]) -> dict[str, Any]:
    row = deepcopy(stable)
    if not isinstance(mobile, dict) or not mobile.keys() <= _MOBILE_KEYS:
        raise ValueError("Invalid assessment archive overlay")
    for index, path in enumerate(_MOBILE_PATHS):
        if str(index) not in mobile:
            continue
        parent = row
        for part in path[:-1]:
            if not isinstance(parent.get(part), dict):
                raise ValueError("Invalid assessment archive base")
            parent = parent[part]
        parent[path[-1]] = mobile[str(index)]
    return row


class AssessmentArchive:
    def __init__(self, catalog: Catalog, history: History):
        self.catalog, self.history = catalog, history
        self.provenance = Provenance(catalog, history, compressed=True)

    def stage(self, body: dict[str, Any], scope: PublicationScope) -> AssessmentPublication:
        logical = canonical(body).encode()
        run_id = hashlib.sha256(logical).hexdigest()
        synthetic = bool(body["synthetic"])
        with self.catalog.evidence_transaction(synthetic) as connection:
            self.catalog.guard_retention(connection)
            existing = connection.execute(
                "SELECT a.object_key,a.invalidated,d.state,d.object_sha256,d.object_bytes "
                "FROM assessment_runs a LEFT JOIN derived_publications d USING(object_key) "
                "WHERE a.run_id=%s",
                (run_id,),
            ).fetchone()
            if existing:
                if existing["invalidated"] or existing["state"] != "committed":
                    raise ValueError("Assessment archive was invalidated")
                checksum = existing["object_sha256"]
                if checksum is None and existing["object_key"].endswith(".json"):
                    checksum = run_id
                if checksum is None:
                    raise ValueError("Assessment archive physical checksum is missing")
                return AssessmentPublication(
                    run_id,
                    body,
                    {
                        "key": existing["object_key"],
                        "sha256": checksum,
                        "bytes": existing["object_bytes"],
                    },
                    (),
                )
        # The schema boundary is the deployment fence. Bridge readers on 35..37
        # retain plain writes while old signed images can still be running.
        modern = self.catalog.one("SELECT 1 AS ready FROM schema_migrations WHERE version=38")
        namespace = "synthetic" if synthetic else "live"
        if modern:
            del logical
            stable, ranks = {}, []
            for row in body["rows"]:
                product_id = row["product_id"]
                if product_id in stable:
                    raise ValueError("Assessment archive product ids must be unique")
                stable[product_id], mobile = _split(row)
                ranks.append({"product_id": product_id, "mobile": mobile})
            stored = {
                "schema_version": "1",
                "representation": REPRESENTATION,
                "logical_run_id": run_id,
                "header": {key: value for key, value in body.items() if key != "rows"},
                "rows": {
                    "stable": self.provenance.records(stable, synthetic, ordered=False),
                    "ranks": self.provenance.records(
                        {
                            str(start // RANK_BLOCK_SIZE): ranks[start : start + RANK_BLOCK_SIZE]
                            for start in range(0, len(ranks), RANK_BLOCK_SIZE)
                        },
                        synthetic,
                    ),
                    "count": len(ranks),
                    "block_size": RANK_BLOCK_SIZE,
                },
            }
            encoded = canonical(stored).encode()
            if len(encoded) > MAX_NODE_BYTES:
                raise ValueError("Assessment archive header exceeds its byte budget")
            encoded = compress_json(encoded)
            checksum = hashlib.sha256(encoded).hexdigest()
            key = f"assessments/{namespace}/{run_id}/{checksum}.json.gz"
            roots = tuple(self.provenance.roots(stored))
        else:
            encoded, checksum = logical, run_id
            key = f"assessments/{namespace}/{run_id}.json"
            roots = scope.roots
        descriptor = {"key": key, "sha256": checksum, "bytes": len(encoded)}
        self.catalog.stage_derivation(key, ())
        with self.catalog.transaction() as connection:
            self.catalog.guard_namespace(connection, synthetic)
            self.provenance.attach(connection, key, roots)
            prior = connection.execute(
                "SELECT object_sha256,object_bytes,state FROM derived_publications "
                "WHERE object_key=%s FOR UPDATE",
                (key,),
            ).fetchone()
            if prior is None:
                raise ValueError("Assessment archive staging record is missing")
            if prior["object_sha256"] is not None and (
                prior["object_sha256"],
                prior["object_bytes"],
            ) != (checksum, len(encoded)):
                raise ValueError("Immutable assessment archive collision")
            connection.execute(
                "UPDATE derived_publications SET object_sha256=%s,object_bytes=%s "
                "WHERE object_key=%s",
                (checksum, len(encoded), key),
            )
        self.history.put(
            key,
            encoded,
            content_type="application/gzip" if modern else "application/json",
        )
        return AssessmentPublication(run_id, body, descriptor, roots)

    def commit(self, connection, publication: AssessmentPublication) -> str:
        """Select one immutable run pointer with the current-row/alert transaction."""
        run_id = publication.run_id
        self.catalog.guard_namespace(connection, publication.body["synthetic"])
        self.catalog.guard_retention(connection)
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("assessment-archive:" + run_id,),
        )
        winner = connection.execute(
            "SELECT object_key,invalidated FROM assessment_runs WHERE run_id=%s", (run_id,)
        ).fetchone()
        if winner:
            if winner["invalidated"]:
                raise ValueError("Assessment archive was invalidated")
            self.provenance.guard(connection, [{"key": winner["object_key"]}])
            # A different compression runtime may produce different physical
            # bytes for the same logical run. Never replace the registered winner.
            return winner["object_key"]
        self.provenance.guard(connection, publication.roots)
        committed = connection.execute(
            "UPDATE derived_publications SET state='committed' "
            "WHERE object_key=%s AND state IN ('staged','committed') RETURNING object_key",
            (publication.descriptor["key"],),
        ).fetchone()
        if not committed:
            raise ValueError("Assessment archive was invalidated during publication")
        connection.execute(
            "INSERT INTO assessment_runs(run_id,as_of,synthetic,policy,object_key,"
            "evidence_datasets) VALUES(%s,%s,%s,%s,%s,'[]')",
            (
                run_id,
                publication.body["as_of"],
                publication.body["synthetic"],
                Jsonb(publication.body["policy"]),
                publication.descriptor["key"],
            ),
        )
        return publication.descriptor["key"]

    def _read(self, connection, run_id: str, synthetic: bool) -> dict[str, Any] | None:
        self.catalog.guard_retention(connection)
        row = connection.execute(
            "SELECT a.object_key,a.invalidated,d.state,d.object_sha256,d.object_bytes "
            "FROM assessment_runs a LEFT JOIN derived_publications d USING(object_key) "
            "WHERE a.run_id=%s AND a.synthetic=%s",
            (run_id, synthetic),
        ).fetchone()
        if not row:
            return None
        if row["invalidated"] or row["state"] not in (None, "committed"):
            raise ValueError("Assessment archive was invalidated")
        compressed = row["object_key"].endswith(".gz")
        checksum = row["object_sha256"] or (None if compressed else run_id)
        if not checksum:
            raise ValueError("Assessment archive physical checksum is missing")
        with pin_cached(
            self.history,
            {"key": row["object_key"], "sha256": checksum, "bytes": row["object_bytes"]},
        ) as path:
            stored = read_json(
                path.read_bytes(),
                compressed=compressed,
                max_bytes=MAX_NODE_BYTES if compressed else None,
            )
        if compressed:
            if (
                stored.get("representation") != REPRESENTATION
                or stored.get("schema_version") != "1"
                or stored.get("logical_run_id") != run_id
                or stored["rows"]["block_size"] != RANK_BLOCK_SIZE
            ):
                raise ValueError("Invalid assessment archive header")
        elif digest(stored) != run_id:
            raise ValueError("Assessment archive logical checksum mismatch")
        return stored

    def page(
        self, run_id: str, synthetic: bool = False, *, offset: int = 0, limit: int = 100
    ) -> dict[str, Any] | None:
        if (
            isinstance(offset, bool)
            or isinstance(limit, bool)
            or not isinstance(offset, int)
            or not isinstance(limit, int)
            or offset < 0
            or not 1 <= limit <= 200
        ):
            raise ValueError("Assessment archive pages require offset>=0 and 1<=limit<=200")
        with self.catalog.evidence_transaction(synthetic) as connection:
            stored = self._read(connection, run_id, synthetic)
            if stored is None:
                return None
            if stored.get("representation") != REPRESENTATION:
                count = len(stored["rows"])
                header = {key: value for key, value in stored.items() if key != "rows"}
                rows = stored["rows"][offset : offset + limit]
            else:
                header, index = stored["header"], stored["rows"]
                count = index["count"]
                end = min(count, offset + limit)
                block_ids = [
                    str(block)
                    for block in range(
                        offset // RANK_BLOCK_SIZE, (end + RANK_BLOCK_SIZE - 1) // RANK_BLOCK_SIZE
                    )
                ]
                blocks = self.provenance.selected_records(
                    index["ranks"], block_ids, connection=connection
                )
                entries = []
                for rank in range(offset, end):
                    block = blocks.get(str(rank // RANK_BLOCK_SIZE))
                    if not isinstance(block, list) or len(block) > RANK_BLOCK_SIZE:
                        raise ValueError("Invalid assessment archive rank block")
                    try:
                        entries.append(block[rank % RANK_BLOCK_SIZE])
                    except IndexError:
                        raise ValueError("Incomplete assessment archive rank block") from None
                bases = self.provenance.selected_records(
                    index["stable"],
                    [entry["product_id"] for entry in entries],
                    connection=connection,
                )
                if len(bases) != len(entries):
                    raise ValueError("Incomplete assessment archive product parts")
                rows = [_join(bases[entry["product_id"]], entry["mobile"]) for entry in entries]
            end = offset + len(rows)
            return {
                "run_id": run_id,
                **header,
                "rows": rows,
                "count": count,
                "next_offset": end if end < count else None,
            }

    def snapshot(self, run_id: str, synthetic: bool = False) -> dict[str, Any] | None:
        """Explicit full reconstruction; ordinary historical pages stay bounded."""
        page = self.page(run_id, synthetic, limit=200)
        if page is None:
            return None
        body = {key: value for key, value in page.items() if key not in {"count", "next_offset"}}
        while page["next_offset"] is not None:
            page = self.page(run_id, synthetic, offset=page["next_offset"], limit=200)
            assert page is not None
            body["rows"].extend(page["rows"])
        if digest({key: value for key, value in body.items() if key != "run_id"}) != run_id:
            raise ValueError("Assessment archive logical checksum mismatch")
        return body
