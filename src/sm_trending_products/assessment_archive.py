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
from .provenance import (
    MAX_NODE_BYTES,
    AnalyticalRecordTooLarge,
    Provenance,
    PublicationPlanLimit,
    PublicationScope,
)

RANK_BLOCK_SIZE = 200
REPRESENTATION = "shared-assessment-v1"
PACKED_REPRESENTATION = "packed-shared-assessment-v1"
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
    stable = dict(row)
    mobile = {}
    for index, path in enumerate(_MOBILE_PATHS):
        parent = stable
        for part in path[:-1]:
            if not isinstance(parent, dict) or part not in parent:
                break
            if not isinstance(parent[part], dict):
                break
            parent[part] = dict(parent[part])
            parent = parent[part]
        else:
            if isinstance(parent, dict) and path[-1] in parent:
                mobile[str(index)] = parent.pop(path[-1])
    return stable, mobile


def _join(stable: dict[str, Any], mobile: dict[str, Any]) -> dict[str, Any]:
    row = dict(stable)
    if not isinstance(mobile, dict) or not mobile.keys() <= _MOBILE_KEYS:
        raise ValueError("Invalid assessment archive overlay")
    for index, path in enumerate(_MOBILE_PATHS):
        if str(index) not in mobile:
            continue
        parent = row
        for part in path[:-1]:
            if not isinstance(parent.get(part), dict):
                raise ValueError("Invalid assessment archive base")
            parent[part] = dict(parent[part])
            parent = parent[part]
        parent[path[-1]] = mobile[str(index)]
    return row


def _same_json(left: Any, right: Any) -> bool:
    # Python considers 1 == 1.0 == True, but their canonical bytes differ.
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        if any(not isinstance(key, str) for key in left) or any(
            not isinstance(key, str) for key in right
        ):
            return canonical(left) == canonical(right)
        return left.keys() == right.keys() and all(
            _same_json(value, right[key]) for key, value in left.items()
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _same_json(a, b) for a, b in zip(left, right, strict=True)
        )
    if type(left) is float:
        return repr(left) == repr(right)
    if type(left) not in (str, int, bool, type(None)):
        return canonical(left) == canonical(right)
    return left == right


def _pack(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep common structure once, with exact typed variable columns per row."""
    paths: list[list[str]] = []
    columns: list[list[Any]] = []

    def visit(values, path):
        first = values[0]
        if all(_same_json(first, value) for value in values[1:]):
            return first
        if (
            isinstance(first, dict)
            and all(isinstance(key, str) for key in first)
            and first.get("format") not in {"sharded-provenance-v1", "sharded-records-v1"}
            and all(isinstance(value, dict) and value.keys() == first.keys() for value in values)
        ):
            return {
                key: visit([value[key] for value in values], [*path, key]) for key in sorted(first)
            }
        paths.append(path)
        columns.append(values)
        return None

    template = visit(rows, [])
    dictionaries = {}
    for index, values in enumerate(columns):
        if not isinstance(values[0], (str, list, dict)):
            continue
        identities: dict[str, int] = {}
        unique: list[Any] = []
        codes: list[int] = []
        total_bytes = 0
        unique_bytes = 0
        for value in values:
            token = canonical(value)
            total_bytes += len(token.encode())
            if token not in identities:
                identities[token] = len(unique)
                unique.append(value)
                unique_bytes += len(token.encode())
            codes.append(identities[token])
        # At most 200 rows: four bytes cover each code plus separator. Only
        # factor a variable column when this conservative bound saves bytes.
        if unique_bytes + 4 * len(values) + 64 < total_bytes:
            dictionaries[str(index)] = unique
            columns[index] = codes
    return {
        "template": template,
        "paths": paths,
        "dictionaries": dictionaries,
        "values": [list(values) for values in zip(*columns, strict=True)]
        if columns
        else [[] for _ in rows],
    }


def _unpack(packed: dict[str, Any], positions: list[int]) -> list[dict[str, Any]]:
    paths, values = packed["paths"], packed["values"]
    dictionaries = packed["dictionaries"]
    if not isinstance(paths, list) or not isinstance(values, list) or len(values) > 200:
        raise ValueError("Invalid assessment archive packed block")
    result = []
    for position in positions:
        if not isinstance(position, int) or not 0 <= position < len(values):
            raise ValueError("Invalid assessment archive packed position")
        cells = values[position]
        if not isinstance(cells, list) or len(cells) != len(paths):
            raise ValueError("Invalid assessment archive packed values")
        row = deepcopy(packed["template"])
        for column, (path, cell) in enumerate(zip(paths, cells, strict=True)):
            if str(column) in dictionaries:
                dictionary = dictionaries[str(column)]
                if type(cell) is not int or not 0 <= cell < len(dictionary):
                    raise ValueError("Invalid assessment archive dictionary code")
                cell = dictionary[cell]
            if not isinstance(path, list) or any(not isinstance(part, str) for part in path):
                raise ValueError("Invalid assessment archive packed path")
            if not path:
                row = deepcopy(cell)
                continue
            parent = row
            for part in path[:-1]:
                if not isinstance(parent, dict) or not isinstance(parent.get(part), dict):
                    raise ValueError("Invalid assessment archive packed template")
                parent = parent[part]
            if not isinstance(parent, dict):
                raise ValueError("Invalid assessment archive packed template")
            parent[path[-1]] = deepcopy(cell)
        if not isinstance(row, dict):
            raise ValueError("Invalid assessment archive packed row")
        result.append(row)
    return result


def _packed_segments(plan, rows, synthetic, start):
    try:
        part = plan.records({"rows": _pack(rows)}, synthetic)
        return [{"start": start, "stop": start + len(rows), "part": part}]
    except AnalyticalRecordTooLarge:
        if len(rows) == 1:
            raise
        middle = len(rows) // 2
        return [
            *_packed_segments(plan, rows[:middle], synthetic, start),
            *_packed_segments(plan, rows[middle:], synthetic, start + middle),
        ]


class AssessmentArchive:
    def __init__(self, catalog: Catalog, history: History):
        self.catalog, self.history = catalog, history
        self.provenance = Provenance(catalog, history, compressed=True)

    @staticmethod
    def _storage(body, run_id, stable, ranks, synthetic, plan, *, packed=False):
        if packed:
            ordered = sorted(stable)
            ordinals = {key: index for index, key in enumerate(ordered)}
            stable_root = plan.records(
                {
                    str(start // RANK_BLOCK_SIZE): _packed_segments(
                        plan,
                        [stable[key] for key in ordered[start : start + RANK_BLOCK_SIZE]],
                        synthetic,
                        start,
                    )
                    for start in range(0, len(ordered), RANK_BLOCK_SIZE)
                },
                synthetic,
            )
            ranks = [
                {"ordinal": ordinals[row["product_id"]], "mobile": row["mobile"]} for row in ranks
            ]
        else:
            stable_root = plan.records(stable, synthetic, ordered=False)
        return {
            "schema_version": "1",
            "representation": PACKED_REPRESENTATION if packed else REPRESENTATION,
            "logical_run_id": run_id,
            "header": {key: value for key, value in body.items() if key != "rows"},
            "rows": {
                "stable": stable_root,
                "ranks": plan.records(
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

    @staticmethod
    def _encode_storage(stored):
        encoded = canonical(stored).encode()
        if len(encoded) > MAX_NODE_BYTES:
            raise AnalyticalRecordTooLarge("Assessment archive header exceeds its byte budget")
        return compress_json(encoded)

    def _adaptive_storage(self, body, run_id, stable, ranks, synthetic):
        try:
            with self.provenance.plan() as current, self.provenance.plan() as packed:
                ordinary = self._storage(body, run_id, stable, ranks, synthetic, current)
                ordinary_bytes = self._encode_storage(ordinary)
                try:
                    templated = self._storage(
                        body, run_id, stable, ranks, synthetic, packed, packed=True
                    )
                    templated_bytes = self._encode_storage(templated)
                except AnalyticalRecordTooLarge:
                    # A single near-limit legacy value can fit the old envelope but
                    # not the template envelope. The candidate must not reduce the
                    # set of valid existing logical rows.
                    current.publish()
                    return ordinary, ordinary_bytes
                old_size, old_puts = current.cost()
                new_size, new_puts = packed.cost()
                # No provider price assumptions and no byte/PUT exchange: select
                # the new representation only when neither physical cost worsens.
                use_packed = (
                    new_size + len(templated_bytes) <= old_size + len(ordinary_bytes)
                    and new_puts <= old_puts
                )
                if use_packed:
                    packed.publish()
                    return templated, templated_bytes
                current.publish()
                return ordinary, ordinary_bytes
        except PublicationPlanLimit:
            # Both contexts close and discard metadata before any fallback
            # upload. The ordinary writer keeps its established 4 MB streaming
            # admission independently of the product universe size.
            pass
        stored = self._storage(body, run_id, stable, ranks, synthetic, self.provenance)
        return stored, self._encode_storage(stored)

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
            adaptive = self.catalog.one("SELECT 1 AS ready FROM schema_migrations WHERE version=40")
            if adaptive and len(stable) >= 32:
                stored, encoded = self._adaptive_storage(body, run_id, stable, ranks, synthetic)
            else:
                stored = self._storage(body, run_id, stable, ranks, synthetic, self.provenance)
                encoded = self._encode_storage(stored)
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
                stored.get("representation") not in {REPRESENTATION, PACKED_REPRESENTATION}
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
            if stored.get("representation") not in {REPRESENTATION, PACKED_REPRESENTATION}:
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
                if stored["representation"] == PACKED_REPRESENTATION:
                    rows = self._packed_rows(connection, index, entries)
                else:
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

    def _packed_rows(self, connection, index, entries):
        ordinals = [entry["ordinal"] for entry in entries]
        if any(type(value) is not int or not 0 <= value < index["count"] for value in ordinals):
            raise ValueError("Invalid assessment archive ordinal")
        if len(set(ordinals)) != len(ordinals):
            raise ValueError("Duplicate assessment archive ordinal")
        blocks = self.provenance.selected_records(
            index["stable"],
            sorted({str(value // RANK_BLOCK_SIZE) for value in ordinals}),
            connection=connection,
        )
        selected: dict[int, dict[str, Any]] = {}
        for segments in blocks.values():
            for segment in segments:
                start, stop = segment["start"], segment["stop"]
                wanted = [value for value in ordinals if start <= value < stop]
                if not wanted:
                    continue
                if not 0 < stop - start <= RANK_BLOCK_SIZE or selected.keys() & set(wanted):
                    raise ValueError("Invalid assessment archive segment")
                packed = self.provenance.selected_records(
                    segment["part"], ["rows"], connection=connection
                ).get("rows")
                if not isinstance(packed, dict) or len(packed["values"]) != stop - start:
                    raise ValueError("Invalid assessment archive packed block")
                selected.update(
                    zip(wanted, _unpack(packed, [value - start for value in wanted]), strict=True)
                )
        if len(selected) != len(entries):
            raise ValueError("Incomplete assessment archive product parts")
        return [_join(selected[entry["ordinal"]], entry["mobile"]) for entry in entries]

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
