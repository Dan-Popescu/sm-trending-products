"""Bounded immutable provenance trees for compact analytical publications.

PostgreSQL owns the existing staged/committed/invalidated lifecycle. This module
only adds immutable dependency edges and hash-addressed S3 nodes. Source payloads
remain in history; a reference page never hydrates a whole analytical universe.
"""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .cache import pin_cached
from .catalog import Catalog
from .contracts import Observation, ProductLink, canonical, utc
from .history import History

LEAF_REFERENCES = 4096
BRANCH_CHILDREN = 16
MAX_NODE_BYTES = 256_000
MAX_DEPTH = 64
NODE_REUSE_CACHE = 8192


class Provenance:
    def __init__(self, catalog: Catalog, history: History):
        self.catalog, self.history = catalog, history
        self._published: OrderedDict[str, dict[str, Any]] = OrderedDict()

    @staticmethod
    def roots(value: Any) -> list[dict[str, Any]]:
        """Find only exposed tree roots; descendants are already linked in SQL."""
        found: dict[str, dict[str, Any]] = {}

        def visit(item: Any) -> None:
            if isinstance(item, dict):
                if item.get("format") in {"sharded-provenance-v1", "sharded-records-v1"}:
                    root = item["root"]
                    found[root["key"]] = root
                else:
                    for child in item.values():
                        visit(child)
            elif isinstance(item, (list, tuple)):
                for child in item:
                    visit(child)

        visit(value)
        return [found[key] for key in sorted(found)]

    def guard(self, connection, roots: Sequence[dict[str, Any]]) -> dict[str, int]:
        self.catalog.guard_retention(connection)
        keys = sorted({root["key"] for root in roots})
        identities: dict[str, int] = {}
        for start in range(0, len(keys), 1000):
            chunk = keys[start : start + 1000]
            rows = connection.execute(
                "SELECT object_key,object_id FROM derived_publications "
                "WHERE object_key=ANY(%s) AND state='committed'",
                (chunk,),
            ).fetchall()
            if len(rows) != len(chunk):
                raise ValueError("Analytical provenance was invalidated")
            identities.update((row["object_key"], row["object_id"]) for row in rows)
        return identities

    def _node(
        self,
        body: dict[str, Any],
        synthetic: bool,
        *,
        inputs: Sequence[str] = (),
        children: Sequence[dict[str, Any]] = (),
    ) -> dict[str, Any]:
        encoded = canonical(body).encode()
        if len(encoded) > MAX_NODE_BYTES:
            raise ValueError("Provenance node exceeds its byte budget")
        checksum = hashlib.sha256(encoded).hexdigest()
        key = f"provenance/{'synthetic' if synthetic else 'live'}/{checksum}.json"
        descriptor = {
            "key": key,
            "sha256": checksum,
            "bytes": len(encoded),
            "count": body["count"],
        }
        if key in self._published:
            # A publication/page still checks authoritative root state under the
            # retention fence. This bounded local memo is never an eligibility gate.
            self._published.move_to_end(key)
            return descriptor
        expected_prefix = f"provenance/{'synthetic' if synthetic else 'live'}/"
        if any(not child["key"].startswith(expected_prefix) for child in children):
            raise ValueError("Provenance children must share their workspace")
        dependencies = sorted(set(inputs))
        # Register dependencies before any upload so retention can fence crashes.
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, dependencies)
            child_ids = self.guard(connection, children)
            connection.execute(
                "INSERT INTO derived_publications(object_key,evidence_datasets,state,"
                "object_sha256,object_bytes) VALUES(%s,'[]','staged',%s,%s) "
                "ON CONFLICT DO NOTHING",
                (key, checksum, len(encoded)),
            )
            prior = connection.execute(
                "SELECT object_id,state,object_sha256,object_bytes FROM derived_publications "
                "WHERE object_key=%s FOR UPDATE",
                (key,),
            ).fetchone()
            if not prior or prior["state"] in {"invalidated", "deleted"}:
                raise ValueError("Provenance object was retired")
            if (prior["object_sha256"], prior["object_bytes"]) != (checksum, len(encoded)):
                raise ValueError("Immutable provenance descriptor collision")
            if prior["state"] == "committed":
                self._remember(descriptor)
                return descriptor
            with connection.pipeline():
                for dataset in dependencies:
                    connection.execute(
                        "INSERT INTO derived_artifact_inputs(object_id,dataset_id) "
                        "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                        (prior["object_id"], dataset),
                    )
                for child in children:
                    connection.execute(
                        "INSERT INTO derived_artifact_edges(parent_id,child_id) "
                        "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                        (prior["object_id"], child_ids[child["key"]]),
                    )
        # No SQL transaction spans the network upload. A tombstone rejects a race.
        self.history.put(key, encoded, content_type="application/json")
        with self.catalog.transaction() as connection:
            self.catalog.guard_inputs(connection, dependencies)
            self.guard(connection, children)
            committed = connection.execute(
                "UPDATE derived_publications SET state='committed' "
                "WHERE object_id=%s AND state IN ('staged','committed') RETURNING object_id",
                (prior["object_id"],),
            ).fetchone()
            if not committed:
                raise ValueError("Provenance was invalidated during publication")
        self._remember(descriptor)
        return descriptor

    def _remember(self, descriptor: dict[str, Any]) -> None:
        self._published[descriptor["key"]] = descriptor
        self._published.move_to_end(descriptor["key"])
        if len(self._published) > NODE_REUSE_CACHE:
            self._published.popitem(last=False)

    def references(
        self,
        references: Sequence[str],
        deliveries: Mapping[str, str],
        synthetic: bool,
        *,
        partitions: Mapping[str, str] | None = None,
    ) -> dict[str, Any] | list:
        """Publish a stable hash trie; appends do not shift every following leaf."""
        values = sorted(set(references))
        if not values:
            return []
        unknown = set(values).difference(deliveries)
        if unknown:
            raise ValueError("Provenance references require their source delivery")

        def build(items: list[str], depth: int, partition: str) -> dict[str, Any]:
            datasets = sorted({deliveries[value] for value in items})
            body = {
                "schema_version": "1",
                "kind": "references",
                "count": len(items),
                "references": items,
                "datasets": datasets,
                "partition": partition,
            }
            if len(items) <= LEAF_REFERENCES and len(canonical(body).encode()) <= MAX_NODE_BYTES:
                return self._node(body, synthetic, inputs=datasets)
            if depth >= MAX_DEPTH:
                raise ValueError("Provenance identifier cannot fit a bounded leaf")
            groups: dict[str, list[str]] = defaultdict(list)
            for value in items:
                groups[hashlib.sha256(value.encode()).hexdigest()[depth]].append(value)
            children = [build(groups[key], depth + 1, partition) for key in sorted(groups)]
            return self._node(
                {
                    "schema_version": "1",
                    "kind": "branch",
                    "count": sum(child["count"] for child in children),
                    "children": children,
                },
                synthetic,
                children=children,
            )

        by_partition: dict[str, list[str]] = defaultdict(list)
        for value in values:
            by_partition[partitions[value] if partitions is not None else ""].append(value)

        def join(items: list[tuple[str, dict[str, Any]]], depth: int) -> dict[str, Any]:
            if len(items) == 1:
                return items[0][1]
            if len(items) <= BRANCH_CHILDREN:
                children = [node for _partition, node in items]
            else:
                if depth >= MAX_DEPTH:
                    raise ValueError("Provenance partition collision limit exceeded")
                groups: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
                for partition, node in items:
                    groups[hashlib.sha256(partition.encode()).hexdigest()[depth]].append(
                        (partition, node)
                    )
                children = [join(groups[key], depth + 1) for key in sorted(groups)]
            return self._node(
                {
                    "schema_version": "1",
                    "kind": "branch",
                    "count": sum(child["count"] for child in children),
                    "children": children,
                },
                synthetic,
                children=children,
            )

        root = join([(key, build(items, 0, key)) for key, items in sorted(by_partition.items())], 0)
        return {"format": "sharded-provenance-v1", "root": root, "count": root["count"]}

    def audit(self, observations: Sequence[Observation], synthetic: bool) -> Any:
        """Retain every loaded fact, including evidence explaining a censored outcome."""
        return self.references(
            [row.observation_id for row in observations],
            {
                row.observation_id: row.raw_dataset_id
                for row in observations
                if row.raw_dataset_id is not None
            },
            synthetic,
            partitions={
                row.observation_id: utc(row.observed_at).date().isoformat() for row in observations
            },
        )

    def audit_products(
        self,
        observations: Sequence[Observation],
        owners: Mapping[str, ProductLink],
        synthetic: bool,
    ) -> list[Any]:
        """Retain input scopes even when metrics, capability or market exclude a row."""
        grouped: dict[str, list[str]] = defaultdict(list)
        deliveries, partitions = {}, {}
        for row in observations:
            owner = owners.get(row.entity_key)
            if owner is not None and row.raw_dataset_id is not None:
                grouped[owner.product_id].append(row.observation_id)
                deliveries[row.observation_id] = row.raw_dataset_id
                partitions[row.observation_id] = utc(row.observed_at).date().isoformat()
        return [
            self.references(grouped[key], deliveries, synthetic, partitions=partitions)
            for key in sorted(grouped)
        ]

    def records(self, records: Mapping[str, Any], synthetic: bool) -> dict[str, Any] | list:
        """Store bounded analytical records with stable content-addressed partitions."""
        if not records:
            return []
        positions = {key: index for index, key in enumerate(records)}

        def build(items: list[tuple[str, Any]], prefix: str) -> dict[str, Any]:
            depth = len(prefix)
            body = {
                "schema_version": "1",
                "kind": "records",
                "count": len(items),
                "records": [
                    {"id": key, "value": value, "position": positions[key]} for key, value in items
                ],
                "prefix": prefix,
            }
            if len(items) <= 100 and len(canonical(body).encode()) <= MAX_NODE_BYTES:
                return {
                    **self._node(body, synthetic, children=self.roots(body["records"])),
                    "prefix": prefix,
                }
            if len(items) == 1 or depth >= MAX_DEPTH:
                raise ValueError("Analytical record exceeds its bounded page budget")
            groups: dict[str, list[tuple[str, Any]]] = defaultdict(list)
            for key, value in items:
                groups[hashlib.sha256(key.encode()).hexdigest()[depth]].append((key, value))
            children = [build(groups[key], prefix + key) for key in sorted(groups)]
            node = self._node(
                {
                    "schema_version": "1",
                    "kind": "branch",
                    "count": sum(child["count"] for child in children),
                    "children": children,
                    "prefix": prefix,
                },
                synthetic,
                children=children,
            )
            return {**node, "prefix": prefix}

        root = build(sorted(records.items()), "")
        return {"format": "sharded-records-v1", "root": root, "count": root["count"]}

    def record(self, value: Any, record_id: str) -> Any:
        """Look up one record through hash prefixes, without scanning sibling pages."""
        if not isinstance(value, dict) or value.get("format") != "sharded-records-v1":
            raise ValueError("A record lookup requires sharded analytical records")
        fingerprint = hashlib.sha256(record_id.encode()).hexdigest()
        node = value["root"]
        with self.catalog.transaction() as connection:
            self.guard(connection, [node])
            for _depth in range(MAX_DEPTH + 1):
                with pin_cached(self.history, node) as path:
                    body = json.loads(path.read_bytes())
                if body.get("count") != node["count"] or body.get("prefix") != node["prefix"]:
                    raise ValueError("Invalid analytical record index")
                if body["kind"] == "records":
                    for item in body["records"]:
                        if item["id"] == record_id:
                            return item["value"]
                    return None
                if body["kind"] != "branch" or len(body["children"]) > BRANCH_CHILDREN:
                    raise ValueError("Invalid analytical record branch")
                matching = [
                    child for child in body["children"] if fingerprint.startswith(child["prefix"])
                ]
                if not matching:
                    return None
                if len(matching) != 1 or len(matching[0]["prefix"]) <= len(node["prefix"]):
                    raise ValueError("Invalid analytical record partition")
                node = matching[0]
        raise ValueError("Provenance depth limit exceeded")

    def attach(self, connection, parent_key: str, roots: Sequence[dict[str, Any]]) -> None:
        """Fence a staged publication through the roots it actually embeds."""
        root_ids = self.guard(connection, roots)
        prior = connection.execute(
            "SELECT object_id,state FROM derived_publications WHERE object_key=%s FOR UPDATE",
            (parent_key,),
        ).fetchone()
        if not prior or prior["state"] in {"invalidated", "deleted"}:
            raise ValueError("Analytical publication was invalidated")
        if prior["state"] == "committed":
            return
        for root in roots:
            connection.execute(
                "INSERT INTO derived_artifact_edges(parent_id,child_id) "
                "VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (prior["object_id"], root_ids[root["key"]]),
            )

    def combine(self, values: Sequence[Any], synthetic: bool) -> dict[str, Any] | list:
        """Join disjoint product reference scopes without loading their leaves."""
        roots = [value["root"] for value in values if isinstance(value, dict)]
        if not roots:
            return []

        def build(items: list[dict[str, Any]], depth: int) -> dict[str, Any]:
            if len(items) == 1:
                return items[0]
            if len(items) <= BRANCH_CHILDREN:
                return self._node(
                    {
                        "schema_version": "1",
                        "kind": "branch",
                        "count": sum(child["count"] for child in items),
                        "children": sorted(items, key=lambda child: child["key"]),
                    },
                    synthetic,
                    children=items,
                )
            if depth >= MAX_DEPTH:
                raise ValueError("Provenance branch collision limit exceeded")
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in items:
                groups[hashlib.sha256(item["key"].encode()).hexdigest()[depth]].append(item)
            children = [build(groups[key], depth + 1) for key in sorted(groups)]
            return self._node(
                {
                    "schema_version": "1",
                    "kind": "branch",
                    "count": sum(child["count"] for child in children),
                    "children": children,
                },
                synthetic,
                children=children,
            )

        root = build(roots, 0)
        return {"format": "sharded-provenance-v1", "root": root, "count": root["count"]}

    def compact(self, value: Any, observations: Sequence[Observation], synthetic: bool) -> Any:
        """Replace reference vectors in one bounded product partition, preserving values."""
        deliveries = {
            row.observation_id: row.raw_dataset_id
            for row in observations
            if row.raw_dataset_id is not None
        }
        partitions = {
            row.observation_id: utc(row.observed_at).date().isoformat() for row in observations
        }
        references: dict[tuple[str, ...], Any] = {}

        def visit(item: Any) -> Any:
            if isinstance(item, dict):
                if item.get("format") == "sharded-provenance-v1":
                    return dict(item)
                result = {}
                for key, child in item.items():
                    if key == "evidence_refs" and isinstance(child, list):
                        identity = tuple(sorted(set(child)))
                        if identity not in references:
                            references[identity] = self.references(
                                identity, deliveries, synthetic, partitions=partitions
                            )
                        result[key] = references[identity]
                    else:
                        result[key] = visit(child)
                return result
            if isinstance(item, list):
                return [visit(child) for child in item]
            return item

        return visit(value)

    def page(self, value: Any, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("Provenance pages require limit 1..200 and nonnegative offset")
        if isinstance(value, list):
            references = value[offset : offset + limit]
            return {
                "references": references,
                "count": len(value),
                "next_offset": offset + len(references)
                if offset + len(references) < len(value)
                else None,
            }
        if not isinstance(value, dict) or value.get("format") not in {
            "sharded-provenance-v1",
            "sharded-records-v1",
        }:
            raise ValueError("Unsupported provenance descriptor")
        root = value["root"]
        found: list[Any] = []

        def read(node: dict[str, Any], skip: int, depth: int) -> None:
            if len(found) >= limit or skip >= node["count"]:
                return
            if depth > MAX_DEPTH:
                raise ValueError("Provenance depth limit exceeded")
            with pin_cached(self.history, node) as path:
                body = json.loads(path.read_bytes())
            if body.get("schema_version") != "1" or body.get("count") != node["count"]:
                raise ValueError("Invalid provenance node")
            if body["kind"] == "references":
                refs = body["references"]
                if len(refs) != node["count"] or len(refs) > LEAF_REFERENCES:
                    raise ValueError("Invalid provenance leaf")
                found.extend(refs[skip : skip + limit - len(found)])
            elif body["kind"] == "records":
                records = body["records"]
                if len(records) != node["count"] or len(records) > 100:
                    raise ValueError("Invalid analytical record leaf")
                found.extend(records[skip : skip + limit - len(found)])
            elif body["kind"] == "branch":
                children = body["children"]
                if (
                    len(children) > BRANCH_CHILDREN
                    or sum(c["count"] for c in children) != node["count"]
                ):
                    raise ValueError("Invalid provenance branch")
                for child in children:
                    if skip >= child["count"]:
                        skip -= child["count"]
                    else:
                        read(child, skip, depth + 1)
                        skip = 0
                        if len(found) >= limit:
                            break
            else:
                raise ValueError("Unsupported provenance node")

        with self.catalog.transaction() as connection:
            self.guard(connection, [root])
            read(root, offset, 0)
        end = offset + len(found)
        return {
            "records" if value["format"] == "sharded-records-v1" else "references": found,
            "count": root["count"],
            "next_offset": end if end < root["count"] else None,
        }
