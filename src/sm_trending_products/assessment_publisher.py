"""Atomic current rows and alert decisions behind bounded PostgreSQL writes."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from itertools import islice
from typing import Any

from .assessment_archive import AssessmentArchive, AssessmentPublication
from .catalog import Catalog
from .collector import redact
from .contracts import AssessmentPolicy, digest

_BATCH_ROWS = 200
_BATCH_PAYLOAD_BYTES = 1024 * 1024


def _chunks[T](items: Iterable[T]) -> Iterator[list[T]]:
    remaining = iter(items)
    while batch := list(islice(remaining, _BATCH_ROWS)):
        yield batch


def _payload_batches(
    items: Iterable[tuple[dict[str, Any], str]],
) -> Iterator[list[tuple[dict[str, Any], str]]]:
    batch: list[tuple[dict[str, Any], str]] = []
    size = 0
    for item in items:
        amount = len(item[1].encode())
        if batch and (len(batch) == _BATCH_ROWS or size + amount > _BATCH_PAYLOAD_BYTES):
            yield batch
            batch, size = [], 0
        # An already-supported individual large row remains writable by itself.
        batch.append(item)
        size += amount
    if batch:
        yield batch


def _alert_rows(rows: Sequence[dict[str, Any]], threshold: float) -> Iterator[dict[str, Any]]:
    return (row for row in rows if row.get("score") is not None and row["score"] >= threshold)


class AssessmentPublisher:
    def __init__(self, catalog: Catalog, archive: AssessmentArchive):
        self.catalog, self.archive = catalog, archive

    def publish(self, publication: AssessmentPublication) -> None:
        policy = AssessmentPolicy.model_validate(publication.body["policy"])
        # Only references are sorted. The archived/returned ranking is unchanged.
        rows = sorted(publication.body["rows"], key=lambda row: row["product_id"])
        with self.catalog.transaction() as connection:
            self.archive.commit(connection, publication)
            for batch in _chunks(rows):
                # Every current row has this existing FK parent. Stable parent
                # locks serialize overlapping publishers before the upsert's
                # READ COMMITTED snapshot, including first-publication races.
                # NO KEY UPDATE stays compatible with ordinary FK key shares.
                connection.execute(
                    "SELECT product_id FROM products WHERE product_id=ANY(%s::text[]) "
                    'ORDER BY product_id COLLATE "C" FOR NO KEY UPDATE',
                    ([row["product_id"] for row in batch],),
                ).fetchall()
            self._current(connection, publication, rows)
            self._alerts(connection, publication, rows, policy)

    @staticmethod
    def _current(connection, publication: AssessmentPublication, rows: Sequence[dict]) -> None:
        for batch in _payload_batches((row, json.dumps(redact(row))) for row in rows):
            parameters: list[Any] = []
            for row, payload in batch:
                parameters.extend(
                    (
                        row["product_id"],
                        publication.run_id,
                        publication.body["synthetic"],
                        row.get("score"),
                        payload,
                    )
                )
            parameters.append(publication.body["as_of"])
            connection.execute(
                "INSERT INTO current_assessments(product_id,run_id,synthetic,score,payload) VALUES "
                + ",".join(["(%s,%s,%s,%s,%s::jsonb)"] * len(batch))
                + " ON CONFLICT(product_id) DO UPDATE SET run_id=EXCLUDED.run_id,"
                "score=EXCLUDED.score,payload=EXCLUDED.payload WHERE "
                "(SELECT as_of FROM assessment_runs WHERE run_id=current_assessments.run_id)<=%s",
                parameters,
            )

    @staticmethod
    def _alerts(
        connection,
        publication: AssessmentPublication,
        rows: Sequence[dict],
        policy: AssessmentPolicy,
    ) -> None:
        lock_keys: set[int] = set()
        for candidates in _chunks(_alert_rows(rows, policy.alert_threshold)):
            lock_keys.update(
                row["lock_key"]
                for row in connection.execute(
                    "SELECT DISTINCT hashtext('alert:'||product_id) AS lock_key "
                    "FROM unnest(%s::text[]) AS candidates(product_id)",
                    ([row["product_id"] for row in candidates],),
                ).fetchall()
            )
        # Sort actual keys, including hash collisions, across all bounded calls.
        # Keep the established lock namespace so prior writers still serialize.
        for keys in _chunks(sorted(lock_keys)):
            connection.execute(
                "SELECT pg_advisory_xact_lock(lock_key) FROM "
                "(SELECT unnest(%s::bigint[]) AS lock_key ORDER BY lock_key) ordered",
                (keys,),
            ).fetchall()
        for candidates in _chunks(_alert_rows(rows, policy.alert_threshold)):
            recent = {
                row["product_id"]
                for row in connection.execute(
                    "SELECT DISTINCT product_id FROM alerts WHERE product_id=ANY(%s::text[]) "
                    "AND NOT invalidated AND created_at>clock_timestamp()-%s*interval '1 hour'",
                    ([row["product_id"] for row in candidates], policy.cooldown_hours),
                ).fetchall()
            }
            # A historical run can still alert even when it did not replace a
            # newer current row. This preserves the existing public behavior.
            new = (row for row in candidates if row["product_id"] not in recent)
            for encoded_batch in _payload_batches((row, json.dumps(row)) for row in new):
                parameters: list[Any] = []
                for row, payload in encoded_batch:
                    parameters.extend(
                        (
                            digest({"run_id": publication.run_id, "product_id": row["product_id"]}),
                            row["product_id"],
                            publication.run_id,
                            publication.body["synthetic"],
                            payload,
                        )
                    )
                connection.execute(
                    "INSERT INTO alerts(alert_id,product_id,run_id,synthetic,payload) VALUES "
                    + ",".join(["(%s,%s,%s,%s,%s::jsonb)"] * len(encoded_batch))
                    + " ON CONFLICT DO NOTHING",
                    parameters,
                )
