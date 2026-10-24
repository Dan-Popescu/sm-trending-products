"""Opt-in SQL-only footprint probe, including the publication identity/index cost.

Run with TEST_DATABASE_URL set and pass --output to retain the native measurements.
The fixtures compare an unshared tree and a DAG whose successive parents reuse
4,096 existing children. They model SQL row shapes, not provider request latency.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

TABLES = (
    "derived_publications",
    "derived_artifact_edges",
    "derived_artifact_inputs",
    "evaluation_artifacts",
)
MIGRATIONS = Path(__file__).parents[2] / "src/sm_trending_products/sql"


def footprint(connection):
    result = {}
    for table in TABLES:
        # Compare equally packed indexes: migration's table rewrite must not
        # receive credit for repacking an otherwise unchanged public-key index.
        connection.execute(sql.SQL("REINDEX TABLE {}").format(sql.Identifier(table)))
        connection.execute(sql.SQL("VACUUM (ANALYZE) {}").format(sql.Identifier(table)))
        result[table] = connection.execute(
            "SELECT pg_relation_size(%s) AS heap_bytes,pg_indexes_size(%s) AS index_bytes,"
            "pg_total_relation_size(%s) AS total_bytes",
            (table, table, table),
        ).fetchone()
    sequence = connection.execute(
        "SELECT pg_get_serial_sequence('derived_publications','object_id') AS name "
        "WHERE EXISTS(SELECT 1 FROM information_schema.columns "
        "WHERE table_schema=current_schema() AND table_name='derived_publications' "
        "AND column_name='object_id')"
    ).fetchone()
    sequence_bytes = (
        connection.execute(
            "SELECT pg_total_relation_size(%s) AS n", (sequence["name"],)
        ).fetchone()["n"]
        if sequence
        else 0
    )
    return {
        "tables": result,
        "publication_sequence_bytes": sequence_bytes,
        "total_bytes": sum(item["total_bytes"] for item in result.values()) + sequence_bytes,
    }


def measure(database_url, edges, shared):
    schema = "artifact_footprint_" + uuid4().hex
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            with connection.transaction():
                for path in sorted(MIGRATIONS.glob("*.sql")):
                    if int(path.name.split("_", 1)[0]) < 26:
                        connection.execute(path.read_text())
            children = min(edges, 4096) if shared else edges
            parents = (edges + 15) // 16
            with connection.transaction():
                # Only task-owned fixture triggers are disabled for bulk loading;
                # foreign keys remain active. This measures layout, not write CPU.
                for table in ("datasets", *TABLES[1:]):
                    connection.execute(
                        sql.SQL("ALTER TABLE {} DISABLE TRIGGER USER").format(sql.Identifier(table))
                    )
                connection.execute(
                    "CREATE TEMP TABLE fixture_nodes AS SELECT i,"
                    "'provenance/synthetic/'||encode(sha256(convert_to(i::text,'UTF8')),'hex')"
                    "||'.json' AS key FROM generate_series(1,%s) i",
                    (children + parents,),
                )
                connection.execute("CREATE UNIQUE INDEX ON fixture_nodes(i)")
                connection.execute(
                    "INSERT INTO derived_publications(object_key,evidence_datasets,state,"
                    "object_sha256,object_bytes) SELECT key,'[]','committed',"
                    "encode(sha256(convert_to(i::text,'UTF8')),'hex'),256 FROM fixture_nodes"
                )
                connection.execute(
                    "INSERT INTO datasets(dataset_id,source,synthetic,state,manifest) "
                    "SELECT encode(sha256(convert_to('dataset-'||i,'UTF8')),'hex'),"
                    "'artifact_layout_fixture',true,'committed','{}' FROM generate_series(1,100) i"
                )
                connection.execute(
                    "INSERT INTO derived_artifact_edges(parent_key,child_key) "
                    "SELECT 'provenance/synthetic/'||encode(sha256(convert_to("
                    "(%s+(n-1)/16+1)::text,'UTF8')),'hex')||'.json',"
                    "'provenance/synthetic/'||encode(sha256(convert_to("
                    "((n-1)%% %s+1)::text,'UTF8')),'hex')||'.json' "
                    "FROM generate_series(1,%s) series(n)",
                    (children, children, edges),
                )
                connection.execute(
                    "INSERT INTO derived_artifact_inputs(object_key,dataset_id) "
                    "SELECT key,encode(sha256(convert_to("
                    "'dataset-'||((i-1)%%100+1),'UTF8')),'hex') "
                    "FROM fixture_nodes WHERE i<=%s",
                    (children,),
                )
                connection.execute(
                    "INSERT INTO evaluation_runs(evaluation_id,kind,synthetic,payload) "
                    "SELECT encode(sha256(convert_to('evaluation-'||i,'UTF8')),'hex'),"
                    "'forward',true,'{}' FROM generate_series(1,%s) i",
                    (parents,),
                )
                connection.execute(
                    "INSERT INTO evaluation_artifacts(evaluation_id,object_key) "
                    "SELECT encode(sha256(convert_to('evaluation-'||(i-%s),'UTF8')),'hex'),key "
                    "FROM fixture_nodes WHERE i>%s",
                    (children, children),
                )
                for table in ("datasets", *TABLES[1:]):
                    connection.execute(
                        sql.SQL("ALTER TABLE {} ENABLE TRIGGER USER").format(sql.Identifier(table))
                    )
            old = footprint(connection)
            started = time.monotonic()
            with connection.transaction():
                connection.execute((MIGRATIONS / "026_compact_artifact_relations.sql").read_text())
            migration_seconds = time.monotonic() - started
            new = footprint(connection)
            counts = {
                table: connection.execute(
                    sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))
                ).fetchone()["n"]
                for table in TABLES
            }
            assert counts == {
                "derived_publications": children + parents,
                "derived_artifact_edges": edges,
                "derived_artifact_inputs": children,
                "evaluation_artifacts": parents,
            }
            return {
                "topology": "shared_children_dag" if shared else "unshared_tree",
                "child_edges": edges,
                "publication_rows": children + parents,
                "index_layout": "reindexed_before_both_measurements",
                "counts": counts,
                "old": old,
                "new": new,
                "reduction_percent": 100 * (1 - new["total_bytes"] / old["total_bytes"]),
                "migration_seconds": migration_seconds,
                "postgres": connection.execute("SELECT version() AS version").fetchone()["version"],
            }
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--edges", nargs="+", type=int, default=[100_000, 500_000])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    measurements = []
    for size in args.edges:
        for reuse in (False, True):
            row = measure(os.environ["TEST_DATABASE_URL"], size, reuse)
            measurements.append(row)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(measurements, indent=2) + "\n")
            print(
                json.dumps(
                    {key: row[key] for key in ("topology", "child_edges", "reduction_percent")}
                )
            )
