"""Shared metadata-only origin repair for atomic and resumable retirement."""

from typing import Any


def repair_entity_origins(
    connection: Any, entity_keys: list[str] | None = None, synthetic: bool | None = None
) -> int:
    # Use manifest metadata only; do not materialize historical Parquet under this lock.
    connection.execute(
        "CREATE TEMP TABLE retention_entity_replacements ON COMMIT DROP AS "
        "SELECT e.entity_key,e.synthetic,replacement.dataset_id,replacement.account_id "
        "FROM entities e JOIN retention_targets expired "
        "ON expired.dataset_id=e.origin->>'dataset_id' "
        "AND (%s::text[] IS NULL OR e.entity_key=ANY(%s)) "
        "AND (%s::boolean IS NULL OR e.synthetic=%s) "
        "CROSS JOIN LATERAL ("
        "SELECT d.dataset_id,CASE WHEN d.manifest->'task_request'->>'source'=e.source "
        "THEN COALESCE(NULLIF(d.manifest->'task_request'->>'account_id',''),e.account_id) "
        "ELSE e.account_id END AS account_id "
        "FROM datasets d JOIN source_policies policy ON policy.source=d.source "
        "WHERE d.state='committed' AND d.source=e.source AND d.synthetic=e.synthetic "
        "AND policy.verified AND d.created_at>=clock_timestamp()-"
        "policy.retention_days*interval '1 day' "
        "AND d.manifest @> jsonb_build_object('normalized',jsonb_build_array("
        "jsonb_build_object('entity_keys',jsonb_build_array(e.entity_key)))) "
        "ORDER BY COALESCE(d.manifest->'task_request'->>'source'=e.source "
        "AND NULLIF(d.manifest->'task_request'->>'account_id','') IS NOT NULL "
        "AND ((e.source IN ('tiktok_ads','tiktok_commercial') "
        "AND d.manifest->'task_request'->>'operation'='query') "
        "OR (e.source='tiktok_display' "
        "AND d.manifest->'task_request'->>'operation' IN ('query','list'))),false) DESC,"
        "d.max_available_at DESC NULLS LAST,d.committed_at DESC,d.dataset_id LIMIT 1"
        ") replacement",
        (entity_keys, entity_keys, synthetic, synthetic),
    )
    restored = connection.execute(
        "UPDATE entities e SET account_id=replacement.account_id,"
        "title='',attributes='{}',origin=jsonb_build_object("
        "'dataset_id',replacement.dataset_id,'source',e.source) "
        "FROM retention_entity_replacements replacement "
        "WHERE e.entity_key=replacement.entity_key AND e.synthetic=replacement.synthetic"
    ).rowcount
    # Current fresh origins are untouched. Expired metadata has no remaining provenance.
    connection.execute(
        "UPDATE entities SET title='',attributes='{}',"
        "origin='{\"retention_purged\":true}'::jsonb "
        "WHERE origin->>'dataset_id' IN (SELECT dataset_id FROM retention_targets) "
        "AND (%s::text[] IS NULL OR entity_key=ANY(%s)) "
        "AND (%s::boolean IS NULL OR synthetic=%s)",
        (entity_keys, entity_keys, synthetic, synthetic),
    )
    return restored
