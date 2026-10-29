"""Bounded source authority shared by individual and page transcript guards."""

from ..identity_rules import LINK_STATE_CTES


def source_checks(connection, product, rows, synthetic, cutoff):
    """Return one ordered decision per (entity, dataset, observation) candidate.

    Scope the shared owner resolver to selected entities, retaining competing
    products and the original cutoff/tie/confidence rules. Every other join is
    keyed by the bounded input; no source history or dependency tree is loaded.
    CASE order preserves the individual guard's first refusal.
    """
    if not rows:
        return []
    return connection.execute(
        "WITH selected AS ("
        "SELECT * FROM unnest(%s::text[],%s::text[],%s::text[]) WITH ORDINALITY "
        "AS s(entity_key,dataset_id,observation_id,position)), "
        "product_links AS (SELECT * FROM product_links WHERE entity_key=ANY(%s)), "
        + LINK_STATE_CTES
        + ", checked AS MATERIALIZED (SELECT clock_timestamp() AS at) "
        "SELECT s.position, delivery.available_at, CASE "
        "WHEN d.dataset_id IS NULL OR d.state<>'committed' "
        "THEN 'enrichment_source_unavailable' "
        "WHEN owner.product_id IS NULL OR owner.product_id<>%s OR owner.confidence<0.9 "
        "THEN 'enrichment_video_not_owned_at_cutoff' "
        "WHEN d.synthetic<>%s OR d.created_at>%s OR e.entity_key IS NULL "
        "THEN 'enrichment_source_unavailable' "
        "WHEN NOT %s AND policy.verified IS DISTINCT FROM true "
        "THEN 'enrichment_source_policy_unverified' "
        "WHEN NOT %s AND policy.retention_days IS NOT NULL "
        "AND extract(epoch FROM (checked.at-d.created_at))>=policy.retention_days::numeric*86400 "
        "THEN 'enrichment_source_retention_expired' "
        "WHEN delivery.available_at IS NULL "
        "THEN 'enrichment_source_observation_unavailable' END AS reason "
        "FROM selected s CROSS JOIN checked "
        "LEFT JOIN datasets d ON d.dataset_id=s.dataset_id "
        "LEFT JOIN resolved_link_owners owner ON owner.entity_key=s.entity_key "
        "LEFT JOIN entities e ON e.entity_key=s.entity_key "
        "AND e.synthetic=d.synthetic AND e.source=d.source "
        "LEFT JOIN source_policies policy ON policy.source=d.source "
        "LEFT JOIN LATERAL (SELECT min(o.available_at) AS available_at "
        "FROM normalized_objects n JOIN observation_deliveries o USING(object_id) "
        "WHERE n.dataset_id=s.dataset_id AND o.entity_key=s.entity_key "
        "AND o.observation_id=s.observation_id AND o.available_at<=%s) delivery ON true "
        "ORDER BY s.position",
        (
            [row[0] for row in rows],
            [row[1] for row in rows],
            [row[2] for row in rows],
            sorted({row[0] for row in rows}),
            synthetic,
            cutoff,
            cutoff,
            product,
            synthetic,
            cutoff,
            synthetic,
            synthetic,
            cutoff,
        ),
    ).fetchall()
