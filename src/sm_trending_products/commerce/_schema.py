"""Additive application-owned schema; acquisition migrations stay unchanged."""

MIGRATIONS = {
    1: """
    CREATE TABLE commerce_dossiers (
        dossier_id text PRIMARY KEY,
        product_id text NOT NULL REFERENCES products(product_id),
        country text NOT NULL,
        currency text NOT NULL,
        synthetic boolean NOT NULL,
        revision integer NOT NULL CHECK (revision > 0),
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL
    );
    CREATE INDEX commerce_dossiers_workspace_page
        ON commerce_dossiers(synthetic, updated_at DESC, dossier_id);
    CREATE TABLE commerce_dossier_revisions (
        dossier_id text NOT NULL REFERENCES commerce_dossiers(dossier_id),
        revision integer NOT NULL CHECK (revision > 0),
        request_id text NOT NULL,
        request_hash text NOT NULL,
        draft jsonb NOT NULL,
        available_at timestamptz NOT NULL,
        PRIMARY KEY (dossier_id, revision),
        UNIQUE (dossier_id, request_id)
    );
    """
}
