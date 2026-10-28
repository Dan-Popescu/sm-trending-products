"""Additive outbox and stable ownership mapping, independent from capture."""

MIGRATIONS = {
    1: """
    CREATE TABLE commerce_delivery_connections (
        connection_id text PRIMARY KEY,
        configuration_hash text NOT NULL,
        provider text NOT NULL,
        synthetic boolean NOT NULL
    );
    CREATE TABLE commerce_delivery_targets (
        connection_id text NOT NULL REFERENCES commerce_delivery_connections(connection_id),
        dossier_id text NOT NULL REFERENCES commerce_dossiers(dossier_id),
        launch_id text NOT NULL,
        sku text NOT NULL,
        applied_revision integer NOT NULL DEFAULT 0 CHECK(applied_revision >= 0),
        product_id text,
        variant_id text,
        hosted_product_url text,
        PRIMARY KEY(connection_id,dossier_id)
    );
    CREATE TABLE commerce_delivery_operations (
        operation_id text PRIMARY KEY,
        connection_id text NOT NULL,
        dossier_id text NOT NULL,
        dossier_revision integer NOT NULL,
        request_id text NOT NULL,
        request_hash text NOT NULL,
        configuration_hash text NOT NULL,
        provider text NOT NULL,
        canonical_product_id text NOT NULL,
        synthetic boolean NOT NULL,
        state text NOT NULL CHECK(state IN('pending','running','uncertain','succeeded','failed')),
        attempt_token text,
        draft jsonb NOT NULL,
        product_id text,
        variant_id text,
        hosted_product_url text,
        error_code text,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        FOREIGN KEY(connection_id,dossier_id)
            REFERENCES commerce_delivery_targets(connection_id,dossier_id),
        FOREIGN KEY(dossier_id,dossier_revision)
            REFERENCES commerce_dossier_revisions(dossier_id,revision),
        UNIQUE(connection_id,dossier_id,request_id)
    );
    CREATE UNIQUE INDEX commerce_delivery_one_unresolved_target
        ON commerce_delivery_operations(connection_id,dossier_id)
        WHERE state IN('pending','running','uncertain');
    """
}
