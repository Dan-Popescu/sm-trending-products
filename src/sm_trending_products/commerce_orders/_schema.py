"""Independent order metadata; no acquisition or dossier schema changes."""

MIGRATIONS = {
    1: """
CREATE TABLE commerce_order_connections (
    connection_id text PRIMARY KEY,fingerprint text NOT NULL,provider text NOT NULL,
    synthetic boolean NOT NULL
);
CREATE TABLE commerce_order_listings (
    operation_id text PRIMARY KEY,
    connection_id text NOT NULL REFERENCES commerce_order_connections(connection_id),
    canonical_product_id text NOT NULL REFERENCES products(product_id),
    dossier_id text NOT NULL,dossier_revision integer NOT NULL,
    product_id text NOT NULL,variant_id text,sku text NOT NULL,currency text NOT NULL,
    synthetic boolean NOT NULL,registered_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX commerce_order_listing_lookup ON commerce_order_listings
    (connection_id,product_id,synthetic);
CREATE TABLE commerce_order_inbox (
    connection_id text NOT NULL REFERENCES commerce_order_connections(connection_id),
    body_sha256 text NOT NULL,order_id text NOT NULL,synthetic boolean NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','completed')),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
    completed_revision integer,
    PRIMARY KEY(connection_id,body_sha256)
);
CREATE INDEX commerce_order_pending ON commerce_order_inbox(connection_id,state,body_sha256);
CREATE TABLE commerce_order_heads (
    connection_id text NOT NULL REFERENCES commerce_order_connections(connection_id),
    order_id text NOT NULL,synthetic boolean NOT NULL,revision integer NOT NULL DEFAULT 0,
    state text NOT NULL DEFAULT 'missing' CHECK(state IN ('missing','current','conflict')),
    source_updated_at timestamptz,payload_sha256 text,fetch_token text NOT NULL,
    PRIMARY KEY(connection_id,order_id)
);
CREATE TABLE commerce_order_snapshots (
    connection_id text NOT NULL,order_id text NOT NULL,revision integer NOT NULL,
    source_updated_at timestamptz NOT NULL,payload_sha256 text NOT NULL,
    payload jsonb NOT NULL,line_links jsonb NOT NULL,reason text NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(connection_id,order_id,revision),
    FOREIGN KEY(connection_id,order_id) REFERENCES commerce_order_heads(connection_id,order_id)
);
CREATE TABLE commerce_order_conflicts (
    connection_id text NOT NULL,order_id text NOT NULL,source_updated_at timestamptz NOT NULL,
    payload_sha256 text NOT NULL,payload jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(connection_id,order_id,source_updated_at,payload_sha256),
    FOREIGN KEY(connection_id,order_id) REFERENCES commerce_order_heads(connection_id,order_id)
);
CREATE TABLE commerce_order_events (
    connection_id text NOT NULL,order_id text NOT NULL,
    kind text NOT NULL CHECK(kind IN ('handoff','tracking','notification')),
    revision integer NOT NULL,request_id text NOT NULL,request_sha256 text NOT NULL,
    order_revision integer NOT NULL,payload jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(connection_id,order_id,kind,revision),
    UNIQUE(connection_id,order_id,kind,request_id),
    FOREIGN KEY(connection_id,order_id,order_revision)
        REFERENCES commerce_order_snapshots(connection_id,order_id,revision)
);
"""
}
