"""Optional application tables, independent from capture's migration protocol."""

MIGRATIONS = {
    1: """
    CREATE TABLE cohort_taxonomies (
        taxonomy_id text PRIMARY KEY, revision integer NOT NULL CHECK(revision>0)
    );
    CREATE TABLE cohort_taxonomy_revisions (
        taxonomy_id text NOT NULL REFERENCES cohort_taxonomies(taxonomy_id),
        revision integer NOT NULL CHECK(revision>0),
        request_id text NOT NULL, request_hash text NOT NULL, payload jsonb NOT NULL,
        available_at timestamptz NOT NULL,
        PRIMARY KEY(taxonomy_id,revision), UNIQUE(taxonomy_id,request_id)
    );
    CREATE TABLE cohort_assignments (
        product_id text NOT NULL REFERENCES products(product_id),
        taxonomy_id text NOT NULL REFERENCES cohort_taxonomies(taxonomy_id),
        revision integer NOT NULL CHECK(revision>0),
        PRIMARY KEY(product_id,taxonomy_id)
    );
    CREATE TABLE cohort_assignment_revisions (
        product_id text NOT NULL, taxonomy_id text NOT NULL,
        revision integer NOT NULL CHECK(revision>0), taxonomy_revision integer NOT NULL,
        category_id text, request_id text NOT NULL, request_hash text NOT NULL,
        payload jsonb NOT NULL, available_at timestamptz NOT NULL,
        PRIMARY KEY(product_id,taxonomy_id,revision),
        UNIQUE(product_id,taxonomy_id,request_id),
        FOREIGN KEY(product_id,taxonomy_id) REFERENCES cohort_assignments,
        FOREIGN KEY(taxonomy_id,taxonomy_revision) REFERENCES cohort_taxonomy_revisions
    );
    CREATE INDEX cohort_assignment_known_at
        ON cohort_assignment_revisions(taxonomy_id,product_id,available_at DESC,revision DESC);
    """
}
