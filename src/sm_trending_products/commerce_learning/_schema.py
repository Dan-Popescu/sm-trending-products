"""Additive optional learning state; source/capture migrations are untouched."""

MIGRATIONS = {
    1: """
    CREATE TABLE commercial_tests (
        test_id text PRIMARY KEY,
        dossier_id text NOT NULL REFERENCES commerce_dossiers(dossier_id),
        dossier_revision integer NOT NULL,
        product_id text NOT NULL REFERENCES products(product_id),
        country text NOT NULL, currency text NOT NULL, synthetic boolean NOT NULL,
        request_id text NOT NULL, request_hash text NOT NULL,
        plan jsonb NOT NULL, dossier_snapshot jsonb NOT NULL, policy_references jsonb NOT NULL,
        planned_at timestamptz NOT NULL,
        ledger_key text UNIQUE,
        ledger_declared_at timestamptz, ledger_decision_id text,
        state text NOT NULL CHECK(state IN ('pending_ledger','ready','recorded')),
        UNIQUE(dossier_id,request_id),
        FOREIGN KEY(dossier_id,dossier_revision)
            REFERENCES commerce_dossier_revisions(dossier_id,revision)
    );
    CREATE INDEX commercial_tests_page ON commercial_tests(synthetic,planned_at DESC,test_id);
    CREATE TABLE commercial_periods (
        test_id text NOT NULL REFERENCES commercial_tests(test_id),
        period_id text NOT NULL,
        period_start timestamptz NOT NULL, period_end timestamptz NOT NULL,
        revision integer NOT NULL CHECK(revision>0),
        PRIMARY KEY(test_id,period_id),
        UNIQUE(test_id,period_start,period_end),
        CHECK(period_end>period_start)
    );
    CREATE TABLE commercial_period_reviews (
        test_id text NOT NULL, period_id text NOT NULL,
        revision integer NOT NULL CHECK(revision>0),
        request_id text NOT NULL, request_hash text NOT NULL,
        payload jsonb NOT NULL, available_at timestamptz NOT NULL,
        complete boolean NOT NULL, reasons jsonb NOT NULL,
        PRIMARY KEY(test_id,period_id,revision), UNIQUE(test_id,request_id),
        FOREIGN KEY(test_id,period_id) REFERENCES commercial_periods(test_id,period_id)
    );
    CREATE TABLE commercial_publications (
        dataset_id text PRIMARY KEY,
        test_id text NOT NULL REFERENCES commercial_tests(test_id),
        period_id text, revision integer,
        kind text NOT NULL CHECK(kind IN ('declaration','actual','withdrawal')),
        observation jsonb NOT NULL,
        state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','published')),
        published_at timestamptz,
        CHECK((period_id IS NULL AND revision IS NULL AND kind='declaration') OR
              (period_id IS NOT NULL AND revision>0 AND kind<>'declaration')),
        UNIQUE(test_id,period_id,revision),
        FOREIGN KEY(test_id,period_id,revision)
            REFERENCES commercial_period_reviews(test_id,period_id,revision)
    );
    CREATE INDEX commercial_publications_test ON commercial_publications(test_id,state);
    """
}
