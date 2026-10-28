"""Only metadata and immutable provenance descriptors enter these optional tables."""

MIGRATIONS = {
    1: """
CREATE TABLE enrichment_transcripts (
    transcript_id text PRIMARY KEY,content_key text NOT NULL,entity_key text NOT NULL,
    source_dataset_id text NOT NULL REFERENCES datasets(dataset_id),
    observation_id text NOT NULL,synthetic boolean NOT NULL,
    descriptor jsonb NOT NULL,available_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX enrichment_transcript_lookup ON enrichment_transcripts
    (synthetic,content_key,available_at DESC,transcript_id);
CREATE TABLE enrichment_daily_budgets (
    synthetic boolean NOT NULL,provider_id text NOT NULL,budget_day date NOT NULL,
    configuration jsonb NOT NULL,requests_reserved integer NOT NULL DEFAULT 0,
    cost_reserved numeric NOT NULL DEFAULT 0 CHECK(cost_reserved>=0),
    PRIMARY KEY(synthetic,provider_id,budget_day)
);
CREATE TABLE enrichment_analyses (
    analysis_id text PRIMARY KEY,cache_key text NOT NULL,
    transcript_id text NOT NULL REFERENCES enrichment_transcripts(transcript_id),
    synthetic boolean NOT NULL,provider_id text NOT NULL,provider_version text NOT NULL,
    state text NOT NULL CHECK(state IN ('running','completed','uncertain')),
    run_token text NOT NULL,reserved_cost numeric NOT NULL CHECK(reserved_cost>=0),
    currency text NOT NULL,descriptor jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz
);
CREATE INDEX enrichment_analysis_lookup ON enrichment_analyses
    (synthetic,cache_key,created_at DESC,analysis_id);
CREATE TABLE enrichment_requests (
    request_id text NOT NULL,synthetic boolean NOT NULL,
    product_id text NOT NULL REFERENCES products(product_id),
    transcript_id text NOT NULL REFERENCES enrichment_transcripts(transcript_id),
    analysis_id text NOT NULL REFERENCES enrichment_analyses(analysis_id),
    request_sha256 text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(synthetic,request_id)
);
CREATE INDEX enrichment_product_requests ON enrichment_requests
    (synthetic,product_id,created_at DESC,request_id);
"""
}
