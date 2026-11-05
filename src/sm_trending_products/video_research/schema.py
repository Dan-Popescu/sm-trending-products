"""Explicit optional metadata, never capture migrations or unbounded comment rows."""

MIGRATIONS = {
    1: """
CREATE TABLE video_research_jobs (
    job_id text PRIMARY KEY,synthetic boolean NOT NULL,content_key text NOT NULL,
    selection jsonb NOT NULL,operation text NOT NULL,provider text NOT NULL,
    configuration jsonb NOT NULL,state text NOT NULL
        CHECK(state IN ('running','completed','uncertain')),
    descriptor jsonb,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,UNIQUE(synthetic,content_key)
);
CREATE TABLE video_research_requests (
    synthetic boolean NOT NULL,request_id text NOT NULL,request_sha256 text NOT NULL,
    job_id text NOT NULL REFERENCES video_research_jobs(job_id),
    PRIMARY KEY(synthetic,request_id)
);
CREATE TABLE video_research_budgets (
    synthetic boolean NOT NULL,day date NOT NULL,configuration_sha256 text NOT NULL,
    operations_reserved integer NOT NULL DEFAULT 0 CHECK(operations_reserved>=0),
    PRIMARY KEY(synthetic,day)
);
CREATE INDEX video_research_product_runs ON video_research_jobs
    (synthetic,(selection->>'product_id'),created_at DESC,job_id);
"""
}
