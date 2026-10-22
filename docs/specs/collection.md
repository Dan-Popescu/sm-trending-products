# Reliable collection and replay

GitHub issue: [#3](https://github.com/Dan-Popescu/sm-trending-products/issues/3). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

Status: approved for autonomous implementation on 2026-10-22.

## Problem Statement

The operator must trust that observations survive retries, failures, and schema changes, and know which fields a source actually exposes.

## Solution

Collect permitted observations into auditable history and expose collection status, coverage, and missingness. Support contract fixtures without credentials and explicitly distinguish them from live acceptance.

## User Stories

1. As an operator, I want a capability report, so that unavailable metrics are visible.
2. As an operator, I want each observation linked to its original response, so that normalization can be audited.
3. As an operator, I want task retries to preserve one logical result, so that counts are not inflated.
4. As an operator, I want interrupted collection recoverable, so that an outage does not lose progress.
5. As an operator, I want raw observations replayable, so that corrected transformations need no repeated external requests.
6. As an operator, I want synthetic data isolated, so that testing cannot trigger live opportunities.

## Implementation Decisions

- Source operations are canonicalized at collection entry points: Display defaults to `list`; Ads and Commercial Content use `query`. Their historical `list` alias is normalized to `query`, including persisted task and refresh origins, so source acceptance and scheduling use the same operation.
- Collectors hint only fingerprints of currently configured live credentials when claiming tasks; synthetic claims remain account-only. Fingerprints are refreshed before each claim so token rotation does not inherit a historical credential block. Atomic Rust reservation remains authoritative. Known blocked work is reported as `quota_waiting` with no error or attempt consumption; Dagster reports the batch as waiting.
- Transient transport/5xx failures use at most three HTTP attempts with bounded full jitter between attempts. Exhausted transient attempts defer the task with jitter; source `Retry-After` minima remain authoritative and permanent authorization failures are not retried as transient errors.
- Source adapters expose capabilities and bounded paginated collection. Rust workers honor source-specific quotas, timeouts, cancellation, backoff, and shared credential limits. Tokens are redacted and never enter datasets.
- Observe supported video identifiers, publication time, duration, description, creator identifiers, and exposed cumulative engagement counters. Capture source meanings, inaccessible fields, and response acquisition context; do not infer audience geography from creator geography.
- Batch raw compressed JSONL and normalized Parquet into immutable objects. A completed manifest defines a readable dataset version. Object writes and PostgreSQL updates use explicit staged/committed recovery, checksums, and reconciliation; no cross-store atomicity assumption.
- Explicit replay version 2 adds source-field drift reporting and immutable normalization revisions with one durable first-normalization availability time. Version 1 remains the collection/default contract; replay aliases, retries and compaction preserve version identity. See [ADR 0006](../adr/0006-immutable-normalization-revisions.md).
- A grouped Display query records requested and missing entity identifiers in its durable raw evidence. An absent requested ID does not become an invented observation or zero counter.
- Stable delivery keys deduplicate retries; a genuinely later observation of the same entity remains distinct. Cursors advance only after durable publication. Lease expiry and fencing reject stale worker completion.
- Schema drift is quarantined with actionable status. One malformed record cannot silently invalidate or advance an entire batch.
- Source-specific retention and deletion jobs remove restricted data and mark dependent datasets/evaluations invalid when necessary. Raw-data auditability is bounded by those rules.

## Testing Decisions

Test controlled source responses through object publication and operator-visible status using real temporary storage services. Verify pagination restart, repeated delivery, stale workers, interrupted publication, missing fields, and replay equivalence. Confirm that incomplete manifests are invisible and synthetic observations are absent from live queries. Real access verification is a separate credential-dependent acceptance check. No existing approved tests exist.

## Out of Scope

Global discovery through an unavailable endpoint, invented sales, private viewer analytics, or any promise that fixtures demonstrate granted access.

## Further Notes

Done means a supported observation can be collected, audited, replayed, and recovered without duplication, with its access limitations visible. Actual scopes and endpoint contracts must be verified before enabling each live adapter.
