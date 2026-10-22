# T01 — Inspect an authorized-video observation end to end

GitHub issue: [#9](https://github.com/Dan-Popescu/sm-trending-products/issues/9). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified by the agent. See the [criterion evidence](../../verification/foundation-acceptance.md). Live-source and production-provider acceptance remain separate tickets.

## What to build

Collect one controlled authorized-video response through the intended Rust, PostgreSQL, object-store, analytical, and private review path. Make the source capability and original observation inspectable. Deliver the smallest complete path, not standalone infrastructure.

## Acceptance criteria

- [x] A deterministic no-key scenario publishes raw and normalized evidence and shows the same entity, counters, provenance, timestamps, missing fields, and synthetic classification in the operator view.
- [x] The Rust collector is invoked as a bounded batch through the intended orchestration boundary; the review view consumes published analytical results rather than querying the source.
- [x] PostgreSQL and S3-compatible test storage are exercised as real services; restart retains the published observation.
- [x] Missing credentials and unverified scopes remain explicit; no fixture sets a real capability to available.
- [x] Tokens never enter stored payload metadata or logs; source and transport errors are visible.
- [x] The verification command and prerequisites are documented; the earlier SQLite experiment is excluded from the approved runtime.

## Blocked by

None — can start after repository setup and ticket approval.

## Specification

[Functional specification](../../specs/collection.md). Shared contracts are defined there; this ticket adds no alternative definitions.
