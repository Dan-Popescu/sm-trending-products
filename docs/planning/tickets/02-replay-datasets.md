# T02 — Replay a completed dataset without source access

GitHub issue: [#10](https://github.com/Dan-Popescu/sm-trending-products/issues/10). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; acceptance evidence is mapped in the [coverage record](../../implementation/coverage.md).

## What to build

Let the operator select a completed raw dataset, rederive normalized evidence without contacting TikTok, and inspect its transformation version and quarantine status.

## Acceptance criteria

- [ ] Replaying fixed raw inputs under a fixed transformation version produces equivalent normalized evidence and stable content fingerprints.
- [ ] A new transformation publishes a separate dataset version without overwriting the old evidence.
- [ ] Completed manifests resolve all objects and checksums; incomplete or corrupt manifests do not enter analytical reads.
- [ ] Unknown source fields or incompatible changes are reported and quarantined without silent data loss or cursor advancement.
- [ ] Source-specific retention or deletion can remove restricted evidence and explicitly invalidate dependent dataset/evaluation references.
- [ ] The operator can distinguish absent, rejected, corrected, and synthetic data in replay results.

## Blocked by

T01 — Inspect an authorized-video observation end to end.

## Specification

[Functional specification](../../specs/collection.md). Shared contracts are defined there; this ticket adds no alternative definitions.
