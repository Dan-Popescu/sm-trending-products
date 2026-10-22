# T05 — Link videos and offers to reviewed products

GitHub issue: [#13](https://github.com/Dan-Popescu/sm-trending-products/issues/13). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified by the agent. See the [criterion evidence](../../verification/foundation-acceptance.md). Identity accuracy on a real market corpus remains a measured business/data question.

## What to build

Give the operator a product review path that accepts or rejects evidence links, records offer identity separately, and preserves the grouping known at earlier times.

## Acceptance criteria

- [x] Two videos and an offer can be reviewed against one canonical product with evidence references and matching confidence.
- [x] A similar but different product can be rejected or assigned to a related family without becoming the same product.
- [x] Manual review and deterministic text candidate suggestions are distinguishable; low-confidence suggestions never silently become confirmed identity.
- [x] A merge and subsequent split preserve prior decisions and can be reconstructed using availability time.
- [x] Likely repeated creatives can be marked without claiming independent creators or ownership from unsupported inference.
- [x] An assessment query made before a link became available cannot include that link.

## Blocked by

T01 — Inspect an authorized-video observation end to end.

## Specification

[Functional specification](../../specs/product-identity.md). Shared contracts are defined there; this ticket adds no alternative definitions.
