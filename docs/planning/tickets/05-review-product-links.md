# T05 — Link videos and offers to reviewed products

GitHub issue: [#13](https://github.com/Dan-Popescu/sm-trending-products/issues/13). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; native PostgreSQL concurrency acceptance pending. See the [coverage record](../../implementation/coverage.md).

## What to build

Give the operator a product review path that accepts or rejects evidence links, records offer identity separately, and preserves the grouping known at earlier times.

## Acceptance criteria

- [ ] Two videos and an offer can be reviewed against one canonical product with evidence references and matching confidence.
- [ ] A similar but different product can be rejected or assigned to a related family without becoming the same product.
- [ ] Manual review and deterministic text candidate suggestions are distinguishable; low-confidence suggestions never silently become confirmed identity.
- [ ] A merge and subsequent split preserve prior decisions and can be reconstructed using availability time.
- [ ] Likely repeated creatives can be marked without claiming independent creators or ownership from unsupported inference.
- [ ] An assessment query made before a link became available cannot include that link.

## Blocked by

T01 — Inspect an authorized-video observation end to end.

## Specification

[Functional specification](../../specs/product-identity.md). Shared contracts are defined there; this ticket adds no alternative definitions.
