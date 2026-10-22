# T08 — Inspect commercial-content evidence separately

**Status:** Software implemented; acceptance evidence is mapped in the [coverage record](../../implementation/coverage.md). Not published to GitHub.

## What to build

Exercise a commercial-content adapter behind an explicit capability gate and let the operator inspect advertising or commercial-post evidence without conflating it with ordinary public-video metrics.

## Acceptance criteria

- [ ] Controlled commercial-content responses traverse durable observation publication and appear with explicit source and distribution context.
- [ ] Reach ranges, source periods, creative identifiers, advertiser fields, and first-observed time retain documented semantics where present.
- [ ] The view never presents a reach bound as an exact video-view count or first observation as a guaranteed ad-launch time.
- [ ] Unverified, denied, or unsuitable access remains disabled for live collection and is visible in the capability report.
- [ ] Contract tests verify pagination, missing fields, provenance, and fixture isolation; real endpoint/version acceptance is tracked separately.
- [ ] No paid access is purchased and no source permission is inferred from contract-test success.

## Blocked by

T01 — Inspect an authorized-video observation end to end.

## Specification

[Functional specification](../../specs/collection.md). Shared contracts are defined there; this ticket adds no alternative definitions.
