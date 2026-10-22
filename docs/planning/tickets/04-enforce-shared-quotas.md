# T04 — Enforce shared quotas across collectors

GitHub issue: [#12](https://github.com/Dan-Popescu/sm-trending-products/issues/12). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Controlled engineering acceptance verified by the agent. See the [criterion evidence](../../verification/foundation-acceptance.md). Live-source acceptance remains T15.

## What to build

Run concurrent collectors sharing a credential budget and make their wait, retry, cancellation, and source-limit behavior visible.

## Acceptance criteria

- [x] Two collector processes cannot exceed the configured shared request budget under controlled time and responses.
- [x] Documented source rate limits and retry hints take precedence over local desired refresh intervals.
- [x] Quota exhaustion yields delayed work rather than dropped tasks or unbounded retries.
- [x] Timeouts, cancellation, bounded queues, and bounded in-flight requests limit resource use during source failure.
- [x] An operator can inspect remaining local budget estimates, rate-limit responses, oldest queued work, and achieved collection intervals.
- [x] Credential-level limits do not reveal credential values in state, logs, or the review view.

## Blocked by

T03 — Recover interrupted collection without duplicate results.

## Specification

[Functional specification](../../specs/discovery.md). Shared contracts are defined there; this ticket adds no alternative definitions.
