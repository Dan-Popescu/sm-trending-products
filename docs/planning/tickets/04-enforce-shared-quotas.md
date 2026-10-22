# T04 — Enforce shared quotas across collectors

GitHub issue: [#12](https://github.com/Dan-Popescu/sm-trending-products/issues/12). Implementation: [PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

**Status:** Software implemented; native PostgreSQL concurrency acceptance pending. See the [coverage record](../../implementation/coverage.md).

## What to build

Run concurrent collectors sharing a credential budget and make their wait, retry, cancellation, and source-limit behavior visible.

## Acceptance criteria

- [ ] Two collector processes cannot exceed the configured shared request budget under controlled time and responses.
- [ ] Documented source rate limits and retry hints take precedence over local desired refresh intervals.
- [ ] Quota exhaustion yields delayed work rather than dropped tasks or unbounded retries.
- [ ] Timeouts, cancellation, bounded queues, and bounded in-flight requests limit resource use during source failure.
- [ ] An operator can inspect remaining local budget estimates, rate-limit responses, oldest queued work, and achieved collection intervals.
- [ ] Credential-level limits do not reveal credential values in state, logs, or the review view.

## Blocked by

T03 — Recover interrupted collection without duplicate results.

## Specification

[Functional specification](../../specs/discovery.md). Shared contracts are defined there; this ticket adds no alternative definitions.
