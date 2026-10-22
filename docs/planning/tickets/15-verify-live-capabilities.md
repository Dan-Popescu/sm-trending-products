# T15 — Validate real source capabilities and enable live collection

**Status:** Acceptance pending; verification software implemented, but real credentials, approved scopes, permitted intended use, and real observations are unavailable. See the [coverage record](../../implementation/coverage.md). Not published to GitHub.

## What to build

Use the user's actual approved access to produce a verified capability report and enable only supported real collection paths.

## Acceptance criteria

- [ ] Record actual app/account scope, source endpoint/version, market coverage, verified date, and applicable retention/use constraints without recording secrets.
- [ ] At least one real observation is collected and inspectable for each enabled adapter; unavailable adapters show the denial or missing prerequisite.
- [ ] Verify authorization, pagination where accessible, actual field semantics, quota behavior observed safely, and token-expiry/revocation handling.
- [ ] A granted authorized-account capability is not presented as broad public discovery access; unavailable sales or retention metrics remain unavailable.
- [ ] Synthetic evidence cannot populate the live candidate universe or live evaluation results.
- [ ] Completion means the access matrix is verified, including explicit denials; it does not mean that every desired metric has become available.
- [ ] If credentials are absent, keep this task ready-for-human and report it pending; offline tests cannot satisfy live acceptance.

## Blocked by

T01 — Inspect an authorized-video observation end to end; T08 — Inspect commercial-content evidence separately; human prerequisite: credentials, approved scopes, and permitted intended use.

## Specification

[Functional specification](../../specs/collection.md). Shared contracts are defined there; this ticket adds no alternative definitions.
