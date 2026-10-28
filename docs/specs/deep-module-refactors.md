# Deep modules for shared correctness and operational rules

Status: approved by the owner on 2026-10-28, subject to demonstrated benefit over
migration cost. Deliveries: #136, #137, #138 and #139.

## Decision and scope

Retain the current Rust/Python, PostgreSQL/S3, bounded analytical and independent
commercial architecture. Deepen existing seams where callers currently need to
coordinate duplicated semantics or implementation protocols. Do not build a
generic dependency framework or rewrite working state machines.

1. **Measurement semantics (#136).** One module owns counter-period meaning.
   New persisted decisions declare the documented-counter contract explicitly;
   absent contracts retain legacy outcome semantics. Pure legacy defaults,
   serialized historical definitions and golden hashes remain unchanged. A
   free-form evaluation version label cannot select historical meaning.
2. **Managed runtime roles (#137).** The existing host adapter owns Docker and
   Compose recipes, representation parsing and qualified role operations. CPU
   and processing modules retain their policies, sequencing and acquisition
   guards. Resource admission exposes the actual capability callers need.
   Preserve command allowlists, lock order and the number of Docker operations.
3. **Assessment eligibility (#138).** One immutable owner derives filtering and
   projection identity from the same context. Callers cannot pair an arbitrary
   callback with an unrelated cache token. Preserve existing published/evaluation
   policies, complete capability scope, raw-window sharing and input provenance.
4. **Authorized execution (#139).** Bind private execution when constructing a
   mutating controller; ordinary production mutations reject missing execution.
   Planning and explicit controlled synthetic adapters remain available. Keep
   private authority, SQL mirrors and existing rollback/checkpoint routing.

The first two concentrate knowledge already repeated across production callers.
The third removes a manual cache-correctness obligation without adding work to
history reads. The fourth is intentionally smaller than a lifecycle redesign:
constructor binding and a mutation guard offer the useful gain at low cost.

## Acceptance and verification

Tests use the already established public seams: assessment/freeze/mature,
persisted Pipeline decisions and cache behavior, qualified host/controller
operations, and privileged runtime commands. Preserve historical golden fixtures,
synthetic isolation, excluded-input invalidation, restarts, ambiguous identities,
pins, acquisition protection and cross-journal crash recovery.

Run focused tests for each slice, Python formatting/lint/types, then the complete
suite on isolated native PostgreSQL with the compiled collector and HTTP Moto.
Retain actual Docker campaign checks in CI. Fixture behavior does not establish
live-source access or a production rollout. No production deployment is part of
these refactors.

Accepted ADRs 0003, 0008, 0009, 0010, 0015 and 0016 continue to govern temporal,
historical, cache, resource and deployment behavior. New semantics must be
versioned rather than rewriting a previously frozen decision.
