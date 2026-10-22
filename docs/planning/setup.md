# Repository setup record

Status: accepted and activated on 2026-10-22. The user approved the standard labels and authorized autonomous selection of recommended implementation defaults. This document records that decision; it is not a pending setup questionnaire.

## Active configuration

| Decision | Accepted choice | Authoritative file |
| --- | --- | --- |
| Agent instructions | AGENTS.md; no duplicate CLAUDE.md | [Repository instructions](../../AGENTS.md) |
| Work tracker | GitHub Issues in Dan-Popescu/sm-trending-products | [Tracker rules](../agents/issue-tracker.md) |
| Triage vocabulary | Five standard labels | [Triage rules](../agents/triage-labels.md) |
| Domain layout | Single context, root glossary and short ADRs | [Domain rules](../agents/domain.md) |
| Language | English documentation and technical identifiers | [Repository instructions](../../AGENTS.md) |

The accepted labels are `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. These strings are configured locally; this record does not claim they were successfully created remotely.

## Delivery method

The approved workflow uses the global PRD, six functional specifications, and fifteen vertical delivery tickets. Implement observable behavior across the relevant boundaries, verify acceptance through public interfaces, and record standards and specification review findings separately. The user authorized choosing recommended defaults and recording assumptions rather than pausing implementation for routine decisions.

The [implementation coverage](../implementation/coverage.md) maps those tickets to software and tests. The [assumptions record](../implementation/assumptions.md) distinguishes autonomous choices from external prerequisites.

## Test boundary

The accepted main boundary is controlled HTTP collection through durable history to operator-visible evidence, with deterministic decision time. Focused numerical and temporal tests supplement it. Native PostgreSQL plus HTTP S3-compatible test services remain the target integration environment; local PGlite/Moto execution is identified as a constrained fallback and does not establish native database concurrency or production durability.

Live TikTok acceptance is separate from contract fixtures. Tokens, actual granted scopes, and permission for the intended use must exist before a live capability can be enabled. No-key tests cannot complete that acceptance.

## Tracker publication

GitHub integration write attempts returned HTTP 403, Resource not accessible by integration. The canonical tracker remains GitHub, while local ticket records preserve approved scope and delivery evidence until publication is possible. No issue numbers or remote labels are invented. Agent configuration is active despite that external publication limitation.
