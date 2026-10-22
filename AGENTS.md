# Repository instructions

Use English for documentation and all technical identifiers, including filenames,
directories, variables, functions, types, tables, fields, metrics, and issue titles.

## Agent skills

### Issue tracker

GitHub Issues is the canonical work tracker. See docs/agents/issue-tracker.md.

### Triage labels

Use the five standard triage labels. See docs/agents/triage-labels.md.

### Domain docs

Use the single-context glossary and architecture decisions. See docs/agents/domain.md.

## Delivery

### Autonomous pull-request delivery

The user explicitly authorized autonomous review and merging on 2026-10-22.
There is no human pull-request review or approval step for this project.
Review the exact proposed revision, fix actionable findings, run the relevant
checks, and merge when the evidence supports the change. Then continue with the
next unblocked, useful slice without waiting for user approval. When a routine
choice is uncertain, select and implement the best-supported recommendation.
Record validation and remaining limits in the pull request. Respect enforced
GitHub protections; do not bypass failed checks or invent missing evidence.
Continue work that does not require TikTok access while source acceptance is
blocked. This authorization does not supply missing credentials or infrastructure.

### Priorities and architecture

The user requires delivery ticket by ticket, ordered by criticality, value and
the dependencies each slice unlocks. Prefer correctness of shared collection,
recovery, quotas and evidence contracts over downstream polish or additional
benchmark variants when a foundational defect is known. A merged PR is a
checkpoint: re-evaluate the remaining frontier and continue the next unblocked
ticket rather than treating the merge as the end of the standing project task.

For foundational changes, trace the existing callers, state transitions and
ownership boundaries before editing. Keep invariants at their authoritative
mutation boundary, use behavioral regression tests, and preserve one source of
truth. Extend the current architecture when it fits; introduce abstractions or
parallel execution paths only for a demonstrated need. Record meaningful
tradeoffs and deferred risks so later tickets do not inherit hidden debt.

Before implementation, read the approved ticket, its specification, CONTEXT.md,
and relevant ADRs. Work on an unblocked vertical slice. Verify externally visible
acceptance criteria and include evidence in the pull request. Keep real and
synthetic observations separate. Do not infer source access from fixtures.

The earlier local SQLite experiment is outside the approved architecture.
Consult docs/planning/README.md for the current implementation status.
