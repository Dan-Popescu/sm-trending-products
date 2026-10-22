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

Before implementation, read the approved ticket, its specification, CONTEXT.md,
and relevant ADRs. Work on an unblocked vertical slice. Verify externally visible
acceptance criteria and include evidence in the pull request. Keep real and
synthetic observations separate. Do not infer source access from fixtures.

The earlier local SQLite experiment is outside the approved architecture.
Consult docs/planning/README.md for the current implementation status.
