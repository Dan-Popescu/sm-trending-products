# Proposed repository configuration

Status: draft for user review. This file does not activate agent instructions or create tracker labels. The setup workflow requires showing this draft before writing the active configuration.

## Findings

- Repository: Dan-Popescu/sm-trending-products, intended GitHub tracker.
- Neither AGENTS.md nor CLAUDE.md exists; the user must choose which to create.
- Single domain context; no monorepo signals.
- The triage skill is available; the standard vocabulary needs confirmation.
- Domain glossary and three architecture decisions have been prepared.
- The GitHub integration previously rejected a write with HTTP 403. Read access does not prove write access.

## Proposed agent instructions

Recommended destination: AGENTS.md, subject to the user's choice. Do not create both instruction files.

```markdown
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
```

## Proposed issue tracker configuration

Destination after review: docs/agents/issue-tracker.md.

```markdown
# Issue tracker

Use GitHub Issues in Dan-Popescu/sm-trending-products as the canonical tracker.
Prefer the connected GitHub integration; use an authenticated CLI if necessary
and available. Do not switch to a different canonical tracker silently.

Publish approved specifications and one issue per approved vertical delivery
ticket. Reference the repository specifications rather than maintaining divergent
copies. Create blockers before dependents, then attach native dependencies and
sub-issue relationships when supported. Otherwise retain explicit issue references
in each issue body. Local draft identifiers are not GitHub issue numbers.

Apply ready-for-agent only to reviewed scopes with satisfied human prerequisites.
Use ready-for-human for credential-dependent live acceptance. A ticket may have
unfinished engineering blockers; work only the frontier whose blockers are done.
Claim work by assigning the issue when supported. Record completion evidence
before closing a ticket. Do not close or rewrite a parent issue automatically.

PRs as a request surface: off.

If publication is denied, preserve drafts and report the blocker. Do not claim
that local files are published issues. Report created identifiers and links after
successful publication; keep the local ticket index synchronized.
```

## Proposed triage vocabulary

Destination after review: docs/agents/triage-labels.md.

| Canonical role | Label | Meaning |
| --- | --- | --- |
| needs-triage | needs-triage | New request awaiting assessment |
| needs-info | needs-info | Required information is missing |
| ready-for-agent | ready-for-agent | Reviewed engineering task; work when blockers are complete |
| ready-for-human | ready-for-human | Next action requires a human, such as source approval |
| wontfix | wontfix | Deliberate decision not to implement |

Do not create duplicate synonyms. A denied source scope is an observed constraint; it is not a reason to fabricate a successful live check.

## Proposed domain configuration

Destination after review: docs/agents/domain.md.

```markdown
# Domain documentation

Single-context layout: root CONTEXT.md for domain vocabulary; docs/adr/ for short
architecture decisions. Before exploring or changing a domain behavior, read the
glossary and relevant ADRs. If documentation is missing, continue with observed
evidence and identify the gap rather than inventing a convention.

CONTEXT.md defines domain concepts; it is not an implementation manual. ADRs
record meaningful tradeoffs, their status, and why they were selected. Flag a
proposed change that conflicts with an accepted ADR before implementing it.

Product requirements live in docs/prd/ and functional specifications in
docs/specs/. Tickets state observable acceptance criteria; implementation details
belong in code and focused architecture decisions. Update only the authoritative
document for a rule and link to it elsewhere.
```

## Proposed test boundary

Prefer one collection-to-visible-evidence scenario boundary: controlled HTTP source and time, real temporary PostgreSQL and S3-compatible storage, then inspect persisted observations and operator-visible evidence. Test lower-level numerical or temporal invariants only where that materially improves diagnosis. Run live API acceptance separately when credentials exist; never make routine offline checks depend on tokens.

## Review sequence

Confirm default labels, choose the instruction filename, and review the configuration. Confirm the test boundary and ticket size/dependencies before publication. These are workflow setup decisions, not a request to reapprove the architecture.
