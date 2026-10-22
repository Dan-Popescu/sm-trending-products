# Delivery plan

Status: architecture approved; setup, specifications, test boundaries, and ticket breakdown proposed for review. No implementation is authorized by these draft tickets alone.

## Reading order

1. [Product requirements](../prd/overview.md): purpose, scope, source constraints, and success criteria.
2. [Domain vocabulary](../../CONTEXT.md): shared meanings of product, offer, observation, and opportunity.
3. [Architecture decisions](../adr/): accepted architectural tradeoffs.
4. Functional specifications:
   - [Reliable collection and replay](../specs/collection.md)
   - [Discovery and collection prioritization](../specs/discovery.md)
   - [Product identity and evidence](../specs/product-identity.md)
   - [Trend assessment](../specs/trend-assessment.md)
   - [Historical and forward validation](../specs/validation.md)
   - [Operations and deployment](../specs/operations.md)
5. [Proposed agent configuration](setup.md): review before activating repository instructions.
6. [Proposed delivery tickets](tickets/README.md): review size and blocking edges before publication.

## Working method

Use Matt Pocock's setup, domain-modeling, to-spec, to-tickets, implement, and code-review workflows. Start from externally visible behavior, specify acceptance criteria, and implement narrow vertical slices. A database layer, collector layer, or dashboard layer alone is not a delivery ticket.

GitHub Issues is the proposed canonical tracker for Dan-Popescu/sm-trending-products. Keep reviewed product requirements in the repository; publish an umbrella issue linking them and one issue per approved delivery ticket. Maintain native blocking links when the integration supports them; otherwise use explicit issue references. Do not mark a ticket ready-for-agent before its scope is approved and its human prerequisites are satisfied.

Read the domain glossary and relevant architecture decisions before implementing. For each ready ticket: select an unblocked slice, verify its acceptance criteria at the highest useful boundary, implement, run the relevant checks, and review the change against its specification. Use test-first development for nontrivial correctness rules. Record evidence in the pull request. Do not merge or deploy automatically.

## Approval checkpoints

- Architecture: approved by the user.
- Agent instructions filename: user choice required between AGENTS.md and CLAUDE.md; neither exists.
- Default triage labels: user confirmation required by the setup skill.
- Configuration below: draft available for editing before activation.
- Test boundary: propose collection-to-visible-evidence scenarios with real PostgreSQL and S3-compatible test services, a controlled HTTP source, and deterministic time. Smaller tests cover financial-independent numerical and temporal invariants where helpful.
- Delivery breakdown: review before publishing issues, as required by the to-tickets skill.
- Live source acceptance: cannot be completed without approved credentials and scopes. Contract fixtures demonstrate software behavior, not granted access or market coverage.

## Current repository state

The remote repository was observed empty. An earlier GitHub write attempt returned HTTP 403, Resource not accessible by integration. Publication remains blocked until write access works; local drafts are not published GitHub issues.

The preexisting local Python/SQLite scaffold is an abandoned experiment. It is not an implementation of this plan and must not be included in the documentation change or silently reused as the approved storage architecture. Preserve it separately until implementation cleanup is explicitly recorded.

All repository documentation and technical identifiers use English, including filenames, variables, functions, tables, fields, metrics, and issue titles. User-facing discussion can remain in French.
