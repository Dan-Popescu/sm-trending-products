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
