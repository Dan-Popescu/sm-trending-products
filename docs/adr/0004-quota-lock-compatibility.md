# Serialize quota counters without locking credential identity

Status: adopted under the user's autonomous implementation authorization.

PostgreSQL remains authoritative for request quotas across collector processes.
Reserve the per-account budget before the shared credential budget. Lock shared
credential counters with `FOR NO KEY UPDATE`: this serializes competing token
writers and remains compatible with the `KEY SHARE` lock taken when a new account
alias references that established credential. The credential key is immutable
during reservation; capacity, refill, tokens and counters are the mutable state.

`FOR UPDATE` unnecessarily locks credential identity. New aliases can each hold a
foreign-key share lock and then deadlock while upgrading to that stronger lock.
Native regressions reproduce both a blocked reservation and the multi-alias
deadlock. A process-local mutex would not coordinate independent collectors;
separate advisory locking would duplicate PostgreSQL's existing row ownership.

An additive migration replaces the reservation function without resetting token
balances, source block times, aliases or request counts. Historical migrations
remain unchanged. Keep native shared-budget contention and upgrade-preservation
checks when modifying this boundary. Changing credential keys or the lock order
requires a fresh concurrency review.

Account reconfiguration settles the existing balance at its old refill rate and
capacity before installing the new settings. Shared credential capacity and refill
follow the minimum of the **current** settings of its recorded account bindings,
not the smallest setting ever encountered. Configuration locks its account first,
then credentials in key order; source blocks use the same order. Neither changing
a limit nor installing the additive migration resets request counters, blocks,
bindings or token balances. Binding records are retained as observed associations;
removing or expiring an association would require an explicit separate policy.

Task claiming and quota-wait reporting share a read-only readiness predicate. It
skips known account cooldown/exhaustion. Live callers may additionally supply the
current credential fingerprint used by that worker; only that exact credential's
state can defer a task. Historical account bindings cannot identify a current
token and never gate claiming. Missing or unknown fingerprints reach Rust's
authoritative atomic reservation, permitting newly rotated credentials. Synthetic
tasks always ignore credential hints, matching Rust's account-only reservation.
The prefilter does not consume tokens, attempts or leases and does not promise a
later reservation succeeds. This keeps blocked queues from consuming healthy
sources' batch slots without preventing token rotation or synthetic collection.

Existing credential reservations settle their locked balance in constant work.
Only a newly recorded binding or explicit account configuration recomputes the
minimum across bound accounts, keeping alias-count scans off the request hot path.
