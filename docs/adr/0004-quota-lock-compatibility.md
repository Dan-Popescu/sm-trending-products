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
