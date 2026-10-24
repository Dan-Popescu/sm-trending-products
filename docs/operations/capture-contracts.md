# Capture obligations and pause admission

Capture contracts declare the sampling intention and the independently verified
ability to replay a source operation. They do not grant source access, reserve
HTTP quota or certify live provider behavior. Unknown, snapshot and ephemeral
sources cannot authorize an acquisition blackout. A historical source requires
an explicit verified horizon and evidence reference.

`CaptureContracts` is the operational interface. Register an immutable
`CapturePolicy` version for a source, account, canonical operation, scope and
workspace. The exact scheduler request signature can select a scoped policy;
`*` is an explicit account/operation fallback. Synthetic policies cannot apply to
live tasks. A version cannot be rewritten, and a newer registration does not
relabel an older task's intention.
The database availability time also prevents a newly declared contract from
qualifying an original window that was already due before registration.

An additive PostgreSQL trigger freezes each new task/page obligation at admission.
The scheduler retains the earliest original member due time in its frozen reason;
later retry due times do not replace it. Pagination retains the original capture
window and policy. The obligation is an extension of the existing dispatch identity,
not a competing queue. Existing tasks receive an explicitly unknown creation-time
bound during upgrade; their historical deadlines are never fabricated.

Only a committed dataset using the raw-capture protocol can acknowledge an
obligation. The catalog verifies source/account/operation/workspace/task/page,
the request payload frozen for that page, raw pointer/checksum,
and the response-completion and commit timestamps. A dataset commit acknowledges
the obligation atomically. Neither task completion, an ordinary normalized dataset,
a heartbeat nor a bare success flag is evidence of this raw capture. Direct
obligation-window changes and fabricated receipt references are rejected by SQL.
Retired receipt datasets remain retired in status; acknowledging them cannot
resurrect their inputs.

Status distinguishes response capture from remote publication. Capture lateness
uses physical response completion, while a receipt becomes visible only once its
remote publication has committed. Historical status does not reveal a receipt
committed after its cutoff. An unknown sampling contract reports an unqualified
window and a null deadline/lateness; a missing tolerance is not invented as zero.
One bounded call returns at most 200 obligations.

For a positive planned blackout, pause admission includes:

- Original oldest uncaptured windows, including older still-pending policy versions.
- Source replay and cursor lifetimes, pause, rollback and safety margin.
- Existing pending work, new arrivals during the blackout, and continued arrivals
  during catch-up. Catch-up capacity must exceed the ongoing arrival rate.
- The declared measured catch-up rate capped by authoritative account/credential
  refill rates; depleted or blocked budgets add waiting time. The request allowance
  must cover catch-up requests, including ongoing arrivals.
- Actual live source approval and a recognized current credential budget. The
  contract itself never creates either.

The result is conditional on the operator's declared measured throughput and
request allowance; it is not a reservation or a substitute for final Rust quota
enforcement. Account/credential contention and provider behavior must still be
qualified by the release controller. A zero-blackout assessment instead requires
a live replacement: it permits only a continuous-acquisition handover and asserts
neither coverage nor permission to stop the last compatible collector.

Native tests exercise retry/pagination immutability, exact scope/workspace
selection, late/retired raw receipts, rejection of ordinary/fabricated receipts,
quota and cursor exhaustion, old-policy constraints and populated-schema upgrade.
Production continuity still requires the raw-first collector and release
qualification tracked under [#80](https://github.com/Dan-Popescu/sm-trending-products/issues/80).
