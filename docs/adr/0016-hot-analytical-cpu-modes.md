# Apply analytical CPU profiles through the trusted host controller

Status: accepted under the owner's 2026-10-28 hot-switch authorization.
Specification: [hot CPU mode](../specs/hot-cpu-mode.md); delivery #122.

Use Docker's live CPU quota update rather than recreating services. Only the
Dagster daemon/webserver and dashboard are eligible. Keeping capture, planning,
normalization and PostgreSQL limits unchanged is the primary continuity boundary;
the watchdog is a conservative supplementary check, not a coverage guarantee.
Live Dagster containers and future Compose settings must explicitly enable
independent acquisition before CPU reduction; otherwise Dagster could still run
legacy collection. Standard increases remain available to recover that state.

The host-private CPU journal and validated Compose pins live in the existing
writable controller state directory. This respects the systemd sandbox and avoids
rewriting credential-bearing runtime.env. Ordinary application images retain no
Docker access. The journal synchronizes intent before mutation and restores the
previous committed profile after an interrupted transition. Unknown runtime
identities block recovery rather than broadening the update target.
When the previous profile would lower CPU for a legacy Dagster collector,
complete the already authorized standard increase after fresh admission instead.

Resource changes share the release host lock and refuse unfinished handovers.
Pending resource intent blocks release mutations until reconciled. A committed
economy profile survives managed processing recreation. Increasing ceilings
requires fresh host admission, including restoration: a former allocation is not
a permanent reservation against unrelated applications.

Profiles deliberately trade analytical throughput for lower allowed CPU without
changing the nominal hourly schedule. Arbitrary per-role tuning, global process
group ceilings, RAM resizing and source cadence changes remain outside this mode.
Future trusted host configuration changes must explicitly reconcile recorded
standard ceilings instead of silently overriding an active private policy.
