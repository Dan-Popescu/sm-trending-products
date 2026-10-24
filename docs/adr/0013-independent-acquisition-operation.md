# Keep acquisition operation independent of analytical release cadence

Status: accepted under the owner's 2026-10-24 authorization to implement #80.

The existing application scheduler remains authoritative for candidate selection
and source budgets. A small independent coordinator invokes that same scheduler
under a nonblocking, workspace-scoped PostgreSQL ownership lock. Its next due tick
and last successful completion are durable. Restarting or replacing an analytical
Dagster process does not advance or erase an unfinished planning obligation.
Failure retains the original due boundary and a safe error category.

An ordinary application release retains PostgreSQL, S3, Tailscale and persistent
volumes. Acquisition, normalization and review may pin different digests of the
same application image. A replacement acquisition process may overlap its
predecessor within measured resource limits, while both use the existing quota
and task-lease protocol. A process heartbeat never replaces lease fencing or
durable capture evidence.

This changes the operational placement in ADR0002: Dagster continues coordinating
analytical, maintenance and outcome batches, while the dedicated acquisition role
owns timely planning and source capture. The application policy, queue and
publication rules are reused, not reimplemented as another scheduler or broker.
Legacy Dagster collection entry points remain available during the compatibility
transition; deployed dedicated acquisition disables their scheduled activation.

The added coordinator journal stores planning progress, not source observations.
Capture obligations, raw publication and release ownership each remain at their
authoritative mutation interface. Image rollback preserves newer evidence rather
than restoring an older database. Online backup and raw-capture tradeoffs have
their own decisions; a successful coordinator tick alone proves neither capture
coverage nor uninterrupted operation of every dependency.

The controlled qualification must exercise two versions, stalled normalization,
planning takeover and failed deployment. Whole-host or shared-database downtime
needs an independently available acquisition/control path and is outside a
two-container guarantee on one VPS.
