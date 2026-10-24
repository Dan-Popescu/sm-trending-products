# Measure capacity before overlapping acquisition versions

The host release adapter must call `HostCapacity.admit()` before pulling a
candidate image and before starting an additional capture container. An already
running container with the exact expected image and worker identity needs no
second overlap allocation. Paths and `CapacityBudget` values come from private,
trusted host configuration; release manifests cannot change them. Source task,
quota and publication fencing remain authoritative.

The probe reads Linux `MemAvailable`, CPU affinity and the effective cgroup v2
memory/CPU bounds through the controller's visible hierarchy. A finite cgroup
memory limit uses `memory.max - memory.current`, rather than total RAM. A host
with an unsupported or unreadable hierarchy is blocked. Running Docker container
memory limits and CPU ceilings are inspected through the pinned local socket;
unlimited or missing limits block admission. Both actual available memory and
headroom against the configured aggregate ceilings must accommodate the
candidate plus the host reserve. CPU ceilings are deliberately conservative:
idle analytical containers still count against the overlap budget.

Docker's reported data root must match the configured absolute host path. Free
space is measured separately on that filesystem and on the persistent capture
spool. When they share a filesystem, their requirements are added. The probe
does not scan, remove or evict spool receipts. Missing directories fail closed;
provision their ownership and mount before enabling releases.

Default trusted budgets are 2 GiB candidate memory, one candidate CPU, 1 GiB
host memory reserve, one host CPU reserve, 4 GiB image download/unpack allowance,
4 GiB additional Docker free space, 256 MiB spool growth and 768 MiB additional
spool free space. Set these for the actual container limits, maximum published
image footprint and shared spool cap. They are conservative admission budgets,
not measurements of an image that has not yet been downloaded. A 4-vCPU machine
may need lower explicitly bounded optional-role limits or paused analytical work
before overlapping collectors; an insufficient result does not stop acquisition.

`assess()` returns only numeric measurements, configured requirements and safe
reason codes. `admit()` raises `release_capacity_unavailable` for missing probes
or `insufficient_release_capacity` for inadequate measured headroom. Docker
commands are read-only, share a ten-second deadline and suppress raw diagnostics.
An inventory change during inspection blocks the attempt.

These checks establish a point-in-time precondition. The deployment host lock
serializes managed releases; it does not reserve capacity against unrelated
processes, storage users or provider failures. Keep the previous collector and
all unshipped receipts when admission blocks. Target VPS load qualification and
ongoing resource monitoring remain necessary for a continuity claim.
