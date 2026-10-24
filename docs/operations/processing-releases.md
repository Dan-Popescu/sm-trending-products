# Refresh planning, normalization and analytical code

The host controller's processing refresh changes only `capture-planner` /
`capture-planner-green`, `normalizer`, `dagster-daemon`, `dagster-webserver` and
`dashboard`. It never replaces collectors, PostgreSQL, storage, networks or
volumes. All roles use a verified immutable release image compatible with the
active acquisition protocol and the actual SQL schema.

An explicit host-local request can use private signed artifacts:

```sh
python -m sm_trending_products.release_cli refresh-processing \
  --config /etc/sm-trends/controller.json \
  --manifest /private/manifest.json --bundle /private/bundle.json
```

Run the same command without manifest/bundle arguments to resume its existing
private intent offline. The configured workflow identity, repository allowlist
and cosign verification still apply. The root-private `processing-refresh.json`
contains the exact desired manifest, proof, planner slot and progress; database
rows do not authorize processing host changes. `processing.env` contains only
immutable role image pins. Both files are atomically replaced and synchronized
before host mutations. For manual Compose operations, load this pin file after
the private runtime environment, alongside the collector's `slots.env`.

The first refresh after a collector handover receives its signed predecessor
descriptor from the authorized source release. It must match the actual old
planner image and the current SQL/protocol contract. Later refreshes use the
signed processing descriptor already retained privately. An unknown prior image
blocks overlap; the new image's compatibility claims are never borrowed for it.

A blocked forward refresh prevents another ordinary deployment from replacing
its private intent. If that image is broken, use the existing host-local rollback
command from the Hostinger console with the same private configuration:

```sh
python -m sm_trending_products.release_cli rollback \
  --config /etc/sm-trends/controller.json --release-id FORWARD_RELEASE_ID
```

The controller's authorized reverse handover can supersede the blocked processing
intent after acquisition returns to its compatible prior image. Only a verified
rollback receives this override; normal forward requests do not. The planner
remains qualified during the change, and newer captured data, cursors and quotas
are retained. A blocked reverse resumes through the controller's normal retry
path rather than requiring a pull-request approval.

The refresh shares the collector release's exclusive host lock. Before and after
each action it requires the same current acquisition identity/generation, a
running container, a fresh registry heartbeat and compatible signed metadata.
An unavailable database, changed generation or failed precondition blocks further
actions. These checks report an operating acquisition path, not proof of complete
source coverage; original capture-window diagnostics remain separate.

Planner replacement overlaps the old and new coordinator. The candidate uses
the configured scheduling budget and existing transactional planning authority;
it never uses a zero-budget probe that could advance a due tick without work.
`coordinator_busy` keeps the predecessor alive and blocks when its bounded retry
ends. Qualification requires a naturally due completed plan attributed to the
candidate's Docker hostname after its current process start. `not_due` qualifies
only when durable coordinator metadata proves that the candidate's background
loop already completed that tick; otherwise the old planner remains available
until a later retry. The proof binds the exact container and start timestamp, so
a process restart invalidates it. The active planner pointer is persisted only
after that proof and a matching running-image check, then the prior planner stops
gracefully. Candidate failure during retirement restores the compatible prior
planner/pointer and blocks. A controller crash may leave both planners running
under the same authoritative cadence lock.

Other roles restart individually with `--no-deps --no-build --pull never`.
Already running exact digests are inspected rather than recreated on retries.
Normalization interruption is recoverable through committed raw inputs and its
existing publication leases. The refresh does not reset source cursors, quotas,
obligations or processing evidence. Capacity preflight applies before a new pull
or process starts; a selected role's actual trusted Compose memory/CPU ceilings
raise the candidate budget when they exceed the collector budget. Missing limits
block cold recovery. Blocked admission never deletes an unshipped receipt.

Native tests exercise transactional planner probes, acquisition changes and
private-intent retry. The actual CLI test kills its controller after a host
action while actual Rust captures a one-shot HTTP response, then resumes without
artifact files or source access. Docker/cosign executables in that test are
explicit command fixtures; real container and signature qualification remain
separate release acceptance checks.
