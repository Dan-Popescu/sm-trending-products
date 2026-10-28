# Change analytical CPU ceilings without restarting capture

The trusted host controller supports `standard` and `economy` modes. Only the
Dagster daemon, Dagster webserver and dashboard CPU ceilings change. Collection,
capture planning, normalization and PostgreSQL retain their resources. Memory,
threads, source quotas, schedules and stored evidence are unchanged.

Economy requires `ACQUISITION_ENABLED=true` in the running Dagster daemon and
webserver and their effective future Compose configuration. Missing/false values
are refused because legacy Dagster jobs could still perform source collection.
Returning to standard remains possible; the guard requests that return if this
precondition changes while economy is active.

With the [4-vCPU pilot profile](../../deploy/production-4cpu.env.example):

| Role | Standard CPU ceiling | Economy CPU ceiling |
| --- | ---: | ---: |
| Dagster daemon | 0.5 | 0.25 |
| Dagster webserver | 0.25 | 0.125 |
| Dashboard | 0.25 | 0.125 |
| All other normal running roles combined | 2 | 2 |
| Total normal running role ceilings | 3 | 2.5 |

These are ceilings, not idle consumption, throughput measurements, a global
process-group limit or a provider price. Economy halves the three configured
standard ceilings, with daemon/web/dashboard floors of 0.25/0.1/0.1 CPU, never
raising a role above standard. Application replicas are deliberately unsupported.
Lower analytical CPU can delay ranking completion and make the UI less responsive;
the nominal hourly ranking schedule stays unchanged. CPU quotas act over short
scheduler intervals and do not pin threads to physical cores.

## Install and operate

Upgrade the independently trusted host controller from a reviewed main revision
that implements this feature. Publishing an application image does not update
that controller. Preserve its private state during this upgrade. Keep the
existing `sm-trends-release.timer` enabled: its polls reconcile interrupted CPU
changes and evaluate the economy capture guard before fetching remote assets.
An application container must never receive the Docker socket.

Use the root Hostinger console, substituting no credentials on the command line:

```bash
# Inspect the proposed ceilings without writing policy or changing containers.
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli cpu-mode \
  --config /etc/sm-trends/release-controller.json --mode economy --dry-run

# Apply live, without Compose up/down or a service restart.
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli cpu-mode \
  --config /etc/sm-trends/release-controller.json --mode economy

/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli cpu-status \
  --config /etc/sm-trends/release-controller.json

# Return to the recorded standard ceilings, subject to current host headroom.
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli cpu-mode \
  --config /etc/sm-trends/release-controller.json --mode standard

# Reconcile an interrupted change immediately instead of awaiting the timer.
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli cpu-recover \
  --config /etc/sm-trends/release-controller.json
```

Commands emit one JSON object. Exit 0 indicates a successful operation or
preview; exit 2 indicates a blocked operation. Status is read-only and reports
the committed mode, ceilings and whether recovery is required; it is not a live
source-coverage certificate.

## Continuity and recovery

The controller uses the same exclusive host lock as releases. New switches
refuse unfinished capture or processing handovers. It verifies the bounded,
uniquely identified running services, then targets immutable container IDs using
`docker update --cpus`. Verification compares process IDs, start times, restart
counts and protected resource settings. It never stops acquisition or changes
source task state. Unsupported custom CPU quota/cpuset configurations, resource
drift, missing roles and ambiguous inventories block the change.

Private `cpu-mode.json` under the existing controller state directory records
standard ceilings, committed pins and any transition intent. It contains no
application environment or credentials. A committed policy overrides inherited
CPU interpolation variables for subsequent managed processing recreation. Do not
edit runtime environment CPU values to override an active policy or remove its
journal to clear a recovery block. Deliberate baseline changes need a separately
reviewed host configuration procedure.

Intent is synchronized before the first Docker update. A command failure may
have applied, so rollback inspects actual quotas. A controller killed mid-switch
leaves intent for a fresh controller to restore the previous committed mode.
If that restoration would lower CPU for legacy Dagster collection, recovery
instead completes the already requested standard increase after checking current
headroom. Insufficient capacity preserves the partial higher limits and intent.
Deployment mutations wait for that recovery. An identity or protected-resource
change blocks automated restoration rather than updating an unexamined runtime.
Once a profile is fully committed, a stopped analytical service may instead be
repaired normally with its persisted pins. An incomplete CPU transition still
requires its exact recorded runtime before repair can proceed.

Before lowering quotas, the controller checks active acquisition health and
overdue/unknown obligations whose original collection time is already due. This
uses the existing indexed pending frontier and inspects at most 1,001 such
obligations; more than 1,000 conservatively blocks economy rather than scanning
a massive queue. Future scheduled work does not consume this budget; deferring
a retry cannot hide its original missed deadline. The guard does not
retrospectively certify every completed capture. In economy, subsequent timer
polls request standard mode if the capture guard becomes unhealthy. This is a
conservative signal: source failures or intentional source withdrawal can also
cause restoration; the guard does not attribute every failure to CPU limits.
It waits through deliberate release draining rather than treating that expected
worker state as an unrelated capture failure. Pending CPU recovery still runs
first. Keep acquisition coverage monitoring enabled independently.

Increasing limits, including rollback, rechecks effective host CPU, all running
Docker ceilings and the configured reserve. Insufficient headroom or a changed
container can therefore leave a visible recovery block. Inspect `cpu-status`
and the service journal, restore capacity or reconcile the independent host
change, then run `cpu-recover`. Collection continues with its untouched settings;
never use Compose down, kill a collector or delete its spool as a remedy.
Pure reductions do not require extra host CPU headroom; recovery to economy
remains possible when another workload has consumed the available reserve.

These protections avoid restarting or throttling capture as part of this mode.
They do not prove that arbitrary workloads meet source deadlines, reserve
resources against unrelated host applications, or guarantee availability through
a VPS, database or upstream outage. Controlled qualification uses synthetic
one-shot sources; the real host and source still require their acceptance checks.
