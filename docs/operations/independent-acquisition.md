# Independent acquisition runtime

Production Compose enables `ACQUISITION_ENABLED=true`. Standalone Python retains
the default `false` for existing replay/recovery callers. In independent mode,
Dagster does not register its automatic collection schedule and rejects manual
legacy collection asset runs before opening source dependencies. Its analytical,
outcome and maintenance jobs continue independently.

The same application image supports these separately replaceable roles:

| Service | Work | Image override |
| --- | --- | --- |
| `capture-planner` | Durable, fenced planning through the existing scheduler | `ACQUISITION_PLANNER_IMAGE` |
| `worker` | Blue raw-capture loop | `CAPTURE_BLUE_IMAGE` |
| `worker-green` | Temporary replacement raw-capture loop, profile `rollout` | `CAPTURE_GREEN_IMAGE` |
| `normalizer` | Publish evidence from saved captures | `NORMALIZER_IMAGE` |
| `dagster-daemon`, `dagster-webserver` | Analytics and maintenance orchestration | `ANALYTICS_IMAGE` |
| `dashboard` | Review | `DASHBOARD_IMAGE` |

Each override falls back to `PIPELINE_IMAGE` for local builds. Controlled releases
must supply immutable image digests and distinct `CAPTURE_BLUE_WORKER_ID` and
`CAPTURE_GREEN_WORKER_ID` values for their runtime generations. Compose passes the
same identity as `WORKER_ID` and the `io.sm-trends.worker-id` label so the host
controller can address the registered process. The `*-local` fallback identities
are for unmanaged local operation. Registry generation checks still apply.

All capture versions and the normalizer retain the same named `spool` volume.
`CAPTURE_SPOOL_MAX_BYTES` defaults to 256 MiB and cannot be configured below 32 MiB.
Admission must preserve unshipped captures when space is exhausted. Replacing an
image must never remove this volume. Source acquisition and normalized publication
have separate progress; stopped analytics or a processing backlog is not itself
a source gap.

The capture service has a 15-minute shutdown grace by default. The controller must
first obtain the acknowledged drain described in
[worker draining](worker-draining.md), and block if accepted work remains.
`CAPTURE_STOP_GRACE_PERIOD` is an additional process bound, not authorization to
force an unsafe cutover. Validate any adjustment against configured source and
publication timeouts.

PostgreSQL is exposed only on host loopback at `POSTGRES_HOST_PORT` (default 5432)
for host-local release control. Its host DSN uses `127.0.0.1` and this port; the
container application DSN continues using `postgres:5432`. Keep both credentials
in private configuration outside Git. Ordinary application containers receive no
Docker socket. Keep the existing Tailscale access policy and host-local controller
privileges; a private database port is not a remote deployment interface.

Use [online backup](backup-and-restore.md#online-acquisition-checkpoint) while
acquisition is active. The synthetic container recovery rehearsal explicitly uses
that mode and retains legacy fixture semantics with `ACQUISITION_ENABLED=false`.
It verifies backup/restore, not live source continuity. Native one-shot-source and
controlled release tests establish their separately declared boundaries.

Per-service resource limits are admission ceilings, not measured capacity. Before
starting `worker-green`, the release controller must measure available CPU, RAM
and disk headroom on the target, including shared spool usage. Retain PostgreSQL,
S3, Tailscale and named volumes during analytical updates. Two workers sharing one
VPS/database do not protect a whole-host or database outage. See
[ADR0013](../adr/0013-independent-acquisition-operation.md).
