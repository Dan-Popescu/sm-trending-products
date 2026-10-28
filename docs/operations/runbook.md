# Operations runbook

Use [analytical execution diagnostics](analytical-run-diagnostics.md) to inspect
assessment/freeze work, cache behavior and failures independently of result hashes.

The production target is a VPS running Docker Compose with PostgreSQL and external
S3 storage. A shared application image runs separate planning, raw-capture,
normalization, analytical and operator-interface containers; PostgreSQL has its
own image. The independent host controller installs signed releases. This
configuration does not establish an actual VPS deployment or live-source access.

## Prerequisites

Use Docker Engine with Compose v2.20 or newer, a dedicated S3 bucket, the trusted
host controller and private Tailscale access. Initial host installation uses the
Hostinger management console; SSH is not part of this deployment. Keep application
and administrative ports closed to the public internet. Permit the connectivity
required by Tailscale and outbound HTTPS for GitHub/GHCR, Sigstore trust material,
the chosen object store and granted TikTok endpoints. See the
[controller installation guide](private-release-controller.md) for trust and
private-file prerequisites.

The checked-in image versions are explicit: PostgreSQL 17.11, Python 3.12.14, Rust 1.99.1, uv 0.12.23, and Moto 5.2.4 for tests. Review upstream security updates before each deployment and update the pins deliberately. Python and Rust dependencies are locked separately.

The two public bind-mounted files under `deploy/postgres/` and `deploy/dagster/`
must be readable by their container users. A restrictive checkout umask may
require `chmod a+rX deploy/postgres/001-create-dagster-db.sh deploy/dagster/dagster.yaml`.
Keep `.env` and credential files private. The image explicitly grants read/traverse
permissions to its public source/scripts for the unprivileged application user.

For managed environments using an intercepting HTTPS proxy, build with an optional
BuildKit `proxy_ca` secret containing the trusted combined CA bundle. The Dockerfile
uses it only during dependency installation; it is not copied into image layers.
Compose preserves external proxy routing and excludes its private service names
from proxying. The isolated restore similarly excludes only its own container names.

The pinned Dagster storage configuration explicitly selects `postgresql+psycopg2`.
Its legacy notification statement is incompatible with SQLAlchemy 2.1's default
psycopg3 server-side parameter binding. Application SQL continues using psycopg3.
Readiness probes use TCP, which remains unavailable during the official image's
temporary socket-only initialization server.

Production S3 must provide the conditional writes, metadata, and consistency used by the storage implementation. Enable encryption and appropriate versioning/lifecycle policy, then verify the actual provider. Synthetic HTTP tests do not certify a provider's durability or IAM policy.

## Configure production

Copy `.env.example` to a private `.env` or inject equivalent secrets through the deployment environment. Restrict its permissions to its owner. Set a generated database password, the matching URL-encoded password in `DATABASE_URL`, the bucket, region, and S3 credentials or the applicable host identity configuration. Inside Compose, the database hostname is `postgres`.

Leave `S3_ENDPOINT_URL` empty for AWS S3. For another production provider, use its HTTPS endpoint. Create the bucket through the provider first; `CREATE_BUCKET_ON_INIT=false` prevents implicit bucket creation. The initialization command can create a bucket only when explicitly configured.

Keep TikTok token fields empty until approval exists. A developer key is not a granted source capability. Enable a live adapter only after its actual endpoint, account, scopes, permitted intended use, quota, retention, and returned fields have been verified.

The database, Dagster storage, spool, and cache use separate named volumes. These are local state, not a replacement for backups or S3 durability. The namespace setting identifies the environment; real/synthetic flags enforce evidence separation. Separate production and development databases/buckets as well.

## Initial provisioning

Follow the [private controller guide](private-release-controller.md) for the first
installation. Initial provisioning and later code updates are distinct operations:

1. Publish a compatible main revision through the release workflow with deployment
   disabled. Verify its signed manifest and use the immutable published image
   digest for the initial application roles. Build images in CI, outside the VPS.
2. Prepare the root-owned runtime configuration, private credentials and external
   bucket. Apply the documented [4-vCPU/16-GiB starting profile](../../deploy/production-4cpu.env.example)
   when appropriate, then qualify actual host headroom and provider behavior.
3. Initialize PostgreSQL/application state and start the initial planner, blue
   capture slot, normalizer and downstream roles. The initialization container
   must finish successfully first. New PostgreSQL volumes create the separate
   Dagster database automatically; existing volumes need that database provisioned.
4. Verify the initial signed container identity and registered worker, adopt that
   baseline with the host controller, then enable its private polling timer.
   Bootstrap adoption does not qualify source coverage.

Validate private configuration with `config --quiet`: printing interpolated
Compose settings can reveal credentials. Keep the actual image/worker pins and
controller state private. Use the controller procedure below for subsequent
updates rather than repeating initial stack startup.

The default application user is unprivileged, root filesystems are read-only, temporary and persistent write locations are explicit, capabilities are dropped, and logs rotate. Resource limits are configurable starting settings, not a tested capacity guarantee. Measure the declared workload and leave memory/disk headroom.

## Private access

The dashboard and Dagster bind to host loopback ports 8501 and 3000. Configure
[Tailscale Serve](https://tailscale.com/kb/1312/serve) on the VPS to forward private
HTTPS access to those existing loopback HTTP services. Restrict access to the
intended operators through tailnet policy and validate application routing on the
actual target. This is the supported application-access boundary; no SSH tunnel
or public Funnel endpoint is required.

Keep actual tailnet names, host addresses and Serve routing in private host
configuration, outside this public repository. These instructions do not claim
that Serve or HTTPS has already been configured on the VPS.

PostgreSQL publishes only `127.0.0.1:${POSTGRES_HOST_PORT:-5432}` for the host-local
controller. Its private host DSN uses that address and port; application containers
use `postgres:5432`. Keep PostgreSQL off public interfaces and outside Serve.

## Local synthetic execution

The optional `local` profile uses a real HTTP Moto S3 emulator. Its state is ephemeral and disappears when its process restarts. Use it only for isolated synthetic testing; it is unsuitable for live evidence or durability testing.

```sh
python3 scripts/create-local-env.py
docker compose --profile local up -d --build
```

The generator refuses to overwrite an existing file and writes mode 0600. It supplies a random local database password, explicit dummy AWS credentials, and enables test-bucket creation. Existing production configuration must never be replaced with this local configuration.

Moto replaced the initially considered MinIO development image after the official Docker Hub repository/tag lookup returned 404. This is a development-service choice, not a change to the production S3 design. If the local emulator restarts, recreate/reingest the synthetic test state or restore a complete synthetic backup; a surviving PostgreSQL volume alone cannot recreate lost objects.

## Health and collection status

Check both process health and data health. The status report and dashboard expose access state, queue depth/age, due work, failed/quarantined work, last evidence time, source capability failures, and history/cache I/O where measured. A running worker with denied scopes is not a functioning source.

Investigate the reported category rather than dumping tokens, request headers, signed URLs, or remote response strings into tickets. Follow an observation's dataset manifest and evidence references when investigating a ranking. Missing evidence and denied access remain visible; they do not become zero activity.

`storage_capacity` reports filesystem total, used and available bytes for cache
and spool, plus the configured cache ceiling. These are constant-time filesystem
counters, not directory-size scans or per-process memory measurements. A spool
directory not yet created explicitly reports its nearest existing ancestor's
filesystem; inaccessible measurements remain unavailable. Shared volumes can
report the same capacity, so do not sum these values. The health view exposes the
same measurements without publishing local paths or filesystem error text.

The independent capture planner uses the application scheduler to admit due
tasks within shared budgets. Capture workers remain running and process that
queue; actual sampling follows the declared source policies and due intervals.
The separate normalizer consumes saved responses. In production independent
mode, Dagster coordinates downstream analysis and maintenance and disables its
legacy automatic collection schedule. See
[independent acquisition](independent-acquisition.md) and
[capture contracts](capture-contracts.md) for cadence and coverage boundaries.

Source retry windows take precedence over the desired refresh interval, including
valid waits longer than a day. Workers persist the due time and release their
lease rather than sleeping for that period. A nonfinite, negative or
unrepresentable delay produces `invalid_retry_hint` and quarantines the task;
inspect the source/adapter contract before enqueueing replacement work.
`collector_timeout` terminates the child collector and preserves retryable work
without publishing partial evidence.

## Frozen ranking policy and outcome evaluation

`sm-trends freeze` and `sm-trends backtest` accept `--policy-file policy.json`
using the same `AssessmentPolicy` JSON as `sm-trends assess`. For example:

```json
{"version":"attention-three-samples-v1","min_observations":3}
```

Use `--config-file evaluation.json` separately for the target, threshold, market,
top-K, horizons, launch delays and `policy_available_at`. A backtest must not use a
policy before its declared availability. With no policy file, the default ranking
is retained. The chosen policy and resolved identity/evidence are frozen in the
decision; subsequent configuration changes do not alter its maturation.

```sh
sm-trends freeze --synthetic --policy-file policy.json --config-file evaluation.json
sm-trends backtest --synthetic --policy-file policy.json --config-file evaluation.json \
  --decision-times-file decisions.json
```

Native controlled tests exercise 3/7/14-day horizons and separate immediate,
2-day and 10-day launch scenarios. Missing boundaries remain censored and launch
delays at or beyond a horizon remain not actionable. Fixture maturity is an
engineering proof; actual prospective outcomes require elapsed real history.

## Backup and recovery

See [backup and restore](backup-and-restore.md). Run an isolated restore exercise after configuration or schema changes and on a regular operational schedule selected by the operator. Backup creation alone does not demonstrate recoverability.

## Updates and maintenance

For independent acquisition, take an [online checkpoint](backup-and-restore.md)
without stopping capture. Validate the candidate against an isolated restored
copy, then follow the [signed private release controller](private-release-controller.md).
The release workflow's deployment request authorizes the application update;
the host polls it over outbound HTTPS. It starts a compatible replacement,
requires committed raw capture
evidence from that worker generation, drains the predecessor and retires it
only after acknowledgement. Missing evidence or unsafe compatibility blocks
the update. The controller then refreshes planning, normalization, analytical
and operator roles through the [processing release procedure](processing-releases.md).
Schema expansion must support both revisions; contraction belongs
to a later separately qualified release. Keep the actual host configuration and
credentials outside this public repository.

The legacy offline checkpoint pauses every application writer and is suitable
only for an inactive deployment or a source window whose recovery has been
explicitly qualified. It is not the production update procedure for unknown or
ephemeral source data. See [independent acquisition](independent-acquisition.md)
and the [controller operations guide](private-release-controller.md) for initial
bring-up and rolling updates.

Compaction publishes a completed replacement before changing the catalog's read frontier, preserves original observation/availability/provenance, and retains raw evidence subject to policy. Retention preview is read-only; applying retention invalidates dependent assessments/evaluations before deleting restricted objects. Versioned or suspended-versioning S3 buckets require listing and deleting every affected version and delete marker; a simple delete marker is insufficient. The service identity therefore needs bucket versioning inspection, version listing, and version deletion permissions in addition to ordinary object reads/writes/deletes. Object Lock or denied deletion leaves explicit pending work for retry. Review source policies and backup retention together: a backup can also contain data that must expire.

Cache eviction must affect only reproducible local copies. If a cached object fails its checksum, reacquire and verify it. Do not delete S3 history to free VPS cache space.

`HISTORY_TARGET_ROWS_PER_FILE` defaults to 1,000 rows and bounds physical fanout
under `HISTORY_PARTITION_BUCKETS`; small source pages therefore avoid producing
one Parquet object per entity. The `history_compaction_batches` Dagster job runs
at the configured `MAINTENANCE_SCHEDULE_MINUTES` cadence (15 minutes by default).
Its persisted schedule remains `history_compaction_schedule`. The maintenance
service can drain multiple compatible groups,
retaining at most 100 datasets per atomic group; see
[resource-budgeted maintenance](maintenance.md) for limits and backlog semantics. Its
default workspace is live; run it explicitly with `synthetic=true` for synthetic
history. It preserves raw provenance and may correctly report `no_work` when no
two eligible small datasets fit the row limit.

The additive object/delivery index migration preserves existing datasets. Their
first evidence-page requests may return `status=indexing` while preparing compact
lookup references from verified legacy Parquet. Retry the same product, workspace,
cutoff and cursor until `status=ready`. Each request processes at most
`HISTORY_INDEX_OBJECTS_PER_CALL` objects (default 32), using 1,000-row Arrow batches.
Current publications index atomically and do not require that preparation. Use
`Pipeline.evidence_page` cursors for stable paging; the compatibility `evidence`
offset API skips bounded pages and raises an explicit pending error during legacy
preparation. Cursors cannot be reused with a different product, workspace or cutoff.

## Scale decisions

Use [benchmarking and costs](benchmarking.md). Increase worker concurrency only within source quotas and after measuring database contention, memory, and store traffic. Move the same independent containers and endpoints first when leaving one VPS. A distributed queue, a new orchestration engine, or distributed SQL requires a measured bottleneck and a separate architectural decision.
