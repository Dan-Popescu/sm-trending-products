# Operations runbook

The supported production target is a VPS running Docker Compose with PostgreSQL and an external S3 bucket. Rust collection, Dagster batch orchestration, native columnar analysis, and the private review interface use one application image. No service is deployed by checking in this configuration.

## Prerequisites

Use Docker Engine with Compose v2.20 or newer, a dedicated S3 bucket, and access to the target VPS. Allow outbound HTTPS to the granted TikTok endpoints and the chosen object store. Configure the VPS firewall and SSH access before rollout.

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

## Build and start

From the repository root:

```sh
docker compose config --quiet
docker compose build
docker compose up -d
docker compose ps
docker compose run --rm --no-deps initialize sm-trends status --json
docker compose run --rm --no-deps initialize sm-trends verify --json
```

Use `config --quiet`: printing the interpolated Compose configuration can reveal credentials. The initialization container must finish successfully before workers and interfaces start. New PostgreSQL volumes create the separate Dagster database automatically; an existing database volume needs that database provisioned before starting Dagster.

The default application user is unprivileged, root filesystems are read-only, temporary and persistent write locations are explicit, capabilities are dropped, and logs rotate. Resource limits are configurable starting settings, not a tested capacity guarantee. Measure the declared workload and leave memory/disk headroom.

## Private access

Only loopback addresses expose the dashboard and Dagster interfaces. Use an SSH tunnel:

```sh
ssh -L 8501:127.0.0.1:8501 -L 3000:127.0.0.1:3000 user@vps
```

Open `http://127.0.0.1:8501` for review and `http://127.0.0.1:3000` for orchestration. These interfaces have operational privileges; loopback binding plus a trusted SSH tunnel is the supported access boundary. Do not change bindings to public addresses without a separately configured authenticated TLS proxy. PostgreSQL has no published host port.

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

The application scheduler decides which candidate tasks are due and how the budget is split. Dagster coordinates batches and downstream assessments. A Dagster run is not created for every individual video.

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

Before upgrading, record the current image/dependency revisions, pause writers, create a complete backup, and validate the new image against a restored copy. Apply database initialization/migrations through the initialization command before restarting workers. Do not assume an old application is compatible with a newer schema.

Compaction publishes a completed replacement before changing the catalog's read frontier, preserves original observation/availability/provenance, and retains raw evidence subject to policy. Retention preview is read-only; applying retention invalidates dependent assessments/evaluations before deleting restricted objects. Versioned or suspended-versioning S3 buckets require listing and deleting every affected version and delete marker; a simple delete marker is insufficient. The service identity therefore needs bucket versioning inspection, version listing, and version deletion permissions in addition to ordinary object reads/writes/deletes. Object Lock or denied deletion leaves explicit pending work for retry. Review source policies and backup retention together: a backup can also contain data that must expire.

Cache eviction must affect only reproducible local copies. If a cached object fails its checksum, reacquire and verify it. Do not delete S3 history to free VPS cache space.

## Scale decisions

Use [benchmarking and costs](benchmarking.md). Increase worker concurrency only within source quotas and after measuring database contention, memory, and store traffic. Move the same independent containers and endpoints first when leaving one VPS. A distributed queue, a new orchestration engine, or distributed SQL requires a measured bottleneck and a separate architectural decision.
