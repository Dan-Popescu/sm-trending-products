# Backup and restore

A complete recovery set contains the PostgreSQL control state, immutable object bytes and metadata, and the application/dependency versions needed to read them. The scripts use `pg_dump` and `pg_restore` from the pinned PostgreSQL container so the client matches the server major version.

## Capture

For a complete copy:

```sh
scripts/backup.sh /absolute/new/backup-directory --copy-objects
```

The directory must not exist. The script uses restrictive permissions, validates the deployment, pauses running writers in this Compose deployment, checks current committed evidence, and captures application and Dagster custom-format dumps. It records hashes of the logical catalog state, including products, identity decisions, alerts, and frozen evaluations. It then streams the dedicated S3 bucket into content-checked local files, retaining object keys, source versions, content type, and metadata. A completed marker is written last. The original running services are restarted on exit.

The script pauses the worker, Dagster daemon, Dagster webserver, and dashboard to obtain a coherent recovery set. Stop any additional deployment or external importer sharing these stores before capture. Do not run retention/deletion from another process during capture. Never use a bucket containing unrelated applications: full-copy mode reads every current object in the dedicated bucket and consumes bandwidth and local disk.

Without `--copy-objects`, the backup contains an object inventory only. Such a backup depends on the retained source objects/versions and cannot pass the independent object-store restore exercise. Bucket versioning and lifecycle rules are useful protection, but an inventory is not a separate durable copy.

Copy completed backups off the VPS, encrypt them, restrict access, and expire them according to the relevant source retention/deletion requirements. The project does not purchase or provision a backup destination.

## Verify and rehearse

```sh
scripts/restore-check.sh /absolute/complete/backup-directory
```

The command checks dump and inventory hashes, creates a private internal Docker network, and starts fresh temporary PostgreSQL and HTTP S3 emulator containers. It restores the application and Dagster databases and copied objects under the original environment namespace, runs the application's read-only manifest/object verification and status report, and compares the restored logical catalog with the captured hashes when present. It then destroys the temporary containers and network. It does not start workers against restored queues and does not write the production bucket or database.

The lower-level database restore command accepts only an empty container whose name starts with `sm-trends-restore-` and which has the isolated-restore label. It refuses existing application tables and performs one transactional restore without a destructive `--clean` step. This deliberately excludes an accidental in-place production restore path.

The local rehearsal uses Moto and checks logical evidence recovery. Native PostgreSQL is used in the container exercise. Real provider versioning, access policies, encryption, and durability require separate provider acceptance. Record elapsed restore time, data volume, missing objects, and verification output; decide recovery objectives from measured results.

## Recovery after an incident

1. Preserve the failed state for diagnosis and stop all writers.
2. Restore a completed recovery set into isolated destinations and verify it.
3. Confirm the restored application's schema/version compatibility and source policies.
4. Point a new deployment at the verified destinations. Rotate or reinject credentials as needed.
5. Reconcile pending/staged work before enabling collection. Confirm duplicate delivery does not inflate observations.
6. Check frozen alert and evaluation status, then enable the approved capabilities and monitor data freshness.

The scripted isolated check is intentionally safer than automating an in-place production cutover. Actual destination selection and cutover require the real infrastructure details; those were not supplied for this implementation.

## Validation status

The snapshot CLI has been exercised against a live HTTP Moto service for capture, verification, restore, metadata preservation, corruption rejection, and refusal to restore over its source. Shell syntax and the Dagster instance configuration have been checked locally. Docker was unavailable in the implementation environment; Compose image build and the full container restore exercise must pass on a Docker-capable host or CI before production rollout.
