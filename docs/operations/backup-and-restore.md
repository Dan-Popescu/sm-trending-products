# Backup and restore

A complete recovery set contains the PostgreSQL control state, immutable object bytes and metadata, and the application/dependency versions needed to read them. The scripts use `pg_dump` and `pg_restore` from the pinned PostgreSQL container so the client matches the server major version.

## Capture

Keep a dedicated backup family directory. Create the first complete recovery set:

```sh
mkdir -p /srv/sm-trends-backups
scripts/backup.sh /srv/sm-trends-backups/base-001 --copy-objects
```

Subsequent recovery sets retain their own full PostgreSQL/Dagster dumps and object
inventory, but copy only added or changed object bytes:

```sh
scripts/backup.sh /srv/sm-trends-backups/delta-002 --copy-objects \
  --base /srv/sm-trends-backups/base-001
scripts/backup.sh /srv/sm-trends-backups/delta-003 --copy-objects \
  --base /srv/sm-trends-backups/delta-002
```

The output directory must not exist; every chain member must be a sibling in the
same dedicated backup family. Version 2 snapshots bind each base by relative path
and metadata checksum. Each latest inventory is complete: a source deletion is
represented by absence, so restore does not resurrect keys omitted by a later
checkpoint. Unchanged bytes remain in their original verified base. The chain
resolver uses a temporary disk-backed index instead of loading the full inventory
into Python memory. Capture places this scratch index on the backup volume rather
than the application's small `/tmp` tmpfs. Provision disk for inventories, hashes,
SQL dumps, new bytes and temporary indexes, then copy the complete required family
off the VPS. Restrict access and encrypt copies.

The script pauses the running worker, Dagster daemon, webserver and dashboard only
while it captures the object LIST inventory, logical catalog fingerprints and database
dumps. No per-object HEAD or payload GET runs during this pause. A session-level
shared PostgreSQL retention fence protects this checkpoint
without keeping an MVCC transaction open. Other deployments and external importers
sharing these stores must be stopped during the checkpoint. The inventory excludes
keys already tombstoned by retention, including pending object deletions, and
requires every committed dataset, valid assessment and committed derived
publication to be present, including transitive provenance nodes. Known catalog
checksums and byte sizes remain authoritative; legacy derived rows without those
fields still require existence and receive content hashes during copying.

After the SQL checkpoint, the original services resume **before per-object metadata
inspection, copying or verifying inherited base bytes**. The retention fence is released before the copy,
so an exclusive maintenance waiter cannot hold later collection behind the backup.
LIST freezes each key's size, ETag and last-modified fingerprint. A conditional
HEAD after resumption must match that fingerprint before a new object's GET pins
the inspected version ID where available and uses `If-Match`.
Copies are hashed against catalog checksums or source checksum metadata. Base files
are locally hash-verified before reuse. For a catalog-owned object, an unchanged
LIST fingerprint and authoritative catalog SHA matching the verified base allow
reuse of its bytes and immutable application metadata without HEAD or GET. An
unchanged managed incremental backup therefore issues zero HEADs and payload
GETs; it still lists every key, reads the SQL catalog and verifies the local chain.
Legacy inventories without a LIST fingerprint acquire it through HEAD once.
Unmanaged objects or legacy catalog rows without an authoritative checksum always
need metadata inspection after resumption; they may reuse verified bytes without
GET when their inspected identity still matches. ETags alone never replace the
authoritative checksum or local base verification.

For known catalog hashes, the restorable `Metadata.sha256` must also match that
hash. Correct payload bytes with a missing or wrong restored checksum header
would break later immutable-write verification, so fresh inspection rejects them.
A base record with incompatible checksum metadata cannot take the zero-HEAD path;
fresh valid source metadata can repair its inventory before verified bytes are reused.
Direct restore rejects incompatible checksum metadata before creating a destination,
including older inventories whose payload files themselves still verify.

Application object keys and metadata are immutable. The catalog-based fast path
restores their already verified base metadata; it does not remotely re-audit every
header on every run. Source mutation before deferred HEAD may make capture fail
rather than recovering a historical provider version. Mutation after HEAD is
covered by the version/conditional GET. Inventory-only captures still HEAD every
key after resumption and do not verify payload content.

Snapshot summaries expose logical `list_requests`, `head_requests`,
`catalog_reused_object_count` and `checkpoint_seconds` alongside copied/reused
counts and downloaded bytes. The checkpoint duration includes the SQL-dump/restart
handshake when used. These SDK operation counts exclude transport retry attempts;
listing, SQL reads and base-chain hashing remain proportional to retained inventory.
The previous full remote verification pass is folded into the copy.

This does not reserve source versions against a provider lifecycle rule or later
retention. If required new bytes disappear or change before copying finishes, the
backup fails and remains incomplete; retry from a new coherent checkpoint. Never
use an incomplete recovery set as a base. Provider conditional/version behavior
requires provider acceptance. The default whole operation deadline is one hour;
set `BACKUP_DEADLINE_SECONDS` from 1 to 86400 if the measured workload requires it.
The shell requires GNU `timeout` to bound inventory, catalog reads and SQL dumps
as well as copying, with an additional 30-second cleanup grace. Failure,
interruption and timeout restart the originally running services.

Without `--copy-objects`, capture produces a metadata inventory only and does not
verify object content. It cannot be used as an incremental base or independently
restored. The source-verification summary labels this boundary explicitly.

Keep every referenced base with its descendants. Do not rename, edit or remove
individual chain members; checksum and path checks reject broken chains. The
maximum chain is 32 members; create a new full base before reaching it. Retention
and source deletion obligations apply to **all** backups, including inherited
bytes: expire affected chains together or create a new permitted full base before
retiring older families. A new inventory omitting a key does not erase its bytes
from older recovery sets. The project does not provision a backup destination.

## Verify and rehearse

```sh
scripts/restore-check.sh /absolute/complete/backup-directory
```

Version 1 full recovery sets remain supported. For a version 2 incremental set,
pass the latest directory; the object restore resolves the full family automatically
and verifies every base, inventory and content hash before writing any object. The
whole family is mounted read-only in the isolated restore container.

The command checks dump and inventory hashes, creates a private internal Docker network, and starts fresh temporary PostgreSQL and HTTP S3 emulator containers. It restores the application and Dagster databases and copied objects under the original environment namespace, runs the application's read-only manifest/object verification and status report, and compares the restored logical catalog with the captured hashes when present. It then destroys the temporary containers and network. It does not start workers against restored queues and does not write the production bucket or database.

The lower-level database restore command accepts only an empty container whose name starts with `sm-trends-restore-` and which has the isolated-restore label. It refuses existing application tables and performs one transactional restore without a destructive `--clean` step. This deliberately excludes an accidental in-place production restore path.

CI additionally invokes `--exercise-synthetic-recovery` on its dedicated fixture
backup. Before capture, the fixture interrupts publication after immutable object
writes and before checkpoint commit. After restoring and comparing the original
catalog, the opt-in probe resumes that expired synthetic task from copied raw
bytes, commits the original dataset identity, and reopens the application to
verify no duplicate observation or task execution. Source requests are forbidden
by the probe, credentials are empty and the Docker network is internal. Existing
committed datasets, identity, alerts and frozen evaluations must remain identical.
The probe refuses live state, enabled capabilities or unrelated task queues; it is
not a general queue-draining or production cutover command. The default restore
check remains read-only after restoring its isolated copy.

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

Native PostgreSQL/HTTP Moto scenarios cover a full base plus two increments, unchanged zero-GET capture, changed versions, deleted keys, corruption or missing bases, path escape rejection, legacy full restore, conditional source races and checkpoint-fence release before copying. A complete marker is published only after all required new content is verified. Shell syntax and the Dagster instance configuration have been checked locally. The later [native CI run](../verification/native-ci.md) built the image and successfully executed the complete isolated native PostgreSQL/HTTP object-store restore, including catalog fingerprint equality. Actual provider behavior and VPS rollout remain separate acceptance gates.
