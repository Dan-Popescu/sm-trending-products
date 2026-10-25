# Archive complete source pages before normalizing observations

Status: accepted for the authorized collection-continuity implementation, issues
#80 and #84.

The legacy collector normalizes a response before Python can archive it. A broken
record decoder can therefore block the next page of an expiring source query.
Application replacement also needs a durable response boundary independent of
analytical publication. Introduce an explicit acquisition protocol without
changing legacy collection or v1 observation identities and replay hashes.

The Rust `capture` command validates the existing source request and uses the
same authorization, quota reservation, HTTP bounds and source URL rules. It
receives one complete successful HTTP body before invoking a synchronous receipt
writer. JSON responses retain the complete JSON value, including unknown fields,
after credential redaction; this is canonical JSON rather than an exact copy of
wire whitespace or duplicate object keys. Invalid JSON retains every sanitized
byte in a hex envelope, including non-UTF8 bytes, without the legacy diagnostic
excerpt limit. Partial or oversized bodies are not complete receipts.

Receipt acquisition only interprets the source envelope and continuation. It
never invokes the record normalizer. Valid continuation can therefore advance
past an unknown record schema, while malformed continuation archives the response
and quarantines the task instead of guessing a cursor. Routine source business
errors are archived as `raw_capture_error` and never settle a capture obligation
or enter normalization. A fenced task `capture_attempt` advances only after the
error dataset commits; a crash adopts the same error and advances it once.
Attempt zero retains its established ID; later attempts add `capture_attempt` to
the identity hash, without changing the original page/request or obligation due
time. Successful pagination resets the attempt counter for the next page.
Rate limits preserve shared quota cooldown and do not consume failure attempts.
Access denial blocks collection until access is recovered, with a fresh attempt
available rather than an immutable error masquerading as the successful page.
Existing source/account capability and
retention-policy gates apply before live acquisition. Synthetic endpoint/time
overrides remain forbidden in live requests, and account scope and request
filters are retained through pagination and later replay.

Before HTTP, reserve 32 MiB against a configurable shared spool budget. The source
body remains bounded at 8 MiB and the serialized receipt at 32 MiB. Private files
and directories, a cross-process budget lock, and 256 fixed acquisition lock
shards bound disk admission and lock inode use. Reaching capacity refuses new
HTTP work; it never evicts unshipped inputs. The per-capture shard lock spans
HTTP and durable receipt publication, so a replacement process waits for and
adopts the previous owner's response instead of fetching the same one-shot page.
Local advisory locks require all acquisition containers to share the same VPS
volume; this is not a distributed multi-host spool protocol.

Write the complete receipt to an exclusive temporary file, fsync the file,
atomically rename it, and fsync its directory before returning to Python.
Response checksums cover the stored JSON span, so floating-point re-encoding
cannot break recovery. A complete temporary receipt can be adopted after a
crash. An incomplete or corrupt receipt fails closed and stays available for
investigation. Reservations survive crashes; retrying the same task can reuse
its reservation. Adopting a complete final receipt also retires a leftover
reservation from the rename-to-cleanup crash window.

A separate bounded spool-maintenance module uses a progressing directory iterator
instead of rescanning a fixed pending prefix. It verifies complete receipt
checksums, skips active writer locks, and preserves unshipped inputs during
ordinary capacity cleanup. Transient S3 failures leave local copies protected
without blocking acquisition. Empty abandoned reservations with no input bytes
can be released. New reservations durably record nonsecret source/account,
workspace and task/page/attempt scope before HTTP, so incomplete private files
can also be retired under an authoritative removal request.

Legal removal is distinct from eviction. Maintenance previews by default and
applies deletion only under a verified source retention deadline, an existing
catalog invalidation, or an explicit PostgreSQL source/account/workspace cutoff
revocation. Credential expiry alone never destroys unshipped inputs. Explicit
cutoffs cover an acquisition started before the cutoff, including an in-flight
request that completes later. Retention for complete receipts starts at physical
capture time; an incomplete reservation receives an additional collector-timeout
grace. The exact task/page/attempt is durably cancelled before removing bytes;
registered datasets retire through the existing invalidation lifecycle.
Before cancellation, store an immutable operational retirement certificate with
the task/page/attempt, source/account/workspace, original physical clock and
exact authority reference. A SQL trigger independently verifies the canonical
capture identity and either its scoped revocation journal row, verified policy
snapshot with elapsed deadline, or exact invalidated dataset. The database owns
the retirement clock. This certificate distinguishes lawful withdrawal from an
ordinary missed or cancelled obligation without inventing a capture receipt.
Retiring an old diagnostic attempt does not withdraw a newer active attempt on
the same page. Committed certificates survive cleanup failure and later policy
changes, so retries can finish deleting already-retired local bytes safely.
Publication-time guards under the retention fence reject expired/revoked receipts
and recheck current live source access. A stale in-memory body cannot republish
after its local file is removed. The scoped spool-removal API does not replace
the existing remote-history source-removal procedure for other datasets.

Publish the receipt through the existing immutable S3 raw object and
staged/committed dataset manifest lifecycle. A raw-only dataset has zero
observations, `kind="raw_capture"`, and `acquisition_protocol="1"`. Its identity
is the hash of `{capture_of: task_id, page_index, protocol: "1"}`. The manifest's
`capture` object contains `capture_id`, `task_id`, `page_index`, `worker_id`,
`lease_version`, `capture_attempt`, `request_fingerprint`, `started_at`, and
`captured_at`. Registered acquisition
also includes the actual acquiring `worker_generation`; unregistered legacy
callers omit it. Worker identity/generation and
physical acquisition time always describe the process that obtained the body;
adoption cannot rewrite them to manufacture replacement readiness. Synthetic
`observed_at` does not replace physical `captured_at`.

Only a committed raw dataset permits fenced source cursor advancement. Existing
task lease/version and namespace/retention publication checks remain authoritative.
Local spool removal follows remote commit, and cleanup failure does not undo a
successful capture. A committed business-error receipt remains local until the
exact task/page/attempt also settles; a crash between commit and attempt
advancement must retain a source-free recovery input. Subsequent acquisition
batches retry cleanup only after
reading and verifying an identical committed remote receipt. Legacy task/page
deliveries must finish through their legacy recovery path rather than refetching
them under a new identity.

Complete checksum-verified local receipts are recovered under the existing task
lease independently of new source HTTP admission. Recovery cannot fetch a
replacement body, does not increment the source failure counter, and preserves
the original acquiring worker/generation and physical clocks. Only the exact
stored task/page/attempt may reopen an `attempt_limit` quarantine; other
quarantines and cancelled tasks remain blocked. Retry due times, worker drain,
namespace fencing, current source access, retention and revocation still apply.
A bounded progressing local scan prevents a recurring directory prefix from
starving later inputs. An additional progressing page of active task identities
recovers already committed business errors whose local files an older image
removed before settlement, using exact dataset-ID lookups and verified remote
bytes rather than retained-history scans. Ordinary budget-admitted Rust adoption
can win before the local scan; it discounts only that new claim's increment,
retaining the original request's failure accounting through repeated storage or
invalidation pauses. No new observation or completion lifecycle is introduced.
Receipt-only recovery alternates with existing priority-ordered source admission;
the turn persists across one-item batches. A failed-receipt backlog therefore
cannot occupy every admission slot while a higher-priority fresh obligation is
due and the spool still has room. Existing priority, quota and retry backoff
remain authoritative; this does not promise equal service across priority tiers.
Migration 036 adds only a partial task index for progressing recovery pages,
including exact attempt-limit quarantines. Page selection takes a fixed number
of potential tasks before evaluating due time and lease expiry, so a future-due
prefix cannot cause an unbounded filtered scan. The same query remains
functionally compatible with schema 35, but its bounded query-work guarantee
requires the new index. Index construction is a one-time additive migration,
not a claim that schema expansion has no write-lock or temporary-space cost.

A separate publisher claims committed raw parents using short PostgreSQL
transactions and `SKIP LOCKED`. Lease rows contain only worker ownership, fence,
expiry and retry metadata; the existing committed normalized child is the sole
publication completion marker. No second dataset lifecycle or message bus is
introduced. Publication guards recheck the current lease and parent eligibility
inside both staging and commit transactions, so expired owners cannot publish.
Failed decoders back off independently and do not starve other parents. Decoding
occurs entirely from archived receipts, without source calls or access tokens.

Schema 037 adds a disposable pending-parent eligibility projection, maintained
transactionally by dataset hooks including compatible old-image writes. Claims
enumerate and sort pending/retry work before probing and locking one parent at
a time; retained completed history no longer drives ordinary polling. Committed
v1 children remain the sole completion fact. A one-time guarded schema backfill
reconstructs existing eligibility; schema-35/36 bridge claims retain the prior
query until expansion. The [selection verification](../verification/publication-frontier.md)
records stale-statistics, lock-order, rollout and measurement limits.

Normalized children use existing immutable replay publication and parent lineage.
Raw-parent replay reserves a processing availability clock independently from
the original observation time. It never backdates derived knowledge to a delayed
source response. Record schema failures produce a derived quarantine while
leaving raw data available for a later supported transformation.
Display query requested/missing-member diagnostics are computed in the derived
publication after normalization, while the raw acquisition receipt stays pristine.
Existing legacy datasets, v1 identifiers/hashes and v2 replay aliases retain their
prior contracts.
Raw parents participate in existing source invalidation, retention, backup and
object verification; zero-row parents are already excluded from compaction.

This boundary protects a response after local fsync and through normalization
failure, publication failure, credential rotation and process replacement. A
planned deployment must drain in-flight HTTP work and retain the shared spool.
It cannot guarantee recovery of bytes lost before the durable receipt boundary,
an arbitrary source outage beyond its recoverability window, or destruction of
the VPS disk before remote archival. Local fsync and committed remote archival
are distinct durability milestones; release readiness must use actual committed
raw receipts from the replacement worker, not heartbeat or adoption metadata.

The new commands are an explicit expand path. The legacy `collect_batch` remains
available during migration; coordinated deployed roles and worker-generation
control are owned by ADR 0013. Introducing standard-library file locks raises
the declared collector minimum Rust version to 1.89; the pinned production and
CI toolchain remains 1.99.1.
