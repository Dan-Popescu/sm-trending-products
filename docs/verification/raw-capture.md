# Raw-first acquisition verification

`tests/test_raw_capture.py` exercises the real Rust subprocess against one-shot
loopback HTTP sources, native PostgreSQL and an isolated HTTP object store. No
TikTok access is required. Its evidence boundary is the supported single-VPS,
shared-spool deployment, not an external live-source availability guarantee.

The native scenarios establish that acquisition continues through valid
pagination while the normalizer is stopped or incompatible; archived pages can
later be normalized with both the source and token removed. They preserve the
source account and filters for Display and Commercial Content request families.
Unknown records remain available in raw history and create an explicit derived
quarantine. Delayed derived observations receive their processing availability
time, and existing verification/invalidation sees their raw parent lineage.

Injected failure after receipt fsync leaves a complete local receipt. Recovery
from the pre-rename temporary file adopts it without another HTTP call or token,
retaining the original acquiring worker. An expired source worker and concurrent
replacement run actual collector processes: exactly one one-shot response is
requested, the stale task fence rejects publication, and the replacement uses the
same durable body. A separately expired normalization lease rejects stale
publication before dataset mutation. A broken first normalizer does not prevent
a second raw parent from publishing.

The suite also verifies preservation beyond the old 65,536-character diagnostic
limit with credentials removed, capacity refusal before HTTP without deleting
pending reservations, incomplete HTTP bodies without receipt/cursor advancement,
and committed cleanup retry without refetching or marking a completed capture as
failed. Fixed lock shards and explicit admission reservations bound local spool
resources; corrupt/incomplete receipts fail closed rather than being silently
replaced by a new source request.

`test_raw_capture_recovery.py` verifies immutable business-error diagnostics,
independent attempt identity after archive, shared rate-limit cooldown,
crash-idempotent attempt advancement and recovery after access denial. Original
query scope and page stay fixed. Display requested/missing-member diagnostics
appear only after normalization.

`test_capture_spool.py` verifies preview/apply of orphan retention, scoped
revocation with workspace separation, preservation during ordinary credential
denial, progressing cleanup after a pending prefix, and source-offline partial
receipt removal using durable reservation metadata. It holds a real HTTP request
in flight while registering revocation: maintenance skips the active writer,
the completed body becomes durable, and publication is rejected under the new
policy. Registered revoked receipts use existing dataset invalidation and retire
their normalized children. Temporary S3 cleanup failures preserve local bytes
while independent source acquisition continues.

`test_capture_retirement.py` exercises the database retirement authority seam.
Borrowing another account's revocation, unverified retention, or a future expiry
cannot create a certificate. Source-removal journals and retirement certificates
reject updates/deletions. A crash after certificate/cancellation commit but
before unlink can finish cleanup after policy changes, with the original audit
unchanged. Retiring an old business-error attempt leaves the newer attempt
active, and positive-attempt identities receive their own qualified certificate.

Legacy regression checks run `test_collector_contract.py`,
`test_collection_review.py`, `test_replay_delivery.py`, and
`test_replay_versions.py` alongside these scenarios. Rust formatting, Clippy with
warnings denied, Rust unit tests, Ruff and mypy verify the implementation surface.
The checked-in suite demonstrates these controlled boundaries; deployment
controller readiness, backup checkpointing and source-specific recovery windows
have separate integration evidence.
