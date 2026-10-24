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

Legacy regression checks run `test_collector_contract.py`,
`test_collection_review.py`, `test_replay_delivery.py`, and
`test_replay_versions.py` alongside these scenarios. Rust formatting, Clippy with
warnings denied, Rust unit tests, Ruff and mypy verify the implementation surface.
The checked-in suite demonstrates these controlled boundaries; deployment
controller readiness, backup checkpointing and source-specific recovery windows
have separate integration evidence.
