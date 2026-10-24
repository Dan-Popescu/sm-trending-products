# Reuse exact product features between source and clock transitions

Status: adopted under the user's scale and cost implementation authorization.

History, reviewed identity and versioned policies remain authoritative. A
product feature projection is disposable, versioned computational state. New
source publications, normalization revisions, changes to active compaction
selection and retirement advance metadata-only entity generations in the same
PostgreSQL transaction. Projections also bind the complete product, resolved
ownership scope and policy configuration. Missing legacy bounds, future-visible
inputs, older requested cutoffs, mismatches and retired audit roots rebuild
through the existing full analytical path.

Between source changes and the next rolling-window/freshness deadline, current
unnormalized features are unchanged except for publication age. Analytics emits
that age's exact median publication anchor from the same usable video timelines
that produce the feature. Reuse advances age from that timestamp, rather than
adding elapsed floating point hours to an old rounded value. Every current
universe is globally normalized again, so crossing age/market cohorts changes
ranks exactly as the complete assessor does. Any evidence's observed-time
lookback or freshness expiry, including a declared source period's own expiry,
ends the reuse interval. Existing inclusive boundaries are preserved.

Only compact result rows belong in projection storage. Audit membership remains
in the existing immutable provenance artifacts, referenced by their committed
publication root; it is never dropped to save calculation work. A caller must
attach a hit's audit root to its normal publication dependencies and retain the
existing authoritative source-retention and frozen-parent guards. Projection
state is not a replacement for any of those checks. Cache writes compare source
generations again and cannot overwrite a newer cutoff with an older run.

Entity generation metadata contains no observation payload and can be rebuilt
from indexed manifests. Conservative maximum-seen time bounds can disable reuse
until an unusually future-dated import becomes visible; this trades performance
for exact historical semantics. A changed product currently rebuilds its full
bounded analytical window. Incremental append/rolling entity state may replace
that fallback only with full-oracle parity for corrections, expiry, ownership,
retention and historical cutoffs; this first layer makes no such delta claim.

## Rolling local evidence

A dirty product can retain its exact bounded observation window as compressed
Arrow/Parquet batches in the existing verified file cache. The state competes for
the same global byte budget and crash-released pins as source files. Missing,
evicted, corrupt or incompatible state rebuilds through History, with no attempt
to fetch a fictitious local-state object from S3. One product's Arrow encoding is
limited to 1,000-row batches and at most 64 MiB or the configured shared cache
ceiling, whichever is smaller.

Current active source-object identities and payload checksums determine the
delta. A newly committed backdated import is therefore included even when its
availability time precedes the old cutoff. Native predicates evict expired rows;
only newly observed source objects need payload reads. Retired or compacted old
membership, a changed checksum, future/unknown bounds or duplicate identifiers
rebuild through the full oracle, preserving conflict detection and oldest source
provenance. Changed products still run the complete feature formulas over the
retained local window; this layer reduces source I/O and does not claim an
incremental update for every individual formula.

A committed audit root must cover **all** cached raw observations, including
inputs excluded by the current market or capability filter. A durable staged
window pointer registers ownership before writing bytes. Root retirement or
pointer replacement queues the local key in the existing retention outbox with
`local_only=true`. Cleanup uses the same cache locks and unlinks but issues no
provider request. Tombstones prevent later admission under the retired key;
a new cache generation binds bytes to its context, cutoff and audit root.
Backups need not copy disposable local state or require it as a remote object.

Maximum-seen source bounds can be reconciled selectively with
`FeatureProjection.rebuild_frontiers(scopes)` after a retired future-dated input.
This reads indexed metadata and advances the disposable generation, without
reading payloads or trusting an older result. Representative cache-state sizes,
source-cache competition and eviction fallbacks must be measured; a smaller
single state is not proof that the complete production working set fits.

Physical deletion currently applies to the shared cache volume used by this
deployment. Independent cache volumes on future multiple hosts require per-host
retirement acknowledgement before claiming that every physical copy is gone;
the shared catalog tombstone and audit-root checks already prevent reuse.

The reproducible native 20-video/120-sample fixture in
`tests/test_feature_windows.py` measured a 121,556-byte state for 2,400 complete
observations. A subsequent 20-video refresh required one new source object:
9,401 downloaded bytes against 1,133,412 bytes and 120 source GETs for the full
oracle after independently clearing source-file cache entries. The entire
analytical result matched the full rebuild. These are controlled fixture
measurements, not production latency or cloud cost estimates. Ten thousand
states with that exact shape would occupy about 1.216 GB of the default shared
2 GB cache, before competing source files, larger attributes or skew. Eviction
and full-history fallback are therefore part of the supported normal path.

Freshness deadlines conservatively satisfy both existing floating-point oracle
checks (elapsed seconds and elapsed hours). A fractional duration that rounds
up to a microsecond cannot extend reuse beyond the true stale transition;
ordinary exact duration equality remains reusable.

Published assessments and frozen evaluation decisions share `AssessmentService`.
It reads feature hits in bounded product batches, restores exact distinct-time
observation counts for freeze eligibility, and attaches cached all-input roots
to the new publication. Misses run the full oracle over `load_window` inputs,
then save unnormalized compact features and optional local rolling state.
Evaluation's market and complete capability contract, including currently
visible grants and future-source scoping, enter the stable context digest.
Family selection and normalization remain current operations over every product.

Dirty products share one source-object read per adaptive bounded partition,
including missing or evicted raw windows. Exact cached deliveries read incidentally
for another product are deduplicated; a changed delivery still rebuilds its full
scope to preserve original source ordering and conflict validation. Typed row
budget failures propagate before source fallback. Raw-window identity binds only
its schema, product namespace, entity keys and lookback: alternating a published
assessment with a capability-filtered freeze cannot discard otherwise reusable
raw facts. Feature identity continues to bind every policy and eligibility gate.

The native 100-product/12-shared-source fixture verifies 12 cold file parses,
zero unchanged parses, one new refresh parse and 13 parses after every raw window
is evicted. Observed times were 5.55 s cold, 0.23 s unchanged and 5.02 s for the
100-product refresh. That fixture has 1,200 initial observations and establishes
shared parsing and current caller behavior; it does not estimate the runtime of
a 100-product window with 240,000 observations.

SQL027 removes a compact feature payload in the same retirement transaction when
its result audit root or all-input audit root becomes invalidated or deleted.
This physically removes copied prices and histories from the current SQL catalog;
merely denying cache reuse would leave those facts in subsequent catalog backups.
The migration also purges preexisting unavailable projections. Both root columns
have partial indexes, so retiring one artifact does not scan every product cache.
Local rolling-window bytes retain their existing fenced deletion outbox.
