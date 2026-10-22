# Replay rejection and publication contract

Offline replay invokes the bounded Rust normalizer against the stored response;
it neither refetches the source nor advances collection tasks. Imported and
compacted records retain their existing contract validation path. The installed
binary implements transformation versions `1` and `2`; unknown requested versions
fail explicitly. Collection and default replay retain version `1` unchanged.
Explicit version `2` reports formerly unmapped source-field paths and field-level
quarantine without discarding valid known counters. Values stay in the raw
archive; incompatible known containers quarantine the affected record.

The replay result reports accepted record count, `quarantine_count`,
`transform_version` and a `complete`/`quarantined` status. Row-level rejections
remain in the completed manifest's quarantine metadata, including when every row
is rejected. A top-level schema failure produces a zero-record quarantined child
of the archived input. Other process/configuration failures remain errors rather
than being recategorized as source-schema failures. Successful normalizer output
must identify the requested transformation version before publication.

Earlier replay code discarded rejection metadata. Corrected replay identities
include nonempty quarantine contents, producing a separate immutable publication
instead of reusing or rewriting a legacy false-clean replay. Clean version-1
replay identities remain unchanged. Original input manifests, prior replay
objects and collection cursors are preserved. The existing database publication
guard and lineage index retain authority over retired parents and descendants;
replay adds no alternative retention or task mutation path.

`tests/test_replay_delivery.py` uses the actual Rust process, native PostgreSQL
and HTTP object storage. It collects mixed-validity and entirely rejected pages,
stops the source, removes the Display token and replays twice. A legacy replay
fixture verifies upgrade behavior without rewriting the old manifest. Another
case replays an archived top-level schema failure. All cases verify task identity,
checkpoint preservation and remote checksum consistency. Native CI also executes
the existing replay/deduplication, retention and recovery scenarios.

Version 2 keeps source observation times but publishes distinct revision IDs with
one durably frozen availability time. Replay aliases resolve to the same original
delivery/version; retries, restart and concurrent attempts reuse its timestamp.
Historical cutoffs retain original evidence. Compaction preserves transformation
versions, and evaluation history counts distinct sampling times rather than the
number of revisions. Imports and compactions cannot invent a new transformation
by changing a label. See [ADR 0006](../adr/0006-immutable-normalization-revisions.md).

## Controlled T02 acceptance

| Criterion | Behavioral evidence |
| --- | --- |
| Fixed inputs/version reproduce stable evidence | Existing replay delivery tests preserve v1 keys and records; versioned replay tests verify repeat, alias and concurrent v2 identity/checksums. |
| A real new transformation preserves the old version | Actual Rust v2 emits field-path reporting and quarantine; native versioned replay checks the original catalog row, old-cutoff evidence and frozen decision remain identical. |
| Only complete verified publications enter reads | Existing pipeline/history/verification scenarios cover staged visibility and corrupt objects; the new interruption/restart case reuses the exact staged manifest and clock. |
| Unknown/incompatible source data is explicit | Rust contracts exercise field-level unknown-path quarantine and incompatible-container rejection; source values remain in the immutable raw archive. Collection cursor state is untouched by offline replay. |
| Retention invalidates restricted descendants | Native maintenance cases cover dependent assessments/evaluations; the new v2 case confirms parent retirement prevents revision resurrection. |
| Operators distinguish versions, rejection and workspace | Replay API/CLI reports version, accepted rows and quarantine status; existing dataset/detail and evidence views expose immutable manifests, revision metadata and source provenance. Existing workspace tests retain synthetic isolation. |

`tests/test_replay_versions.py`, `tests/test_replay_delivery.py`, the collector
contracts and evaluation history regressions exercise the installed Rust process,
native PostgreSQL and HTTP object storage. Exact-revision execution results are
recorded on the delivery PR. Provider-specific storage and live-source acceptance
remain separate.
