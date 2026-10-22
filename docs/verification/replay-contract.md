# Replay rejection and publication contract

Offline replay invokes the bounded Rust normalizer against the stored response;
it neither refetches the source nor advances collection tasks. Imported and
compacted records retain their existing contract validation path. The installed
binary implements transformation version `1`; unknown requested versions fail
explicitly. A future semantic transformation must ship its actual implementation
and version before T02's new-transformation criterion can be accepted.

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

T02 remains open until the separate new-transformation behavior is implemented
and its complete original acceptance checklist is reconciled. Provider-specific
storage and live-source acceptance remain separate.
