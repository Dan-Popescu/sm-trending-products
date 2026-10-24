# Fence rolling releases and verify immutable artifacts at the host

Status: accepted under the owner's continuity implementation authorization.
Specification: GitHub issue #80; delivery #87.

Application replacement uses the existing PostgreSQL task/publication authority.
A separate small deployment journal stores release progress and signed manifest
proof, never copies of source task or observation payloads. The controller offers
plan, apply, status and rollback. Retrying an interrupted action is idempotent;
active-pointer commit and its journal phase share one SQL transaction.

A host-private file lock surrounds the PostgreSQL session advisory lock. This
keeps a controller whose database connection failed from overlapping host changes
with its successor. A monotonic target fence protects journal writes; neither a
heartbeat nor a deployment record replaces source-task lease fencing.

Release identity is an immutable image digest plus revision, supported SQL range
and capture protocol. CI and the host use the same canonical JSON plus newline
payload bytes. The host verifies the Sigstore bundle through cosign with a fixed
workflow certificate identity, GitHub OIDC issuer and image repository allowlist.
Proof is reverified on apply; a database trusted flag cannot authorize a deploy.
Missing verifier, missing signature or incompatible schema/protocol blocks before
host mutations. Synthetic tests explicitly enable a fixture proof; ordinary host
controllers do not permit that method. End-to-end signature publication remains
part of the CI/host installation qualification, not a claim of the fixture tests.

Publishing a candidate does not authorize deployment. The signed deployment
request bundle is retained with its SQL request and verified again on every
host CLI mutation or automatic resume. A separate root-private, fsynced intent
seal binds the immutable candidate/predecessor identities, generations and schema
to that request. App-writable journal rows cannot manufacture deployment or
rollback authority. A root-private highest accepted request protects monotonicity
if the SQL request history is altered. Host-console rollback records its own
private authority referencing the previously authorized run; cancellation intent
also survives independently of mutable SQL progress. Corruption or missing
authority blocks all host mutations.
The private authority also stores execution progress. SQL phase, completion and
cancellation flags are mirrors and cannot choose a privileged path. Each
controller reread binds the same sealed manifests/proofs, worker identities and
creation time, then checkpoints private progress after transitions. A crash
between SQL active-pointer commit and its private checkpoint resumes from the
sealed verified phase and rechecks actual candidate captures and drain before
recommitting. This extra journal is deployment metadata only; source task,
publication and quota ownership remain in PostgreSQL.

The existing worker and optional worker-green Compose services are the only
mutable host slots. Fixed arguments select the local Docker socket, private
configuration, image digests and worker identity labels. An occupied unknown
running slot blocks replacement. PostgreSQL, other services, volumes and networks
are never recreated by the rolling adapter. Ordinary application containers get
no Docker socket. Raw command diagnostics and signature tool output are omitted
because they can contain credentials. Host paths and trust settings never come
from the release manifest.

The replacement must have the expected image/worker identity and actual fresh
committed raw captures linked to qualified original obligations. Unknown capture
deadlines cannot pass as zero breaches. Adopted old-worker receipts do not qualify
the replacement. Proof binds the acquisition worker's persisted generation;
restarting a candidate invalidates its earlier proof and requires fresh captures.
Drain and resume always carry the journal's expected predecessor generation.
A changed predecessor generation blocks rather than targeting an unexamined
replacement. No due source work means the controller waits with the old path
operating rather than issuing gratuitous live canaries. Capture evidence is
separate from Docker process health and worker admission.
Proof also includes older never-captured backlog and validates each receipt
overlapping the handover. A fresh receipt in one scope cannot hide a retired
handover receipt in that scope. Historical receipts intentionally retired before
this handover do not impose a lifetime retention guarantee on deployment.
An immutable, independently checked retirement certificate can withdraw an
exact captured receipt, or the current cancelled page/attempt with no receipt.
An older retired diagnostic attempt cannot hide its active retry. Neither a
generic cancellation nor unexplained invalidation withdraws an obligation.

Every latest registered capture policy also requires its own post-handover,
current-generation witness, even when its next daily sample is not due yet.
Wildcard policies match actual scoped requests under that exact contract
version. Qualification waits for natural work rather than issuing extra live
requests. Witness age follows the greater of configured freshness and the
declared cadence plus lateness, allowing differently phased sparse sources to
qualify without ignoring fast-source deadlines. Registered policies currently
have no inactive lifecycle; an old-input removal cutoff does not deactivate a
future collection path.

The old worker stops claiming only through its acknowledged operation barrier;
it must own no lease before retirement. A deadline blocks the release and never
turns into a forced SIGKILL. Host stop uses an unlimited graceful deadline after
that barrier. A failed replacement resumes old admission. Controller restart
rechecks candidate health/evidence and the drain barrier before retiring a prior
worker, including after the active pointer was committed.

Rollback before commit cancels candidate admission after restoring the predecessor.
Rollback after commit creates a reverse handover to the verified compatible prior
image. Its explicit journal reference allows recovery from an unhealthy current
image. SQL data, quotas, cursors and captured inputs are not rewound. An aborted
intent stays aborted; a new explicit request identity admits a deliberate retry.

Schema expansion must already be compatible with both versions; arbitrary SQL or
scripts are not accepted in manifests. Destructive contraction belongs to a later
release after old readers leave. Hardware failure, source unavailability and a
whole-VPS/database outage retain the separate availability limits in #80.
