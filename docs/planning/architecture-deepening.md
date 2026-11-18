# Targeted architecture deepening

Approved scope: deepen the three opportunities reviewed on 2026-11-18, implementing
only changes with demonstrated locality and leverage. Baseline: main
`3935a35e2e8c360f7dc71546dd604e244bfb6dc6`. Preserve externally observable behavior;
this work does not claim measured CPU, RAM, storage or live-source improvements.

## 1. Owned video files

Concentrate the local filesystem protocol used by selected-video download,
transcription and export behind a small owned-media interface. The implementation
owns canonical locations, invocation ownership, bounded integrity verification,
staged export and cleanup. PostgreSQL remains authoritative for job state and
durable completion; source and provenance checks stay at the actual effect.

- Preserve request identities, descriptors, result formats and execution budgets.
- An unknown completion or failure acknowledgement must preserve downloaded bytes.
- A failed invocation must never clean another invocation's existing directory.
- Verify media before use, detect replacement or modification during reads, and
  refuse paths escaping the owned workspace.
- Export in bounded chunks, without overwriting an existing destination; publish
  only after final source/provenance authorization, removing failed staged output.
- Keep the existing real and fixture process adapters. Do not introduce a common
  job framework or merge collection and selected-video enrichment.
- Verify through public run, result, export and transcription behavior, retaining
  failure, source-withdrawal, acknowledgement-loss and integrity regression tests.

## 2. Trend comparability

The trend module owns scientific measurement identity and the conditions under
which cutoff diagnostics are comparable. Decision changes retain changed-selection
policy, window alignment, references and compact presentation.

- Concentrate measured-period fingerprint exclusions, adjacency, preview
  completeness, correction/context and continuing-panel gates in the producer's
  module. Keep refusal precedence and admitted ratio bounds unchanged.
- Preserve exact report payloads and hashes, existing guarded reads, provenance
  checks and read budgets. Do not add persistence fields, I/O or adapters.
- Verify valid comparisons and every refusal category through the comparison
  operation using real generated diagnostics, plus existing public decision-change
  regression tests.
- Avoid moving decision-specific policy into the scientific module.

## 3. Qualification resource lifetime

Concentrate resource ownership shared by offline and research qualification cases:
temporary directory, unique PostgreSQL schema, loopback object-store process and
the isolated worker process session. Keep each workload's supervision loop,
resource guard thresholds, checkpoint merge and report accounting explicit.

- Cleanup stops owned workers before object storage and SQL teardown; bound waits
  and schema teardown, preserve conservative failure/unknown accounting.
- Preserve interruption and deadline priority, resistant-child termination and
  sibling isolation, terminal checkpoints and unknown usage represented as null.
- Preserve existing report contracts and workload-specific metrics/refusals,
  including actual collector/research overlap qualification.
- Exercise real subprocess behavior and native PostgreSQL where available; do not
  substitute mock-only evidence for real resource lifecycle guarantees.
- Reject a generic callback-driven supervisor. Move shared helpers out of nested
  script imports only where needed by the two existing workloads.

## Delivery and review

Deliver each slice independently. Apply relevant formatting, lint, type and
behavioral checks, then independent standards and specification reviews of the
exact patch. Follow repository CI to completion and merge only passing revisions.
Record unavailable local infrastructure and distinguish synthetic qualification
from live source acceptance. Existing ADRs 0008, 0012, 0017, 0018, 0019 and 0020
remain in force. No deployment, credentials or live platform activation is needed.
