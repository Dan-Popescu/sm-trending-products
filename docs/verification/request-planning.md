# Request planning and collector setup profile

Scope: controlled request-planning regressions, not live source acceptance.

The planner groups compatible Display refreshes into twenty-ID requests and
commercial refreshes into one origin-query cycle. Every selected entity retains
its own reason, tier and interval. Physical task counts and covered entity counts
are reported separately; exploration remains allocated in physical request units.

Behavioral regressions cover forty Display IDs in two tasks, active group
membership, fixture/tier separation, commercial query coalescing, discovery plus
refresh sharing one cycle, and a legitimate later cycle. Native PostgreSQL/Moto
and the real Rust collector exercise one grouped Display request with a missing
ID and one grouped commercial query across two pages. The latter also checks a
new member of an already-active query after continuation adds default page size
and search ID. These scenarios do not grant live TikTok capabilities.

## Process lifetime decision

A local debug collector sample on 2026-10-22 ran thirty fresh processes per case.
`--version` had median 2.28 ms and p95 2.76 ms; normalizing one archived record had
median 3.13 ms and p95 3.88 ms. This isolates CLI/runtime/process overhead from
source HTTP, PostgreSQL connections, S3 and analytical workloads. Shared-host
noise and the debug profile prevent extrapolating these values to production.

Grouping already removes nineteen of twenty process starts for a compatible
full Display batch. This setup-only sample does not justify adding a persistent
worker protocol yet. Keep bounded process isolation, timeout, cancellation and
lease fencing. Profile HTTP/database setup at the target source quota and latency
before introducing client sessions; any such change must retain atomic quota
reservation for every HTTP attempt and crash recovery across publication.
