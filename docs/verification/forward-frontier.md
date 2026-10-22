# Isolated, nonblocking forward-outcome recovery

Claiming a due forward outcome also recovers expired leases and invalidated
decisions. Those transitions belong to the requested real/synthetic workspace.
They select frontier rows with `FOR UPDATE OF f SKIP LOCKED` before updating;
recovery cannot wait on a row that another worker owns while healthy work is due.
The due-row selection independently excludes invalidated decisions. Existing
lease tokens and commit-time dependency checks continue to fence stale results.

The previous cleanup statements updated both workspaces and could block before
the already-nonblocking final claim. Six native regressions reproduced the two
cleanup categories across workspaces and while an independent transaction held
an expired or invalidated frontier row. The corrected public `mature_due` path
completes other due work while that lock remains held, then recovers the skipped
row after release. Two additional cases preserve active unexpired leases.

`tests/test_forward_claims.py` and the existing evaluation integration scenarios
use actual PostgreSQL backends, including concurrent workers claiming disjoint
decision sets. The change follows the same ownership boundary as collection
claim cleanup; it introduces no new scheduler, lease protocol or schema migration.
