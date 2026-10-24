#!/usr/bin/env bash
# Reproducible native workload and bounded quota failure; controlled HTTP only.
set -euo pipefail
repository_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_directory"
: "${TEST_DATABASE_URL:?Set TEST_DATABASE_URL to an isolated native test database.}"
mkdir -p output
# A crash must not be mistaken for a previously successful local report.
rm -f output/offline-benchmark.json output/offline-quota.json
uv run --locked python scripts/offline-benchmark.py \
  --records 1000 --product-count 50 --workers 4 --source-latency-ms 10 \
  --hot-fraction 0.6 --compact --output output/offline-benchmark.json \
  >output/offline-benchmark.log 2>&1
quota_status=0
uv run --locked python scripts/offline-benchmark.py \
  --records 80 --product-count 4 --workers 2 \
  --request-capacity 1 --requests-per-second 0.0001 \
  --output output/offline-quota.json >output/offline-quota.log 2>&1 || quota_status=$?
if [[ "$quota_status" -ne 1 ]]; then
  echo "Expected benchmark quota failure exit 1; received $quota_status." >&2
  exit 1
fi
uv run --locked python - <<'PY'
import json
from pathlib import Path

sample = json.loads(Path("output/offline-benchmark.json").read_text())
quota = json.loads(Path("output/offline-quota.json").read_text())
states = {row["state"]: row["count"] for row in quota["measurements"]["queue_final"]}
checks = {
    "sample_checks": sample["ok"] is True and all(sample["checks"].values()),
    "sample_rows": sample["measurements"]["actual_records"] == 1000,
    "sample_queue_drained": sample["measurements"]["queue_final"] == [
        {"state": "done", "count": 50}
    ],
    "compaction_preserves_rows": sample["compaction"]["status"] == "compacted"
    and sample["compaction"]["rows"] == 1000
    and sample["compaction"]["normalized_objects_after"]
    < sample["compaction"]["normalized_objects_before"],
    "compaction_preserves_assessment": sample["checks"][
        "post_compaction_semantics_unchanged"
    ] is True,
    "cold_and_warm_cache_measured": all(
        sample["phases"][f"analysis_{stage}_cold"]["cache_before"]["file_bytes"] == 0
        and sample["phases"][f"analysis_{stage}_cold"]["compute_cache_before"]
        == {"feature_projections": 0, "raw_windows": 0}
        and sample["phases"][f"analysis_{stage}_cold"]["storage_traffic"]["get_requests"] > 0
        and sample["phases"][f"analysis_{stage}_cold"]["analysis_work"]["projection_hits"] == 0
        and sample["phases"][f"analysis_{stage}_warm"]["analysis_work"]["projection_hits"]
        == sample["phases"][f"analysis_{stage}_cold"]["analysis_work"]["projection_misses"]
        > 0
        and sample["phases"][f"analysis_{stage}_warm"]["analysis_work"]["projection_misses"] == 0
        and sample["phases"][f"analysis_{stage}_warm"]["storage_traffic"]["get_requests"] == 0
        and sample["phases"][f"analysis_{stage}_warm"]["storage_traffic"]["download_bytes"] == 0
        for stage in ("pre", "post")
    ),
    "quota_failure_reported": quota["ok"] is False
    and quota["checks"]["collection_completed"] is False
    and quota["measurements"]["processed_pages"] == 1
    and all(error["category"] == "quota_exhausted" for error in quota["errors"]),
    "quota_work_retained": states == {"pending": 3, "done": 1}
    and sum(states.values()) == quota["measurements"]["queue_initial_tasks"] == 4,
    "partial_failure_measured": quota["complete"] is True
    and quota["measurements"]["actual_records"] == 20
    and quota["measurements"]["storage_traffic"]["put_requests"] > 0
    and quota["measurements"]["task_latency_seconds"]["count"] == 1,
    "synthetic_boundary": all(
        result["synthetic"] is True
        and result["verification_boundary"]["production_acceptance"] is False
        for result in (sample, quota)
    ),
}
failed = [name for name, passed in checks.items() if not passed]
print(json.dumps({"ok": not failed, "checks": checks, "failed_checks": failed}))
if failed:
    raise SystemExit(1)
PY
