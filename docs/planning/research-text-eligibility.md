# Bounded transcript eligibility

## S1 — Lean final progression check (#184)

`TranscriptInsights.eligible_text_ids` shares the transcript row selection,
authority checks and cursor construction with the existing readers. It returns
only eligible identities and row-level refusals, without loading transcript
payloads, querying analysis state or reserving work. Content progression uses it
inside its existing final text/metric retention fence. Analysis-aware previews
retain their current behavior.

Native PostgreSQL validation: 158 tests passed across `test_text_hydration.py`,
`test_content_progression.py`, `test_content_insights.py` and
`test_transcript_insights.py`. Complete progression bytes and report IDs match the
previous final check for completed, reused, running and uncertain analyses. The
four-text fixture removes eight analysis queries; completed analysis also avoids
its extra source checks (247 to 187 driver executes), while running and uncertain
states fall from 195 to 187. These synthetic fixture counts include transaction
controls and do not measure database CPU or deployment capacity. Paging over
withdrawn rows, concurrent withdrawal and the shared final retention fence pass.

Commands (source `/workspace/work/performance-fixes/test-env.sh` first):

```bash
PYTHONPATH=$PWD/src:$PWD/tests flock -w 300 /tmp/night-native-tests.lock pytest -q -s tests/test_text_hydration.py tests/test_content_progression.py tests/test_content_insights.py tests/test_transcript_insights.py
ruff check src/sm_trending_products/enrichment/service.py src/sm_trending_products/content_progression/service.py tests/test_text_hydration.py tests/test_content_progression.py
mypy src/sm_trending_products/enrichment src/sm_trending_products/content_progression
```
