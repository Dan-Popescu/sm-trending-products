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

## S2 — Set-based metadata authority (#187)

Initial hydration selection and final identity selection use one bounded metadata
guard. The shared source query also serves individual checks: committed dataset,
cutoff-specific unique owner and confidence, namespace/entity/source match,
current policy verification, database retention clock and observation delivery.
Its CASE order retains the previous first refusal for each row. The ownership CTE
is restricted to selected entity keys while retaining every competing product's
decisions. At most 200 candidate rows enter a page guard.

Publication checks use the same fenced, chunked root lookup as `Provenance.guard`.
The new partial lookup attributes missing roots to individual rows; the complete
guard retains its refusal on the first incomplete 1,000-root chunk. No descendant
walk or extra pooled connection is introduced. Every selected canonical text
still receives a fresh individual transcript/source/publication check immediately
before hydration. Eligibility proofs are not reused between calls, and the
analysis-aware historical barriers remain unchanged.

Native checks cover mixed live-source refusals, maximum integer retention policy,
historical and ambiguous ownership, actual withdrawal, partial publications,
pagination and complete report bytes/IDs. Verification of policy, retention,
ownership, source identity, delivery and publication changes after page selection
confirms that the immediate hydration refresh still rejects each changed row.
The expanded six-file run passed 175 cases; its new one-row measurement assertion
was corrected to allow equal SQL counts and passed the focused rerun. All nine
focused checks passed, including an external EXPLAIN probe. Full-source Ruff and
Mypy passed.

Measured driver executes (including transaction controls), compared with invoking
the same individual guard for each candidate:

| Page/population size | Identity + hydrated pages, individual | Batched | Identity page alone |
| --- | ---: | ---: | ---: |
| 1 | 46 | 46 | 18 |
| 2 | 68 | 56 | 18 |
| 8 | 200 | 116 | 18 |

Complete progression takes 81 executes in both one-text paths and 235 versus 151
for eight texts, with exact report bytes and IDs. The eight-text content report
takes 98 executes, with all eight individual hydration authority refreshes still
present. These figures isolate application/database round trips, not server CPU.
With 5,000 unrelated ownership links, native EXPLAIN used `identity_history` for
the selected entity, reading one matching row in one loop (three shared-buffer
hits); this is a local synthetic plan, not a deployment capacity claim.

Additional validation commands:

```bash
PYTHONPATH=$PWD/src:$PWD/tests flock -w 300 /tmp/night-native-tests.lock pytest -q -s tests/test_text_hydration.py tests/test_content_progression.py tests/test_content_insights.py tests/test_transcript_insights.py tests/test_analytical_compute.py tests/test_artifact_gc.py
PYTHONPATH=$PWD/src:$PWD/tests flock -w 300 /tmp/night-native-tests.lock pytest -q -s tests/test_text_hydration.py tests/test_content_progression.py -k 'hydration_refreshes or progression_batches' /tmp/research-s2-source-plan/test_source_plan.py
ruff format --check src tests scripts
ruff check src tests scripts
mypy src
```

Local evidence: `/tmp/research-s2-tests.log`,
`/tmp/research-s2-final-checks.log`, and
`/tmp/research-s2-source-plan/plan.json`.
