# Bounded selection of useful supplied text

`TextSelection` recommends a small queue of eligible, unanalysed supplied texts
for a declared product shortlist. Preview reads import metadata and source guards;
it does not load transcript bodies, fetch a source, reserve a provider call or
change a collection schedule. Initialize transcript module revision 2 explicitly
before using it.

```python
from sm_trending_products.text_selection import TextSelection, TextSelectionConfig

selection = TextSelection(pipeline.catalog, pipeline.provenance)
plan = selection.preview(
    ["selected-product", "second-product"], synthetic=False,
    config=TextSelectionConfig(
        version="reviewed-shortlist-v1", max_selected=5,
        max_candidates=50, per_product_candidates=20,
        max_total_text_characters=40_000, desired_theme_ids=("time", "price"),
    ),
)
# Explicitly apply the reviewed bounded queue to the free local analyzer:
result = selection.execute(plan, request_prefix="shortlist-2026-10-28")
```

The fixed limits are ten products, 200 candidate rows in total, 100 rows per
product, twenty selected texts and 320,000 selected characters. Default limits
are ten products, 100 candidate rows, twenty rows per product, ten selected texts
and 80,000 characters. The existing 16,000-character per-text import bound also
applies. Smaller configured bounds remain authoritative after model validation;
mutated or constructed Pydantic instances are revalidated at every public seam.

The queue prioritizes product diversity, explicitly tagged missing themes, then
known language, variant and creator diversity among unanalysed eligible texts.
These coverage dimensions use immutable import metadata. `known_theme_ids` are
optional operator attestations with a provenance reference and availability
time. Selection does not guess a missing theme by reading an unselected body.
`theme_coverage_unknown` remains explicit. Unknown creator/variant values do not
count as observed diversity. Supported free analysis languages are EN and FR;
language is not a measured audience country.

Plans explain duplicate canonical content, unsupported/unknown language,
unreviewed legacy metadata or length, completed/unresolved analysis, character
budget, selection-count budget, missing eligible supplied evidence and candidate
page limits. Older imports remain usable for guarded theme aggregation but are
not automatically chosen when their metadata is absent. An unchanged import does
not backfill its old row. A new explicit contextual import can add reviewed
metadata without changing canonical text-analysis identity.

Each product page carries a `next_cursor`. Continue with
`preview(product_ids, as_of=plan["as_of"], after={product_id: cursor}, config=...)`.
Only products present in that shortlist may have cursors. Counts and diversity
describe the inspected pages, not all source content or a global video universe.
A missing eligible text, unsupported text and a supported text with no theme
mention are different evidence gaps.

The JSON-ready plan includes the complete configuration and its hash, method,
product order, namespace, cutoff, cursors, selected IDs, reasons, budgets and stable
`plan_id`. Keep the complete plan for replay. Replaying the same cutoff/config
returns the same plan while its evidence remains eligible. Running and uncertain
local analyses share an `unresolved` barrier because the original module has no
immutable transition history for those states; the selector does not claim a
later completion was known earlier. Source withdrawal or retention expiry may
invalidate an old plan.

`execute` revalidates the whole plan and recomputes it from authoritative metadata
at its pinned cutoff. Changes to its IDs, namespace, cutoff, configuration or
items cannot redirect that plan. Each execution then uses **current** ownership,
source and retirement guards through `TranscriptInsights.analyze`, without an
adapter argument. It returns per-text completion or a safe denial reason; it
does not bypass a denied text or raise a daily limit. A stable request prefix and
canonical text key reuse existing analyses across duplicate applications.

Execution is limited to `local-rules`. There is no external-provider, ASR, media
download or source-fetch option. The transcript service's immutable daily
configuration and atomic request/cost reservations remain authoritative. A queue
can exceed the remaining daily allowance; later rows are denied without calling
the analyzer. A saved plan cannot reset a running/uncertain reservation. Existing
unknown paid outcomes retain their barriers and costs even when free local
selection is applied. The method does not establish that the messages predict
purchases or advertising performance.

Selection shares the analyzer's completed-cache eligibility rule. When an old
completed result loses its source lifecycle, a separately eligible new transcript
generation can be selected and analyzed again. Retired running or uncertain
generations continue to block automatic repetition; retirement is not proof that
their reserved work never happened.

Native PostgreSQL scenarios in `tests/test_text_selection.py` cover stable
metadata-only previews, diversity, pages, explicit local application, canonical
reuse, authoritative budgets, unresolved outcomes, model bypasses, source
withdrawal and current ownership checks. They use synthetic evidence and a local
object-store fixture, not live platform access.

## Operator preview and explicit local application

```bash
sm-trends research select-texts preview --product-id selected-product \
  --product-id second-product --config selection-config.json > plan.json
sm-trends research select-texts apply --file plan.json --request-prefix shortlist-2026-10-28
```

`preview` accepts repeated `--product-id` flags, optional `--as-of` and repeated
`--after PRODUCT=TRANSCRIPT_CURSOR` flags for the declared shortlist. The optional
configuration file is a complete `TextSelectionConfig` JSON object. Both config
and complete plan files must be regular JSON files no larger than 128 KiB; file
and scope checks happen before opening service configuration. An unusually large
plan requires a smaller candidate page before it can be applied through these
operator paths. There is no external adapter or paid-provider flag.

Use `--synthetic` on both preview and apply for synthetic plans. Apply requires
its visible namespace flag to match the saved plan, retains its exact cutoff and
configuration, and rechecks current eligibility through `execute`. Exit code 0
means the command completed, code 1 means application returned a partial result
with per-text denials, and code 2 means input or execution was rejected. JSON
errors do not echo input values or private file paths. Argument-parser failures
likewise omit submitted flags and values. Reuse a stable request prefix to read
the same local operations; another prefix still reuses canonical
analysis rather than reserving duplicate work.

The private selection panel previews only after an explicit button, offers a
complete plan download, then requires the reviewed file to be uploaded before
**Apply reviewed plan with free local rules**. The file must match the currently
visible namespace and selected product set. Configuration and plan uploads are
size-checked before their bodies are copied. Only stable operation identifiers
are remembered in session state; previews, quotes and execution results are not
cached there. Rendered output redacts current environment-bound secrets. If
redaction would change a plan, download is refused instead of offering a modified
plan with an invalid identity. Nonsecret downloads retain the exact executable
plan. Any interaction clears the previous derived output. Source
withdrawal and daily budgets remain authoritative at every application.

Native CLI and Streamlit cases, including preview-only behavior, budget denials,
saved-plan replay and withdrawal after preview, are in
`tests/test_text_selection_operator.py`.
