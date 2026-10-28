# Quoted content themes

`ContentInsights` consolidates candidate problems, benefits, objections and
selling angles in a bounded page of supplied texts for one confirmed product.
It reads permissioned subtitles, human transcripts and descriptions through the
existing transcript evidence boundary. It does not acquire comment bodies,
audio, media or new source data, and it does not call an analysis provider.

Initialize the optional transcript module explicitly using `sm-trends transcripts
init`. This applies additive module revision 2 without changing capture schema
40. Reads do not initialize tables or backfill older imports.

```python
from sm_trending_products.content_insights import ContentInsights

insights = ContentInsights(pipeline.catalog, pipeline.provenance)
result = insights.aggregate("selected-product", synthetic=False, limit=100)
next_page = insights.aggregate(
    "selected-product", synthetic=False, as_of=result["as_of"],
    limit=100, after=result["next_cursor"],
) if result["next_cursor"] else None
```

The service accepts at most 200 transcript rows per page and 16,000 characters
per imported text. Each text returns at most 24 mentions, with `truncated=True`
when that cap is reached. Method `quoted-theme-mentions-v2` preserves per-theme
coverage: a later unscanned theme is `not_fully_scanned`, with an explicit
`text_mention_limit_reached` gap, rather than reporting absence. Each theme also
reports supported, unsupported and fully/partially scanned text counts. A language
supported by one theme does not establish support for another theme's phrase
list; incomplete language coverage remains an explicit aggregate gap.
Counts describe the current bounded page. Continuation
pages are separate panels; adding their counts can duplicate a canonical text
whose metadata versions straddle pages. The response preserves a cutoff, cursor,
method, taxonomy version/hash/review reference and deterministic `aggregate_id`.
Replay requires retaining the exact taxonomy configuration, not just its name.
Availability and retention are checked again when replaying; withdrawal overrides
historical access.

The initial EN/FR taxonomy covers time, ease of use, price, reliability,
portability, demonstration and comparison. Every mention contains its theme,
category, exact quote and offsets into the normalized supplied text. Accent and
case matching preserve original quote offsets. Prefix negation and question or
modal markers produce `negated` or `ambiguous` candidate interpretations. These
small lexical rules are not a full language parser. An ordinary candidate phrase
is not confirmation that the described problem or benefit is true.

`unsupported_language`, `no_mention` and `not_available` have distinct meanings.
An unknown or unsupported language is not evidence that a theme was absent.
Descriptions remain descriptions; a quoted demonstration is not verified proof.
Seller messages are not buyer intent. Advertising effectiveness and independent
people remain unmeasured. Identical normalized words across texts are reported
only as `normalized_text_similarity_only`, never a verified creative fingerprint.

Distinct-text counts deduplicate logical video/source-kind/language/text versions
within a page. Repeated analysis requests and metric refreshes do not add texts.
Theme counts retain the candidate/negated/ambiguous breakdown, source kind,
language, known source-scoped creator IDs and attributed roles. Known creator IDs
do not establish different real people. A context correction chooses the latest
eligible version within the current page without changing canonical analysis
reuse.

Optional explicit `TranscriptContext` can accompany a transcript import:

```python
from sm_trending_products.enrichment import TranscriptContext

context = TranscriptContext(
    reference="operator-review:source-metadata-17",
    known_at="2026-10-28T12:00:00Z",
    creator_id="source-creator-id",
    creator_role="seller",
    variant="blue-350ml",
    known_theme_ids=("time",),
)
```

The reference must identify reviewed metadata supporting the attribution. The
operator is responsible for that review. The module labels it `operator_attested`;
it does not verify a buyer identity, purchase or customer status. Known roles
require a creator ID; omitted context leaves role and creator unknown. `known_at`
must not exceed the import cutoff, and future synthetic context remains hidden
until its availability time. Context lives in the same retired provenance graph
as its transcript. SQL stores bounded metadata, hashes and descriptors, not raw
text. Older imports keep their exact IDs, content keys and descriptors; their
missing selection metadata remains unknown. An explicit context import creates
a new metadata version while retaining the canonical analysis key.

For a reviewed merge or split, construct a `ContentTaxonomy` with a new version,
review timestamp/reference and up to 16 `ThemeRule` entries, each containing at
most 16 `ThemePhrase` entries. Phrases declare `en` or `fr` and a category of
`problem`, `benefit`, `objection` or `angle`. Pass the complete taxonomy to
`aggregate`. A taxonomy reviewed after the cutoff is rejected. The service does
not silently rewrite an old taxonomy or infer a custom theme's effectiveness.

The guarded `TranscriptInsights.eligible_texts` and `read_texts` interfaces are
also public bounded seams. The former returns metadata without loading text;
the latter accepts at most 200 explicit transcript IDs. Both preserve the exact
supporting observation, ownership, namespace, cutoff, source policy and retirement
checks. Native scenarios are in `tests/test_content_insights.py`.

## Operator inspection

The research command reads the same bounded module report:

```bash
sm-trends research content --product-id selected-product --limit 100
sm-trends research content --product-id selected-product --synthetic \
  --as-of 2026-10-28T23:00:00Z --limit 20 --after TRANSCRIPT_CURSOR
sm-trends research content --product-id selected-product --taxonomy reviewed-taxonomy.json
```

`--taxonomy` accepts a complete reviewed `ContentTaxonomy` JSON object, at most
128 KiB. File shape, size, identifiers, cursor, panel limit and cutoff are checked
before database or object-store configuration is opened. Help needs no service
configuration. Invalid input and unavailable operations return safe JSON on
standard error with exit code 2; the input document and file path are not echoed.
Argument parsing also emits a fixed JSON error without echoing submitted flags
or invalid values.
The command does not initialize optional tables or create an analysis request.

The private quoted-theme panel performs work only after **Inspect quoted themes**.
Its displayed cutoff, taxonomy and exact quotes come from the same service. Every
interaction drops the prior derived report; another inspection checks source
eligibility again. Unsupported language, partial scans and missing eligible text
remain visible. No report or source quote is retained in dashboard session state.
Rendered reports redact current environment-bound secrets before display.
Native CLI and Streamlit cases are in `tests/test_content_research_operator.py`.
