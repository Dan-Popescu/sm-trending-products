# Reviewed product-match suggestions

`sm_trending_products.matching.ProductMatcher` adds optional, read-only candidate
ranking. It leaves the original `Identity.suggest_links` and all historical
assessment policies and hashes unchanged. A suggestion is never a confirmed
product link. Use the existing explicit identity-review boundary for decisions.
Before using identifiers, explicitly run `ProductMatcher(catalog).initialize_indexes()`
once for that database. Text-only matching works without this optional setup.
Suggestion reads never create indexes or repair an unfinished build automatically.

Run `sm-trends match-products --product-id PRODUCT --profile /private/profile.json`
for the JSON report (`--synthetic` selects fixtures). `--as-of` accepts an explicit
timezone-aware cutoff; `--limit` and `--candidate-limit` bound the search. The
profile JSON has the same aliases/identifiers/brand/variants fields as below.
Run `sm-trends match-products --initialize-indexes` explicitly before the first
identifier lookup. This online setup is separate from read-only matching and
does not alter the capture migration version. A missing/invalid index produces
`matching_indexes_not_ready`, never an unindexed fallback or a false empty search.
The private dashboard's Catalog view has an optional **Explain multilingual
product matches** control. Candidate reads run only after an explicit button
action; suggestions do not change product links.

```python
from sm_trending_products.matching import MatchingProfile, ProductMatcher

result = ProductMatcher(catalog).suggest(
    product_id="travel-bottle",
    as_of=cutoff,  # timezone-aware datetime: evidence must already be available
    synthetic=False,
    profile=MatchingProfile(
        aliases=("gourde de voyage", "Reiseflasche"),
        identifiers={"ean": "4006381333931"},
        brand="Example",
        variants={"capacity": "350 ml", "color": "blue"},
    ),
    limit=20,
    candidate_limit=500,
)
```

The profile is a bounded request, not a mutation of the canonical product or
source metadata. Unknown profile fields and invalid barcode check digits are
rejected. Supported identifier fields are `gtin`, `ean`, `upc`, `sku`, `mpn` and
`model`. Variant fields are `size`, `color`, `capacity`, `voltage` and `material`.
Missing source facts remain explicit; numeric JSON values are not silently
converted to barcodes that may have lost leading zeros.

## Retrieval and explanation

Retrieval uses the existing PostgreSQL `entity_title_search` index. Canonical
name and up to eight supplied aliases form alternative search phrases. Each
phrase requires all its informative terms, with at most 16 terms per phrase.
Accented originals and accent-folded query variants support explicit European
language aliases. No external translation, embedding or inference is run. An
accentless query cannot universally retrieve an accented title through this
index; supply the source spelling when needed. A missing alias may miss a
translation.

Explicit identifiers also retrieve candidates whose titles have no shared words,
using six optional sparse B-tree indexes rather than a JSON-wide index or a
metadata scan. GTIN, EAN and UPC compare in a common 14-digit representation,
with supported ASCII digits, valid check digits and leading zero padding. Source
compatibility characters are normalized with NFKC and ASCII whitespace is trimmed.
SKU, MPN and model retain case and punctuation after compatibility normalization.
Only bounded string identifiers participate; a numeric JSON barcode is unknown.
If any required index is missing, invalid or has a different definition, the
request raises `MatchingIndexesNotReady` with code `matching_indexes_not_ready`
and the affected index names. An empty result never stands in for unavailable
identifier search. Run the explicit initializer, then retry.

The returned `review-candidates-v2` score combines token overlap and character
trigram overlap. It is a heuristic, not a calibrated probability. Equivalent
valid GTIN/EAN/UPC facts can agree across supported code lengths. SKU, MPN and
model compare exactly after Unicode compatibility normalization. Brand and
textual variant comparisons fold accents and case. Compatible supported units
can agree for a pure size, capacity or voltage value. Conflicting supplied facts,
different title quantities or numbers, and another confirmed owner are explicit
review warnings and cap the score at 0.25. Quantities declared by the canonical
name, matched/supplied aliases and explicit variants are compared with the title.
For example, a declared 350 ml capacity versus a title saying 500 ml is a conflict
even when source capacity attributes are missing. Missing attributes remain listed
as missing; they do not erase the title conflict. Numbers in a title can describe
marketing copy; the warning does not establish a different physical variant.

Every result includes `requires_review: true`, matched alias and terms, matching
facts, missing facts, conflicts and ownership status. Text similarity never
establishes creator independence or a creative fingerprint. Similar titles do
not cause creators or videos to be merged, and do not establish independent
adoption. Suggestions never write link decisions, family membership or scoring
history.

## Bounds and historical safety

- `limit`: 1–100 returned suggestions, default 20.
- `candidate_limit`: 1–1,000 indexed candidates, default 500, and at least `limit`.
- Ranking uses at most 4,096 title characters per candidate and only the 12
  relevant string facts, each at most 256 characters. Arbitrary source attributes
  and creative metadata are not copied into the response.
- Identifier hits enter the bounded window first, followed by text-only hits;
  ties use stable entity keys. The response exposes `identifier_hit`. A full
  window sets `candidate_truncated`; it is a partial search, not an exhaustive
  best-match list. Even a broad text query cannot displace an exact identifier
  hit solely because its entity key sorts later.
- Ownership resolution is restricted to candidate entities, but considers every
  product in the selected real/synthetic workspace. It loads only the latest
  event per product/entity pair, without source evidence payloads, and uses the
  shared ownership resolver. A later global owner supersedes an earlier claim;
  equally recent competing owners remain ambiguous. More than 10,000 current
  pairs sets `ownership_truncated` and leaves ownership unresolved for the whole
  request. A truncated history never supplies a winner.
- Rows, text ranking and ownership materialization are bounded. PostgreSQL still
  must inspect matching index postings and historical link events; broad queries
  or long histories can take longer. The catalog's statement timeout remains in
  force. These bounds are not a fixed CPU-time guarantee for a database of any
  size.

Reads cross the catalog evidence guard. Product creation, entity discovery,
observation and link decisions must be available at the cutoff. For a source
projection, its originating dataset must still be committed, match source and
workspace, and have committed and become available by the cutoff.

Entity projections hold current metadata, not a historical title/attribute log.
When a later delivery replaced a projection after the cutoff, matching excludes
it instead of passing future metadata into a past decision. The response names
this scope `eligible_current_metadata_only`; it is not an exhaustive historical
replay. Manual projections without a dataset origin rely on their recorded
entity discovery/observation times. Do not backdate manual changes or claim that
this interface reconstructs unrecorded metadata revisions.

Native PostgreSQL scenarios in `tests/test_matching.py` verify the boundary,
multilingual retrieval, explicit conflicts, cutoff filtering, workspace isolation,
transferred/ambiguous ownership, budgets, and absence of identity mutations.
Fixtures do not establish source permissions or measured real-world match recall.


## Optional index setup and infrastructure cost

`initialize_indexes()` creates only the six declared partial expression indexes
for `ean`, `gtin`, `upc`, `sku`, `mpn` and `model`, with the real/synthetic flag as
the first index column. It does not change source tables, triggers, capture
versions or `schema_migrations`. Names fingerprint the definition, and readiness
checks inspect the actual index keys, expression, predicate and validity. The
supported PostgreSQL 17.11 deparser shape is verified in native tests.

Setup uses a dedicated autocommit database connection and sequential
`CREATE INDEX CONCURRENTLY` statements. Normal entity inserts can continue while
an index build waits on an earlier writer; the native scenario verifies this.
Another initializer is refused by a separate session advisory lock, without
using acquisition's locks. Concurrent DDL and long database transactions can
still delay setup. The session has a two-second lock timeout and a 30-minute
statement timeout. A cancelled build can leave an invalid index. Text-only reads
continue; identifier reads fail explicitly. Rerunning setup drops only an invalid
index with the expected definition, concurrently, and rebuilds it. A valid setup
is idempotent. A conflicting definition requires operator inspection.

The build disables parallel maintenance workers and uses `maintenance_work_mem`
of 32 MiB for its own backend. This is a sort-memory setting, not a total VPS RAM
or vCPU guarantee. Each concurrent build normally scans the entity heap twice;
preparing all six therefore performs roughly 12 heap passes, plus sorting, index
writes and WAL. Schedule this explicit one-time operation away from peak capture
if the VPS has limited disk bandwidth. It is never run on startup or on a normal
suggestion request.

Storage follows **source entities with populated identifier fields**, not the
number of canonical products. Empty, non-string, too-long and unsupported barcode
values are excluded. Normalized text keys are also capped at 256 characters so
Unicode expansion cannot exceed the B-tree tuple limit and break later capture
writes. There is no broad JSONB GIN index.

A controlled PostgreSQL 17.11 fixture with 10,000 entities, all six identifiers
populated and distinct short references, used **2,015,232 bytes** across the six
indexes (60,000 entries). Setup took 0.334 seconds on the warm local test database;
that duration is not a production throughput forecast. The native query-plan
scenario also checks indexed identifier retrieval among 10,000 unrelated entities,
without a sequential scan of the entity table.

For budgeting, allow roughly **0.2–1.5 GB per million entities if all six fields
are populated**, with short-to-moderate ASCII references. Sparse coverage is much
smaller: 100,000 populated barcode entries in one field are roughly 4–10 MB.
These are planning ranges, not quotas; long Unicode references, page fill and
bloat can increase storage, while repeated identifiers can benefit from B-tree
deduplication. Add backup copies and WAL separately.

Each entity insert or attributes update evaluates up to six partial predicates
and can maintain up to six index entries. Expression indexes on `attributes` can
also reduce heap-only-update opportunities for unrelated attribute changes. This
is a recurring write/CPU/storage tradeoff, accepted only when explicit identifier
recall is wanted; source rows without identifiers add no identifier entries.
Measure sustained upsert rate, WAL growth and index sizes on the actual VPS before
claiming a production capacity. The one-time setup and recurring write overhead
are distinct costs.
