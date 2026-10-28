# Selected transcript insights

This optional application module imports supplied subtitles, human transcripts
or descriptions for explicitly selected, confirmed product videos. It does not
transcribe audio, acquire media, download models, call a paid service by default,
or change collection schedules. Capture continues independently if enrichment
fails. Initialize this module explicitly after initializing the base application;
its additive SQL tables leave capture schema version 40 unchanged.

## Operator commands and private review

Initialize the base application first, then initialize this optional module with
`sm-trends transcripts init`. Viewing the private page, listing results or importing
text does not initialize module tables. `sm-trends transcripts --help` and each
subcommand's `--help` work without database or object-store configuration.

Prepare a UTF-8 JSON file with the `TranscriptInput` fields shown below. Import
requires the exact reviewed video, committed dataset, delivered observation and
permission reference; the file's `synthetic` field selects its workspace.

```sh
sm-trends transcripts import --file /private/selected-text.json
sm-trends transcripts analyze --product-id PRODUCT --transcript-id TRANSCRIPT \
  --request-id STABLE-REQUEST --synthetic
sm-trends transcripts show --product-id PRODUCT --request-id STABLE-REQUEST --synthetic
sm-trends transcripts list --product-id PRODUCT --limit 20 --synthetic
```

Use `--synthetic` only for synthetic results; omit it for real evidence. The import
command takes workspace from the JSON rather than a `--synthetic` flag. Repeat the
same analysis request to read its existing result. A different request ID for the
same eligible content and extractor also reuses the existing analysis instead of
reserving another run. `list` returns bounded summaries without quotes; use its
`next_cursor` as `--after` to continue. Commands print JSON on stdout and redacted
JSON errors on stderr with a nonzero status. No command provides an external
adapter, downloads videos or enables paid execution.

Import and budget files are read with a **128 KiB** byte cap before runtime
configuration. Supplied text has a separate **16,000-character** limit. A JSON
file with excessive escaping can hit the byte cap sooner; UTF-8 files avoid
unnecessary Unicode escaping. Invalid content is rejected without printing it.
The daily default is 100 new free local analyses. To choose a smaller daily or
per-video text limit, configure it before today's first run:

```sh
sm-trends transcripts budget --file /private/local-budget.json --synthetic
```

For example, the budget file can contain
`{"daily_requests":25,"max_text_characters":8000}`. The default provider is
`local-rules`; daily limits cannot be reset or raised by repeating configuration.
The `budget --provider` option records a provider's limits, and does not supply or
activate a paid adapter. External adapters remain outside this operator surface.

The private dashboard's **Transcript insights** view imports one supplied text,
then offers separate **Analyze selected text with local rules** and **Read existing
analysis request** actions. No analysis or evidence read runs automatically when
the page opens. Request IDs are remembered per product and workspace, independently
of Streamlit's temporary widget state, including after switching products or views.
Source-derived results are never stored in session state. After a rerun, explicitly
request the result again; the service rechecks source availability and ownership.
Withdrawal of its source blocks subsequent CLI/UI reads and excludes its summary,
even before physical object deletion. Already delivered browser output cannot be
retracted without a subsequent rerun or interaction.

Quotes are rendered as JSON text, including HTML-looking source text. Presentation
filters redact credentials; offsets continue to refer to the original normalized
source evidence. The guarded service retains faithful quotes for inspection.
Permissioned transcript evidence is distinct from customer/order data; do not
supply real customer contact data through this interface.

## Public interface

```python
from sm_trending_products.enrichment import TranscriptInput, TranscriptInsights

insights = TranscriptInsights(pipeline.catalog, pipeline.provenance)
insights.initialize()
transcript = insights.import_transcript(TranscriptInput(
    product_id="selected-product",
    entity_key="authorized_source:video:123",
    dataset_id="existing-committed-dataset",
    observation_id="existing-delivered-observation",
    synthetic=False,
    text="Supplied text with permission to process it.",
    source_kind="human_transcript",  # alternatively subtitles or description
    language="en",
    permission_reference="operator-record-of-permission",
))
result = insights.analyze(
    "stable-operation-id", "selected-product", transcript["transcript_id"],
)
page = insights.summaries("selected-product", limit=100)
```

`result(request_id, product_id, synthetic=False, as_of=None)` reads an existing
operation. `summaries(..., after=page["next_cursor"])` provides bounded pages,
without full quotations. Identifiers, ownership and namespace are checked by the
service; the CLI and private UI should cross this interface. No optional module
tables are initialized by a read.

The import must identify a committed source dataset and an actual observation
of this video delivered in that dataset. The existing point-in-time ownership
rules require a confirmed owner with confidence at least 0.9. A more recent
low-confidence owner does not restore an older owner's access. Real imports
require verified, unexpired source retention policy and an explicit permission
reference. Imported permission does not grant remote API capability. A source
publication date alone never establishes evidence availability. An explicit
future cutoff is supported only for controlled synthetic evidence.

## What the result means

The free English/French extractor uses small phrase lists. It returns quotations
and offsets from the supplied text with candidate labels: problem, benefit,
claimed proof, objection, call to action and offer. The first sentence is labeled
as a potential opening hook by position. These are unvalidated interpretations,
not measured advertising effectiveness, verified product claims or sales
predictions. Unsupported categories remain absent; no missing speech is invented.

Descriptions remain labeled `description` with `speech_evidence=False`. Subtitles
and human transcripts retain their stated source kind; the application does not
independently verify their accuracy against audio. Model adapters also return
bounded offsets into supplied text, rather than generating untraceable quotes.

Text is normalized to NFC Unicode, LF line endings and trimmed boundaries; inner
spacing is preserved. Reuse keys include the video, real/synthetic namespace,
source kind, language, canonical text hash, provider and extractor version. A
metrics refresh does not rerun unchanged eligible text. A changed extractor
version deliberately creates new work. Source-policy expiry or withdrawal makes
the old result unavailable; a later independently authorized observation can
form a new lifecycle generation. An unresolved remote outcome still blocks an
automatic repeated call for the same logical text and version after retirement.

## Bounded work and costs

Each call processes one selected video with at most 16,000 text characters and
returns at most 24 labels, each covering at most 500 characters. Summary pages
contain at most 200 requests. There is no scan of all videos and no automatic
background enqueue. The free provider defaults to 100 new analyses per UTC day;
its current-day limit can be configured explicitly before first use:

```python
from sm_trending_products.enrichment import EnrichmentBudget

insights.configure_budget("local-rules", EnrichmentBudget(daily_requests=500),
                          synthetic=False)
```

Daily configuration is immutable once recorded. Repeating the same configuration
is safe; attempts to reset or raise that day's budget are rejected. External
providers need a separate explicit configuration each UTC day with
`allow_external=True`, `daily_requests`, exact decimal `daily_cost_limit`,
`maximum_request_cost`, `currency` and `max_text_characters`. An adapter must
provide an enabled, versioned `ProviderSpec` with a positive worst-case
`reserved_cost` and matching currency. Its implementation must enforce that
request bound at the provider boundary. The seam supplies no paid provider or
production credentials. Test adapters are entirely synthetic.

Before executing an adapter, PostgreSQL atomically reserves one request and the
declared maximum cost, and records the stable operation identity. Concurrent
workers and repeated operation IDs share the reservation; content aliases share
one analysis. No database transaction remains open during the adapter call.
The stored amount is a reservation, not an actual invoice. Reservations are
conservatively retained rather than refunded without billing evidence.

A timeout, invalid output or lost result becomes `uncertain`. A process death
can leave `running`. Both states retain their reservation and suppress automatic
retries, including aliases and restarts. A real adapter must provide an explicit
authoritative reconciliation design before production activation; this slice
deliberately exposes no force-retry or automatic timeout reset. Changing the
request ID does not bypass that rule. Selecting another extractor version is
new work, not a retry or proof that the old call was free.

## Evidence lifecycle and storage

Text, permission references and quoted derivations are stored through the
existing immutable Provenance object graph. Transcript roots depend on their
actual dataset; insight roots depend on the transcript. Optional
`enrichment_` SQL tables contain only IDs, hashes, descriptors, budgets and
execution metadata. Source invalidation and existing object retirement therefore
withdraw these derived contents without an independent text archive escaping
source policy. Reads guard source and publication state even before physical
object deletion completes. Real and synthetic graphs and budgets stay separate.

Result availability includes the actual completion time. Replaying a cutoff
before completion does not expose later enrichment, even if the video's source
evidence existed earlier. Future product ownership changes likewise do not
rewrite an earlier owner decision.

Native PostgreSQL scenarios in `tests/test_transcript_insights.py` cover reuse,
competing workers, cost and request exhaustion, uncertain outcomes, ownership,
namespace, temporal availability and source withdrawal during an external call.


Native operator scenarios in `tests/test_transcript_operator.py` additionally
exercise the public command and Streamlit paths: explicit initialization,
import/local analysis/replay/list/withdrawal, daily limits and reused reservations,
workspace and observation rejection, byte/text caps, stable request identities,
credential-safe quoted HTML, and the absence of stale session output. These use
isolated PostgreSQL schemas and a controlled local S3-compatible fixture. They do
not establish real source permissions, speech accuracy, advertising effectiveness
or production provider access.
