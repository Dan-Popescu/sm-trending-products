# Synthetic product-to-commerce rehearsal

This checks the software path before choosing a real store or supplier. It does
not measure product demand, establish source access or prove profitability. No
real customer, payment, shipment, email, paid analysis or platform account is used.

## Isolated setup

Use a separate scratch PostgreSQL database/schema, S3 bucket and
`PIPELINE_NAMESPACE`. Do not point this command at the collecting deployment.
Configure the existing application with `DATABASE_URL`, `S3_ENDPOINT_URL`,
`S3_BUCKET` and private local object-store credentials, then run the normal
`sm-trends init --create-bucket` in that scratch environment. This command does
not provision a VPS, cloud account or paid service.

The object store must be the local HTTP fixture: numeric loopback/localhost,
the existing Compose `objectstore` service, or the isolated restore-check service.
Default AWS endpoints and arbitrary remote hosts are rejected. A fresh run also
refuses any real product or enough existing synthetic products to exceed eight
total products. This bounds the existing evaluation's product scope. All optional
commercial/enrichment tables are initialized explicitly by the rehearsal; capture
SQL versions and collection workers are unchanged.

```sh
python -m sm_trending_products.commerce_rehearsal --synthetic --run-id local-pilot-1 --output /private/rehearsal.json
python -m sm_trending_products.commerce_rehearsal --synthetic --run-id local-pilot-1 --verify --output /private/rehearsal-verified.json
```

`sm-trends commerce rehearse` is the equivalent operator entry point and accepts
the same flags. Its `--help` works without application configuration.

`--synthetic` is mandatory before opening application resources. Output files are
created with private permissions and never overwrite an existing file. Exit code
0 means a completed rehearsal or successful evidence verification, 1 means a
recorded incomplete/failed run, and 2 means rejected setup or output. Inspect the
JSON `status`, `execution_mode`, `failed_stage` and safe diagnostic code.

## What the run exercises

The bounded fixture contains one new product, three videos and **59 observations**
over seven synthetic days. Two linked video histories are sampled every six hours;
the third is an unconfirmed, conflicting variant candidate. No source API or raw
video/audio is downloaded.

1. Describe attention over **24, 72 and 168 hours**, with measured intervals.
   Matching explains explicit English/French aliases and variant conflicts;
   suggestions still require review.
2. Import authored synthetic transcript text and analyze it with free local rules.
   Two requests reuse one versioned analysis. External enrichment remains disabled;
   daily request limits apply and reserved cost is zero.
3. Save a ready France/EUR dropshipping dossier with explicit supplier, availability,
   delivery, return terms, price and tax basis. This is one country, not all Europe.
4. Declare the complete actual-cost contract and freeze a prospective decision
   **before** creating the shop draft or ingesting order outcomes. The example
   records launch delays of 0, 12, 24 and 48 hours.
5. Start a temporary numeric-loopback WooCommerce HTTP fixture. The actual adapter
   creates one owned **draft**, verifies shop currency/tax settings, then reads
   signed order hints, duplicate hints, delayed hints and a gateway-confirmed refund.
   The server permits only selected draft/read paths and at most **40 requests**.
6. Record manual supplier handoff, unverified tracking and a platform-notification
   **request**. Neither supplier acceptance, carrier delivery nor email delivery is
   claimed. The temporary HTTP server is shut down when the step finishes.
7. Review explicit synthetic statements: **168 EUR gross − 24 EUR refund − 24 EUR
   net output tax = 120 EUR net revenue**; **80 EUR actual costs** gives **40 EUR
   profit**. These are declared test figures. WooCommerce's mutable order total and
   paid timestamp do not establish captured money; no capture is invented.
8. Publish the complete period, then record a later correction with incomplete
   refund coverage. The newer cutoff becomes **censored/unknown** while the old
   cutoff remains **40 EUR**. Whole-period financial data does not establish the
   shorter delayed-launch outcomes automatically.

The report contains compact counts, decisions and opaque evidence pointers. Fake
customer contact data travels only through the controlled HTTP boundary; raw
webhook bodies, credentials and customer fields are excluded from stored order
snapshots, learning observations and the report.

## Replay and failure recovery

The same run ID returns `replayed_completed_run` after completion. It is a
**historical report**, not another execution, timing measurement or fresh business
result. `--verify` separately reads the saved operation, order, manual records,
transcript analysis, immutable evaluations and history objects. It performs zero
provider/source requests and does not rerun maturation or create a store.

An interrupted or failed run remains diagnosable. Repeating its run ID reports
`incomplete_run_no_retry`; it never recreates a possibly completed draft. Use its
saved dossier/connection identifiers to inspect the durable operations:

```sh
sm-trends commerce draft-operations --connection-id REPORTED-CONNECTION --id REPORTED-DOSSIER --synthetic
sm-trends commerce draft-operation --operation-id OPERATION --synthetic
```

The ephemeral shop is intentionally gone after the rehearsal. Investigate the
recorded state; choose a **new run ID** for a fresh fixture in a sufficiently small
scratch namespace. Do not delete source history, reset outboxes, clear capture
locks or rewrite old financial decisions to force a retry. For cleanup, retire
the dedicated scratch environment only after confirming its database, bucket and
namespace are separate from production. There is no automatic destructive rollback.

## Backup and restore evidence

Use the existing canonical `scripts/backup.sh ... --copy-objects --online` and
`scripts/restore-check.sh ... --exercise-synthetic-recovery`. PostgreSQL dumps
contain the optional module tables. Object inventory includes normal history and
committed derived publications, including transcript packs. The core catalog
fingerprint alone does **not** prove the optional commerce tables were restored.

Seed this run before the backup, and call `verify_rehearsal` in the existing
isolated restored environment. Its public evidence checks cover dossier/delivery
identity, sanitized order and manual intents, financial revision history,
published actual/withdrawal intents with their saved dataset pointers, the
declaration's published intent, source observation and identity decision,
transcript history and source/ledger objects. All three financial publication
pointers must still match the completed run. A native end-to-end test is evidence
of the workflow; a successful actual restore execution is separate evidence.

## Choices required before real activation

Keep WooCommerce/Shopify connections disabled until choosing a platform and
supplying private configuration. Store files reference environment-variable names,
not token/password values; see [store connections](store-connections.md).

Choose an individual European country and its actual currency, tax settings,
returns/contact terms, payment setup and notification owner. Review supplier
quotes, product/marketing rights, stock, delivery promises and landed costs.
Dropshipping is the starting mode; stocked suppliers and durable brands can use
the same versioned commercial facts later. Add the selected gateway's settlement
evidence before calling cash/profit complete. Enable hosted checkout and native
transactional messages only through the chosen platform's real configuration.
The rehearsal authorizes none of these live actions.
