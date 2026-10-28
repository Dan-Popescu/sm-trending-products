# Private launch preparation

The launch desk prepares commercial scenarios. It does not activate a store,
publish products, request money, send mail or contact a supplier. The initial
scope is individual European countries and dropshipping, with stock/external
fulfillment also representable. No country, currency or VAT regime is inferred.

## Initialize and create

Use the existing private PostgreSQL configuration (`DATABASE_URL`). These
commands do not create an S3 client or contact object storage. Base application
migrations must already exist. Optional commercial tables are created explicitly:

```sh
sm-trends commerce --help
sm-trends commerce init
sm-trends commerce template > /private/dossier.json
```

Edit the template. Choose an existing product ID, a stable dossier ID, one ISO
country and currency, and the correct real/synthetic workspace. Enter decimal
amounts as strings, for example `"12.50"`; do not use floating-point JSON numbers.
Missing money stays unknown. A declared `"0"` is a known zero cost.

```sh
sm-trends commerce save --file /private/dossier.json --expected-revision 0 --request-id CREATE-1
sm-trends commerce list --limit 100 --offset 0
sm-trends commerce show --id PRODUCT-FR
sm-trends commerce preview --id PRODUCT-FR --output /private/product-draft.html
```

The private Streamlit dashboard also has a **Launch preparation** view with
editable fields and a downloadable escaped HTML draft. It never initializes
tables implicitly. Drafts contain authored copy, not copied source videos or raw
source evidence. Preview files are created with private permissions and an
existing file is never overwritten. Treat downloaded files as private documents.

## Revision and readiness rules

An edit supplies the last observed revision, for example `--expected-revision 1`,
and a new request ID. Competing edits cannot overwrite each other. A retry with
the same request ID and same input returns its original revision; reusing the ID
for a different input is rejected. Product, country, currency and workspace stay
fixed within one dossier; create another dossier for another market scenario.

To become ready, a dossier needs explicit product copy, supplier/quote facts,
unexpired quote, available stock, delivery range, return terms, an explicit
`test` decision and complete scenario costs. Reads check quote expiry against
current PostgreSQL time. A previously saved ready revision can become stale.

Required cost categories are goods, shipping, payment fees, nonrecoverable taxes,
returns allowance and fulfillment. An inclusive price subtracts the separately
declared tax per order; an exclusive price is declared net revenue. Input taxes
which cannot be recovered belong to nonrecoverable taxes. The application does
not calculate tax law. A scenario's maximum acquisition cost subtracts all
declared costs and desired profit from net revenue. A negative result blocks the
scenario. None of these estimates is measured profit or a sales prediction.

`--synthetic` on reads selects the isolated simulated workspace. The save record
declares its own workspace and must match its canonical product. Collection and
capture schema version remain unchanged; store preparation is independent of
collection workers. Real store configuration and measured commercial results
are separate delivery steps tracked in the [delivery map](../planning/commerce-learning.md).
