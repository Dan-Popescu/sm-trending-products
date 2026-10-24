# Massive-collection cost scenarios

Issue: [#70](https://github.com/Dan-Popescu/sm-trending-products/issues/70).
Price observations: **2026-10-24**. This is an offline planning calculator; it
does not buy resources, contact a source API, or certify any machine's capacity.

## Reproduce and inspect

```sh
PYTHONPATH=src python scripts/cost-scenarios.py \
  --prices docs/operations/costs/prices-2026-10-24.json \
  --coefficients docs/operations/costs/illustrative-coefficients.json \
  --output output/cost-scenarios.json
```

This produces 216 explicit configurations: 5,000/10,000/50,000 products,
30/90 retained days, six VPS offers, two storage offers and 0.5/1/2 physical-byte
sensitivity. Each has 20 distinct videos/product, four refreshes/day and 96
assessment publications/day. The committed [compact result matrix](../verification/massive-cost-scenarios.json)
contains base quantities and all sensitivity results; the full generated report
also contains every directly runnable configuration. Add `--compact` to reproduce
the committed review matrix without repeating each complete configuration.

Run one configuration without database or cloud credentials:

```sh
sm-trends cost --config output/selected-scenario.json
```

Extract `configuration` from a generated scenario to that file. Change coefficients
or dated rates rather than editing calculator code. `usage_overrides` accepts
named physical quantities from an actual retained-history report; unknown names
are rejected. Record measurement revision, shape and hardware in coefficient
`source`. An override is a final billing quantity, not an input that implicitly
recalculates other quantities. Inspect the whole configuration after replacing it.

## Published prices, not remembered rates

The [catalog](costs/prices-2026-10-24.json) records rates, source URLs, regions,
units and included resources. All example prices are USD; no exchange conversion
is invented and mixed-currency combinations are rejected. `tax_rate=null` leaves
tax-inclusive total unknown. No new-account promotion is assumed. Recheck the
official price and actual account eligibility before purchasing.

| Offer | Base published monthly price | Pricing qualification |
| --- | ---: | --- |
| DigitalOcean Regular Basic, 4/8/16 GiB | $24 / $48 / $96 | Shared CPU; included disk 80/160/320 GiB |
| AWS Lightsail Linux public IPv4, 4/8/16 GB | $24 / $44 / $84 | General Purpose, burstable CPU; advertised disk 80/160/320 GB |
| R2 Standard | $0.015/GB-month | Class A $4.50/million, Class B $0.36/million; Internet egress free |
| Backblaze B2 pay-as-you-go | $0.00695/GB-month | Standard storage API calls free; public egress up to 3x average stored data, then $0.01/GB |

Sources: [DigitalOcean machines](https://www.digitalocean.com/pricing/droplets),
[volumes](https://www.digitalocean.com/pricing/volumes),
[bandwidth](https://docs.digitalocean.com/platform/billing/bandwidth/),
[Lightsail](https://aws.amazon.com/lightsail/pricing/),
[R2](https://developers.cloudflare.com/r2/pricing/),
[B2](https://www.backblaze.com/cloud-storage/pricing) and
[B2's effective May 2026 update](https://www.backblaze.com/blog/backblaze-pricing-and-product-updates/).
The B2 update supersedes the stale $0.005 storage note still visible on its
transaction page. Paid event notifications are outside the standard API assumption.

R2 PUT and LIST calls share Class A; GET and HEAD share Class B. This calculator
implements its published whole-unit rounding. The free tier is **disabled** unless
`account_free_tier_available=true`: storage and request allowances must be unused
by other account workloads. Two buckets using the same store configuration share
one modeled account, so quotas and rounding apply once, not once per bucket.
Different store configurations represent separate accounts and their transfer
charges remain separate. Verify account grouping before claiming included usage.

DigitalOcean transfer is converted from GiB to decimal bytes; additional block
disk is priced in allocated whole GiB. Lightsail disk/transfer advertising is
interpreted conservatively as decimal GB/TB, and its extra disk has an 8 GB minimum.
Inbound consumes its transfer quota, but only outbound can produce an overage.
Regional Lightsail overage is left unpriced; a case exceeding its quota reports
an unknown cost instead of treating the transfer as free. Regions with half the
advertised allowance require a different machine configuration.
The upstream HTTP response-wire coefficient is also unknown until measured;
stored gzip raw bytes do not measure network response bytes. Consequently these
Lightsail infrastructure totals remain unknown even when known storage-download
traffic fits its allowance. DigitalOcean's free inbound traffic does not need
this coefficient to price its infrastructure. A known zero R2 egress price also
remains zero when the underlying byte quantity is unknown; priced B2 egress does
not receive the same assumption.

## What the arithmetic includes

The active payload window is modeled at constant retained size; SQL tombstones
are modeled at the workload's explicit `operating_age_days` (365 in examples).
The resulting month is a conservative planning month at that age, not a measured
first-month invoice. Object storage GB-month averages and provider/internal caches
must be replaced by actual metered values for billing acceptance.

- Raw, normalized and manifest originals remain counted. Compaction adds both
  raw and normalized copies, larger manifests, extra PUTs, and source reads;
  it does not subtract original storage while lineage retains it.
- Reference trees and compact assessment publications consume real objects and
  bytes. A full compact assessment is still written at every declared publication;
  unchanged source windows do not make these bytes free.
- SQL delivery/index state and artifact state are separate coefficients. Retired
  key tombstones continue accumulating after payload expiry; marker rows, indexes
  and their contribution to subsequent full catalog dumps are included.
- WAL, cache, operating-system/spool space and complete logical dumps consume
  VPS disk. The default 20% headroom is a declared planning margin, not a measured
  safe bound or a source quota guarantee.
- Full bases, new payload deltas, every full SQL dump and repeated full inventory
  are counted in backup storage. Periodic full families do not delete bases still
  required by descendants. Reservation includes each pre-window base and delta
  ancestor, including its dump and inventory. At daily capture, 30-day retention
  and a full every 30 captures, the worst calendar phase requires two bases,
  57 deltas and 59 captured directories. These are retained storage reservations;
  they do not invent 59 captures or transfers during a 30-day month.
- The existing backup interface uses a local POSIX family while verifying and
  extending it. `local_backup_family_fraction=1` reserves that entire family on
  local/attached disk **in addition** to its offsite copy. A remote POSIX volume
  needs its own explicit price; changing a fraction to zero does not make an
  unsupported S3-mounted backup path valid.
- One monthly isolated restore with a **cold offsite fetch to POSIX** is assumed.
  This fetch materializes one selected base/delta chain before running the local
  restore. The real resolver hashes every ancestor-owned payload, including keys
  omitted from the latest inventory after expiry. Therefore GETs and transfer
  include one full base, every intervening payload delta, every chain inventory
  and ancestor `snapshot.json`. Only the target SQL dumps are needed. Unrelated
  retained families are excluded; local hashing and upload of the latest live
  inventory do not cause a second offsite GET. The archive-fetch transport is an
  explicit planning assumption, not a network operation in `restore-check.sh`.
  Simultaneous restored payload and database copies reserve local disk. Set these
  coefficients to the actual separate rehearsal host when applicable; that host's
  compute and storage bill then belongs in `other_monthly_cost`.
- Backup HEAD/LIST inventory work is explicit. An unchanged payload delta may
  issue zero payload GETs while still inspecting millions of object metadata rows.
  `backup_heads_per_object_per_capture=1` reflects the pre-optimization #66 path;
  use a reduced value only after the revised checkpoint path is measured.

Full-backup periods cannot be shorter than capture intervals. A full period that
is not a multiple of the capture interval rounds up to the next capture: weekly
captures with a 90-day target give a full every 13 captures (91 days). The maximum
chain is 32 snapshots, not 32 calendar days. Monthly request and transfer planning
starts at a full capture and counts only actual captures. Retained-family storage
uses the worst calendar phase independently: for `N` retained captures and a full
every `K` captures, reserve `ceil((N-1)/K)+1` bases and `N+K-1` total captured
directories. Parent catalogs and inventories are retained as well as payloads.

Cold recovery independently reserves the longest valid **single** chain of `K`
captures: payload `S + (K-1) * capture_days * new_bytes_per_day`, corresponding
owned-object GETs, `K` inventories, and one target dump. Each ancestor adds two
metadata requests (its inventory and snapshot JSON); the existing declared target
metadata-file allocation is applied once. Profiles explicitly assume 4 KiB per
ancestor snapshot JSON; this is not a measured size or guaranteed upper bound.
An unknown ancestor size keeps incremental recovery bytes unknown, while a
single full restore and zero monthly rehearsals retain their known budgets.

Source refresh requests are a **lower bound**, assuming distinct videos and full
compatible groups of 20. Discovery, pagination, fragmented accounts, failed
requests and retries can add work. `source_api_monthly_cost` remains unknown.
`other_monthly_cost` also remains unknown until monitoring, DNS, notifications,
support and other real charges are supplied. Neither is silently zeroed.

## Illustrative results and decision

These numbers use the committed coefficient assumptions, **not** a massive
production measurement. Several byte coefficients use old controlled fixture
scales; marker/dump/compaction coefficients are explicit planning assumptions.
Use the load harness's new physical totals before making a purchasing decision. The retained-import ratios cover only `view_count` and `creator_id`; descriptions, prices, sales, transcripts and media payloads require new coefficients.

For the DigitalOcean 8 GiB offer, with extra priced block disk, identical source
and backup store and no account free allowance:

| Products | Retained days | Modeled R2 infrastructure/month | Modeled B2 infrastructure/month | Planned peak disk |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 | 30 | $221.92 | $116.72 | 861 GB |
| 5,000 | 90 | $428.62 | $200.91 | 1,716 GB |
| 10,000 | 30 | $405.48 | $199.94 | 1,708 GB |
| 10,000 | 90 | $823.72 | $368.32 | 3,418 GB |
| 50,000 | 30 | $1,880.50 | $866.30 | 8,484 GB |
| 50,000 | 90 | $3,986.74 | $1,715.73 | 17,029 GB |

Amounts exclude tax, unknown source API fees and other unknown services. The
complete bill is therefore `null`; the separately named infrastructure estimate
does not imply affordability, a fitted CPU/RAM configuration, or provisioning.
In this assumed shape, repeated backup metadata inspections materially increase
R2 request charges; verified B2 API pricing removes those transaction fees.
Local recovery-family disk can cost much more than remote object bytes. The
apparently inexpensive base VPS plan alone is consequently an incomplete budget.

Start with a measured CPU/RAM requirement and the complete physical disk budget,
then compare providers using that feasible resource envelope. Verify request-rate
and conditional/version behavior before choosing a store based on price. Prefer
reducing avoidable metadata work and repeated recovery bytes before buying larger
machines. A price comparison cannot establish the bottleneck or the source grant.

## Coefficient evidence and checkpoint alternatives

Each numeric coefficient in the supplied profiles has an `evidence` entry:
`measured_ratio` includes an immutable fixture path and its limited shape;
`assumed` identifies planning-only values; `contract` identifies a required
behavior such as the POSIX backup family; `unknown` preserves missing values.
Sensitivity-scaled measurements become assumptions, not additional measurements.
The 420 GB source and multi-terabyte disk examples above must never be described
as observed massive-load requirements: the fresh load harness's physical counts
must replace them before a sizing decision. Metadata bytes can vary with key
length, index occupancy, sharing and post-retention tombstone accumulation.

The [optimized checkpoint profile](costs/optimized-checkpoint-coefficients.json)
models verified catalog/LIST/base reuse separately from the #66
baseline. It still reserves one full inventory HEAD per object for each full
base, one HEAD per new object and a configurable legacy/unknown-object rate on
other captures. It never replaces every HEAD with zero merely because unchanged
payload GETs are zero. The [second matrix](../verification/massive-cost-scenarios-optimized-checkpoint.json)
uses identical bytes and prices; only request work changes. At the illustrative
10,000-product/90-day DO 8 GiB shape, modeled R2 infrastructure changes from
$823.72 to $592.96/month. The corresponding B2 amount stays $368.32 because
its standard API calls are already priced at zero. These are scenario
comparisons outside the measured inventory shape. The
[native 1,001-object checkpoint measurement](../verification/cost-checkpoint-native-1001.json)
verifies the request coefficients on revision `a95e2fc`: a full capture retains
1,001 HEADs and GETs but moves all HEADs out of the writer checkpoint; an unchanged
capture uses zero HEADs/GETs and two LIST requests. Native checkpoint durations
were 4.169 to 0.234 seconds for full capture and 3.842 to 0.211 seconds unchanged.
These are loopback PostgreSQL/Moto timings, not provider latency or million-object
runtime predictions. Seven independent native probes also checked known-checksum
metadata, legacy-base repair, version races and corrupt base rejection.

## Fresh retained-history measurements

The [partial measured profile](costs/measured-retained-import-coefficients.json)
uses only the completed physical seed phase of the new 1,000-product case:
2.4 million committed observations, 1,000 raw objects totaling 114,024,891 bytes,
1,000 normalized objects totaling 101,414,747 bytes, and 1,000 manifests totaling
1,898,800 canonical bytes. Seed SQL occupied 1,913,012,224 bytes with no published
derived objects at that measurement. The [measurement artifact](../verification/measured-retained-import-inventory.json)
pins its revision, shape and separate supplemental capture.

This is not a full-case pass: later analytical phases were not accepted by that
measurement. The profile deliberately leaves unmeasured dump, WAL and derived
work unknown. Its 2,400 observations per physical dataset differ from a normal
20-ID refresh response; Arrow footers/dictionaries, manifests and metadata
membership density make per-observation ratios layout-sensitive. Never copy
the compact import's normalized-byte ratio into the small-file collection model
without separate measurement. Updating final cost scenarios from a successful
complete retained-history run remains a distinct calibration step.

## Verification

`tests/test_costs.py` checks independent row/request arithmetic, retained copies,
backup families and rehearsal disk, account-wide R2 quotas and rounded billing,
B2 shared egress, allocated volume minimums, unknown handling, currency and tax,
bad numeric inputs, configuration typos and the credential-free CLI. Scenario
generation is pure offline arithmetic; no cloud resources are created.
`tests/test_cost_restore_chain.py` runs the real filesystem resolver: an expired
parent-owned payload and its inventory remain required even when absent from
the final live inventory. The arithmetic regression prices that single chain
without charging ancestor SQL dumps or unrelated retained families.
