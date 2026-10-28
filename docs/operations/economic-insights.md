# Economic breakpoints and cash exposure

Economic insights extend a saved launch dossier with operator-supplied joint
scenarios. They do not estimate demand, predict sales, calculate tax law or record
actual profit. No storefront, supplier, payment, mail or source request is made.
The existing [launch preparation](launch-preparation.md) guide describes the
underlying dossier and its required cost categories.

The public interface is shared by application entry points:

```python
from sm_trending_products.commercial_sensitivity import EconomicInsights, ScenarioProfile

profile = ScenarioProfile.model_validate_json(profile_json)
result = EconomicInsights(launch_service).describe(
    dossier_id="PRODUCT-FR",
    expected_revision=3,
    scenario_profile=profile,
    synthetic=False,
)
```

`launch_service` is the application's existing `LaunchService`. This read never
initializes database tables. The selected workspace must match the dossier.
The supplied revision must be its current revision; an intervening edit produces
`revision_conflict`. The result contains the exact source draft, revision,
database evaluation time, quote-readiness reasons, profile and profile hash.
Expired quotes remain visibly blocked even when scenario arithmetic is complete.
Inspect `source_facts.readiness` before treating a dossier as ready to review.

## A bounded profile

All monetary amounts and rates below are exact decimal strings. These are
illustrative operator assumptions, not recommended prices, returns or volumes.
The dossier supplies currency, ordinary costs, the desired profit per order and
any values that a scenario does not override. Missing dossier costs stay unknown.

```json
{
  "schema_version": "1",
  "scenarios": [
    {
      "name": "supplier-and-return-range",
      "basis": "Alternative offer and explicitly supplied output-tax amount",
      "retail_price": "36.00",
      "tax_basis": {"prices_include_tax": true, "tax_per_order": "6.00"},
      "returns": {
        "rate": "0.05",
        "loss_per_return": "30.00",
        "basis": "Total incremental refund/handling loss after recovery; ordinary order costs excluded"
      },
      "grid": {
        "goods_costs": ["8.00", "10.00"],
        "ad_costs": ["4.00", "7.00"],
        "return_rates": ["0.00", "0.10"]
      }
    }
  ],
  "cash": {
    "basis": "Ten declared orders; output tax reserved before settlement; not a sales forecast",
    "settlement_basis": "net_revenue_before_costs_and_returns",
    "horizon_days": 30,
    "opening_cash": "0.00",
    "orders": [{"day": 2, "quantity": 10}],
    "supplier_payments": [
      {"offset_days": -2, "fraction": "0.50"},
      {"offset_days": 2, "fraction": "0.50"}
    ],
    "settlements": [
      {"offset_days": 3, "fraction": "0.40"},
      {"offset_days": 7, "fraction": "0.60"}
    ],
    "cost_payment_days": {
      "shipping": 0,
      "payment_fees": 0,
      "nonrecoverable_taxes": 0,
      "returns_allowance": 14,
      "fulfillment": 0,
      "ad_cost": 0
    }
  }
}
```

The three axes expand jointly into eight points. There are at most 16 named
scenarios, five points per axis and 128 expanded points across the profile. Axis
values and scenario names must be distinct. Grid axes are explicit values, with
no interpolation, distribution, probabilities or confidence interval. A scenario
may instead supply one `ad_cost` and `cost_overrides` dictionary using the
dossier's six cost names. A cost cannot be both a single override and a grid axis.
The root `cash` plan applies the same declared volume and timing to every point.

Every changed price requires a new `tax_basis` and a nonblank `basis`. This
prevents reuse of a fixed dossier tax amount at a different price. An inclusive
tax basis needs a tax amount no larger than that price; an exclusive basis declares
the price as net revenue and omits `tax_per_order`. No VAT rate is inferred.
Changing the existing `returns_allowance` directly also requires `basis`.

## Returns and breakpoints

Without `returns`, the dossier's declared returns allowance is charged once.
With `returns`, `rate * loss_per_return` **replaces** that allowance. The loss is
the total additional economic loss for one return, after any declared recovery,
including its refund if applicable. Goods, shipping and other ordinary order
costs are already charged; do not include them again in the return loss. A return
model and a returns-allowance override cannot be supplied together. No extra
refund is independently deducted by this module.

Each point reports net revenue, pre/post-ad contribution and surplus over the
declared profit target. `maximum_ad_cost` and `maximum_goods_cost` are the highest
amounts meeting that target while all other point inputs remain fixed. Negative
limits mean the target is unattainable with a nonnegative cost. A missing ad cost
leaves post-ad contribution, supplier-cost ceiling and the target outcome unknown;
the existing maximum-ad ceiling can still be calculated from complete base costs.

Return tolerance additionally requires the explicit replacement return model
and ad cost. It reports an unattainable target, a threshold, or that all rates
from zero to one meet the target. Zero declared loss reports `no_sensitivity`.
Thresholds include their exact numerator and denominator; a recurring decimal
display uses 60-digit calculation precision. These limits describe the supplied
loss model, not an observed return rate.

The summary counts complete and incomplete points. Its surplus range covers
complete points only; `all_meet_target` is unknown if any point is incomplete.
A favorable range does not override missing facts or blocked quote readiness.

## Cash interpretation and limits

Cash requires explicit opening cash, daily order quantities, supplier-payment
fractions, settlement fractions and payment offsets for each nonzero ordinary
cost and ad spend. Each payment schedule must sum exactly to one. Supplier timing
applies to goods; the other offsets apply to their named per-order amounts.
Offsets are relative to each order day. A negative supplier offset represents a
deposit before ordering, but its resulting day must not precede scenario day zero.
Zero costs need no payment offset. Missing timing produces unknown cash results,
never an assumed immediate payment. Zero order quantities are explicit zero volume.

`settlement_basis` declares receipts as net revenue **after reserving output tax**,
before the modeled costs and return loss. This is a net cash scenario, not a
processor's actual gross bank settlement or an assertion about tax payment dates.
If the supplied net settlement basis is inappropriate, obtain the missing facts
before relying on this scenario. Tax reserve, refunds and return loss must each
be represented once under the declared economics basis.

Daily inflows and outflows are netted. `peak_cash_exposure` is the largest cash
drawdown from opening cash; `additional_cash_required` is the deficit after using
the declared opening cash. Positive unit contribution can coexist with either.
Intraday ordering is unknown. `beyond_horizon` reports payments and receipts
scheduled after the horizon; they do not affect the reported horizon balance.
Return costs are prorated scenario losses at the declared allowance-payment
offset, not predictions of individual returned orders.

Horizons, order days and ordinary cost offsets are bounded to 0–365 days; payment
offsets are bounded to ±365 days, with nonnegative settlement offsets. There are
at most 366 order-day entries, eight parts per payment schedule and one million
orders across a plan. Currency conversion, traffic, fees, further expenses and
new facts are never supplied automatically. Floats, booleans used as amounts,
nonfinite numbers and excessive precision/size are rejected. The public read
revalidates nested mutable or bypassed Pydantic models before any dossier lookup.
Python sequence inputs must be materialized lists or tuples within these bounds;
sets, frozensets and lazy iterators are rejected before iteration. Nested models
are revalidated even when supplied inside a tuple.
Exact zero is normalized before scenario arithmetic and formatting, including
negative zero and scientific notation with extreme exponents. Imported dossier
zero amounts receive the same arithmetic protection; source facts retain their
compact original decimal representation. This does not relax the nonzero amount
or rate limits.

## Operator access

Run a saved profile against the exact current dossier revision:

```sh
sm-trends research economics --dossier-id PRODUCT-FR --expected-revision 3 \
  --file /private/economics.json
```

Add `--synthetic` only for the isolated simulated workspace. `--help` works before
database configuration. The command validates a regular JSON file of at most
128 KiB before loading configuration or opening SQL. It creates no history or
object-store client and never initializes commerce tables. Run the existing
`sm-trends commerce init` explicitly when preparing a new workspace.

Success returns one JSON report and exit code zero. Invalid input, a missing
dossier, a namespace mismatch, a stale expected revision or unavailable SQL
returns a fixed JSON error category on stderr with exit code two. Private input
values, JSON keys, file paths and database diagnostics are not included in errors.
Reading a scenario does not save a dossier revision or launch anything.

The private economic view selects a product, an explicit dossier and its expected
revision. The dossier list reads at most 100 workspace records per page and filters
that page to the selected product; an empty page does not establish that no
dossier exists. Upload the bounded profile and click **Inspect economics** to run.
The table shows contribution, cost ceilings, return tolerance and missing facts.
Choose one expanded point, up to 128, to inspect its daily cash flows. Cash remains
unknown when its required timing or volume assumptions are absent. The view
refreshes the dossier on every interaction and retains no derived scenario report
in session state; after a concurrent edit, explicitly choose the current revision
and inspect again.
