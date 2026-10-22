# Controlled commercial-outcome acceptance

T12 reuses the frozen evaluation contract, entity timelines and immutable result
publication. Native PostgreSQL and HTTP object storage exercise imported
commercial evidence. This establishes software behavior; it establishes no real
sales advantage, causal attribution or actual business profitability.

| Original T12 criterion | Executed evidence |
| --- | --- |
| Period-compatible commercial evidence | `tests/test_commercial_outcome_delivery.py` freezes three reviewed products, then measures lifetime-compatible offer-counter differences. Existing `tests/test_evaluation.py` and `tests/test_outcome_timelines.py` cover resets, incompatible periods, interval gaps, late interval reports and boundary coverage. Unsupported evidence remains censored. |
| Missing candidates remain visible | The native three-product scenario retains an ambiguous bounded result, a missing result and an estimated result. Selected count stays three; observed count is zero and precision remains unknown. Later eligible observations increase observed coverage to one third without dropping the other candidates. |
| Conditional transaction baseline | No sales-popularity baseline is emitted for the original mixed/missing universe. A later decision with fresh exact transaction counters across the same universe includes the baseline and selects the highest counter. `tests/test_financial_contract.py` additionally excludes incompatible cross-product periods and partial transaction coverage, while allowing ordinary price-only offers. The native delivery scenario exercises incompatible periods followed by aligned later counters without rewriting the earlier decision. Frozen eligibility reasons remain inspectable. |
| Bounds and estimates stay separate | The bounded sales delta is 10–60 units against threshold 30, hence ambiguous. The model-versioned estimated counter remains estimated, outside observed precision; its imported evidence retains the declared conversion assumption. Exact, bounded and estimated counts remain separate, with precision bounds and descriptive uncertainty. These are inputs and sensitivity scenarios, not calibrated demand claims. |
| Realized profit requires actual complete inputs | New decisions freeze reviewed ledger keys and required cost components in `profit_contract`. `tests/test_realized_amounts.py` checks missing, empty, nonfinite and negative components, explicit zeros, extra actual costs and direct/prepared parity. The native financial delivery scenario persists complete, missing and zero-cost ledgers alongside a price-only offer; actual Streamlit displays the frozen definition and stored results after restart. `tests/test_profit_revisions.py` verifies five incomplete/incompatible corrections censor future reports. Attention alone supplies no realized ledger result. |
| Frozen definitions survive late inputs | Late offer observations change a later matured result, while the earlier report and decision remain byte-for-byte equivalent as JSON. Backtest and prospective paths produce identical snapshots/reports. Financial delivery retains an earlier report after a missing-required-fee correction; revision scenarios preserve earlier cutoffs. `tests/test_financial_contract.py` verifies legacy snapshots without the new contract retain their raw configuration and integrity hash, and a declared missing ledger stays censored. Live workspace evaluations remain empty. |

The review projects the frozen target into bounded SQL summaries, so operators
can distinguish attention, sales and profit before opening details. The existing
frozen configuration owns the financial contract; no second ledger database or
mutable outcome cache is introduced. Maturation reads only declared financial
ledgers for new profit decisions and reuses them across scenarios. Invalid scope
rejects before historical reads. The latest available revision is selected before
validating its completeness, so invalid new knowledge cannot revive superseded
profit. Backtest metadata counts policy financial measurements without counting
them again for each baseline, and `profit_claim` remains false even for successful
synthetic financial results.

Exact-head CI, including native behavior, benchmark/quota exhaustion, image build
and isolated backup/restore, is recorded on the PR before autonomous merge.
Production-provider, source-access and real-market acceptance remain separate.
