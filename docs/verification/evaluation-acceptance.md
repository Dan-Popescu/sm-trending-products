# Controlled evaluation acceptance

T10 and T11 verify point-in-time methodology and immutable prospective snapshots
under the agent's standing review/merge authority. Real predictive advantage and
prospective market performance require eligible, elapsed real history. Controlled
time advancement does not establish those business results.

## T10: point-in-time ranking replay

| Criterion | Executed evidence |
| --- | --- |
| Frozen universe, scope, target and versions | `tests/test_policy_delivery.py` uses the actual persisted API and CLI to freeze evaluation configuration and the chosen `AssessmentPolicy`. A three-sample requirement changes eligible products, matches published assessment and retains known/eligible counts. Snapshots retain identity decisions, reasons, cutoff, horizons and delays. |
| No future evidence, ownership, capabilities or policy | `tests/test_evaluation_integration.py` excludes later ownership/capability grants and later-available counters through stored history. Numerical evaluation cases cover corrections and future policy availability. The policy delivery rejects a policy declared unavailable at its decision cutoff. |
| Fair reproducible baselines | Each snapshot's popularity, growth and seeded family-matched random selections use its identical eligible universe. Native policy delivery verifies every baseline remains inside the stricter universe; evaluation cases verify deterministic comparison and conditional sales baselines. |
| Family grouping, splits and dependence | Native integration preserves historical family membership after changes and selects only one shared-family member. Reports declare no fitting, record validation/test split labels and count repeated product/family decisions. No trained-model generalization or untouched real holdout is inferred. |
| Metrics, coverage and uncertainty | Evaluation cases verify exact/bounded/censored outcomes, lead-time bounds and missing history. Native policy delivery verifies measured precision, observed sample count and uncertainty at mature horizons. The actual Streamlit report displays the stored complete backtest report rather than a ranking-only surrogate. |
| Fixed-input reproducibility and insufficient history | Native replay yields the same evaluation ID after normalization replay; forward/backtest snapshots and outcome reports match. Policy/CLI delivery repeats identically. Empty/missing history is reported explicitly in evaluation and durable-frontier cases. |

## T11: prospective decision software

| Criterion | Executed evidence |
| --- | --- |
| Immutable audit snapshot | Native policy delivery preserves the original full payload after a new policy, maturation and replay. Existing integration persists eligible evidence/identity/capability history. The snapshot digest rejects edited payloads before measuring outcomes. |
| 3/7/14-day maturity | Native policy delivery advances controlled availability through all three horizons and checks pending versus matured states, precision and observed sample counts. |
| Separate action delays | The same delivery exercises immediate, two-day and ten-day action windows. A delay at or beyond its horizon stays not actionable; it does not receive a fabricated success result. |
| New policy cannot rewrite old decisions | A stricter policy creates a distinct persisted decision and changes selection. Repeating the old maturation retains its result and its original two-sample requirement; new snapshots retain the three-sample version. |
| Missing/deleted evidence and recovery | Native integration retries missing outcomes through frozen grace, then finishes censored; invalidation rejects further maturation. `tests/test_forward_claims.py` preserves workspace/lease boundaries and skips locked recovery rows while other work completes. |
| Operator distinguishes result kinds/workspaces | `tests/test_evaluation_delivery.py` opens actual Streamlit and selects forward, outcome and backtest details. Each displayed full payload matches its stored API result; the live workspace remains empty. |

The public policy plumbing reuses bounded analytical preparation and existing
snapshot serialization. Outcome configuration and ranking policy retain their
separate meanings; maturation does not consult current ranking defaults. No new
policy registry, fitting engine or mutable decision store is introduced.

Exact-head CI, native test counts and complete deployment/isolated-recovery
evidence are recorded on the acceptance PR before merge. The original PRD and
specification parents remain open, as do source/provider and capacity gates.
