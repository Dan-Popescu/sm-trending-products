# Irrecoverable observation continuity delivery

The owner approved autonomous implementation, delegation, review and merge on
2026-10-24. [Specification #80](https://github.com/Dan-Popescu/sm-trending-products/issues/80)
defines planned-update continuity; it does not claim all upstream data is visible
or recoverable. Start revision: `ab3bba9`.

| Delivery | Issue | Engineering blockers |
| --- | --- | --- |
| Source recovery contracts and capture obligations | [#82](https://github.com/Dan-Popescu/sm-trending-products/issues/82) | None |
| Generation-fenced worker draining | [#83](https://github.com/Dan-Popescu/sm-trending-products/issues/83) | None |
| Durable raw-first capture and source-free publication | [#84](https://github.com/Dan-Popescu/sm-trending-products/issues/84) | None |
| Independent acquisition runtime | [#85](https://github.com/Dan-Popescu/sm-trending-products/issues/85) | #82, #83, #84 |
| Coherent online backup | [#86](https://github.com/Dan-Popescu/sm-trending-products/issues/86) | #84, #85 |
| Resumable compatible release and rollback | [#87](https://github.com/Dan-Popescu/sm-trending-products/issues/87) | #82, #83, #84, #85 |
| Trusted image publication and private deployment | [#88](https://github.com/Dan-Popescu/sm-trending-products/issues/88) | #87 |
| Adversarial planned-update qualification | [#89](https://github.com/Dan-Popescu/sm-trending-products/issues/89) | #82–#88 |
| Actual Hostinger pilot/source acceptance | [#90](https://github.com/Dan-Popescu/sm-trending-products/issues/90) | #89; actual infrastructure and source access |

Parallel implementation occurs in isolated worktrees with explicit module
ownership. Independent reviews inspect immutable revisions; the integration owner
resolves shared contracts, re-runs meaningful integration checks and merges only
after exact-head CI. An individual slice's passing tests are not acceptance of the
whole continuity specification. GitHub issue state is authoritative.

The target is Hostinger KVM 4, four vCPU, 16 GB RAM and 200 GB NVMe, private
Tailscale access without SSH. No target address, production S3 identity or live
source approval is inferred from those hardware specifications. #90 remains a
separate environment-dependent acceptance gate.
