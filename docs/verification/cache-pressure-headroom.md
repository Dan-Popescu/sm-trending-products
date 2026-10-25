# Amortize source-cache pressure with bounded optional reclamation

Scope: the pressure follow-up to prelaunch ticket #100. The initial
[accounting correction](cache-admission-accounting.md) avoids cache-wide walks
while there is room. A permanently full cache would still need one inventory per
miss; the additional policy addresses that normal massive-collection case.

## Policy and limits

Only when an admission genuinely requires eviction, the existing cache module
may reclaim additional source-class bytes: at most **1% of the configured cache
budget, capped at 32 MiB**. This is a headroom target with a strict extra-byte
bound. Required eviction retains its existing behavior, including large objects.
Optional eviction skips an object if its complete size exceeds the remaining
extra allowance; a later smaller eligible object can still be reclaimed.
Aliased inodes which would not release bytes are also skipped. Active pinned
inodes and an explicit keep-path remain protected. No optional reclamation runs
just because a conservative charge needed reconciliation or a limit was checked.

The source-before-window ordering remains authoritative. Once the actual
requested bytes fit, optional trimming stops before any recognized `window-*`
file. Current main already writes these names and promotes a legacy window when
it is read. Cold, unpromoted pre-PR78 window files have opaque names which cannot
be distinguished from source files without extra ownership lookup. They retain
their existing source-like eviction class; no blanket priority guarantee is
claimed for those unknown legacy files. Their verified read, promotion and
correct full-history fallback are unchanged. No naming migration or per-admission
SQL lookup is introduced.

This trades a bounded amount of source working space for fewer inventory walks.
It can increase source GETs when a hot working set nearly fills the budget. A
cache dominated by protected windows, pins, keep-paths, or source files larger
than the optional allowance may not reach the target; pressure can still require
repeated inventories. The hard byte budget and retention rules are unchanged.

## Actual pressure measurement and GET tradeoff

The [raw results](cache-pressure-headroom.json) use the same script against
baseline `ac5ddcb9e8713e311b6772528e48b0ee8ade9b33` and the candidate module
identified by SHA256. A four-CPU/16 GiB managed host is shared with other
controlled work. The fixture starts exactly full: an 8 MiB budget, 30,000 inert
source files plus one 64 KiB recognized window. It admits 256 new 1 KiB local
payloads through actual cache operations, including stripe locks and fsync.
Initial reconciliation and final inventory reporting are outside the timer.

| Measurement | Baseline | Bounded policy |
| --- | ---: | ---: |
| Complete 256-admission sequence | 54.241 s | 1.046 s |
| Process CPU for that sequence | 54.168 s | 1.044 s |
| Inventory walks | 256 | 4 |
| Median admission | 205.251 ms | 0.509 ms |
| p95 admission | 267.731 ms | 0.834 ms |
| Slowest admission | 361.529 ms | 233.641 ms |
| Mean admission, including pressure walks | 211.880 ms | 4.085 ms |
| Source bytes evicted | 262,154 B | 336,658 B |
| Original recognized window retained | yes | yes |

The slowest candidate admission still includes a full inventory. The mean and
complete-sequence values expose that work rather than hiding it behind a median.
This isolated pressure sequence is about 52 times faster; this is not an
end-to-end pipeline or cloud latency multiplier.

A separate near-fit fixture has a 1 MiB budget and 1,024 source files of 1 KiB.
Thirty-two are cold; the other 992 would fit alongside the new 32 KiB window.
Baseline removes only the 32 cold files and later needs **zero** GETs for hot
reuse. The bounded policy removes those 32 plus ten optional files and later
requires **ten extra GETs, 10,240 downloaded bytes**. The window remains present
and the final payload fits the same budget. These source reads use a memory
provider; no actual cloud round-trip time or provider bill is inferred.

Run both immutable checkouts using the same candidate script:

```sh
PYTHONPATH=/tmp/cache-admission-baseline/src python scripts/measure-cache-admission.py --pressure --file-counts 30000 --repetitions 256 --output output/cache-pressure-baseline.json
PYTHONPATH=src python scripts/measure-cache-admission.py --pressure --file-counts 30000 --repetitions 256 --output output/cache-pressure-candidate.json
```

Focused regressions cover amortized inventories, the exact near-fit GET/byte
tradeoff, strict extra-byte bounds, the 32 MiB cap using sparse logical files,
oversized-source skipping, recognized window protection, active pins, keep-paths
and aliases. Existing native feature-window/retirement/pressure tests continue
to verify analytical parity and correct fallback.
