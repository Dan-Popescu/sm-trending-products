# Comparing 15-minute and hourly assessments

The default is now **one assessment per hour** (`ASSESSMENT_SCHEDULE_MINUTES=60`),
approved in [#117](https://github.com/Dan-Popescu/sm-trending-products/issues/117).
The goal is to identify commerce opportunities over multiple days, rather than
react to every brief intraday change. One hour is an initial operating choice,
not a proven optimum. Two- or three-hour intervals can be evaluated later; they
are not enabled here, and the current minute-based setting supports at most 60.

Collection, normalization and assessment remain separate operations. Changing only
`ASSESSMENT_SCHEDULE_MINUTES` changes when rankings and alert checks run; it does
not change collection settings or remove captured source observations.
The dashboard shows the latest published SQL assessment, not a score calculated
afresh every time the page opens.

This reduces nominal triggers from 96 to 24 per day. It changes no score formula,
lookback or alert threshold. An explicit value of 15 remains supported; existing
deployment overrides must be changed deliberately to adopt 60. Reload the
analytics code-server deployment after changing the environment. The persisted
Dagster schedule identity remains `opportunity_rankings_schedule`.

## A reproducible transient signal

The [pure behavioral test](../../tests/test_assessment_cadence.py) uses the real
assessment engine and eight synthetic cumulative view measurements. All times
are UTC on 2026-10-25; products and their confirmed video links exist at 07:00.
Each product has one video. Creator and publication metadata are unknown. This
is a controlled example, not a production observation or a capacity benchmark.

| Measurement time | Product A views | Product B views | Product C views |
| --- | ---: | ---: | ---: |
| 08:00 | 100 | 100 | 100 |
| 09:00 | 200 | 300 | 400 |
| 10:08 | 1,200 | — | — |
| 10:37 | — | 3,000 | — |

A dash means no new measurement. Each measurement becomes usable at its listed
time. Both schedule comparisons receive the identical complete observation set;
the engine excludes facts not yet available at each scoring time. The default
policy uses the attention score and an alert threshold of **70**.
Growth rates use the intervals between source captures, not the interval between
rankings. Hourly ranking does not turn a 15-minute capture interval into one hour
or erase the measurements between rankings.

| Time | Verified A score | A rank | 15-minute assessment | Hourly assessment |
| --- | ---: | ---: | --- | --- |
| 10:00 | 35.9 | 3 | Below threshold | Below threshold |
| 10:15 | 86.0 | 1 | Above threshold | No scheduled assessment |
| 10:30 | 86.0 | 1 | Above threshold | No scheduled assessment |
| 10:45 | 63.8 | 2 | Below threshold | No scheduled assessment |
| 11:00 | 63.8 | 2 | Below threshold | Below threshold |

B's new measurements lower A's relative score even though A's own recorded
views remain 1,200. The alert rule checks the score, not a fixed top-N rank.
With 15-minute assessments, A can create an alert at 10:15 if no recent alert
blocks it. The default 24-hour cooldown would prevent another immediate alert
on A. With hourly assessments, A never meets the threshold at an executed tick.
The test proves scoring and threshold opportunities; it does not write actual
SQL alerts, exercise a UI or send notifications.

Run without PostgreSQL, S3 or live access from an installed project environment:

```sh
POLARS_MAX_THREADS=2 PYTHONPATH=src python -m pytest -q tests/test_assessment_cadence.py
```

## Delay, preservation and separate evaluation jobs

For a signal that stays above threshold until the next successful assessment,
the theoretical scheduling wait changes from **at most 15 to at most 60 minutes**.
The worst-case scheduling bound increases by 45 minutes. Mean waits are **7.5
and 30 minutes** only when usable signals arrive uniformly across each interval.
These figures assume aligned regular schedules, negligible computation and no
queueing or failures; capture, normalization and processing delays add to them.
A transient signal like the example can instead be missed, rather than delayed.

Hourly scheduling means 24 nominal triggers per day instead of 96, or **75%
fewer triggers**. It does not establish a 75% CPU saving. Existing product-feature
caches already reuse valid calculations; time, freshness, window expiry and
global normalization still matter when deciding which assessments can be skipped.

Both schedules retain the same source facts in this example. Later historical
analysis or ML can use retained facts. Replaying 10:15 requires the evidence,
product links, policies and eligibility history known at that time; later
corrections must not silently become earlier knowledge. Replaying a missed
opportunity does not create an alert that actually existed at 10:15 or a genuine
prospective decision. Live forward decisions cannot simply be backdated.

The existing forward-test job independently freezes decisions at 00:00, 06:00,
12:00 and 18:00 UTC. Outcome checks run at 00:30, 06:30, 12:30 and 18:30; an
outcome becomes complete only when its configured evidence window permits it.
These jobs retain their own schedules if only the assessment interval changes.
Freezing computes its own eligible point-in-time assessment, rather than copying
the dashboard's last published result. No forward-test decision is automatically
added for the transient 10:15 signal.

See [historical storage](history-storage.md) for cadence history, exact cached
features and preservation constraints. Production alert requirements and measured
CPU cost must inform any later schedule change.
