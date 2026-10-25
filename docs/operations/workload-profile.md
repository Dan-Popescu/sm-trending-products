# Explicit workload and planning profile

Before source access or deployment, compare a declared video workload with the
configured planner admission limit:

```sh
sm-trends capacity --products 10000 --videos-per-product 20 --refreshes-per-day 4
```

This command needs no database or credentials. The example assumes distinct
videos and fully grouped requests of twenty compatible Display IDs. It reports
800,000 observations/day, at least 40,000 refresh requests/day and 72 million
observations over a declared 90-day history. Shared videos reduce demand;
account/tier fragmentation, discovery, pagination and retries increase it.
Use `--videos-per-request` to declare lower grouping and
`--source-requests-per-day` for an actual approved request budget.

The deployment now admits up to `SCHEDULER_REQUEST_BUDGET=500` new physical tasks
every `COLLECTION_SCHEDULE_MINUTES=5`, independently of
`ASSESSMENT_SCHEDULE_MINUTES=15`. This is an upper planning bound of 144,000
tasks/day, not a source grant, measured throughput or a sizing certificate.
Intervals must divide an hour. Candidate scan limits, queue backpressure,
available accounts and protected exploration still constrain actual scheduling.

The [historical storage guide](history-storage.md) explains the inherited
15-minute assessment default, the distinction between a new current assessment
and a historical replay, and the constraints on future S3 cold storage.
Rust continues to reserve every real HTTP attempt against authoritative quotas.
Increasing planning admission does not bypass those controls.

The collection Dagster job plans and collects bounded work. Analytical publication
runs as a separate job so a small collection batch does not itself trigger a
full-universe rebuild. The independent long-lived worker drains the queue;
without that service, the small Dagster observation batch alone may not drain
the configured planning offer. The existing global Dagster concurrency cap remains
one to preserve the VPS resource bound; measure job duration and queue age before
changing it. Later exact incremental feature projections reduce analytical work.

For every target profile, record desired and achieved refresh intervals, quota
waits, oldest queue age, worker capacity and analytical duration. Price measured
CPU, memory, SQL/index/WAL/backup storage, object requests/bytes and egress using
the actual provider's dated rates. This arithmetic does not infer permissions or
capacity from a successful configuration.
