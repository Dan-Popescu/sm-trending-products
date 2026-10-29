# Controlled research report measurements

`scripts/research-workload.py` measures the existing public trend, decision brief,
content and metadata selection reports, plus the new change and progression
reports. It extends the controlled measurement
workflow in [benchmarking.md](benchmarking.md): it imports the existing offline
runner's URL scoping, hardware facts, process-session discovery, owned disk and
SQL/object summaries, atomic checkpoints and process-group cleanup. It reuses the
existing `costs.Store` contract and charge calculation. No service, permanent
dependency, collector build or deployed schedule is added.

Run from the repository with development dependencies already installed and an
explicit dedicated native PostgreSQL test database. Only loopback PostgreSQL URLs
whose database name contains `test` are admitted. `ACQUISITION_ENABLED` must be
false. The harness always creates its own random schema and loopback HTTP Moto
server; it does not accept an operator's object-store endpoint or existing state.

```sh
export TEST_DATABASE_URL=postgresql://test_user:test_password@127.0.0.1:5432/pipeline_test
PYTHONPATH=src uv run --locked --extra dev python scripts/research-workload.py \
  --output /tmp/research-workload/combined.json --case combined \
  --products 2 --entities 2 --samples 8 --texts 2 --text-characters 128 \
  --panel-size 2 --timeout 120 --max-disk-mib 256
```

This default is 40 synthetic source rows, four supplied texts, 512 text characters
and four offers with two observations each. Nothing is collected from a source;
no paid analyzer or commerce provider is invoked. An exact 2099 cutoff keeps
fixture availability independent of the machine's current date. The date is a
synthetic logical clock, not historical source access.

For per-report attribution, run each case sequentially. Each invocation owns a
fresh process, schema and Moto service. A combined case intentionally measures
shared work within one report set and must not be derived by subtracting counters
from concurrent runs.

```sh
for report in trend brief content selection combined; do
  PYTHONPATH=src uv run --locked --extra dev python scripts/research-workload.py \
    --case "$report" --output "/tmp/research-workload/$report.json" \
    --products 2 --entities 1 --samples 3 --texts 2 --text-characters 96 \
    --panel-size 2 --timeout 60 --max-disk-mib 64 || break
done
```

A larger campaign is explicit and still bounded. Inspect its report before
launching another case; do not run large campaigns in CI or on a nearly full
workspace. Temporary state uses `TMPDIR` (normally `/tmp`), so an output directory
on a small workspace does not move the scratch state there.

```sh
PYTHONPATH=src uv run --locked --extra dev python scripts/research-workload.py \
  --case combined --shape revisions --products 5 --entities 5 --samples 25 \
  --panel-size 10 --texts 5 --text-characters 1600 --batch-rows 200 \
  --timeout 180 --max-disk-mib 256 \
  --output /tmp/research-workload/revisions.json
```

The admitted maximum is 10,000 source rows, 100 import batches, 10 products,
20 video entities and 20 supplied texts per product, 169 samples per entity,
20 offers per product, 16,000 characters per text and 320,000 total supplied text
characters. `regular` spaces the complete history across seven days, `skewed`
concentrates growth in one entity without changing row count, and `revisions`
adds one backdated corrected sample per video. `--batch-rows` controls source
object shape. `--max-rows` and `--text-limit` remain actual public report budgets;
a smaller budget is recorded as a refusal, never an equivalent faster result.

Each worker has a separate setup cache. Cold execution starts with an absent
application file cache; warm execution opens a fresh application instance over
the cold cache. Neither phase clears PostgreSQL, OS or Moto caches. Setup includes
schema/module initialization, import, reviewed links and supplied text publication.
Execution includes application construction, current guard checks, full reports,
complete result hashes, public IDs and encoded result size. Application close and
usage flush occur outside phase times and within the supervisor total. Fresh
worker lifetime CPU includes imports and close/flush overhead separately; reaped
child CPU has its own scope and excludes PostgreSQL/Moto. Both cold
and warm results must have identical complete hashes and IDs for success. Content
has no public report ID, so its full-output hash is retained explicitly.

Brief cases exercise observed trend/competition/content panels and honest missing
optional cohort, matching and economic panels. They do not claim to benchmark
populated dossiers or every optional module. The new change/progression cases use the same small concrete `reports` dispatcher;
see the explicit profiles below. There is no generic execution or callback framework.

The supervisor stops at the declared deadline (at most 180 seconds), the sampled
owned disk limit (1..512 MiB), less than 512 MiB filesystem free space, or less than
256 MiB available cgroup/host headroom when that measurement exists. Every object
write also admits cumulative encoded upload bytes before sending it. Disk includes
owned files and schema relations, with a final check; SQL WAL, SQL temporary files,
logs and Moto RAM remain outside that disk measurement. Sampling can overshoot
a disk stop between checkpoints. Cleanup kills only the owned process group,
stops its Moto service, drops only its random schema and removes only its scratch
directory; SQL cleanup has a five-second statement timeout. Cleanup errors fail
the case and remain in the report.

Wall time and Python process CPU are actual measurements. Worker CPU excludes
PostgreSQL and supervisor/Moto; the supervisor records its own CPU separately.
Endpoint RSS and process lifetime peak RSS include imports/setup and are not a
phase peak. Session RSS is sampled at 250 ms, can miss short peaks, and is null
when `/proc` descendant links are unavailable. Discovered RSS is only a lower
bound in that case. Host/VPS and complete process-tree peaks remain null.

Completed and normally refused phases retain their actual request/byte counters.
After forced termination, `checkpointed_usage` is a known lower bound and `usage`
is null for the unaccounted in-flight work; missing work is not reported as zero.
Logical request counts and actual HTTP attempts are separate. No provider invoice,
CPU price, monthly retention duration, production capacity or VPS sizing guarantee
is inferred from these local synthetic runs.

`--prices FILE` optionally accepts a bounded JSON `costs.Store` object (at most
64 KiB), including explicit currency, region, source URL and observation date.
The reader admits a regular file descriptor before its capped read; FIFOs and
devices are refused without waiting for a writer. It applies the existing charge rules separately to measured request and download
quantities, without assuming account free-tier availability. Storage duration
stays unknown; `production_cost` remains null. Price application never modifies
`usage`. Retry billing and account-wide usage still require provider evidence.
For monthly planning, use the broader [cost scenario tools](costs.md) with explicit
coefficients rather than multiplying a tiny report's time into a VPS estimate.

The normal pytest suite includes only small native behavior cases. It verifies
scratch isolation, source/cloud refusal, actual output identity, cold/warm
accounting, explicit row-budget refusal, deadline cleanup, unknown measurements
and price/usage separation. Runtime throughput has no machine-dependent CI gate.

A [14-row existing-report baseline](../verification/research-workload.md) records
a successful exact-revision run and an earlier memory-stopped attempt. Both are
local synthetic evidence, with their actual resource limits and cleanup retained.

The internal worker also validates scratch ownership and loopback test endpoints
before constructing application services. Before initialization, a read-only
namespace dependency check requires an existing, completely empty bootstrap
schema. A reserved schema name and matching bucket alone cannot authorize writes
to populated state. Direct worker entry therefore refuses both foreign schemas
and populated reserved schemas before core DDL or bucket initialization; this
boundary does not depend on the supervisor being the caller.

## Explicit new-report cases

`--case changes` measures `Pipeline.decision_changes().compare`; `--case progression`
measures `Pipeline.content_progression().describe`. `--case research` measures both
in one invocation. These concrete adapters retain the public module's complete
output; they do not create an alternative execution framework. The original
`combined` case continues to measure its four existing report interfaces.

When new-report case dimensions are omitted, the bounded fixture defaults to one
product, eight videos with eight samples each, four offer rows and eight supplied
texts (68 source rows). Video points are exactly 24 hours apart over seven days,
with stable known creators, publication ages, explicit public-video distribution,
source and market. Texts are known at each video's first point. Four English
human transcripts contain the reviewed `time` phrase, and four contain no theme
mention. Each video has one text; both arms have four known creator identifiers,
identical 24-hour measured duration, full coverage and aligned endpoints. These
synthetic identifiers do not establish independent people.

The selected U02 cutoffs are 24 hours apart. Its continuing 24-hour panel is
complete, with twice the declared growth in the last interval; other requested
kernel windows retain their actual comparability status. U03 uses a 24-hour
window and reaches `observed_association` with actual candidate and absent arms.
An empty or unavailable report cannot become a successful measurement: the
harness records its complete identity and actual work, then reports a refusal
before attempting a warm phase. Explicit sparse shapes therefore remain useful
failure measurements, without being advertised as faster equivalent work.

```sh
for report in changes progression research; do
  PYTHONPATH=src uv run --locked --extra dev python scripts/research-workload.py \
    --case "$report" --timeout 90 --max-disk-mib 64 \
    --output "/tmp/research-workload/new-$report.json" || break
done
```

Cold and warm phases now compare complete canonical bytes as well as hashes and
public IDs. `reported_work` retains the authoritative comparison windows and
association group counts, durations, coverage, contexts and reasons for checking
what the measured operation actually reached. `sql_execute_calls` counts the
isolated worker's `psycopg.Connection.execute` calls during each measured phase,
including its own pool threads. It is not a server statement/CPU count and excludes
close/usage flush outside the phase. Storage counters, actual CPU, endpoint RSS,
lifetime RSS and unknown host/process-tree peaks retain their separate scopes.

Both supervisor and worker validate effective libpq host, host address and database
name, including URI query overrides and inherited `PGHOSTADDR`. Active service
indirection is refused. A loopback-looking URL cannot override its actual
connection to a remote host or a non-test database. These guards affect only this
research harness, not the existing offline collector benchmark's operator URL
contract.

[Measured new-report cases](../verification/research-workload-new-reports.md)
retain separate successful change/progression/combined runs, a distinct skewed
fixture and an actual sparse refusal with its consumed work. These measurements
include actual SQL/CPU/RSS/storage scope and complete cold/warm byte identity.
