# Acknowledged collector draining

The collection daemon registers a unique worker identity and a generation in
PostgreSQL before accepting work. The existing task queue, request budgets and
publication lease tokens remain authoritative. A short worker admission lock is
held only while claiming a task, never across HTTP or object-store operations.

Inspect and request a drain through the non-interactive commands:

```sh
sm-trends worker-status --worker-id WORKER
sm-trends worker-drain --worker-id WORKER
```

`worker-drain` commits a barrier against subsequent claims. Its `draining` result
does **not** mean an accepted source operation has finished. Only the owning
operation loop acknowledges the settled boundary by entering `drained`. Retirement
requires that state, a non-null `drain_acknowledged_at`, and zero `owned_leases`.
Expired owned leases are also reported; they are not silently treated as completed
work. Existing dataset commit checks continue rejecting obsolete task owners.

A remotely drained daemon stays alive and updates its heartbeat. Exiting would
allow Compose's `unless-stopped` restart policy to re-admit it accidentally. A
compatible predecessor can be resumed if its replacement fails:

```sh
sm-trends worker-resume --worker-id WORKER --generation GENERATION
```

Resume can cancel a pending drain or release an acknowledged drain, and repeating
it on the same running generation is idempotent. Stopped or obsolete generations
cannot resume. A new process must use a fresh identity, or register a stopped
identity with no owned leases to obtain a new generation. Retain registry rows as
generation tombstones; deleting them and reusing their identities would weaken
stale-process fencing.

SIGTERM/SIGINT requests local stopping. The worker checks the signal before each
task in a batch, finishes the already accepted operation and then exits. It marks
itself `stopped` only when no leased task remains; an exceptional exit with
unfinished owned work remains `draining`, preventing a false clean-stop report.
Signal handlers are restored when the command returns.

The local stop callback is forwarded through the normal pipeline/collection
interface. Registered claims must carry their current generation and workspace;
unregistered existing callers retain their previous behavior. A release controller
must therefore require this registered protocol before claiming controlled
handover of older binaries. A heartbeat alone proves neither source coverage nor
successful capture. The process shutdown grace must still be long enough for
the configured HTTP and publication bounds; a timed-out drain must block a
release rather than authorize forced termination of an unprotected capture.

`tests/test_worker_draining.py` exercises native PostgreSQL admission races,
generation/workspace isolation, publication after a drain, pending-drain
cancellation and actual Rust/HTTP collection in a signaled process. It also keeps
a remotely drained process alive, resumes it and verifies the pending tasks and
immutable objects. This establishes draining behavior, not the later raw-first
capture or whole-release continuity acceptance in #80.
