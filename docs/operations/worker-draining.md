# Acknowledged collector draining

The collection daemon registers a unique worker identity and a generation in
PostgreSQL before accepting work. The existing task queue, request budgets and
publication lease tokens remain authoritative. A short worker admission lock is
held only while claiming a task, never across HTTP or object-store operations.

Inspect and request a drain through the non-interactive commands:

```sh
sm-trends worker-status --worker-id WORKER
sm-trends worker-drain --worker-id WORKER --generation GENERATION
sm-trends worker-drain --worker-id WORKER --generation GENERATION --timeout-seconds 5 --wait-seconds 30
```

`worker-drain` commits a barrier against subsequent claims. Its `draining` result
does **not** mean an accepted source operation has finished. Only the owning
operation loop acknowledges the settled boundary by entering `drained`. Retirement
requires that state, a non-null `drain_acknowledged_at`, and zero `owned_leases`.
An already stopped process can instead supply `state=stopped`, a non-null
`stopped_at`, the same expected generation, and zero owned leases.
Expired and unexpired owned leases are also reported; expired work is not silently
treated as completed. Existing dataset commit checks continue rejecting obsolete
task owners. Drain and resume validate the expected generation under the same
admission lock: a delayed command for generation 1 cannot affect generation 2.

The admission-lock deadline defaults to five seconds. The optional acknowledgement
wait is separately bounded. A timeout returns `ok=false`, `status=blocked`, a reason
and the latest worker/lease diagnostics, and exits unsuccessfully. If SQL prevents
even a bounded diagnostic read, `worker` is null. Neither timeout terminates the
accepted operation or authorizes retirement. The release controller must retain the
predecessor and decide whether to continue waiting or cancel the drain explicitly.

A remotely drained daemon stays alive and updates its heartbeat. PostgreSQL also
retains the requested pause through graceful or unclean process restarts, so
Compose's restart policy cannot reopen admission. A compatible predecessor can be
resumed if its replacement fails:

```sh
sm-trends worker-resume --worker-id WORKER --generation GENERATION
```

Resume can cancel a pending drain or release an acknowledged drain, and repeating
it on the same running generation is idempotent. Stopped or obsolete generations
cannot resume. A cleanly stopped identity can be registered immediately when it
has no unexpired leases. An unclean fixed blue/green identity requires both a
heartbeat older than 30 seconds and zero unexpired leases before a replacement
obtains a new generation. Earlier restart attempts fail safely and can be retried.
Idle and backpressure waits refresh the heartbeat at least every ten seconds,
including when the configured polling interval is sixty seconds.
Expired tasks remain in the existing queue for fenced reclamation; they are not
deleted or reported as complete. Old generations cannot claim, renew an expired
lease, or overwrite a superseding task publication.

The durable `drain_requested_at` field is cleared only by explicit resume.
Re-registration preserves it and starts a paused replacement in `draining`, even
if the predecessor had already stopped. The replacement must acknowledge its own
settled boundary; an acknowledgement from the old generation is not reused. An
expired task still owned by this identity can delay acknowledgement until another
worker recovers it, or the operator explicitly resumes the current generation to
recover it. Retain registry rows as generation tombstones; deleting them and
reusing their identities would weaken stale-process fencing.

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
cancellation, bounded row/table-lock contention, and actual Rust/HTTP collection
in a signaled process. It also restarts a remotely drained process without
reopening admission, recovers a killed fixed worker after lease expiry, and
verifies the pending tasks and immutable objects. This establishes draining
behavior, not the later raw-first capture or whole-release continuity acceptance
in #80.
