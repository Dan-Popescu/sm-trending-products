# Operate signed pilot updates without SSH

The host-local controller polls GitHub over outbound HTTPS. It exposes no HTTP
listener and does not require SSH, a public Docker endpoint or a GitHub runner on
the VPS. Tailscale continues to protect application access. Docker privilege stays
in this independent host process; ordinary application images have no socket.

Installation is a one-time Hostinger root-console operation. This public
repository contains placeholders only. Keep the VPS address, tailnet identifiers,
bucket names, account identifiers and every credential in private host files.

## Install the trusted controller

First install Docker Engine with Compose, Python 3.12+, and the pinned cosign
version used by the release workflow. Verify the binary checksum from the
official release before installing `/usr/local/bin/cosign`. The independent host
controller package must come from an explicitly reviewed revision of main. Do
not install it from the downloaded application image or from a pull request.
Use the repository's locked dependencies in a root-owned virtual environment at
`/opt/sm-trends/controller`, and retain that trusted source separately from
`/opt/sm-trends/runtime`. Application publication never updates this controller.

For example, from the root-owned, reviewed controller checkout with `uv` installed:

```bash
UV_PROJECT_ENVIRONMENT=/opt/sm-trends/controller uv sync --locked --no-dev --no-editable
```

Prepare the runtime Compose configuration at `/opt/sm-trends/runtime/compose.yaml`.
All source files used by Compose, including bind-mounted files, must be owned by
root and unwritable by application users. The blue/green capture slots must have
the expected worker identity labels; schema expansion and platform initialization
must already be complete. The controller runs no migration or arbitrary script.
Both old and new image descriptors must support the actual SQL schema version.

Configure the actual Docker data root and shared spool volume mountpoint in the
private configuration. Measured host admission runs before image pull and new
candidate start. It checks effective RAM/CPU ceilings, existing running container
limits, image/unpack disk headroom and spool growth reserve. Missing or
insufficient measurements block the update without deleting spool data. The
example budgets must be confirmed against measured image size and the pilot's
capture envelope; they are not host qualification evidence.
The explicit 4-vCPU pilot profile budgets a 768-MiB, 0.75-CPU candidate and
0.25 CPU reserve. It depends on the matching production Compose resource profile;
unmodified larger limits on other roles may conservatively block overlap. The
capacity module's default budget remains more conservative.

For the 4-vCPU/16-GiB starting profile, append
[`deploy/production-4cpu.env.example`](../../deploy/production-4cpu.env.example)
to the private runtime environment before supplying credentials. Its running
role CPU ceilings total 3 CPUs; one 0.75-CPU replacement and the configured
0.25-CPU reserve fit within four. Running memory ceilings total 7.25 GiB before
the replacement. Actual available memory and disk must still pass admission;
these configured limits do not certify throughput or recovery windows. Other
applications on the VPS count toward admission and can block overlap.

Create `/etc/sm-trends` as root, mode 0700. Copy
`deploy/controller/release-controller.example.json` to
`/etc/sm-trends/release-controller.json`, substitute the repository/trust settings,
and use mode 0600. Create two additional mode-0600 files:

- `runtime.env`: private Compose runtime settings, including S3 and application
  database credentials. It must select the actual Compose project and initial
  digest/worker identity used below.
- `controller-database-url`: one PostgreSQL URL reachable from the host, normally
  `postgresql://USER:PASSWORD@127.0.0.1:PORT/DATABASE`. PostgreSQL must bind that
  host port only to loopback. This differs from the container-internal URL using
  the `postgres` service name. Do not place the URL in command arguments.

The installer and CLI refuse writable trust files, symbolic link inputs, missing
verification executables and invalid private-file permissions. There is no
production unsigned mode. Download URLs are fixed to the configured public
GitHub repository; release metadata cannot provide commands or host paths.

## Adopt the initial running release

Publish the first compatible main revision using the release workflow, with
deployment disabled. Bring up the initial platform and one capture slot using
that immutable signed image and a unique configured worker identity. Keep
initialization separate from rolling updates: no earlier compatible production
slot exists during the first installation. Source credentials, an approved
sampling contract and live-source acceptance are still required for real data.

Adopt the initial slot, substituting its full published revision and worker ID:

```bash
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli bootstrap \
  --config /etc/sm-trends/release-controller.json \
  --revision FULL_40_CHARACTER_MAIN_REVISION --slot blue --worker-id INITIAL_WORKER_ID
bash deploy/controller/install.sh
```

Bootstrap verifies the signed image descriptor, actual container image/identity,
SQL compatibility and registered worker. It records an active baseline; it does
not assert capture coverage. The installer arms polling only after adoption.

## Request and monitor an update

Run the main release workflow with the desired full main revision and `deploy`
enabled. CI requires successful checks for that exact revision, publishes its
image by digest, signs the manifest, then signs a pilot deployment request.
The host polls `pilot-deploy-request` assets every 30–35 seconds and verifies the
exact main workflow certificate identity. The signed sequence must increase;
changing a request at an accepted sequence or replaying an older sequence blocks.
Request and bundle replacement is not atomic, so a partial upload may transiently
block verification. The next poll retries while acquisition keeps running.

The controller starts the replacement, waits for fresh durable raw captures from
its current worker generation, drains the predecessor at its acknowledged task
boundary, commits the active pointer, then retires the old slot. No naturally due
work means it waits. An existing or newly found coverage problem blocks rather
than being treated as a healthy heartbeat. A blocked handover resumes from the
same journal; even an outbound network failure cannot prevent resuming an already
accepted request. A newer request waits for the accepted handover to finish.

Inspect status or trigger an immediate poll from the Hostinger console:

```bash
systemctl start sm-trends-release.service
journalctl -u sm-trends-release.service --since '15 minutes ago'
/opt/sm-trends/controller/bin/python -m sm_trending_products.release_cli status \
  --config /etc/sm-trends/release-controller.json
```

The CLI emits one JSON object. Exit 0 means success; exit 2 means explicitly
blocked. The timer treats a blocked result as an expected retry and retains its
reason in the journal. `status --release-id ID` shows detailed handover progress.
`apply --release-id ID` resumes a known journal without fetching a new request.
`rollback --release-id ID` cancels an unfinished candidate or plans a reverse
handover after commit; apply the returned reverse release ID to run it. The
reverse handover also requires fresh generation-bound capture evidence.

Do not run blanket `docker compose down`, rebuild all services, delete volumes,
restore a database snapshot, or use an ordinary `compose up` that forgets current
slot image/worker settings. The controller atomically writes private
`/var/lib/sm-trends/release/slots.env` before starting a slot. For deliberate
manual Compose operations, load it as a second `--env-file` after `runtime.env`;
it contains image and worker pins only. Docker restarts retain the configured container
identity. Plan host-controller and runtime-configuration upgrades separately;
this application puller intentionally cannot replace its own trusted code.

The offline tests cover native PostgreSQL, bounded local HTTP downloads, actual
CLI subprocesses and explicit Docker/verifier command fixtures. They do not
demonstrate cryptographic publication, actual VPS rollout or TikTok continuity;
those require their separate qualification evidence.
