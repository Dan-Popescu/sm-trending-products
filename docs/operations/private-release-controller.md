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

GitHub publishes a signed candidate after successful main-branch CI. Repository
visibility and GHCR package visibility are separate: make this public-code image
package public once if the host should pull without registry credentials. A
private package instead needs a read-only registry credential in a private Docker
configuration readable by the host controller. The application image contains no
production environment files or credentials. Never put a registry token in the
Compose template or GitHub release assets.

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

The service places cosign's TUF trust cache under its private writable state
directory and initializes it during installation. This is required because
`ProtectHome=true` hides cosign's default home-directory cache. The cache path
uses cosign's supported `TUF_ROOT` setting; it does not change the trusted
workflow identity or disable signature verification. See
[cosign's TUF configuration](https://github.com/sigstore/cosign/blob/v3.1.4/pkg/cosign/tuf.go).

## Request and monitor an update

For live analytical CPU changes, use the [hot CPU mode commands](hot-cpu-mode.md)
after upgrading this independently trusted controller. Their private state shares
the release lock and persists CPU pins for later processing creation. An
interrupted CPU transition must recover before release mutations proceed.

Run the main release workflow with the desired full main revision and `deploy`
enabled. CI requires successful checks for that exact revision, publishes its
image by digest, signs the manifest, then signs a pilot deployment request.
The host polls `pilot-deploy-request` assets every 30–35 seconds and verifies the
exact main workflow certificate identity. The signed sequence must increase;
changing a request at an accepted sequence or replaying an older sequence blocks.
Request and bundle replacement is not atomic, so a partial upload may transiently
block verification. The next poll retries while acquisition keeps running.
Accepted bundles are retained and verified again on every mutation or resume.
Root-private intent seals and a highest-request record protect deployment
authority from application database writes. Preserve this private controller
state across updates and backups; missing or mismatching authority blocks safely.

The controller starts the replacement, waits for fresh durable raw captures from
its current worker generation, drains the predecessor at its acknowledged task
boundary, commits the active pointer, then retires the old slot. No naturally due
work means it waits. An existing or newly found coverage problem blocks rather
than being treated as a healthy heartbeat. A blocked handover resumes from the
same journal. Accepted work resumes without downloading its GitHub assets again;
signature validation still requires available trusted Sigstore material, including
the initialized TUF cache. A newer request waits for the accepted handover to finish.

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
If SQL committed a transfer before its private checkpoint, rollback first
requalifies and completes that approved transfer, then reverses it while keeping
the replacement collecting. This private intent survives another controller
crash and takes priority over blocked processing. The unchanged earlier GitHub
request cannot undo a completed rollback; a newer signed request can supersede it.

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

Back up the private controller configuration, accepted-request counter,
authorization seals, processing journal and CPU policy/recovery journal separately
through a trusted encrypted host-admin backup. The application PostgreSQL/S3 checkpoint deliberately cannot
read these root-only files. After disaster recovery, keep the deployment timer
disabled until the restored SQL, actual containers and private controller state
are reconciled; an older SQL dump must not silently reset deployment authority.

## Additive capture schema expansions

The current bridge image supports schemas 35 through 40. Migration 036 adds only
the indexed active-task recovery frontier; the durable receipt and task remain
authoritative. The same recovery query works before the index exists, but bounded
indexed execution requires expansion. It is not a second queue or evidence store.

For an existing schema-35 host, first qualify and finish the normal signed
handover to a bridge image declaring support for 35–36, while the database stays
at 35. The current 35–40 image can bridge these expansions; the earlier 35–36
recovery image can bridge migration 036 only. Move every retained
acquisition/processing role to a compatible image before applying the additive
migrations through the trusted maintenance procedure.
The release controller does not execute migrations. Index creation briefly
requires database locks; measure its duration and source-deadline headroom on the
target host before expansion. The controlled fixture is not a deployment proof.

A previously signed 35-only manifest stays incompatible with schema 36. Do not
edit its signed bytes or relax the host's schema checks. After expansion, rollback
must use an already qualified image whose declared range includes 36; keep that
bridge available. A fresh installation can initialize the current schema directly.

Migration 037 adds the reconstructible pending-normalization projection, dataset
maintenance hooks, a committed-child lookup index and the atomic lease-takeover
guard. Before expanding to 37, finish handover of every retained role to a signed
35–37 image while still on 35 or 36. On those older schemas normalization keeps
its existing query; it discovers the new projection after expansion without a
restart. Actual acquisition/recovery also works on 35 without the new task index.

The migration's exclusive dataset-table lock drains row-locking publication
claimers before lease-trigger DDL and backfill. Ordinary dataset reads and source
HTTP/local spooling remain available; remote dataset staging/commit and new
publisher claims wait. Index creation and one-time history backfill must fit the
configured transaction/statement/lock timeouts and source deadline headroom.
The controlled 500,000-parent experiment is not a production migration window.
After expansion, only rollback images declaring support for 37 are admissible;
neither an older 35-only nor 35–36 signature becomes compatible automatically.

Migration 038 is a reader-format boundary for shared compressed analytical
archives. It does not rewrite old objects or add a second publication registry.
First complete the normal signed handover of every retained acquisition and
processing role to an image declaring support through 38, while SQL remains at
35, 36 or 37. Bridge binaries continue plain assessment writes before 38. Apply
038 through trusted maintenance only after handover; subsequent assessments
discover the gate and write the new representation without a restart. Earlier
plain archives stay readable and unchanged. Rollback after expansion requires a
qualified image supporting 38; a previous 35–37 image cannot read the new shared
format. The controller neither runs migrations nor bypasses signed ranges.

Migration 039 follows 038 and permits an explicitly verified absence of a source
expiry cap. First complete signed handover of every retained role to an image
supporting 39 while the database remains on a supported earlier version. Only
then apply 039 through trusted maintenance and attest an unlimited source
policy. The bridge rejects unlimited policy writes below 39; all existing finite
policies retain their values. After expansion, rollback requires a qualified
image supporting 39 even if no unlimited policy has yet been configured. Older
code assumes an integer cap and cannot safely read NULL policies. Read the
[source retention procedure](source-retention.md) before changing any policy.

Migration 040 is a packed-archive reader-format boundary after 039, without a
second publication registry or source protocol change. Complete the signed
handover of every retained role to an image supporting 40 while the database
remains on a supported earlier version, then apply 040 through trusted
maintenance. Bridge images write plain assessments on 35–37 and ordinary shared
assessments on 38–39. On 40, the archive may select packed shared blocks only
when both new stored bytes and PUT count are no greater than the ordinary plan.
All previous formats remain readable. Source collection and original objects
are unchanged. After expansion, rollback requires a qualified image declaring
support for 40; a prior 35–39 signature cannot read the packed representation.

Each of the two disposable pre-upload plans is limited to 4,000,000 charged
bytes and 1,000 nodes, counting bodies, descriptors and dependencies, including
reused nodes. The temporary location may be RAM-backed `/tmp`; this is not disk
offload. On a limit, both plans close and discard metadata before the existing
bounded ordinary writer uploads. This bounds retained planning data, not the
complete assessment result or total process RAM. See the
[qualified measurements and read tradeoff](../verification/adaptive-assessment-archive.md).
