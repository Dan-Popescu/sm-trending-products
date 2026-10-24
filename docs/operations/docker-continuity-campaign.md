# Controlled two-image Docker campaign

This opt-in campaign exercises the actual Compose release host, registered
workers, PostgreSQL journal, immutable S3 raw captures and Rust HTTP collector.
It runs on an isolated development Docker daemon with fixture credentials and
fresh project volumes. It does not access a VPS, Tailscale or production storage.

The fixture builder reuses an existing dependency-compatible runtime foundation,
copies the current **installed** Python package and compiled collector into one
overlay, then creates two distinctly labelled immutable images. Both variants use
the recorded runtime revision. These fixtures verify release mechanics; they are
not evidence of a production Dockerfile build or compatibility between arbitrary
historical application revisions. The production image and signed release bundle
still require their own CI build and trust verification.

Start an isolated local registry on unused loopback port 80, then build fixtures:

```sh
docker run --detach --name continuity-fixture-registry \
  --publish 127.0.0.1:80:5000 registry:2.8.3
python scripts/build-continuity-fixtures.py \
  --base-image sm-trending-products:validated-foundation \
  --output /tmp/continuity-fixture-images.json
```

The helper accepts only a `localhost/` repository, retains Docker registry
credentials unchanged and checks both CLIs inside the resulting image before
publishing to the fixture registry. Runtime source must be committed. The
collector must run with the runtime foundation's C library. The host Docker daemon
must support Compose, local registry pulls and `host-gateway` addressing. Allow
enough disk for two worker containers with the daemon's storage driver.
The builder requires at least 8 GiB free for VFS: repeated nested fixture
foundations copy the filesystem and can exhaust a small managed environment.
Reuse the original foundation, retire obsolete fixture generations, and avoid
running a disk-heavy native campaign concurrently on that daemon.
By default the helper extracts the collector from that foundation. Use
`--collector-binary` only when testing newly compiled acquisition code with an
otherwise unchanged dependency foundation.

Run the bounded, opt-in tests from the matching runtime checkout:

```sh
PYTHONPATH=src:tests \
CONTINUITY_DOCKER_MANIFEST=/tmp/continuity-fixture-images.json \
CONTINUITY_DOCKER_EVIDENCE_DIR=/tmp/continuity-evidence \
pytest -q tests/test_docker_release_campaign.py
```

CI calls `scripts/ci-continuity.sh` after the production image build. Its default
foundation is `sm-trending-products:local`; `CONTINUITY_BASE_IMAGE` can select
another already built runtime. The wrapper creates and cleans up its own registry
and writes JUnit results plus sanitized evidence under `output/continuity-*`.
Archive the JSON evidence and JUnit file, not the extracted collector executable.
For local validation with an older dependency foundation,
`CONTINUITY_COLLECTOR_BINARY` supplies the newly compiled collector directly.

The campaign checks these externally visible behaviors:

- After the controller is killed just after a real Compose candidate start, a
  fresh controller resumes the existing journal and requires fresh raw-capture
  proof from that candidate generation.
- An accepted one-shot HTTP response completes and becomes durable before its
  predecessor is retired. Both images remain alive while that operation is busy.
- A running candidate without capture proof leaves the predecessor admitting
  work. Rollback remains blocked while that candidate has an accepted one-shot
  operation; the predecessor continues collecting during that drain. The
  candidate's response becomes durable before it is stopped and the attempt is
  declared aborted.
- Compose restart retains a requested pause. Delayed drain or resume commands
  cannot affect the replacement generation, and only its explicit resume permits
  new source requests.

Each worker mounts a fixture-only loopback HTTP relay to the controlled source.
This keeps the collector's strict loopback-only synthetic endpoint policy intact.
The real CLI and Rust acquisition path still perform each request and publication.

Each scenario requires one request per one-shot page, all declared fixture
obligations durably captured before their deadlines, and valid immutable objects.
Sanitized evidence contains image digests, runtime revision, obligation counts and
scenario results. It excludes host connection details and credentials. Test
cleanup removes only its randomly named Compose project and fixture volumes; the
fixture registry and images remain available for inspection. Without the manifest
environment variable the tests skip, so ordinary unit runs need no Docker daemon.

These observations establish the tested synthetic source behavior. They do not
establish real TikTok recoverability, production source credentials, remote
storage durability, or an unconditional guarantee for responses interrupted
before durable local capture.
