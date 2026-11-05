# Selected-video comments, downloads and Large-v3

Issue #200. This optional module does not change acquisition scheduling, SQL
migrations or collector images. Opening the dashboard makes no provider call.
Downloads and transcription are off by default. Use a separate private operator
process on Linux, preferably with its own container memory/CPU limits.

## Install and initialize explicitly

```sh
uv sync --locked --extra video-research
# Only the unofficial browser adapter requires this explicit browser installation:
uv run playwright install chromium
sm-trends video-research init
sm-trends video-research videos --product-id PRODUCT --limit 20
```

The dependency extra never downloads a browser or speech model automatically.
Installation is not live-source acceptance. Source policies and confirmed video
ownership remain required, as for existing transcript imports. Copy an actual
`selection` from the listing, then supply your own `permission_reference`.
Fixtures do not grant real access. Check the applicable source permissions and
approved purposes independently for each backend.

## Private configuration

Create a private JSON file outside the repository. Paths must be absolute.
The workspace must initially be empty, owned by this workflow and mode 0700.
Do not share a workspace between namespaces/databases. One active operation per
workspace is enforced with a file lock; concurrent attempts refuse immediately.

```json
{
  "enabled": false,
  "allow_unofficial": false,
  "allow_download": false,
  "allow_transcription": false,
  "work_dir": "/srv/private/selected-videos",
  "max_daily_operations": 20,
  "max_comments": 100,
  "max_pages": 3,
  "max_media_bytes": 100000000,
  "max_workspace_bytes": 2000000000,
  "timeout_seconds": 180,
  "transcription_timeout_seconds": 1800,
  "cpu_threads": 2,
  "memory_mb": 8192,
  "max_duration_seconds": 300,
  "model": "large-v3",
  "compute_type": "int8"
}
```

`memory_mb` is the entire optional worker session budget, not a prediction of
model RAM. Admission checks available host/cgroup-v2 memory plus 256 MiB reserve;
it is a measurement, not a reservation against unrelated projects. RSS and the
process count are sampled every 50 ms, so short spikes can escape sampling. CPU
affinity limits simultaneous hardware threads and low process priority helps
shared hosts; it is not a fractional vCPU quota. Transcription additionally has
an address-space limit. Browsers reserve large virtual ranges and use sampled
RSS rather than that address-space limit. File/time/process limits terminate tracked owned descendants, including detached
browser sessions, using Linux pidfds to avoid signalling recycled process IDs.
Processes are discovered through ancestry and stable start times; sub-50-ms
children that immediately detach and reparent cannot be guaranteed discoverable.
The supervisor terminates the
owned child session on ordinary failures and timeouts. An external supervisor
or container is still needed to enforce hard aggregate limits and kill all
children if the parent itself is forcibly killed. No claim of guaranteed OOM
isolation or benchmarked Large-v3 throughput is made.

Keep at least the configured media limit plus 16 MB free on disk. New work also
reserves capacity for one media file plus 2 MB of result data. Workspace growth and remaining free space are sampled every second during
execution; this is not a hard filesystem quota and short growth spikes may exceed
the configured value before termination. Cached result reads
consume no new operation budget and do not need that extra capacity. The workspace
has a bounded 20,000-entry inventory; archive/export intentionally rather than
letting an unbounded history of local media accumulate. This feature never deletes
useful source history automatically or uploads raw media to S3 automatically.

## Comments: choose either backend

The unofficial adapter uses the free, self-hosted
[davidteather/TikTok-Api](https://github.com/davidteather/TikTok-Api). Enable
`enabled` and `allow_unofficial`. An optional existing session token can be
provided only through `TIKTOK_MS_TOKEN`. No proxy purchase, session recovery,
CAPTCHA bypass or automatic fallback to another provider is configured. Blocking,
missing sessions and source changes can make this backend fail; zero software
license cost does not guarantee free successful collection at scale.

For official Research, add this configuration with the actual approved project:

```json
{
  "reference": "actual-approval-reference",
  "project_id": "approved-project",
  "expires_at": "2027-01-01T00:00:00Z",
  "scope": "research.data.basic",
  "token_env": "TIKTOK_RESEARCH_ACCESS_TOKEN"
}
```

Store it under `research`, set `enabled`, and provide the token in the named
environment variable. Never put a token in JSON or Git. Not selling products yet
does not establish eligibility: use must match the approved Research purpose.
The adapter does not apply for approval or refresh credentials. It calls the fixed
[official comment endpoint](https://developers.tiktok.com/doc/research-api-specs-query-video-comments)
with at most 100 records per page. Expired approval also prevents later result
reads until the same project's authority is valid again.

Example request (replace all selected IDs with real listing output):

```json
{
  "request_id": "comments-video-123-initial",
  "selection": {
    "product_id": "PRODUCT",
    "entity_key": "tiktok:video:123",
    "dataset_id": "ACTUAL_DATASET",
    "observation_id": "ACTUAL_OBSERVATION",
    "synthetic": false,
    "permission_reference": "recorded-permission-for-this-use"
  },
  "operation": "comments",
  "provider": "unofficial",
  "video_url": "https://www.tiktok.com/@ACTUAL_HANDLE/video/123",
  "refresh_key": "initial"
}
```

Use `provider: research` for the independent official sample; it needs no URL.
Canonical URLs must have the matching numeric ID and no tracking query string.
Do not reuse a request ID for another input. To request a deliberate later sample,
change both `request_id` and `refresh_key`. A new alias for the same input reuses
the completed job. Running/uncertain outcomes never trigger automatic retries,
including after a process crash. An operator must investigate and explicitly
choose a new refresh key before repeating uncertain external work.

```sh
# Validate and prepare only: no database or external operation.
sm-trends video-research run --request request.json --config private.json
# Admit and execute only when deliberately enabled/configured:
sm-trends video-research run --request request.json --config private.json --execute
sm-trends video-research show --request-id comments-video-123-initial \
  --product-id PRODUCT --config private.json --limit 20
```

Daily reservations use the database's UTC date and count operations, not underlying
HTTP calls or money. They are shared between backends/download/transcription and
are consumed before execution, including uncertain failures. Changing aliases,
backend or model cannot reset them; daily limit/workspace changes are refused
once that day's policy is established. Unofficial SDK request counts are unknown;
`max_pages` applies to the official connector. Both backends enforce `max_comments`.

Persisted fields: comment ID, selected video ID, text, available timestamp,
likes/reply counts and parent ID. No author profile is retained. Email-like text
is filtered; this is not comprehensive removal of personal information. Official
returned pages may be exhausted, but neither backend claims a complete census
(including replies/moderated/deleted comments). Paged reads load only requested
comment texts. Simple French/English phrase rules surface price, delivery,
purchase and negative-experience candidates with quotes. These are clues in this
sample, not measured purchases, unique buyers or verified demand.

## On-demand video files

Enable `allow_download`, use `operation: download`, a new request ID and the
matching canonical URL. Only one selected progressive MP4 at up to 720p is
accepted; there is no playlist or automatic bulk download. The selected ID and
duration are checked before downloading. Private files have byte/time/disk limits
and a recorded SHA-256 checksum. A source video must still be remotely available;
a URL cannot recover deleted/private videos that were never downloaded.

Export with the saved download job ID and the original selected manifest:

```sh
sm-trends video-research export --job-id JOB_SHA --selection selection.json \
  --config private.json --output /private/output/video.mp4
```

Export streams through a private temporary file, verifies the checksum and refuses
to overwrite any destination. Dashboard browser exports are capped at 100 MB.
Source/ownership/retention are checked before reads, export and publication;
previously delivered browser/local copies cannot be retracted. A download does
not grant permission to republish someone else's video in advertising.

## Local Whisper Large-v3

Explicitly provision a compatible CTranslate2 Large-v3 checkpoint (for example
[Systran/faster-whisper-large-v3](https://huggingface.co/Systran/faster-whisper-large-v3))
in a private absolute directory. Supply `model.bin`, `config.json`, `tokenizer.json`
and `preprocessor_config.json`; verify its provenance and record the SHA-256 of
`model.bin` in `model_sha256`, with `model_dir` in configuration. No implicit model
or tokenizer download is allowed. The recorded hash identifies the operator's
provisioned artifact; it does not independently certify that an arbitrary directory
contains authentic Large-v3 weights.

Enable `allow_transcription`, set `operation: transcribe`, a new request ID and
`media_job_id` from a completed selected download. Optional `language: fr` fixes
the language; omission enables Whisper language detection. The default `int8`
precision reduces CPU/RAM cost while retaining Large-v3; explicit `float32` is
available if the machine has sufficient headroom. There is no smaller-model or
turbo fallback. Largest-model quality is not guaranteed for every short/noisy clip.

Results retain timestamps, machine origin, model/media checksums, library version
and unverified-speech status. Audio duration is limited and transcripts are capped
at 16,000 characters. Silence, unsupported output or exceeded budgets can fail
instead of silently truncating. Same media/model/settings reuse a saved result.
Import into the existing optional transcript insights is explicit:

```sh
# Existing transcript module must also have been initialized.
sm-trends video-research import-transcript --request-id SPEECH_REQUEST \
  --product-id PRODUCT --config private.json
sm-trends video-research doctor --config private.json
```

The dashboard exposes the same selected actions and reads without a background
collection schedule. Actual TikTok access, the approved Research project, real
Large-v3 accuracy and CPU/RAM throughput require separate controlled acceptance
with credentials/model/hardware; fixture tests cannot establish them.
