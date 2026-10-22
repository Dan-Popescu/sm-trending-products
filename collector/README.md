# Rust collector

`sm-collector` fetches one bounded API page and normalizes its evidence. PostgreSQL
owns task leases and shared credential budgets; the Python control plane owns
durable raw/Parquet publication and cursor advancement. A process exit never
advances a task cursor on its own.

## Build and verify

Use Rust 1.88 or newer. Dependency resolution is committed in Cargo.lock.

```bash
cargo build --locked --release --manifest-path collector/Cargo.toml
cargo fmt --manifest-path collector/Cargo.toml --check
cargo clippy --locked --manifest-path collector/Cargo.toml -- -D warnings
PYTHONPATH=src python -m pytest tests/test_collector_contract.py
```

## Collection protocol

Read exactly one JSON object from stdin and emit exactly one JSON object on
stdout. `--help` describes the available command. Default invocation collects;
`normalize` replays archived responses without HTTP or access tokens.

Collection input:

```json
{"source":"tiktok_display","operation":"list","account_id":"authorized-account","synthetic":false,"payload":{"max_count":20}}
```

The production origin is fixed to `https://open.tiktokapis.com`; redirects are
disabled. Tokens come exclusively from `TIKTOK_DISPLAY_ACCESS_TOKEN` or
`TIKTOK_CCL_ACCESS_TOKEN`. Real collection requires DATABASE_URL and calls
`reserve_request(source, account_id, credential_key)` before every HTTP attempt,
including retries. For real requests, credential_key is the SHA-256 digest of the
high-entropy access token so the same actual credential shares a budget even
across endpoint types or account aliases. Synthetic requests pass null and do not
create a credential claim. The raw token is never sent to PostgreSQL. That SQL
function must return `allowed boolean` and
`retry_after_seconds double precision`. It must reject missing budget records.

Synthetic controlled-source tests may specify `endpoint_override` using a numeric
loopback HTTP address and `observed_at` with an explicit timezone. Neither
override is permitted for live collection. Synthetic requests without a database
are allowed only for isolated contract tests; production control-plane tests use
the same PostgreSQL quota contract.

Success output carries source, account_id, observed_at, synthetic, raw, records,
next_payload, quarantine, request_count, and normalizer_version. Records use the
shared Observation contract. A null next_payload ends pagination. An incomplete
or malformed continuation is a schema failure. Display cursors are integer
milliseconds; commercial continuation uses the returned opaque search_id.

Failure exits nonzero and emits a safe `error` object containing category, code,
and an optional retry_after_seconds. Categories are authorization, configuration,
quota, rate_limit, transport, source, schema, and internal. No raw request errors,
authorization headers, source error messages, or credential values are logged.
The process flushes stdout before exit. HTTP 429 returns its retry hint immediately
for scheduler-level waiting; transient transport/5xx failures retry at most three
total requests with bounded backoff. Response bodies and stdin are size limited.

Malformed schema responses also include `error.artifact` with sanitized raw
content, observed_at, source, account_id, and synthetic classification so the
control plane can archive a zero-row quarantined dataset without advancing the
task. Invalid JSON preserves a bounded text prefix with an explicit truncation
flag; known credentials are redacted before truncating. Oversize responses do not
include an artifact because preserving them would defeat the enforced bound.

## Replay protocol

```json
{"request":{"source":"tiktok_display","operation":"list","account_id":"authorized-account","synthetic":true,"payload":{}},"raw":{"data":{"videos":[{"id":"example","view_count":4}],"has_more":false},"error":{"code":"ok"}},"observed_at":"2026-10-01T12:00:00Z"}
```

Replay retains observation identity and collection time and reports zero external
requests. Replaying an archived response does not grant or verify source access.

## Measurement semantics

- Display metrics use view_count, like_count, comment_count, and share_count.
  Missing counters are absent measurements with explicit missing-field reasons.
- Commercial ad reach uses `reach`, never view_count. Exact integers and explicit
  integer ranges retain their reported precision. Rounded strings such as `11K`
  remain `reach_raw` attributes with unknown numerical bounds because the source
  does not document the rounding rule. No invented conversion or error band is
  applied.
- Ads and commercial-content records expose no invented engagement, unit-sales,
  retention, completion, or swipe-away measurements. Distribution context remains
  distinct from authorized-account videos.
- The authorized Display account supplies creator_id; creator geography does not
  become audience geography or target market. Text resemblance does not create a
  verified creative fingerprint.

See the repository's source-contract research for endpoint documentation and
schema inconsistencies. Live acceptance requires actual approved credentials and
verified intended use; controlled HTTP tests establish software behavior only.
