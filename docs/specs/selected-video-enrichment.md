# Selected-video comments, media and local transcription

Approved by the owner on 2026-11-05. Baseline main is
`567aec8c431a3e94022e1ae51d5f224d51c1ab85`; local equivalent tree is
`e7d466b6cfb52e1ffd771a525751e4c0608f2ef1` (commit 0502d808).

## Outcome and boundaries

An optional explicit operator workflow enriches selected, confirmed product
videos without modifying acquisition SQL, scheduling or the Rust collector.
It provides two independent comment backends: an official TikTok Research PoC
and the self-hosted open-source TikTok-Api wrapper. Neither fixture success nor
the absence of current sales establishes Research eligibility. Real Research
execution requires separately recorded approval, scope, expiry and credentials.
No access restriction, login challenge or CAPTCHA is bypassed. No paid provider,
proxy purchase, live-source activation or model download occurs implicitly.

Video listing resolves current reviewed ownership and source availability.
Explicit selected-video downloads use yt-dlp, remain off by default, preserve
source/product attribution and have file/time/disk limits. Remote availability
is not guaranteed: deleted/private videos cannot be recovered from an unstored
URL. Download permission and advertising reuse rights are separate.

Local transcription uses a provisioned Whisper Large-v3 checkpoint through
faster-whisper on CPU. It never silently substitutes a smaller/turbo model.
CPU affinity, address-space/file/output/time limits and one active worker bound
the optional work. Missing dependencies/models/approval are actionable refusals.
Tests do not download models or require third-party credentials.

## Acceptance

1. Deep optional module and `sm-trends video-research` CLI expose initialization,
   selected-video listing, explicit comment collection, media download,
   transcription and guarded result reads. Help requires no database or SDK.
2. Immutable typed input/config manifests bound comment count/pages/text, media
   bytes, model identity, CPU/memory/time and daily operation budgets. Secrets
   enter only named environment variables and never manifests/results/errors.
3. One durable request identity and atomic daily reservation precede external
   execution. Same-input retries reuse completion; changed-input request IDs
   are rejected. Uncertain/running outcomes never cause automatic repeated calls.
   No DB transaction remains open during provider/model execution.
4. Every admission, reused result and final delivery checks namespace, reviewed
   product ownership, actual committed observation, source availability and
   retention. Mid-run withdrawal prevents result publication/delivery. Returned
   text is bounded and source-derived publications use existing provenance.
5. Research PoC follows the documented fixed endpoint, bearer token, numeric
   video ID, 100-record pages, cursor/has_more/error semantics. Open-source
   collection is optional, bounded and explicitly labels partial/unknown coverage.
   Compare outputs without treating either sample as a complete comment census.
6. Video downloads accept only selected TikTok video URLs matching the observed
   ID, have no playlist/automatic bulk download, use private owned directories
   and never replace an existing operator file. Source loss prevents later reads.
7. Large-v3 transcription keeps timestamps and explicit machine provenance,
   reuses the same artifact/model/settings result, and can feed existing text
   insights without pretending the text was supplied by a human. Import remains
   explicit and preserves source guards and the 16,000-character text limit.
8. Real PostgreSQL/HTTP behavioral tests cover concurrency/replay, permissions,
   publication fences, redaction, bounded pagination, worker timeout/cleanup,
   local model-only loading and source withdrawal. Actual dependency contracts
   are exercised with controlled fake modules/processes; missing live access,
   provisioned model and host benchmarks remain reported acceptance limits.
9. Independently review Standards and Spec at the exact final revision. Run
   relevant checks and the complete CI; autonomously merge when all required
   checks pass and the tested tree matches the prospective merge.

## Technology contracts

- Research endpoint: https://developers.tiktok.com/docs/en/research-api-specs-query-video-comments
- Eligibility: https://developers.tiktok.com/docs/en/research-api-faq/
- Open-source backend: https://github.com/davidteather/TikTok-Api
- Selected downloads: https://github.com/yt-dlp/yt-dlp
- Local model execution: https://github.com/SYSTRAN/faster-whisper

Research and unofficial sources remain separate provider identities. Persist
only selected comment fields, excluding author profiles. Unknown coverage and
counter meanings remain explicit; comment intent is not a measured purchase.
