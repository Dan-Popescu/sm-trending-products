# Native CI verification

Observed on 2026-10-22 for commit
`f31a4dcd81fe4a0c2acafcab73fcc4fac2070770` in
[PR #1](https://github.com/Dan-Popescu/sm-trending-products/pull/1).

[Workflow run 39816229325](https://github.com/Dan-Popescu/sm-trending-products/actions/runs/39816229325)
and [job 119955630951](https://github.com/Dan-Popescu/sm-trending-products/actions/runs/39816229325/job/119955630951)
completed successfully. The job ran Rust formatting, Clippy and tests; Python
formatting, lint and types; the native PostgreSQL suite; shell validation;
Compose validation and image construction; and the isolated recovery rehearsal.

## Behavioral evidence

The suite reported **142 passed in 38.29 seconds** against PostgreSQL 17.11 and
an HTTP Moto object store. The downloaded JUnit report independently confirms
142 tests, zero failures, zero errors, and zero skipped tests. These native
contention scenarios explicitly passed:

- Shared credential quota remains strict across parallel account aliases.
- Parallel collection workers claim each due task once.
- Parallel evaluation workers claim the frozen forward frontier without duplicates.

The deployment image built successfully. The recovery script seeded two synthetic
observations, one reviewed product, an alert, and a frozen forward decision. It
then invoked the actual backup and isolated restore scripts, restored the
application and Dagster databases and four S3 objects, verified evidence, and
matched the restored catalog fingerprints to the captured state. The log confirms
that products, identity, alerts, and frozen evaluations match. The recovery report
records `status: verified` and `catalog_fingerprint_matched: true`.

## Retained artifact

The run uploaded `verification-results` as artifact **12443036549**, containing
`test-results.xml` and `recovery-report.json`. Its ZIP SHA256 is:

```text
98f07245d19c005f21f9762c7e032164cf226c9e3773b7c579be252500b6bd7e
```

The artifact was downloaded and its digest checked against GitHub's artifact
metadata before reading both reports. GitHub records its expiry as 2027-01-20;
retain a verified copy separately if this evidence must outlive Actions retention.

## Boundary

This verifies native PostgreSQL behavior and a synthetic container recovery
exercise using HTTP Moto 5.2.4. It does not verify a VPS rollout, production S3
provider behavior, live TikTok credentials/scopes or market coverage, or a
production capacity and cost envelope. Those remain separate acceptance gates.
The recorded run applies to the exact commit above; later changes require their
own CI result.
