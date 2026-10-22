# VPS operation, recovery, and scale evidence

Status: approved for autonomous implementation on 2026-10-22.

## Problem Statement

The operator needs the complete pipeline to run economically on a VPS, survive failures, and provide evidence for when distributing work would be worthwhile.

## Solution

Package the same pipeline for local and VPS operation with resource limits, health/status views, backups, recovery procedures, and a reproducible load benchmark.

## User Stories

1. As an operator, I want a documented deployment, so that I can run the system on my VPS.
2. As an operator, I want credentials kept out of evidence and logs, so that collection does not expose secrets.
3. As an operator, I want stale sources and failed batches visible, so that silent data gaps cannot masquerade as stable trends.
4. As an operator, I want a tested restore, so that backups provide actual recoverability.
5. As an operator, I want peak memory and object-store traffic measured, so that scale costs can be estimated.
6. As an operator, I want workers movable without changing evidence contracts, so that cloud migration remains an option.

## Implementation Decisions

- Docker Compose separates PostgreSQL, Rust workers, Dagster services, analytical batches, and private Streamlit access. Constrain worker concurrency, buffer sizes, analytical threads, and cache capacity. Do not expose databases or orchestration controls publicly.
- Local development uses an S3-compatible test service; production uses the selected S3 bucket. Cache recent completed partitions on VPS disk and compact small files in controlled jobs, preserving manifest consistency.
- Monitor queue age, achieved refresh interval, source update lag, access failures, rate-limit responses, failed/quarantined batches, schema drift, checkpoint delay, disk pressure, RSS, CPU, throughput, and object-store requests/bytes.
- Back up PostgreSQL and verify referenced object versions/manifests. Restore into an isolated environment, reconcile objects and checkpoints, and demonstrate that committed observations and frozen alerts survive.
- Structured logs carry run/task/source identifiers and redacted error categories. Health checks distinguish service availability from data freshness and usable source access.
- Benchmark a defined distribution of records, object sizes, source latency, skew, worker counts, and analytical workloads. Establish baseline resource use, then a documented operating envelope with safety headroom. Record all assumptions before cost extrapolation.
- Scale by moving independent container workers and storage endpoints. Managed infrastructure, orchestration replacement, and distributed analytical engines require measured triggers and a later decision.

## Testing Decisions

Deploy the supported configuration, ingest controlled evidence, produce a visible assessment, interrupt workers, and restore into a fresh environment. Confirm data equivalence, idempotent recovery, restricted access, redaction, and bounded resource behavior under the declared envelope. Validate a cold-cache run to expose object-store costs. No existing approved tests exist.

## Out of Scope

Purchasing infrastructure, actual VPS deployment without access, cloud migration, multi-region high availability, or unmeasured claims of unlimited scale.

## Further Notes

Done means reproducible deployment, a successful restore exercise, and measured limits. Recovery objectives, machine sizing, and recurring cost estimates follow the baseline and the user's operational requirements rather than invented throughput targets.
