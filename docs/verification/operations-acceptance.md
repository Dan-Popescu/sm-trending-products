# Controlled deployment and recovery acceptance

T13 accepts the supported deployment and isolated rehearsal, while actual VPS
rollout waits for the specified machine and access as its final criterion states.
HTTP Moto is an emulator; these checks do not certify a production S3 provider.

| Original T13 criterion | Executed evidence |
| --- | --- |
| Complete Compose deployment and inspectable results | `scripts/ci-recovery.sh` runs the built application image with PostgreSQL, HTTP object storage, worker, Dagster daemon/webserver and dashboard. It seeds controlled observations, reviewed identity, an assessment, alert and forward decision; HTTP/heartbeat checks require operator services to be ready. Actual Streamlit acceptance in `tests/test_evaluation_delivery.py`, `tests/test_attention_delivery.py` and commercial delivery scenarios inspects published results through the same facade. |
| Private controls and redaction | Compose publishes interfaces only on loopback and publishes no database port. The runbook documents SSH tunnels. Services run unprivileged with explicit writable volumes, dropped capabilities and read-only roots. Dashboard/collector contracts exercise secret suppression; configuration checks avoid printing interpolated secrets. |
| Distinct operational signals | Native status and actual health review expose source capability failures, queue states/due times, expired leases, last observations, failed/quarantined work and dataset quarantine counts. `tests/test_storage_status.py` checks available filesystem capacity plus configured cache ceiling, unreadable-volume unknowns and real UI rendering. No evidence scan or filesystem error text is needed. Process readiness is measured separately by the container rehearsal. |
| Isolated database and object restoration | The actual backup/restore scripts copy and hash object bytes/metadata, restore application and Dagster databases into fresh containers on an internal network, verify manifests and compare logical fingerprints including identity, alerts and frozen decisions. Native container CI is the merge gate. |
| Restart reconciles checkpoints without duplicates | The recovery set includes an expired synthetic lease with objects written before checkpoint commit. The explicit synthetic restore probe resumes the same staged dataset from copied raw bytes with source requests forbidden, then reopens the application and verifies zero duplicate observations or task executions. Existing committed datasets and other logical fingerprints remain unchanged. `tests/test_restore_checkpoint.py` also verifies live-state refusal before mutation; process recovery tests cover three hard termination boundaries. |
| Cache, compaction and resource bounds | Cache reservation/pin tests exercise independent processes, downloads, reduced budgets and active readers. Maintenance tests preserve immutable manifest lineage and original provenance through compaction. Native bounded-analysis tests reject excessive row partitions. Compose enforces memory/CPU and writable-disk/cache settings; this is bounded software behavior, not a universal sustained capacity guarantee. T14 owns workload-specific measurements and headroom. |
| Operational instructions and actual rollout boundary | The runbook, backup/restore, cache and benchmarking guides provide the supported procedure. Actual deployment, provider semantics, SSH/firewall configuration and machine-specific acceptance remain pending external infrastructure details. No machine, bucket subscription or advertising budget is provisioned by this acceptance. |

The status capacity measurement reads filesystem counters in constant time.
Missing directories use an explicitly labeled existing ancestor; inaccessible
volumes remain unknown. Shared volumes may report the same filesystem and must
not be summed. This adds observability without a second cache inventory, recursive
directory scan or competing operational state store.

Before autonomous merge, the PR records exact-head Rust/Python/native results,
benchmark/quota checks, deployment build and the strengthened isolated rehearsal.
The evidence applies to controlled engineering; production acceptance remains
visible as a separate gate in the coverage record and runbook.
