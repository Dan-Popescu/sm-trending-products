# Separate operational state from immutable analytical history

Status: accepted by the user.

PostgreSQL owns operational state, while S3 holds batched compressed source responses and normalized Parquet history published through manifests. Dagster OSS coordinates batches and their dependencies; an application scheduler prioritizes collection within source budgets. This supports replay and moving workers from a VPS to cloud infrastructure, at the cost of explicitly recovering partial writes across stores rather than assuming one transaction covers them.
