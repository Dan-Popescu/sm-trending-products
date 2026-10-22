"""Dagster coordinates bounded batches; the application scheduler selects entities."""

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    Definitions,
    Failure,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from .contracts import now
from .pipeline import Pipeline


class BatchConfig(Config):
    synthetic: bool = False
    request_budget: int = 100
    batch_size: int = 20


def _report_batch(context: AssetExecutionContext, result: dict, **counts: int) -> None:
    errors = result.get("errors", [])
    categories = sorted({error.get("category", "unknown") for error in errors})
    expected_wait = {"rate_limited", "quota_exhausted", "lease_lost"}
    failed = bool(set(categories) - expected_wait)
    quota_waiting = result.get("quota_waiting", 0)
    waiting = bool(errors or quota_waiting or result.get("waiting", False))
    context.add_output_metadata(
        {
            **counts,
            "errors": len(errors),
            "quota_waiting": quota_waiting,
            "outcome": "failed" if failed else "waiting" if waiting else "complete",
        }
    )
    if failed:
        raise Failure(
            description="Batch contains failed work; inspect the operational task status.",
            metadata={"error_categories": ", ".join(categories), "failed_items": len(errors)},
        )


@asset
def collection_schedule(context: AssetExecutionContext, config: BatchConfig) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.schedule(config.request_budget, synthetic=config.synthetic)
    context.add_output_metadata(
        {"synthetic": config.synthetic, "scheduled": result.get("scheduled", 0)}
    )
    return result


@asset(deps=[collection_schedule])
def observation_batch(context: AssetExecutionContext, config: BatchConfig) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.collect_batch(config.batch_size, synthetic=config.synthetic)
    _report_batch(context, result, processed=result["processed"])
    return result


@asset(deps=[observation_batch])
def opportunity_assessments(context: AssetExecutionContext, config: BatchConfig) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.assess(now(), synthetic=config.synthetic)
    context.add_output_metadata({"run_id": result["run_id"], "product_count": len(result["rows"])})
    # Do not store the full evidence history in Dagster's IO manager.
    return {"run_id": result["run_id"], "products": len(result["rows"])}


@asset(deps=[opportunity_assessments])
def prospective_decisions(context: AssetExecutionContext, config: BatchConfig) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.freeze(now(), synthetic=config.synthetic)
    context.add_output_metadata({"evaluation_id": result["evaluation_id"]})
    return {"evaluation_id": result["evaluation_id"]}


@asset
def outcome_maturation(context: AssetExecutionContext, config: BatchConfig) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.mature_due(
            limit=config.batch_size, synthetic=config.synthetic, as_of=now()
        )
    _report_batch(
        context,
        result,
        evaluated_decisions=result["processed"],
        completed_decisions=result["completed"],
    )
    return result


@asset
def source_retention(context: AssetExecutionContext) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.retention(apply=True)
    context.add_output_metadata(
        {
            name: result.get(name, 0)
            for name in ("dataset_count", "deleted_objects", "failed_objects", "pending_objects")
        }
    )
    if result.get("failed_objects", 0) or result.get("pending_objects", 0):
        raise Failure(
            description="Retention has pending deletions; resolve storage failures and retry."
        )
    return result


@asset
def history_compaction(context: AssetExecutionContext, config: BatchConfig) -> dict:
    """One bounded compaction group per scheduled invocation."""
    with Pipeline.from_env() as pipeline:
        result = pipeline.compact(synthetic=config.synthetic, max_datasets=100)
    context.add_output_metadata({"status": result["status"], "rows": result.get("rows", 0)})
    return result


collection_job = define_asset_job(
    "collection_batches",
    selection=AssetSelection.assets(
        collection_schedule, observation_batch, opportunity_assessments
    ),
)
forward_job = define_asset_job(
    "forward_decisions", selection=AssetSelection.assets(prospective_decisions)
)
outcomes_job = define_asset_job(
    "forward_outcomes", selection=AssetSelection.assets(outcome_maturation)
)
retention_job = define_asset_job(
    "retention_policy", selection=AssetSelection.assets(source_retention)
)

compaction_job = define_asset_job(
    "history_compaction", selection=AssetSelection.assets(history_compaction)
)

defs = Definitions(
    assets=[
        collection_schedule,
        observation_batch,
        opportunity_assessments,
        prospective_decisions,
        outcome_maturation,
        source_retention,
        history_compaction,
    ],
    jobs=[collection_job, forward_job, outcomes_job, retention_job, compaction_job],
    schedules=[
        ScheduleDefinition(
            job=compaction_job,
            cron_schedule="45 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=collection_job,
            cron_schedule="*/15 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=forward_job,
            cron_schedule="0 */6 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=outcomes_job,
            cron_schedule="30 */6 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=retention_job,
            cron_schedule="15 3 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
