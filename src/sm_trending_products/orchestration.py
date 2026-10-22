"""Dagster coordinates bounded batches; the application scheduler selects entities."""

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    Definitions,
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
    context.add_output_metadata({"processed": result["processed"], "errors": len(result["errors"])})
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
        # One bounded pass; later runs continue the remaining frontier.
        frontier = pipeline.catalog.all(
            "SELECT evaluation_id FROM evaluation_runs WHERE kind='forward' "
            "AND synthetic=%s AND NOT invalidated ORDER BY created_at DESC LIMIT 100",
            (config.synthetic,),
        )
        completed = [
            pipeline.mature(item["evaluation_id"], now())["evaluation_id"] for item in frontier
        ]
    context.add_output_metadata({"evaluated_decisions": len(completed)})
    return {"evaluations": completed}


@asset
def source_retention(context: AssetExecutionContext) -> dict:
    with Pipeline.from_env() as pipeline:
        result = pipeline.retention(apply=True)
    context.add_output_metadata({"deleted_datasets": len(result.get("deleted", []))})
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

defs = Definitions(
    assets=[
        collection_schedule,
        observation_batch,
        opportunity_assessments,
        prospective_decisions,
        outcome_maturation,
        source_retention,
    ],
    jobs=[collection_job, forward_job, outcomes_job, retention_job],
    schedules=[
        ScheduleDefinition(job=collection_job, cron_schedule="*/15 * * * *"),
        ScheduleDefinition(job=forward_job, cron_schedule="0 */6 * * *"),
        ScheduleDefinition(job=outcomes_job, cron_schedule="30 */6 * * *"),
        ScheduleDefinition(job=retention_job, cron_schedule="15 3 * * *"),
    ],
)
