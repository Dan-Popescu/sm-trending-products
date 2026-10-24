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
from .settings import PlanningSettings


class BatchConfig(Config):
    synthetic: bool = False
    request_budget: int | None = None
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
    _require_legacy_collection()
    with Pipeline.from_env() as pipeline:
        budget = config.request_budget
        if budget is None:
            budget = PlanningSettings().scheduler_request_budget
        result = pipeline.schedule(budget, synthetic=config.synthetic)
    context.add_output_metadata(
        {"synthetic": config.synthetic, "scheduled": result.get("scheduled", 0)}
    )
    return result


@asset(deps=[collection_schedule])
def observation_batch(context: AssetExecutionContext, config: BatchConfig) -> dict:
    _require_legacy_collection()
    with Pipeline.from_env() as pipeline:
        result = pipeline.collect_batch(config.batch_size, synthetic=config.synthetic)
    _report_batch(context, result, processed=result["processed"])
    return result


def _require_legacy_collection() -> None:
    if PlanningSettings().acquisition_enabled:
        raise Failure(
            description="Dedicated acquisition is enabled; use the capture planner and capture "
            "workers. Legacy Dagster collection is disabled."
        )


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
        result = pipeline.retention_drain()
    context.add_output_metadata(
        {
            name: result.get(name, 0)
            for name in (
                "dataset_count",
                "deleted_objects",
                "failed_objects",
                "pending_objects",
                "oldest_pending_object_age_seconds",
                "expired_root_count",
                "oldest_expired_root_age_seconds",
                "pending_finalization_count",
                "pending_artifact_gc_count",
                "artifact_relations_deleted",
                "artifact_gc_completed_parents",
                "operations",
            )
        }
    )
    context.add_output_metadata(
        {
            "status": result.get("status", "unknown"),
            "stop_reason": result.get("stop_reason", "unknown"),
            "pending_source_invalidations": len(result.get("pending_source_invalidations", [])),
        }
    )
    if result.get("failed_objects", 0) or result.get("status") == "blocked":
        raise Failure(
            description="Retention is blocked or has failed deletions; inspect its backlog."
        )
    return result


@asset
def history_compaction(context: AssetExecutionContext, config: BatchConfig) -> dict:
    """Drain bounded compaction groups within the configured invocation budget."""
    with Pipeline.from_env() as pipeline:
        result = pipeline.compact_drain(synthetic=config.synthetic, max_datasets=100)
    context.add_output_metadata({"status": result["status"], "rows": result.get("rows", 0)})
    context.add_output_metadata(
        {
            "stop_reason": result.get("stop_reason", "unknown"),
            "pass_backlog": result.get("pass_backlog", {}),
            "active_backlog": result.get("active_backlog", {}),
        }
    )
    return result


collection_job = define_asset_job(
    "collection_batches",
    selection=AssetSelection.assets(collection_schedule, observation_batch),
)
assessment_job = define_asset_job(
    "opportunity_rankings", selection=AssetSelection.assets(opportunity_assessments)
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
    jobs=[collection_job, assessment_job, forward_job, outcomes_job, retention_job, compaction_job],
    schedules=[
        ScheduleDefinition(
            job=compaction_job,
            cron_schedule=PlanningSettings.cron(PlanningSettings().maintenance_schedule_minutes),
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        *(
            []
            if PlanningSettings().acquisition_enabled
            else [
                ScheduleDefinition(
                    job=collection_job,
                    cron_schedule=PlanningSettings.cron(
                        PlanningSettings().collection_schedule_minutes
                    ),
                    default_status=DefaultScheduleStatus.RUNNING,
                )
            ]
        ),
        ScheduleDefinition(
            job=assessment_job,
            cron_schedule=PlanningSettings.cron(PlanningSettings().assessment_schedule_minutes),
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
            cron_schedule=PlanningSettings.cron(PlanningSettings().maintenance_schedule_minutes),
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
