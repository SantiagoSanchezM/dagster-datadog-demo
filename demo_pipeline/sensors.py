from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)
from openlineage.client import facet_v2
from openlineage.client.event_v2 import RunState

Assertion = facet_v2.data_quality_assertions_dataset.Assertion

from .openlineage_integration import ASSET_TABLES, JOB_NAME, emit_job_event, emit_task_event


@run_status_sensor(run_status=DagsterRunStatus.STARTED, default_status=DefaultSensorStatus.RUNNING)
def openlineage_job_started_sensor(context: RunStatusSensorContext) -> None:
    emit_job_event(
        run_id=context.dagster_run.run_id,
        job_name=context.dagster_run.job_name,
        event_type=RunState.START,
    )


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, default_status=DefaultSensorStatus.RUNNING)
def openlineage_job_success_sensor(context: RunStatusSensorContext) -> None:
    run_id = context.dagster_run.run_id

    # one OpenLineage task node summarizing every Dagster asset check that ran,
    # with pass/fail results attached to the dataset(s) they validate
    dq_by_table: dict[str, list[Assertion]] = {}
    records = context.instance.get_records_for_run(
        run_id=run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION
    ).records
    for record in records:
        data = record.event_log_entry.dagster_event.event_specific_data
        asset_name = data.asset_key.path[-1]
        table = ASSET_TABLES.get(asset_name)
        if table is None:
            continue
        dq_by_table.setdefault(table, []).append(
            Assertion(assertion=data.check_name, success=bool(data.passed))
        )

    if dq_by_table:
        tables = list(dq_by_table)
        emit_task_event(
            dagster_run_id=run_id,
            parent_job_name=JOB_NAME,
            task_name="data_quality_validation",
            event_type=RunState.START,
            inputs=tables,
        )
        emit_task_event(
            dagster_run_id=run_id,
            parent_job_name=JOB_NAME,
            task_name="data_quality_validation",
            event_type=RunState.COMPLETE,
            inputs=tables,
            outputs=tables,
            dq_assertions=dq_by_table,
        )

    emit_job_event(
        run_id=run_id,
        job_name=context.dagster_run.job_name,
        event_type=RunState.COMPLETE,
    )


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING)
def openlineage_job_failure_sensor(context) -> None:
    emit_job_event(
        run_id=context.dagster_run.run_id,
        job_name=context.dagster_run.job_name,
        event_type=RunState.FAIL,
        error=context.failure_event.message,
    )
