"""Custom OpenLineage instrumentation for the demo_pipeline Dagster job.

Emits OpenLineage RunEvents straight to Datadog's Data Observability intake
(https://docs.datadoghq.com/data_observability/jobs_monitoring/openlineage/).
The openlineage-python HTTP transport is configured purely through env vars
(OPENLINEAGE_URL / OPENLINEAGE_API_KEY) - see docker-compose.yml / .env.
"""

import functools
import uuid
from datetime import datetime, timezone

from openlineage.client import facet_v2
from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState

data_quality_assertions_dataset = facet_v2.data_quality_assertions_dataset
error_message_run = facet_v2.error_message_run
job_type_job = facet_v2.job_type_job
parent_run = facet_v2.parent_run

PRODUCER = "https://github.com/datadog/dagster-datadog-demo"
JOB_NAMESPACE = "dagster://demo-pipeline"
DATASET_NAMESPACE = "duckdb://demo-pipeline/warehouse"

# maps dagster asset name -> duckdb "schema.table" it materializes
ASSET_TABLES = {
    "raw_customers": "raw.customers",
    "raw_orders": "raw.orders",
    "cleaned_orders": "staging.orders",
    "enriched_orders": "staging.enriched_orders",
    "revenue_by_region": "analytics.revenue_by_region",
}

_client: OpenLineageClient | None = None


def get_client() -> OpenLineageClient:
    global _client
    if _client is None:
        _client = OpenLineageClient()
    return _client


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _job_type_facets(job_type: str) -> dict:
    return {
        "jobType": job_type_job.JobTypeJobFacet(
            processingType="BATCH",
            integration="dagster",
            jobType=job_type,
            producer=PRODUCER,
        )
    }


def task_run_id(dagster_run_id: str, task_name: str) -> str:
    """Deterministic OpenLineage run id shared by the START/COMPLETE/FAIL events of one task."""
    return str(uuid.uuid5(uuid.UUID(dagster_run_id), task_name))


def emit_job_event(
    *,
    run_id: str,
    job_name: str,
    event_type: RunState,
    job_type: str = "DAG",
    error: str | None = None,
) -> None:
    run_facets = {}
    if error:
        run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
            message=error, programmingLanguage="python", producer=PRODUCER
        )
    event = RunEvent(
        eventType=event_type,
        eventTime=_now(),
        run=Run(runId=run_id, facets=run_facets),
        job=Job(namespace=JOB_NAMESPACE, name=job_name, facets=_job_type_facets(job_type)),
        producer=PRODUCER,
    )
    get_client().emit(event)


def emit_task_event(
    *,
    dagster_run_id: str,
    parent_job_name: str,
    task_name: str,
    event_type: RunState,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    dq_assertions: dict[str, list[data_quality_assertions_dataset.Assertion]] | None = None,
    error: str | None = None,
) -> None:
    """Emit a START/COMPLETE/FAIL event for one asset (a 'task' in the DAG)."""
    input_datasets = [Dataset(namespace=DATASET_NAMESPACE, name=t) for t in (inputs or [])]

    output_datasets = []
    for table in outputs or []:
        facets = {}
        if dq_assertions and table in dq_assertions:
            facets["dataQualityAssertions"] = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=dq_assertions[table], producer=PRODUCER
            )
        output_datasets.append(Dataset(namespace=DATASET_NAMESPACE, name=table, facets=facets))

    run_facets = {
        "parent": parent_run.ParentRunFacet(
            run=parent_run.Run(runId=dagster_run_id),
            job=parent_run.Job(namespace=JOB_NAMESPACE, name=parent_job_name),
            producer=PRODUCER,
        )
    }
    if error:
        run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
            message=error, programmingLanguage="python", producer=PRODUCER
        )

    event = RunEvent(
        eventType=event_type,
        eventTime=_now(),
        run=Run(runId=task_run_id(dagster_run_id, task_name), facets=run_facets),
        job=Job(namespace=JOB_NAMESPACE, name=task_name, facets=_job_type_facets("TASK")),
        inputs=input_datasets,
        outputs=output_datasets,
        producer=PRODUCER,
    )
    get_client().emit(event)


def with_lineage(task_name: str, *, inputs: list[str] | None = None, outputs: list[str] | None = None):
    """Decorator for a Dagster asset compute fn: emits START/COMPLETE/FAIL as an OpenLineage task."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(context, *args, **kwargs):
            run_id = context.run_id
            emit_task_event(
                dagster_run_id=run_id,
                parent_job_name=JOB_NAME,
                task_name=task_name,
                event_type=RunState.START,
                inputs=inputs,
            )
            try:
                result = fn(context, *args, **kwargs)
            except Exception as e:
                emit_task_event(
                    dagster_run_id=run_id,
                    parent_job_name=JOB_NAME,
                    task_name=task_name,
                    event_type=RunState.FAIL,
                    inputs=inputs,
                    outputs=outputs,
                    error=str(e),
                )
                raise
            emit_task_event(
                dagster_run_id=run_id,
                parent_job_name=JOB_NAME,
                task_name=task_name,
                event_type=RunState.COMPLETE,
                inputs=inputs,
                outputs=outputs,
            )
            return result

        return wrapper

    return decorator


JOB_NAME = "daily_refresh_job"
