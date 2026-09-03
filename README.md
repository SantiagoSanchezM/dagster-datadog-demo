# Dagster + Datadog Data Observability demo

A self-contained Dagster OSS deployment (Postgres + code server + webserver + daemon,
all via `docker compose`) running a realistic 5-asset ELT pipeline over DuckDB, instrumented
with custom [OpenLineage](https://openlineage.io/) events sent directly to
[Datadog Data Observability](https://docs.datadoghq.com/data_observability/jobs_monitoring/openlineage/)
(lineage + data quality).

## What's here

- **`daily_refresh_job`**: `raw_customers` / `raw_orders` -> `cleaned_orders` -> `enriched_orders` -> `revenue_by_region`,
  loaded from fabricated CSV seed data (`demo_pipeline/seed_data/`) with realistic messiness
  (duplicates, bad amounts, orphan foreign keys) baked in.
- Two Dagster asset checks (`non_negative_revenue`, `no_orphan_customers`) feeding a
  `dataQualityAssertions` OpenLineage facet.
- `cleaned_orders` has a 25% chance of raising, to simulate a downstream lock timeout and demo
  how a failed task/run shows up in both Jobs Monitoring and Lineage in Datadog.
- A schedule running the job every 5 minutes (`demo_pipeline/schedules.py`).
- `demo_pipeline/openlineage_integration.py` + `demo_pipeline/sensors.py`: emit OpenLineage
  `RunEvent`s (START/COMPLETE/FAIL) at both the job (DAG) and per-asset (task) level, straight
  to Datadog's intake, authenticated via `OPENLINEAGE_API_KEY`.

## Setup

1. Copy `.env.example` to `.env` and fill in your own Datadog API key / site:
   ```
   cp .env.example .env
   ```
   `DD_SITE` / the intake host in `OPENLINEAGE_URL` should match your org's Datadog site
   (`datadoghq.com`, `datadoghq.eu`, `us5.datadoghq.com`, etc).

2. Build and start everything:
   ```
   docker compose up -d --build
   ```

3. Open the Dagster UI at http://localhost:3000. The schedule starts running automatically
   every 5 minutes, or trigger a run manually:
   ```
   docker compose exec dagster-webserver dagster job launch -j daily_refresh_job
   ```

4. Check Datadog under **Data Observability > Jobs Monitoring / Lineage** for `daily_refresh_job`
   and its asset-level tasks, including the `data_quality_validation` node and any failed runs.

## How the OpenLineage instrumentation works

Datadog's Data Job Monitoring and Lineage products are both built on
[OpenLineage](https://openlineage.io/)'s data model: a **Job** (a task or DAG) has **Runs**
(one execution, with a `runId` and a `START`/`COMPLETE`/`FAIL` lifecycle), and each Run declares
the **Datasets** it reads (`inputs`) and writes (`outputs`). Datadog builds Jobs Monitoring from
the Job/Run side of that model and the Lineage graph from the Dataset edges those runs declare.
Full facet/endpoint reference: [Custom Jobs using OpenLineage](https://docs.datadoghq.com/data_observability/jobs_monitoring/openlineage/).

This repo emits that model **directly** — no Datadog Agent involved. The `openlineage-python`
client is configured purely through env vars (`OPENLINEAGE_URL`, `OPENLINEAGE_API_KEY` in `.env`),
which point it at Datadog's intake (`https://data-obs-intake.<site>/api/v1/lineage`) and add the
`Authorization: Bearer <key>` header automatically.

There are two instrumentation layers, both in `demo_pipeline/`:

1. **Per-asset (task) lineage** — `openlineage_integration.py`'s `with_lineage(...)` decorator
   wraps each asset's compute function: it emits a `START` event before the function runs and a
   `COMPLETE` or `FAIL` event after, declaring that asset's input/output tables as OpenLineage
   Datasets (namespace `duckdb://demo-pipeline/warehouse`, name `schema.table`).

   ```python
   # demo_pipeline/assets.py
   @asset(group_name="staging", compute_kind="duckdb", deps=[raw_orders])
   @with_lineage("cleaned_orders", inputs=["raw.orders"], outputs=["staging.orders"])
   def cleaned_orders(context, duckdb_resource):
       ...
   ```

   On `COMPLETE`, the decorator also runs a `SELECT COUNT(*)` against each declared output table
   and attaches it as an `outputStatistics` facet (`rowCount`) on that Dataset - this is what makes
   real record counts show up on dataset nodes in the Lineage graph, since Datadog only displays
   "basic stats such as row or column count" when a facet actually supplies them. Dataset identity
   (the node existing at all) doesn't require a live DB connection, but any stats shown on it do -
   we compute those ourselves here since DuckDB isn't one of Datadog's natively-scanned warehouses
   (Snowflake, BigQuery, Redshift, Postgres, etc.). A deeper "Quality Monitoring" experience
   (freshness/volume anomaly detection) is a separate Datadog product built around those native
   connectors; getting that would mean pointing Datadog at a real supported warehouse rather than
   this demo's local DuckDB file.

   That one line is enough for `cleaned_orders` to show up as a Job in Datadog with a `raw.orders
   -> cleaned_orders -> staging.orders` edge in Lineage, and a `FAIL` run in Jobs Monitoring
   whenever the simulated lock-timeout fires.

2. **Job-level (DAG) lifecycle + data quality** — `sensors.py` defines three Dagster
   [run status sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)
   that emit the top-level `daily_refresh_job` DAG's `START`/`COMPLETE`/`FAIL` events. On success,
   the sensor also reads the results of Dagster's native asset checks
   (`no_orphan_customers`, `non_negative_revenue`) for that run and emits them as a
   `dataQualityAssertions` facet on a dedicated `data_quality_validation` task node — so a failed
   check shows up as a data quality issue on the relevant dataset in Lineage, not just a log line.

Other facets used along the way: `jobType` (marks each event as a `TASK` or `DAG`, required by
Datadog), `parent` (links every task run back to its parent DAG run), `errorMessage` (attached on
`FAIL` events), and a custom `tags` facet carrying `env` (see `OL_ENV_TAG` in `.env`) since
`openlineage-python`'s pinned version here doesn't ship a generated `TagsJobFacet` class.

## Notes

- `.env` is gitignored — never commit real credentials.
- `dagster_home/` runtime artifacts (run storage cache, logs, telemetry) are gitignored;
  only `dagster_home/dagster.yaml` (Postgres storage config) is tracked.
