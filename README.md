# Dagster + Datadog Data Observability demo

A self-contained Dagster OSS deployment (Postgres + code server + webserver + daemon,
all via `docker compose`) running a realistic 5-asset ELT pipeline over DuckDB, instrumented
with custom [OpenLineage](https://openlineage.io/) events sent directly to
[Datadog Data Observability](https://docs.datadoghq.com/data_observability/jobs_monitoring/openlineage/)
(lineage + data quality).

## What's here

- **`daily_refresh_job`**: `raw_customers` / `raw_orders` -> `cleaned_orders` -> `enriched_orders` -> `revenue_by_region`,
  seeded with realistic messy data (duplicates, bad amounts, orphan foreign keys).
- Two Dagster asset checks (`non_negative_revenue`, `no_orphan_customers`) feeding a
  `dataQualityAssertions` OpenLineage facet.
- `raw_orders` has a 25% chance of raising, to simulate an upstream outage and demo how a
  failed run shows up in Datadog.
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

## Notes

- `.env` is gitignored — never commit real credentials.
- `dagster_home/` runtime artifacts (run storage cache, logs, telemetry) are gitignored;
  only `dagster_home/dagster.yaml` (Postgres storage config) is tracked.
