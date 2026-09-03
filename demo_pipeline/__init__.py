from dagster import Definitions, load_asset_checks_from_modules, load_assets_from_modules

from . import assets
from .resources import duckdb_resource
from .schedules import daily_refresh_schedule
from .sensors import (
    openlineage_job_failure_sensor,
    openlineage_job_started_sensor,
    openlineage_job_success_sensor,
)

all_assets = load_assets_from_modules([assets])
all_checks = load_asset_checks_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={"duckdb_resource": duckdb_resource},
    schedules=[daily_refresh_schedule],
    sensors=[
        openlineage_job_started_sensor,
        openlineage_job_success_sensor,
        openlineage_job_failure_sensor,
    ],
)
