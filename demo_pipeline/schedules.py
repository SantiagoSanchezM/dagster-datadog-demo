from dagster import AssetSelection, DefaultScheduleStatus, ScheduleDefinition, define_asset_job

daily_refresh_job = define_asset_job(name="daily_refresh_job", selection=AssetSelection.all())

daily_refresh_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
