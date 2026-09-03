import random

import pandas as pd
from dagster import AssetCheckResult, AssetExecutionContext, asset, asset_check
from dagster_duckdb import DuckDBResource
from faker import Faker

from .openlineage_integration import with_lineage

fake = Faker()
Faker.seed(42)
random.seed(42)

REGIONS = ["NA", "EMEA", "APAC", "LATAM"]

# separate RNG so this stays truly intermittent across runs, independent of the
# fixed seed above (which only exists to keep the synthetic data reproducible)
_flaky_rng = random.SystemRandom()


def _simulate_flaky_upstream_source(failure_rate: float = 0.25) -> None:
    """Randomly simulate the upstream order-service timing out, for demo purposes."""
    if _flaky_rng.random() < failure_rate:
        raise RuntimeError("Timed out fetching orders from the upstream OLTP order service")


@asset(group_name="raw", compute_kind="python")
@with_lineage("raw_customers", outputs=["raw.customers"])
def raw_customers(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Synthetic customer records, as if landed from a CRM export."""
    rows = [
        {
            "customer_id": i,
            "customer_name": fake.name(),
            "region": random.choice(REGIONS),
            "signup_date": fake.date_between(start_date="-2y", end_date="today"),
        }
        for i in range(1, 501)
    ]
    df = pd.DataFrame(rows)
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM df")
    context.add_output_metadata({"num_rows": len(df)})


@asset(group_name="raw", compute_kind="python")
@with_lineage("raw_orders", outputs=["raw.orders"])
def raw_orders(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Synthetic order records, as if landed from an OLTP order service."""
    _simulate_flaky_upstream_source()
    rows = []
    for i in range(1, 5001):
        # sprinkle in some dirty data to make cleaning + data-quality checks meaningful
        amount = round(random.uniform(5, 500), 2)
        if random.random() < 0.02:
            amount = -amount  # bad data: negative order amount
        rows.append(
            {
                "order_id": i,
                "customer_id": random.randint(1, 520),  # some orphan customer_ids on purpose
                "order_amount": amount,
                "order_date": fake.date_between(start_date="-1y", end_date="today"),
            }
        )
    # duplicate a handful of rows to simulate upstream dupes
    rows += random.sample(rows, 25)
    df = pd.DataFrame(rows)
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE OR REPLACE TABLE raw.orders AS SELECT * FROM df")
    context.add_output_metadata({"num_rows": len(df)})


@asset(group_name="staging", compute_kind="duckdb", deps=[raw_orders])
@with_lineage("cleaned_orders", inputs=["raw.orders"], outputs=["staging.orders"])
def cleaned_orders(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Dedupe orders and drop rows with non-positive amounts."""
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute(
            """
            CREATE OR REPLACE TABLE staging.orders AS
            SELECT DISTINCT order_id, customer_id, order_amount, order_date
            FROM raw.orders
            WHERE order_amount > 0
            """
        )
        num_rows = conn.execute("SELECT COUNT(*) FROM staging.orders").fetchone()[0]
    context.add_output_metadata({"num_rows": num_rows})


@asset(group_name="staging", compute_kind="duckdb", deps=[cleaned_orders, raw_customers])
@with_lineage("enriched_orders", inputs=["staging.orders", "raw.customers"], outputs=["staging.enriched_orders"])
def enriched_orders(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Join cleaned orders against the customer dimension."""
    with duckdb_resource.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE staging.enriched_orders AS
            SELECT o.order_id, o.order_amount, o.order_date,
                   c.customer_id, c.customer_name, c.region
            FROM staging.orders o
            INNER JOIN raw.customers c ON o.customer_id = c.customer_id
            """
        )
        num_rows = conn.execute("SELECT COUNT(*) FROM staging.enriched_orders").fetchone()[0]
    context.add_output_metadata({"num_rows": num_rows})


@asset(group_name="analytics", compute_kind="duckdb", deps=[enriched_orders])
@with_lineage("revenue_by_region", inputs=["staging.enriched_orders"], outputs=["analytics.revenue_by_region"])
def revenue_by_region(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Final aggregate table consumed by a downstream BI dashboard."""
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.execute(
            """
            CREATE OR REPLACE TABLE analytics.revenue_by_region AS
            SELECT region, COUNT(*) AS num_orders, ROUND(SUM(order_amount), 2) AS total_revenue
            FROM staging.enriched_orders
            GROUP BY region
            ORDER BY total_revenue DESC
            """
        )
        preview = conn.execute("SELECT * FROM analytics.revenue_by_region").fetch_df()
    context.add_output_metadata({"preview": preview.to_markdown()})


@asset_check(asset=enriched_orders)
def no_orphan_customers(duckdb_resource: DuckDBResource) -> AssetCheckResult:
    """Data-quality check: every enriched order must resolve to a known customer."""
    with duckdb_resource.get_connection() as conn:
        orphan_count = conn.execute(
            """
            SELECT COUNT(*) FROM staging.orders o
            LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        ).fetchone()[0]
    return AssetCheckResult(passed=orphan_count == 0, metadata={"orphan_order_count": orphan_count})


@asset_check(asset=revenue_by_region)
def non_negative_revenue(duckdb_resource: DuckDBResource) -> AssetCheckResult:
    """Data-quality check: aggregated revenue per region must never be negative."""
    with duckdb_resource.get_connection() as conn:
        min_revenue = conn.execute("SELECT MIN(total_revenue) FROM analytics.revenue_by_region").fetchone()[0]
    return AssetCheckResult(passed=min_revenue is not None and min_revenue >= 0, metadata={"min_revenue": min_revenue})
