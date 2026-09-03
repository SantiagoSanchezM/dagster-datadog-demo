import random
from pathlib import Path

import pandas as pd
from dagster import AssetCheckResult, AssetExecutionContext, asset, asset_check
from dagster_duckdb import DuckDBResource

from .openlineage_integration import with_lineage

SEED_DATA_DIR = Path(__file__).parent / "seed_data"

# RNG for the intermittent demo failure below - independent of the fabricated
# seed data, which is static and committed to the repo (see seed_data/README.md)
_flaky_rng = random.SystemRandom()


def _simulate_flaky_cleaning_step(failure_rate: float = 0.25) -> None:
    """Randomly simulate the cleaning step failing, for demo purposes."""
    if _flaky_rng.random() < failure_rate:
        raise RuntimeError("Lock timeout acquiring staging.orders while deduplicating raw.orders")


@asset(group_name="raw", compute_kind="python")
@with_lineage("raw_customers", outputs=["raw.customers"])
def raw_customers(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Customer records landed from a CRM export (fabricated, committed under seed_data/)."""
    # keep_default_na=False: "NA" is a real region code here (North America), not a null marker
    df = pd.read_csv(SEED_DATA_DIR / "customers.csv", keep_default_na=False)
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM df")
    context.add_output_metadata({"num_rows": len(df)})


@asset(group_name="raw", compute_kind="python")
@with_lineage("raw_orders", outputs=["raw.orders"])
def raw_orders(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Order records landed from an OLTP order service (fabricated, committed under seed_data/).

    Deliberately messy - includes duplicate rows, negative amounts, and orphan
    customer_ids - to make the cleaning step and data-quality checks meaningful.
    """
    df = pd.read_csv(SEED_DATA_DIR / "orders.csv")
    with duckdb_resource.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE OR REPLACE TABLE raw.orders AS SELECT * FROM df")
    context.add_output_metadata({"num_rows": len(df)})


@asset(group_name="staging", compute_kind="duckdb", deps=[raw_orders])
@with_lineage("cleaned_orders", inputs=["raw.orders"], outputs=["staging.orders"])
def cleaned_orders(context: AssetExecutionContext, duckdb_resource: DuckDBResource) -> None:
    """Dedupe orders and drop rows with non-positive amounts."""
    _simulate_flaky_cleaning_step()
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
