"""
DuckDB + ADBC Flight SQL Demo
==============================
Demonstrates DuckDB querying Elasticsearch via Arrow Flight SQL,
using zero-copy Arrow columnar transport.
"""

import os
import sys
import time

import adbc_driver_flightsql.dbapi as flight_sql
import duckdb
from tabulate import tabulate


def wait_for_flight_sql(host: str, port: int, max_retries: int = 30) -> None:
    """Wait for the Arrow Flight SQL server to be ready."""
    uri = f"grpc://{host}:{port}"
    print(f"Waiting for Flight SQL server at {uri}...")
    for attempt in range(max_retries):
        try:
            conn = flight_sql.connect(uri)
            conn.close()
            print("Flight SQL server is ready.\n")
            return
        except Exception:
            time.sleep(2)
    print("ERROR: Flight SQL server not available after retries.", file=sys.stderr)
    sys.exit(1)


def run_demo(host: str, port: int) -> None:
    """Run the DuckDB + Flight SQL demo queries."""
    uri = f"grpc://{host}:{port}"

    # Connect to Flight SQL via ADBC
    flight_conn = flight_sql.connect(uri)

    # Connect DuckDB (in-memory)
    db = duckdb.connect()

    print("=" * 60)
    print("  DuckDB + Arrow Flight SQL + Elasticsearch Demo")
    print("=" * 60)

    queries = [
        (
            "1. Browse all orders (SELECT *)",
            "SELECT * FROM ecommerce ORDER BY order_date LIMIT 10",
        ),
        (
            "2. Revenue by country",
            """
            SELECT country, COUNT(*) as order_count, SUM(total_price) as revenue
            FROM ecommerce
            GROUP BY country
            ORDER BY revenue DESC
            """,
        ),
        (
            "3. Revenue by category",
            """
            SELECT category, COUNT(*) as order_count, SUM(total_price) as revenue
            FROM ecommerce
            GROUP BY category
            ORDER BY revenue DESC
            """,
        ),
        (
            "4. Average order value by payment method",
            """
            SELECT payment_method, COUNT(*) as orders, AVG(total_price) as avg_order_value
            FROM ecommerce
            GROUP BY payment_method
            ORDER BY avg_order_value DESC
            """,
        ),
        (
            "5. Order status distribution",
            """
            SELECT status, COUNT(*) as ct
            FROM ecommerce
            GROUP BY status
            ORDER BY ct DESC
            """,
        ),
        (
            "6. Top 5 customers by spend",
            """
            SELECT customer_name, country, SUM(total_price) as total_spend
            FROM ecommerce
            GROUP BY customer_name, country
            ORDER BY total_spend DESC
            LIMIT 5
            """,
        ),
    ]

    for title, sql in queries:
        print(f"\n{'─' * 60}")
        print(f"  {title}")
        print(f"{'─' * 60}")
        print(f"  SQL: {' '.join(sql.split())}\n")

        try:
            # Execute via Flight SQL, get Arrow table
            cursor = flight_conn.cursor()
            cursor.execute(sql.strip())
            arrow_table = cursor.fetch_arrow_table()
            cursor.close()

            # Register Arrow table in DuckDB and query it
            db.register("result", arrow_table)
            result = db.execute("SELECT * FROM result").fetchall()
            columns = [desc[0] for desc in db.description]

            print(tabulate(result, headers=columns, tablefmt="rounded_grid"))
            print(f"  ({len(result)} rows)")

            db.unregister("result")

        except Exception as e:
            print(f"  ERROR: {e}")

    # DuckDB-specific analytics on top of Flight SQL data
    print(f"\n{'─' * 60}")
    print("  7. DuckDB cross-query analytics (join two Flight SQL results)")
    print(f"{'─' * 60}")

    try:
        # Fetch full dataset once
        cursor = flight_conn.cursor()
        cursor.execute("SELECT * FROM ecommerce")
        full_data = cursor.fetch_arrow_table()
        cursor.close()

        db.register("ecommerce", full_data)

        # Run a DuckDB-native analytical query
        analytics_sql = """
            WITH country_stats AS (
                SELECT
                    country,
                    COUNT(*) as orders,
                    SUM(total_price) as revenue,
                    AVG(total_price) as avg_order
                FROM ecommerce
                GROUP BY country
            )
            SELECT
                country,
                orders,
                ROUND(revenue, 2) as revenue,
                ROUND(avg_order, 2) as avg_order,
                ROUND(100.0 * revenue / SUM(revenue) OVER (), 1) as pct_revenue
            FROM country_stats
            ORDER BY revenue DESC
        """
        print(f"  SQL (DuckDB-native on Arrow data):\n  {' '.join(analytics_sql.split())}\n")

        result = db.execute(analytics_sql).fetchall()
        columns = [desc[0] for desc in db.description]
        print(tabulate(result, headers=columns, tablefmt="rounded_grid"))
        print(f"  ({len(result)} rows)")

        db.unregister("ecommerce")

    except Exception as e:
        print(f"  ERROR: {e}")

    flight_conn.close()
    db.close()

    print(f"\n{'=' * 60}")
    print("  Demo complete!")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    host = os.environ.get("FLIGHT_SQL_HOST", os.environ.get("ES_HOST", "flight-sql-duckdb"))
    port = int(os.environ.get("FLIGHT_SQL_PORT", "32010"))

    wait_for_flight_sql(host, port)
    run_demo(host, port)
