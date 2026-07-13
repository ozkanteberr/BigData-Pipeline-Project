"""
Bootstrap Apache Superset with the olist_dw Star Schema database,
virtual joined datasets, 7 BI charts, and a Business BI Dashboard.

This script talks to the Superset REST API v1.

Usage:
  python scripts/bootstrap_dw_superset.py
"""

from __future__ import annotations

import argparse
import sys
import time

import requests

DEFAULT_SUPERSET_URL = "http://localhost:8088"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"

SPARK_DW_DB_NAME = "Spark Olist DW"
SPARK_DW_SQLALCHEMY_URI = "hive://spark-thriftserver:10000/olist_dw"


# ── helpers ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap Superset with Olist Star Schema DW connection, charts, and dashboard."
    )
    parser.add_argument(
        "--superset-url",
        default=DEFAULT_SUPERSET_URL,
        help=f"Superset base URL. Default: {DEFAULT_SUPERSET_URL}",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=12,
        help="Number of times to retry reaching Superset before giving up.",
    )
    return parser.parse_args()


class SupersetClient:
    """Minimal Superset REST API v1 wrapper."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.access_token: str | None = None
        self.csrf_token: str | None = None

    # ── auth ──────────────────────────────────────────────────────

    def login(self, username: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db"},
        )
        resp.raise_for_status()
        self.access_token = resp.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
        self._refresh_csrf()

    def _refresh_csrf(self) -> None:
        resp = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        resp.raise_for_status()
        self.csrf_token = resp.json()["result"]
        self.session.headers.update({"X-CSRFToken": self.csrf_token})

    # ── generic helpers ───────────────────────────────────────────

    def _post(self, endpoint: str, json_body: dict) -> dict:
        resp = self.session.post(f"{self.base_url}{endpoint}", json=json_body)
        if resp.status_code == 409:
            print("  -> Already exists (409), skipping.")
            return resp.json() if resp.text else {}
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error details: {resp.text}")
            raise e
        return resp.json()

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        resp = self.session.get(f"{self.base_url}{endpoint}", params=params)
        resp.raise_for_status()
        return resp.json()

    # ── database ──────────────────────────────────────────────────

    def find_database(self, name: str) -> int | None:
        data = self._get("/api/v1/database/", params={"q": f"(filters:!((col:database_name,opr:eq,value:'{name}')))"})
        results = data.get("result", [])
        return results[0]["id"] if results else None

    def create_database(self, name: str, sqlalchemy_uri: str) -> int:
        existing_id = self.find_database(name)
        if existing_id:
            print(f"  -> Database '{name}' already exists (id={existing_id}).")
            return existing_id
        data = self._post("/api/v1/database/", {
            "database_name": name,
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        })
        db_id = data.get("id")
        print(f"  -> Created database '{name}' (id={db_id}).")
        return db_id

    # ── dataset ───────────────────────────────────────────────────

    def find_dataset(self, table_name: str, database_id: int) -> int | None:
        data = self._get("/api/v1/dataset/", params={
            "q": f"(filters:!((col:table_name,opr:eq,value:'{table_name}'),(col:database,opr:rel_o_m,value:{database_id})))"
        })
        results = data.get("result", [])
        return results[0]["id"] if results else None

    def create_virtual_dataset(self, name: str, sql: str, database_id: int) -> int:
        existing_id = self.find_dataset(name, database_id)
        if existing_id:
            print(f"  -> Virtual Dataset '{name}' already exists (id={existing_id}).")
            return existing_id
        data = self._post("/api/v1/dataset/", {
            "database": database_id,
            "table_name": name,
            "sql": sql,
        })
        ds_id = data.get("id")
        print(f"  -> Created virtual dataset '{name}' (id={ds_id}).")
        return ds_id

    # ── chart ─────────────────────────────────────────────────────

    def create_chart(self, chart_def: dict) -> int:
        data = self._post("/api/v1/chart/", chart_def)
        chart_id = data.get("id")
        print(f"  -> Created chart '{chart_def.get('slice_name')}' (id={chart_id}).")
        return chart_id

    # ── dashboard ─────────────────────────────────────────────────

    def find_dashboard(self, slug: str) -> int | None:
        data = self._get("/api/v1/dashboard/", params={"q": f"(filters:!((col:slug,opr:eq,value:'{slug}')))"})
        results = data.get("result", [])
        return results[0]["id"] if results else None

    def create_dashboard(self, title: str, slug: str, chart_ids: list[int]) -> int:
        existing_id = self.find_dashboard(slug)
        if existing_id:
            print(f"  -> Dashboard '{title}' already exists (id={existing_id}).")
            return existing_id
        data = self._post("/api/v1/dashboard/", {
            "dashboard_title": title,
            "slug": slug,
            "published": True,
        })
        dash_id = data.get("id")
        print(f"  -> Created dashboard '{title}' (id={dash_id}).")
        return dash_id


# ── queries & charts ──────────────────────────────────────────────────

SQL_DW_JOINED = """
SELECT 
  f.order_item_key,
  f.order_id,
  f.price,
  f.freight_value,
  f.item_revenue,
  f.delivery_time_days,
  f.review_score,
  o.order_status,
  o.order_purchase_timestamp,
  p.product_category_name_english,
  s.seller_id,
  s.seller_city,
  s.seller_state,
  c.customer_city,
  c.customer_state
FROM olist_dw.fact_order_items f
LEFT JOIN olist_dw.dim_orders o ON f.order_id = o.order_id
LEFT JOIN olist_dw.dim_products p ON f.product_id = p.product_id
LEFT JOIN olist_dw.dim_sellers s ON f.seller_id = s.seller_id
LEFT JOIN olist_dw.dim_customers c ON f.customer_id = c.customer_id
"""

SQL_PAYMENTS_JOINED = """
SELECT 
  pay.payment_type,
  pay.payment_value,
  o.order_purchase_timestamp
FROM olist_dw.dim_payments pay
LEFT JOIN olist_dw.dim_orders o ON pay.order_id = o.order_id
"""


def build_chart_definitions(dw_joined_id: int, payments_joined_id: int) -> list[dict]:
    return [
        {
            "slice_name": "1. Monthly Revenue Trends",
            "description": "Total order item revenue aggregated by month",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"revenue","expressionType":"SQL","sqlExpression":"SUM(item_revenue)"}],"groupby":[],"time_column":"order_purchase_timestamp","time_grain_sqla":"P1M","row_limit":50000}',
        },
        {
            "slice_name": "2. Revenue by Product Category (Top 15)",
            "description": "Top product categories by total item revenue",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"revenue","expressionType":"SQL","sqlExpression":"SUM(item_revenue)"}],"groupby":["product_category_name_english"],"order_desc":true,"row_limit":15}',
        },
        {
            "slice_name": "3. Top-Performing Sellers (Top 15)",
            "description": "Top 15 sellers by total item revenue generated",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"revenue","expressionType":"SQL","sqlExpression":"SUM(item_revenue)"}],"groupby":["seller_id"],"order_desc":true,"row_limit":15}',
        },
        {
            "slice_name": "4. Sales by Customer State",
            "description": "Total revenue generated by customer state",
            "viz_type": "pie",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"revenue","expressionType":"SQL","sqlExpression":"SUM(item_revenue)"}],"groupby":["customer_state"],"row_limit":30}',
        },
        {
            "slice_name": "5. Average Delivery Time by State (Days)",
            "description": "Average actual delivery days grouped by customer state",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"avg_delivery_days","expressionType":"SQL","sqlExpression":"AVG(delivery_time_days)"}],"groupby":["customer_state"],"order_desc":true,"row_limit":30}',
        },
        {
            "slice_name": "6. Payment Method Trends (Value)",
            "description": "Evolution of payment methods over time by value",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": payments_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"total_value","expressionType":"SQL","sqlExpression":"SUM(payment_value)"}],"groupby":["payment_type"],"time_column":"order_purchase_timestamp","time_grain_sqla":"P1M","row_limit":5000}',
        },
        {
            "slice_name": "7. Average Review Score by Product Category (Top 15)",
            "description": "Average review score per product category",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dw_joined_id,
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"avg_review_score","expressionType":"SQL","sqlExpression":"AVG(review_score)"}],"groupby":["product_category_name_english"],"order_desc":true,"row_limit":15}',
        },
    ]


# ── main ──────────────────────────────────────────────────────────────

def wait_for_superset(base_url: str, retries: int) -> None:
    url = f"{base_url}/health"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                print(f"Superset is ready at {base_url}")
                return
        except requests.ConnectionError:
            pass
        print(f"Waiting for Superset... (attempt {attempt}/{retries})")
        time.sleep(10)
    print("ERROR: Superset did not become healthy in time.", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    args = parse_args()
    base_url = args.superset_url

    print("--- Waiting for Superset ---")
    wait_for_superset(base_url, args.retries)

    print("\n--- Authenticating ---")
    client = SupersetClient(base_url)
    client.login(ADMIN_USERNAME, ADMIN_PASSWORD)
    print("  -> Logged in as admin.")

    print("\n--- Creating Spark DW Database Connection ---")
    db_id = client.create_database(SPARK_DW_DB_NAME, SPARK_DW_SQLALCHEMY_URI)

    print("\n--- Creating Virtual Joined Datasets ---")
    dw_joined_id = client.create_virtual_dataset("dw_olist_joined", SQL_DW_JOINED, db_id)
    payments_joined_id = client.create_virtual_dataset("dw_payments_joined", SQL_PAYMENTS_JOINED, db_id)

    print("\n--- Creating 7 Business BI Charts ---")
    chart_defs = build_chart_definitions(dw_joined_id, payments_joined_id)
    chart_ids: list[int] = []
    for chart_def in chart_defs:
        chart_ids.append(client.create_chart(chart_def))

    print("\n--- Creating Business BI Dashboard ---")
    client.create_dashboard(
        title="Olist Business Intelligence (Star Schema)",
        slug="olist-bi",
        chart_ids=chart_ids,
    )

    print("\n[OK] Phase 2 Bootstrap complete!")
    print(f"   Open Superset at {base_url} and check the 'Olist Business Intelligence (Star Schema)' dashboard.")


if __name__ == "__main__":
    main()
