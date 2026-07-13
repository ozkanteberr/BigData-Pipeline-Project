"""
Bootstrap Apache Superset with a Spark ThriftServer connection,
starter charts, and a dashboard for the Olist Big Data Pipeline.

This script talks to the Superset REST API v1.

Usage:
  python scripts/bootstrap_superset.py
  python scripts/bootstrap_superset.py --superset-url http://localhost:8088
"""

from __future__ import annotations

import argparse
import sys
import time

import requests

DEFAULT_SUPERSET_URL = "http://localhost:8088"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"

SPARK_DB_NAME = "Spark Olist"
SPARK_SQLALCHEMY_URI = "hive://spark-thriftserver:10000/olist"


# ── helpers ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap Superset with Olist Spark connection, charts, and dashboard."
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
            print(f"  ↳ Already exists (409), skipping.")
            return resp.json() if resp.text else {}
        resp.raise_for_status()
        return resp.json()

    def _put(self, endpoint: str, json_body: dict) -> dict:
        resp = self.session.put(f"{self.base_url}{endpoint}", json=json_body)
        resp.raise_for_status()
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

    def create_dataset(self, table_name: str, database_id: int, schema: str = "olist") -> int:
        existing_id = self.find_dataset(table_name, database_id)
        if existing_id:
            print(f"  -> Dataset '{table_name}' already exists (id={existing_id}).")
            return existing_id
        data = self._post("/api/v1/dataset/", {
            "database": database_id,
            "table_name": table_name,
            "schema": schema,
        })
        ds_id = data.get("id")
        print(f"  -> Created dataset '{table_name}' (id={ds_id}).")
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
        position_data = generate_position_json(chart_ids)
        if existing_id:
            print(f"  -> Dashboard '{title}' already exists (id={existing_id}), updating charts layout...")
            self._put(f"/api/v1/dashboard/{existing_id}", {
                "position_json": position_data
            })
            return existing_id
        data = self._post("/api/v1/dashboard/", {
            "dashboard_title": title,
            "slug": slug,
            "position_json": position_data,
            "published": True,
        })
        dash_id = data.get("id")
        print(f"  -> Created dashboard '{title}' (id={dash_id}).")
        return dash_id


def generate_position_json(chart_ids: list[int]) -> str:
    import json
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"]
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": []
        }
    }
    
    for i, cid in enumerate(chart_ids):
        chart_key = f"CHART-{cid}"
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": cid
            }
        }
        row_idx = i // 2 + 1
        row_key = f"ROW-{row_idx}"
        if row_key not in position:
            position[row_key] = {
                "type": "ROW",
                "id": row_key,
                "children": []
            }
            position["GRID_ID"]["children"].append(row_key)
        position[row_key]["children"].append(chart_key)
        
    return json.dumps(position)


# ── chart definitions ─────────────────────────────────────────────────

def build_chart_definitions(dataset_ids: dict[str, int]) -> list[dict]:
    """Return a list of chart payload dicts for Superset API v1."""
    return [
        {
            "slice_name": "Orders Over Time",
            "description": "Monthly order count over time",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": dataset_ids["orders"],
            "datasource_type": "table",
            "params": '{"metrics":["count"],"groupby":[],"time_column":"order_purchase_timestamp","time_grain_sqla":"P1M","row_limit":10000}',
        },
        {
            "slice_name": "Payment Method Distribution",
            "description": "Distribution of payment types across all orders",
            "viz_type": "pie",
            "datasource_id": dataset_ids["order_payments"],
            "datasource_type": "table",
            "params": '{"metrics":["count"],"groupby":["payment_type"],"row_limit":100}',
        },
        {
            "slice_name": "Customer Geography — Top 20 Cities",
            "description": "Top 20 cities by customer count",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dataset_ids["customers"],
            "datasource_type": "table",
            "params": '{"metrics":["count"],"groupby":["customer_city"],"order_desc":true,"row_limit":20}',
        },
        {
            "slice_name": "Revenue by Product Category",
            "description": "Total revenue per product category (top 15)",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": dataset_ids["order_items"],
            "datasource_type": "table",
            "params": '{"metrics":[{"label":"total_revenue","expressionType":"SQL","sqlExpression":"SUM(price)"}],"groupby":["product_id"],"order_desc":true,"row_limit":15}',
        },
        {
            "slice_name": "Order Status Breakdown",
            "description": "Breakdown of orders by their current status",
            "viz_type": "pie",
            "datasource_id": dataset_ids["orders"],
            "datasource_type": "table",
            "params": '{"metrics":["count"],"groupby":["order_status"],"row_limit":50}',
        },
    ]


# ── main ──────────────────────────────────────────────────────────────

def wait_for_superset(base_url: str, retries: int) -> None:
    """Block until Superset health endpoint responds."""
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

    # 1. Wait for Superset
    print("--- Waiting for Superset ---")
    wait_for_superset(base_url, args.retries)

    # 2. Authenticate
    print("\n--- Authenticating ---")
    client = SupersetClient(base_url)
    client.login(ADMIN_USERNAME, ADMIN_PASSWORD)
    print("  -> Logged in as admin.")

    # 3. Create database connection
    print("\n--- Creating Spark Database Connection ---")
    db_id = client.create_database(SPARK_DB_NAME, SPARK_SQLALCHEMY_URI)

    # 4. Create datasets (one per Olist table)
    print("\n--- Creating Datasets ---")
    table_names = [
        "customers",
        "geolocation",
        "order_items",
        "order_payments",
        "order_reviews",
        "orders",
        "products",
        "sellers",
        "product_category_name_translation",
    ]
    dataset_ids: dict[str, int] = {}
    for table in table_names:
        dataset_ids[table] = client.create_dataset(table, db_id)

    # 5. Create charts
    print("\n--- Creating Charts ---")
    chart_defs = build_chart_definitions(dataset_ids)
    chart_ids: list[int] = []
    for chart_def in chart_defs:
        chart_ids.append(client.create_chart(chart_def))

    # 6. Create dashboard
    print("\n--- Creating Dashboard ---")
    client.create_dashboard(
        title="Olist E-Commerce Overview",
        slug="olist-overview",
        chart_ids=chart_ids,
    )

    print("\n[OK] Bootstrap complete!")
    print(f"   Open Superset at {base_url} and check the 'Olist E-Commerce Overview' dashboard.")


if __name__ == "__main__":
    main()
