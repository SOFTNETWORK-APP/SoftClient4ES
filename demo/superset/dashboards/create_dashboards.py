"""
Create demo charts and dashboards in Superset via REST API.
Runs inside the Superset container after init.
"""

import json
import sys
import time

import requests

SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin123"


def authenticate(session):
    """Authenticate and return access token + CSRF token."""
    # Get CSRF token
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")

    # Login
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
    )
    resp.raise_for_status()
    access_token = resp.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Get CSRF token (again, now authenticated)
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    csrf_token = resp.json()["result"]
    session.headers.update({"X-CSRFToken": csrf_token})

    return session


def get_dataset_id(session, table_name="ecommerce"):
    """Find the dataset ID for the given table name."""
    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/", params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]})})
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if not results:
        return None
    return results[0]["id"]


def create_chart(session, dataset_id, name, viz_type, params):
    """Create a chart and return its ID."""
    params["viz_type"] = viz_type
    params["datasource"] = f"{dataset_id}__table"

    payload = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": name,
        "viz_type": viz_type,
        "params": json.dumps(params),
    }

    resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload)
    if resp.status_code == 422:
        print(f"    (chart '{name}' may already exist)")
        return None
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    print(f"    Created chart '{name}' (id={chart_id})")
    return chart_id


def simple_metric(column, aggregate, label):
    """Build a SIMPLE metric definition."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label,
    }


def sql_metric(expression, label):
    """Build a SQL metric definition."""
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
    }


def count_star_metric(label="Orders"):
    """COUNT(*) metric."""
    return {
        "expressionType": "SQL",
        "sqlExpression": "COUNT(*)",
        "label": label,
    }


def create_all_charts(session, dataset_id):
    """Create all demo charts, return list of (chart_id, name) tuples."""
    charts = []

    # 1. Revenue by Country — horizontal bar
    cid = create_chart(session, dataset_id, "Revenue by Country", "echarts_timeseries_bar", {
        "x_axis": "country",
        "metrics": [simple_metric("total_price", "SUM", "Revenue")],
        "groupby": [],
        "order_desc": True,
        "row_limit": 10,
        "show_legend": False,
        "x_axis_sort": "Revenue",
        "x_axis_sort_asc": False,
    })
    if cid:
        charts.append(cid)

    # 2. Orders by Category — pie chart
    cid = create_chart(session, dataset_id, "Orders by Category", "pie", {
        "metric": count_star_metric(),
        "groupby": ["category"],
        "row_limit": 10,
        "show_labels": True,
        "label_type": "key_percent",
        "number_format": "SMART_NUMBER",
        "donut": False,
        "show_legend": True,
    })
    if cid:
        charts.append(cid)

    # 3. Order Status Distribution — pie chart (donut)
    cid = create_chart(session, dataset_id, "Order Status", "pie", {
        "metric": count_star_metric(),
        "groupby": ["status"],
        "row_limit": 10,
        "show_labels": True,
        "label_type": "key_value",
        "number_format": "SMART_NUMBER",
        "donut": True,
        "show_legend": True,
    })
    if cid:
        charts.append(cid)

    # 4. Avg Order Value by Payment Method — bar
    cid = create_chart(session, dataset_id, "Avg Order Value by Payment", "echarts_timeseries_bar", {
        "x_axis": "payment_method",
        "metrics": [simple_metric("total_price", "AVG", "Avg Order Value")],
        "groupby": [],
        "order_desc": True,
        "row_limit": 10,
        "show_legend": False,
        "y_axis_format": ",.2f",
    })
    if cid:
        charts.append(cid)

    # 5. Revenue by Category — bar
    cid = create_chart(session, dataset_id, "Revenue by Category", "echarts_timeseries_bar", {
        "x_axis": "category",
        "metrics": [simple_metric("total_price", "SUM", "Revenue")],
        "groupby": [],
        "order_desc": True,
        "row_limit": 10,
        "show_legend": False,
        "x_axis_sort": "Revenue",
        "x_axis_sort_asc": False,
        "y_axis_format": ",.2f",
    })
    if cid:
        charts.append(cid)

    # 6. Top Customers — table
    cid = create_chart(session, dataset_id, "Top Customers by Spend", "table", {
        "metrics": [simple_metric("total_price", "SUM", "Total Spend"), count_star_metric()],
        "groupby": ["customer_name"],
        "order_desc": True,
        "timeseries_limit_metric": simple_metric("total_price", "SUM", "Total Spend"),
        "row_limit": 10,
        "all_columns": [],
        "include_time": False,
        "order_by_cols": [],
        "table_timestamp_format": "smart_date",
    })
    if cid:
        charts.append(cid)

    return charts


def build_dashboard_layout(chart_ids):
    """Build a simple grid layout for the dashboard."""
    # Superset dashboard layout uses a component tree with rows/columns
    # Each chart is placed in a CHART component inside a ROW
    components = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": "E-Commerce Analytics (Elasticsearch)"},
        },
    }

    # Place charts in rows of 2
    row_idx = 0
    for i in range(0, len(chart_ids), 2):
        row_id = f"ROW-row{row_idx}"
        row_children = []

        for j, cid in enumerate(chart_ids[i : i + 2]):
            chart_comp_id = f"CHART-chart{i + j}"
            col_id = f"COLUMN-col{i + j}"
            row_children.append(col_id)
            components[col_id] = {
                "type": "COLUMN",
                "id": col_id,
                "children": [chart_comp_id],
                "meta": {"width": 6, "background": "BACKGROUND_TRANSPARENT"},
            }
            components[chart_comp_id] = {
                "type": "CHART",
                "id": chart_comp_id,
                "children": [],
                "meta": {
                    "width": 6,
                    "height": 50,
                    "chartId": cid,
                    "sliceName": "",
                },
            }

        components[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        components["GRID_ID"]["children"].append(row_id)
        row_idx += 1

    return components


def create_dashboard(session, chart_ids):
    """Create the demo dashboard."""
    position_json = build_dashboard_layout(chart_ids)

    payload = {
        "dashboard_title": "E-Commerce Analytics",
        "slug": "ecommerce-demo",
        "position_json": json.dumps(position_json),
        "published": True,
    }

    resp = session.post(f"{SUPERSET_URL}/api/v1/dashboard/", json=payload)
    if resp.status_code == 422:
        print("    (dashboard may already exist)")
        return None
    resp.raise_for_status()
    dashboard_id = resp.json()["id"]
    print(f"    Created dashboard 'E-Commerce Analytics' (id={dashboard_id})")
    return dashboard_id


def associate_charts_with_dashboard(session, chart_ids, dashboard_id):
    """Link each chart to the dashboard (required for Superset to display them)."""
    for cid in chart_ids:
        resp = session.put(
            f"{SUPERSET_URL}/api/v1/chart/{cid}",
            json={"dashboards": [dashboard_id]},
        )
        if not resp.ok:
            print(f"    WARNING: Could not associate chart {cid} with dashboard {dashboard_id}")


def main():
    print("==> Creating demo charts and dashboard...")

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})

    # Wait for Superset to be ready
    for _ in range(30):
        try:
            resp = session.get(f"{SUPERSET_URL}/health")
            if resp.status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(2)

    try:
        authenticate(session)
    except Exception as e:
        print(f"    ERROR: Authentication failed: {e}", file=sys.stderr)
        sys.exit(1)

    dataset_id = get_dataset_id(session)
    if not dataset_id:
        print("    ERROR: 'ecommerce' dataset not found. Skipping dashboard creation.", file=sys.stderr)
        sys.exit(1)

    print(f"    Found dataset 'ecommerce' (id={dataset_id})")

    chart_ids = create_all_charts(session, dataset_id)

    if chart_ids:
        dashboard_id = create_dashboard(session, chart_ids)
        if dashboard_id:
            associate_charts_with_dashboard(session, chart_ids, dashboard_id)
        print(f"==> Done! {len(chart_ids)} charts created.")
    else:
        print("    No charts created (may already exist).")


if __name__ == "__main__":
    main()
