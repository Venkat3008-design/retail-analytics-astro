from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="retail_sales_catchup_enabled_daily",
    description="Processes historical retail sales data with catchup enabled.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    tags=["retail", "sales", "catchup", "governance"],
    doc_md="""
    ### Retail Sales Catchup Enabled DAG

    This DAG simulates daily historical retail sales processing.

    Governance standards:
    - Owner: Data Engineering
    - Retries: 2
    - Retry delay: 5 minutes
    - Catchup: Enabled
    - Max active runs: 2
    - Tags: retail, sales, catchup, governance

    Purpose:
    This DAG demonstrates how Airflow creates historical DAG runs
    when a DAG has a past start date and catchup is enabled.
    """,
)
def retail_sales_catchup_enabled_daily():

    @task
    def extract_sales_data(**context):
        logical_date = context["logical_date"]
        print("Starting sales extraction")
        print(f"Processing logical date: {logical_date}")
        print("Extracted retail sales data from source system")

    @task
    def transform_sales_data(**context):
        logical_date = context["logical_date"]
        print("Starting sales transformation")
        print(f"Processing logical date: {logical_date}")
        print("Cleaned sales records")
        print("Calculated daily revenue, order count, and store totals")

    @task
    def load_sales_data(**context):
        logical_date = context["logical_date"]
        print("Starting sales load")
        print(f"Processing logical date: {logical_date}")
        print("Loaded processed sales data into analytics warehouse")

    extract = extract_sales_data()
    transform = transform_sales_data()
    load = load_sales_data()

    extract >> transform >> load


retail_sales_catchup_enabled_daily()
