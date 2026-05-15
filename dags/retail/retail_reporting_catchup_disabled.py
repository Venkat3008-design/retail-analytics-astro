from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "analytics_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="retail_reporting_catchup_disabled_daily",
    description="Runs daily retail reporting workflow with catchup disabled.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["retail", "reporting", "catchup-disabled", "governance"],
    doc_md="""
    ### Retail Reporting Catchup Disabled DAG

    This DAG simulates a daily retail reporting workflow.

    Governance standards:
    - Owner: Analytics Engineering
    - Retries: 1
    - Retry delay: 3 minutes
    - Catchup: Disabled
    - Max active runs: 1
    - Tags: retail, reporting, catchup-disabled, governance

    Purpose:
    This DAG demonstrates how Airflow behaves when a DAG has
    a historical start date but catchup is disabled.

    With catchup disabled, Airflow avoids creating all missed
    historical DAG runs and focuses on the latest scheduled interval.
    """,
)
def retail_reporting_catchup_disabled_daily():

    @task
    def collect_reporting_inputs(**context):
        logical_date = context["logical_date"]
        print("Collecting reporting input data")
        print(f"Processing logical date: {logical_date}")
        print("Collected sales summary, store summary, and product metrics")

    @task
    def generate_daily_retail_report(**context):
        logical_date = context["logical_date"]
        print("Generating daily retail report")
        print(f"Processing logical date: {logical_date}")
        print("Created daily revenue, orders, and performance report")

    @task
    def publish_report(**context):
        logical_date = context["logical_date"]
        print("Publishing retail report")
        print(f"Processing logical date: {logical_date}")
        print("Published report to analytics dashboard")

    collect = collect_reporting_inputs()
    generate = generate_daily_retail_report()
    publish = publish_report()

    collect >> generate >> publish


retail_reporting_catchup_disabled_daily()
