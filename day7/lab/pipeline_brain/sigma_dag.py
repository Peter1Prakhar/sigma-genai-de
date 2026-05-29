"""
Sigma DataTech Transaction Analytics DAG

Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions.
This DAG defines three Python tasks:
  - extract_bronze: ingest raw CSVs to Bronze Parquet
  - transform_silver: clean, enrich, deduplicate to Silver
  - build_gold: generate the 3 Gold aggregation tables

SLAs / Alerts:
  - Tasks are expected to complete before the SLA window.
  - SLA misses trigger an alert email and warning log.
  - Task failures trigger centralized failure logging.
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

ALERT_EMAILS = ["alerts@sigmadatatech.com"]

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    error_message = context.get('exception', 'Unknown error')
    logging.error(
        f"Dag: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}"
    )


def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    subject = f"SLA Miss Alert: {dag_id}"
    html_content = (
        f"<p>DAG <strong>{dag_id}</strong> missed its SLA for execution date "
        f"<strong>{execution_date}</strong>.</p>"
    )
    logging.warning(f"SLA miss for DAG {dag_id} at {execution_date}")
    send_email(to=ALERT_EMAILS, subject=subject, html_content=html_content)


def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet."""
    ti = context['ti']
    execution_date = context['execution_date']
    logging.info(
        f"[extract_bronze] Starting task_id={ti.task_id} execution_date={execution_date}"
    )
    # TODO: implement CSV ingestion logic for raw transactions and merchants.
    logging.info(
        f"[extract_bronze] Completed task_id={ti.task_id} execution_date={execution_date}"
    )


def transform_silver(**context):
    """Clean, enrich, and deduplicate records into Silver."""
    ti = context['ti']
    execution_date = context['execution_date']
    logging.info(
        f"[transform_silver] Starting task_id={ti.task_id} execution_date={execution_date}"
    )
    # TODO: implement Silver layer transformation with type casting, quality filters,
    #       deduplication, and merchant enrichment.
    logging.info(
        f"[transform_silver] Completed task_id={ti.task_id} execution_date={execution_date}"
    )


def build_gold(**context):
    """Generate the three Gold aggregation tables."""
    ti = context['ti']
    execution_date = context['execution_date']
    logging.info(
        f"[build_gold] Starting task_id={ti.task_id} execution_date={execution_date}"
    )
    # TODO: implement Gold aggregations for merchant_performance, customer_ltv,
    #       and daily_summary.
    logging.info(
        f"[build_gold] Completed task_id={ti.task_id} execution_date={execution_date}"
    )


dag = DAG(
    dag_id='sigma_transaction_pipeline',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions",
    tags=['sigma', 'transactions', 'daily'],
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
)

# Task Definitions
extract_bronze_task = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

# Task Dependencies
extract_bronze_task >> transform_silver_task >> build_gold_task
