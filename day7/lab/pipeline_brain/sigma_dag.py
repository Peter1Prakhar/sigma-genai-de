from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import logging
import json

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

dag = DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions",
    tags=['sigma', 'transactions', 'daily'],
    sla_miss_callback=lambda context: send_email(
        to=["alerts@sigmadatatech.com"],
        subject="SLA Miss Alert",
        html_content=f"DAG {context['dag'].dag_id} missed its SLA for execution date {context['execution_date']}"
    )
)

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"Dag: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet"""
    logging.info(f"Starting Bronze layer extraction for {context['execution_date']}")
    # Ingestion logic here
    logging.info(f"Completed Bronze layer extraction for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver"""
    logging.info(f"Starting Silver layer transformation for {context['execution_date']}")
    # Transformation logic here
    logging.info(f"Completed Silver layer transformation for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing

def build_gold(**context):
    """Generate the 3 Gold aggregation tables"""
    logging.info(f"Starting Gold layer build for {context['execution_date']}")
    # Aggregation logic here
    logging.info(f"Completed Gold layer build for {context['execution_date']}")
    raise Exception("Simulated failure")  # For testing

# Task Definitions
t1 = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

t3 = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

# Task Dependencies
t1 >> t2 >> t3
