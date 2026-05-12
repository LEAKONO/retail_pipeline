"""
retail_pipeline_dag.py
Orchestrates the full retail analytics pipeline.
Schedule: Daily at 6am
Tasks: ingestion → dbt run → dbt test
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/home/leakono/Engineer/retail_pipeline"
DBT_DIR     = f"{PROJECT_DIR}/dbt_retail"
VENV_PYTHON = f"{PROJECT_DIR}/venv/bin/python"
DBT_BIN     = f"{PROJECT_DIR}/venv/bin/dbt"

default_args = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="retail_pipeline",
    description="Daily retail analytics pipeline — ingestion + dbt",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "ingestion", "dbt"],
) as dag:

    run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} main.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test",
    )

    run_ingestion >> dbt_run >> dbt_test
