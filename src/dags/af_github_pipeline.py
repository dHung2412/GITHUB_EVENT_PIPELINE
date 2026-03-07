"""
GitHub Events Pipeline DAG
====================================
Orchestrates the full Bronze -> Silver -> Gold pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_events_pipeline',
    default_args=default_args,
    description='Hourly GitHub events processing pipeline',
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['github', 'etl', 'hourly'],
)

# Cấu hình chung cho Spark Submit
SPARK_SUBMIT_BASE = (
    "spark-submit "
    "--master local[2] "
    "--conf spark.driver.memory=2g "
    "--conf spark.executor.memory=2g "
    "--jars '/opt/airflow/dags/spark_jobs/utils/jars/*'"
)

# Task 1: Bronze to Silver (Spark)
bronze_to_silver = BashOperator(
    task_id='transform_bronze_to_silver',
    bash_command=f"""
    {SPARK_SUBMIT_BASE} \
        --name bronze_to_silver_job \
        /opt/airflow/dags/spark_jobs/bronze_silver/process_bronze_to_silver.py
    """,
    dag=dag,
)

# Task 2, 3, 4: dbt run - Models
DBT_BASE = "cd /opt/dbt_project && dbt"

dbt_run_silver = BashOperator(
    task_id='dbt_run_silver_models',
    bash_command=f'{DBT_BASE} run --select silver+ --target dev',
    dag=dag,
)

dbt_run_gold = BashOperator(
    task_id='dbt_run_gold_models',
    bash_command=f'{DBT_BASE} run --select gold+ --target dev',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test_data_quality',
    bash_command=f'{DBT_BASE} test',
    dag=dag,
)

# Task dependencies
bronze_to_silver >> dbt_run_silver >> dbt_run_gold >> dbt_test
