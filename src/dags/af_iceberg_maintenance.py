from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'iceberg_table_maintenance',
    default_args=default_args,
    description='Unified Maintenance for Bronze & Silver Iceberg Tables',
    schedule='@daily', # Chạy hàng ngày, task weekly sẽ check date bên trong script hoặc dùng schedule riêng
    catchup=False,
    max_active_runs=1,
    tags=['iceberg', 'maintenance', 'daily', 'weekly'],
)

# Cấu hình chung cho Spark Submit
SPARK_BASE = """
    spark-submit \
        --master local[2] \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=1g \
        --jars "/opt/airflow/dags/spark_jobs/utils/jars/*" \
"""

def create_maintenance_task(task_id, script_path, is_weekly=False):
    # Nếu là weekly, chúng ta có thể thêm logic trigger hoặc đơn giản là để scheduler của Airflow lo
    return BashOperator(
        task_id=task_id,
        bash_command=f"{SPARK_BASE} /opt/airflow/dags/spark_jobs/maintenance/{script_path}",
        dag=dag,
    )

# Daily Tasks
bronze_daily = create_maintenance_task('bronze_daily_compaction', 'maintenance_bronze_by_day.py')
silver_daily = create_maintenance_task('silver_daily_compaction', 'maintenance_silver_by_day.py')

# Weekly Tasks (Trong thực tế bạn có thể tách DAG weekly riêng, 
# nhưng gom vào đây và check execution_date cũng là một cách)
bronze_weekly = create_maintenance_task('bronze_weekly_cleanup', 'maintenance_bronze_by_week.py')
silver_weekly = create_maintenance_task('silver_weekly_cleanup', 'maintenance_silver_by_week.py')

# Thứ tự thực hiện (Tùy chọn: chạy song song hoặc nối tiếp)
[bronze_daily, silver_daily]
