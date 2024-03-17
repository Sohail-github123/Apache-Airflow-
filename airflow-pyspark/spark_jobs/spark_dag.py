from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime ,timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 15),
}

dag = DAG(
    "pyspark_dag",
    default_args=default_args,
    description="simple pyspark",
    schedule_interval=None,
)

csv_to_parquet = BashOperator(
    task_id="csv_to_parquet",
    bash_command='spark-submit /home/sohail/airflow/dags/spark_jobs/csv_to_parquet.py ',
    dag=dag,
)

add_columns= BashOperator(
    task_id="add_columns",
    bash_command='spark-submit /home/sohail/airflow/dags/spark_jobs/add_column.py ',
    dag=dag,
)

csv_to_parquet >> add_columns
