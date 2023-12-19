from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

from src.components.data_etl import run_data_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 16, 12),
    'email': ['harsh220902@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'reddit_dag',
    default_args=default_args,
    description='My first airflow'
)

run_etl = PythonOperator(
    task_id='complete_reddit_etl',
    python_callable=run_data_etl,
    dag=dag
)

run_etl