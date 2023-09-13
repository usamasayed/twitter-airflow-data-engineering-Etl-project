from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import twitter_etl

default_args = {
    'owner':'airflow',
    'start_date':dt(2023, 9, 12),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    description='etl_deg_project',
    default_args= default_args
)

run_twitter_etl = PythonOperator(
    task_id = 'complete_twitter_etl',
    python_callable= twitter_etl,
    dag=dag,
)

run_twitter_etl