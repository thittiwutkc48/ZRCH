from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dag_ingest.helper.load_data_to_local import load_data_to_local

DAG_NAME = "data_ingest_postgres"
default_args = {"owner": "airflow"}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval= "0 13 1 * *",
    catchup=False ,
    max_active_runs=1
) as dag:
    
    task_load_data_postgres_db = PythonOperator(
        task_id='load_data_to_local',
        python_callable=load_data_to_local,
        dag=dag,
    )

load_data_to_local


