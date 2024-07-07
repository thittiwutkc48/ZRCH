from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from dag_ingestion.customer_transactions_ingestion.default_config import default_config
from dag_ingestion.customer_transactions_ingestion.helper.helper import process_json
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['thittiwut.n@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  
    'retry_delay': timedelta(minutes=5),  
}
config = Variable.get("customer_transactions_ingestion", default_var=default_config)

with DAG(
    dag_id="customer_transactions_ingestion",
    default_args=default_args,
    description=' ',
    start_date=datetime(2024, 6, 1),
    schedule_interval= "0 9 1 * *",
    catchup=False ,
    max_active_runs=1
) as dag:
    
    run_postgres_delete_query = PostgresOperator(
        task_id='run_postgres_delete_query',
        postgres_conn_id='postgres_conn', 
        sql=config["delete_query"],
        dag=dag,
    )

    task_process_json = PythonOperator(
        task_id='task_process_json',
        python_callable=process_json,
        op_kwargs={
        'data_ingest_id':config['data_ingest_id'],
        'data_processed_id':config['data_processed_id'],
        'service_account_file':config['service_account_file'],
        'date_execute':config['date_execute'],
        'schema_name':config['schema_name'],
        'table_name':config['table_name'] 
        },
        dag=dag,
    )

run_postgres_delete_query >> task_process_json


