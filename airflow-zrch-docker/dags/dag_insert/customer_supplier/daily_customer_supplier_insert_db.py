from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from dag_insert.customer_supplier.default_config import default_config

config = Variable.get("daily_customer_supplier_load_clean_encrypt_data", default_var=default_config)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['thittiwut.n@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  
    'retry_delay': timedelta(minutes=5),  
}

dag = DAG(
    'daily_customer_supplier_load_clean_encrypt_data',
    default_args=default_args,
    description='A DAG to truncate, load, clean, and encrypt data from source to customer/supplier tables',
    schedule_interval='0 9 * * *',  # Run every day at 9 AM
    start_date=datetime(2024, 1, 1, 9, 0),
    catchup=False,
)

truncate_customers_task = PostgresOperator(
    task_id='truncate_customers_task',
    postgres_conn_id="postgres_conn",
    sql=config["truncate_customer_sql"],
    dag=dag,
)

truncate_suppliers_task = PostgresOperator(
    task_id='truncate_suppliers_task',
    postgres_conn_id="postgres_conn",
    sql=config["truncate_supplier_sql"],
    dag=dag,
)

extract_customer_task = PostgresOperator(
    task_id='extract_customer_task',
    postgres_conn_id="postgres_conn",
    sql=config["sql_customer"],
    dag=dag,
)

extract_supplier_task = PostgresOperator(
    task_id='extract_supplier_task',
    postgres_conn_id="postgres_conn",
    sql=config["sql_supplier"],
    dag=dag,
)

truncate_customers_task >> extract_customer_task
truncate_suppliers_task >> extract_supplier_task
