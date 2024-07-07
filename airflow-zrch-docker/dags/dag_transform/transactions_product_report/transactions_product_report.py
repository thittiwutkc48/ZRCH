from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from dag_transform.transactions_product_report.default_config import default_config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['thittiwut.n@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  
    'retry_delay': timedelta(minutes=5),  
}
config = Variable.get("transactions_product_report", default_var=default_config)

dag = DAG(
    'transactions_product_report',
    default_args=default_args,
    description='create view table transactions_product_report monthly process',
    schedule_interval='0 10 1 * *', 
    start_date=datetime(2024, 1, 1, 9, 0), 
    catchup=False,  
)

run_postgres_query = PostgresOperator(
    task_id='run_postgres_query',
    postgres_conn_id='postgres_conn', 
    sql=config["sql"],
    dag=dag,
)

run_postgres_query
