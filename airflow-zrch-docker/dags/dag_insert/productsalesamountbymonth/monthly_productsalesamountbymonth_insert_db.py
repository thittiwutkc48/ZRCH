from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from dag_insert.productsalesamountbymonth.default_config import default_config


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['thittiwut.n@gmail.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  
    'retry_delay': timedelta(minutes=5),  
}
config = Variable.get("monthly_productsalesamountbymonth_insert_db", default_var=default_config)

dag = DAG(
    'monthly_productsalesamountbymonth_insert_db',
    default_args=default_args,
    description='Insert data table productsalesamountbymonth Monthly process starting on the 1st day of each month /param:insert_param2',
    schedule_interval='0 9 1 * *', 
    start_date=datetime(2024, 1, 1, 9, 0), 
    catchup=False,  
)

run_postgres_delete_query = PostgresOperator(
    task_id='run_postgres_delete_query',
    postgres_conn_id='postgres_conn', 
    sql=config["delete_query"],
    dag=dag,
)

run_postgres_insert_query = PostgresOperator(
    task_id='run_postgres_insert_query',
    postgres_conn_id='postgres_conn', 
    sql=config["insert_query"],
    dag=dag,
)

run_postgres_delete_query >> run_postgres_insert_query
