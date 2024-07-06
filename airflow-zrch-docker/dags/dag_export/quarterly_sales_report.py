from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from postgres_opertor import create_postgres_engine
import os
from airflow.models import Variable
from dag_export.helper.helper import extract_data,create_excel,upload_to_drive,get_quarter_months

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
    'quarterly_sales_report',
    default_args=default_args,
    description='Generate and upload quarterly sales report to Google Drive/param:export_param1',
    schedule_interval='0 9 1 1,4,7,10 *',  # Run on the 1st day of each quarter at 9 AM
    start_date=days_ago(1),
    catchup=False
)

def generate_and_upload_report(year_month,local_fol,folder_id,engine):
    if year_month == " " :
        current_date = datetime.now()
        year_month = current_date.strftime("%Y-%m")

    os.makedirs(local_fol,exist_ok=True)
    quarter, year_months ,file_name = get_quarter_months(year_month)
    data_frames = extract_data(year_months,engine)

    if not len(data_frames[year_month]) :
        print(f"Data Not Available for quarter:{quarter} list_month:{year_months}")
        return   

    file_path = local_fol + f"/{file_name}.xlsx"
    create_excel(data_frames,file_path)
    upload_to_drive(file_path, folder_id)
    os.remove(file_path)
    print("Quarterly Sales Report To Google Drive Done")

generate_and_upload_report = PythonOperator(
    task_id='generate_and_upload_report',
    python_callable=generate_and_upload_report,
    provide_context=True,
    op_kwargs={
        'year_month': Variable.get("export_param1"),
        'local_fol': "/tmp",
        'folder_id': Variable.get("export_drive_id"),
        'engine' : create_postgres_engine()
    },
    dag=dag )

generate_and_upload_report