# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# from dag_ingest.helper.load_data_to_local import load_data_to_local

# DAG_NAME = "data_ingest_postgres"
# default_args = {"owner": "airflow"}

# def process_csv() :
#     data_ingest_id = "1cvf7Z3JUlGLrsDx4uOAah8-xgYaCCJ11"
#     service_account_file = "key/data-project-test-001-26c6f0773834.json"

#     scope = ['https://www.googleapis.com/auth/drive']
#     credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)
#     drive_service = build('drive', 'v3', credentials=credentials)
#     list_file = list_folder(drive_service,parent_folder_id=data_ingest_id)
#     local_fol = "/tmp"
#     os.makedirs(local_fol,exist_ok=True)
#     engine = create_postgres_engine()
    
#     for file in list_file:
#         file_name = f'{local_fol}/{file["name"]}'
#         print(file["id"], file["name"])
#         download_file(drive_service, file["id"], file_name)
#         df = pd.read_csv(file_name)
#         combined_df = combined_df.append(df, ignore_index=True) 
#         os.remove(file_name)

#     combined_df.to_sql('customer_product_transactions', con=engine, if_exists='append', index=False)
#     print("Data has been successfully processed and inserted into the PostgreSQL table 'customer_product_transactions'")

# with DAG(
#     dag_id=DAG_NAME,
#     default_args=default_args,
#     start_date=datetime(2024, 6, 1),
#     schedule_interval= "0 13 1 * *",
#     catchup=False ,
#     max_active_runs=1
# ) as dag:
    
#     task_load_data_postgres_db = PythonOperator(
#         task_id='load_data_to_local',
#         python_callable=load_data_to_local,
#         dag=dag,
#     )

# load_data_to_local


