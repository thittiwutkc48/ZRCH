import pandas as pd
import sqlite3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_drive_opertor import list_folder,download_file
from postgres_opertor import create_postgres_engine
import os

def load_data_to_local():
    data_ingest_id = "1AAdi3CYw9yRdNLiG1Epm_TffHDBEMVVW"
    scope = ['https://www.googleapis.com/auth/drive']
    service_account_file = "key/data-project-test-001-26c6f0773834.json"
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)
    drive_service = build('drive', 'v3', credentials=credentials)
    list_file = list_folder(drive_service,parent_folder_id=data_ingest_id)
    local_fol = "/tmp"
    os.makedirs(local_fol,exist_ok=True)
    engine = create_postgres_engine()
    for file in list_file :
        file_name =  f'{local_fol}/{file["name"]}'
        print(file["id"],file["name"])
        download_file(drive_service,file["id"], file_name)

        if file["name"].split(".")[1] == "db" :
            sqlite_conn = sqlite3.connect(file_name)
            sqlite_cursor = sqlite_conn.cursor()
            query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
            sqlite_cursor.execute(query_tables)
            tables = sqlite_cursor.fetchall()
            for table in tables:
                table_name = table[0]
                print(f"Processing table: {table_name}")
                query_data = f"SELECT * FROM [{table_name}];"
                df = pd.read_sql_query(query_data, sqlite_conn)
                df.columns = df.columns.str.replace(' ', '').str.lower()
                table_name = table_name.replace(" ", "").lower()
                df.to_sql(table_name, engine, if_exists='replace', index=False)

        elif file["name"].split(".")[1] == "csv" or  file["name"].split(".")[1] == "xlsx"  :
            if file["name"].split(".")[1] == "csv" :
                df = pd.read_csv(file_name)
            elif file["name"].split(".")[1] == "xlsx"  :
                df = pd.read_excel(file_name)
            table_name = (file["name"].split(".")[0]).replace(" ", "").lower()
            print(f"Processing table: {table_name}")
            df.columns = df.columns.str.replace(' ', '').str.lower()
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        os.remove(file_name)

    sqlite_cursor.close()
    sqlite_conn.close()
    engine.dispose()
    print("Ingest data to PostgreSQL Success")