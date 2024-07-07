import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_drive_operator import list_folder,download_file,upload_to_drive
from postgres_operator import create_postgres_engine
import os
import json

def process_json(data_ingest_id,data_processed_id,service_account_file,date_execute,schema_name,table_name) :
    scope = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)
    drive_service = build('drive', 'v3', credentials=credentials)
    list_file = list_folder(drive_service,parent_folder_id=data_ingest_id)
    local_fol = "/tmp"
    date_str = str(date_execute).replace("-","")
    dest_path = f"{local_fol}/{table_name}_{date_str}.csv"
    os.makedirs(local_fol,exist_ok=True)
    engine = create_postgres_engine()
    all_data_frames = []

    for file in list_file:
        file_name = f'{local_fol}/{file["name"]}'
        print(file["id"], file["name"])
        download_file(drive_service, file["id"], file_name)
        
        with open(file_name, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        all_data_frames.append(df)
        os.remove(file_name)

    if all_data_frames:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
    else:
        print("No data to process")
        return
    combined_df.drop_duplicates(inplace=True)
    combined_df.sort_values(by='timestamp', ascending=True, inplace=True)
    combined_df['partition_date'] = date_execute
    try:
        combined_df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='append', index=False)
        print(f"Data ingested into PostgreSQL table '{schema_name}.{table_name}' successfully.")
    except Exception as e:
        print(f"Error ingesting data into PostgreSQL: {e}")

    combined_df.to_csv(dest_path, index=False)
    print(f"Combined data saved to CSV file: {dest_path}")

    upload_to_drive(drive_service, dest_path, data_processed_id)
    os.remove(dest_path)
    print("upload_to_drive succeeded")
