import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_drive_operator import list_folder,download_file,upload_to_drive
from postgres_operator import create_postgres_engine
import os

def process_csv(data_ingest_id,data_processed_id,service_account_file,date_execute,schema_name,table_name) :
    scope = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)
    drive_service = build('drive', 'v3', credentials=credentials)
    list_file = list_folder(drive_service,parent_folder_id=data_ingest_id)
    local_fol = "/tmp"
    dest_path = f"{local_fol}/{table_name}.csv"
    os.makedirs(local_fol,exist_ok=True)
    engine = create_postgres_engine()
    all_data_frames = []

    for file in list_file:
        file_name = f'{local_fol}/{file["name"]}'
        print(file["id"], file["name"])
        download_file(drive_service, file["id"], file_name)
        df = pd.read_csv(file_name)
        all_data_frames.append(df)
        os.remove(file_name)

    if all_data_frames:
        combined_df = pd.concat(all_data_frames, ignore_index=True)
    else:
        print("No data to process")
        return

    combined_df = combined_df[combined_df['price'] != 'invalid_price']
    combined_df['price'] = pd.to_numeric(combined_df['price'], errors='coerce') 
    combined_df = combined_df[combined_df['price'] >= 0]  
    combined_df['product_name_tmp'] = combined_df['product_id'].str.replace('P00', 'Product ')
    combined_df.drop(columns=['product_name'], inplace=True)
    combined_df.rename(columns={'product_name_tmp': 'product_name'}, inplace=True)
    combined_df['updated_timestamp'] = date_execute

    column_order = ['product_id', 'product_name', 'category', 'price','updated_timestamp']
    combined_df = combined_df[column_order]
    combined_df.sort_values(by='product_id', ascending=True, inplace=True)

    try:
        combined_df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists='replace', index=False)
        print(f"Data ingested into PostgreSQL table '{schema_name}.{table_name}' successfully.")
    except Exception as e:
        print(f"Error ingesting data into PostgreSQL: {e}")

    combined_df.to_csv(dest_path, index=False)
    print(f"Combined data saved to CSV file: {dest_path}")

    upload_to_drive(drive_service, dest_path, data_processed_id)
    os.remove(dest_path)
    print("upload_to_drive succeeded")

