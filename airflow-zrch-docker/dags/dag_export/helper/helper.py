import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
from datetime import datetime

def extract_data(year_months,engine):
    data_frames = {}
    for year_month in year_months:
        query = f"""
            SELECT * 
            FROM public.productsalesamountbymonth 
            WHERE yearmonth = '{year_month}'
        """
        try :
            df = pd.read_sql(query, engine)
            data_frames[year_month] = df 
        except :
            pass

    return data_frames

def create_excel(data_frames, file_path):
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        for sheet_name, df in data_frames.items():
            date_obj = datetime.strptime(sheet_name, "%Y-%m")
            df.to_excel(writer, sheet_name=str(date_obj.strftime('%b')) , index=False)

def upload_to_drive(file_path, folder_id):
    scope = ['https://www.googleapis.com/auth/drive']
    service_account_file = "key/data-project-test-001-26c6f0773834.json"
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scope)
    drive_service = build('drive', 'v3', credentials=credentials)
    file_name = os.path.basename(file_path)
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    existing_files = drive_service.files().list(q=query, fields="files(id)").execute()
    files = existing_files.get('files', [])

    if files:
        for file in files:
            try:
                drive_service.files().delete(fileId=file['id']).execute()
                print(f"Deleted existing file '{file_name}' with ID: {file['id']}")
            except Exception as e:
                print(f"Failed to delete existing file '{file_name}': {str(e)}")

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    print(f"File '{file_name}' uploaded successfully with ID: {file.get('id')}")


def get_quarter_months(date_execute):
    date_obj = datetime.strptime(date_execute, "%Y-%m")
    year = date_obj.year
    month = date_obj.month

    if month in [1, 2, 3]:
        quarter = 1
        months = ['01', '02', '03']
    elif month in [4, 5, 6]:
        quarter = 2
        months = ['04', '05', '06']
    elif month in [7, 8, 9]:
        quarter = 3
        months = ['07', '08', '09']
    elif month in [10, 11, 12]:
        quarter = 4
        months = ['10', '11', '12']
    
    year_months = [f"{year}-{month}" for month in months]
    first_month =datetime.strptime(year_months[0], '%Y-%m')
    third_month =datetime.strptime(year_months[2], '%Y-%m')
    file_name = str(year) + str(first_month.strftime('%b')) + "-" + str(third_month.strftime('%b'))

    return quarter, year_months ,file_name
