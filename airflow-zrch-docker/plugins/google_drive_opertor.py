import os
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

def upload_file(drive_service,file_path, folder_id=None):
    """Upload a file to Google Drive."""
    file_name = os.path.basename(file_path)
    file_metadata = {
        'name': file_name,
        'parents': [folder_id] if folder_id else []
    }
    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    print(f"File '{file_name}' uploaded successfully with ID: {file.get('id')}")
    return file.get('id')

def list_folder(drive_service,parent_folder_id=None):
    """List folders and files in Google Drive."""
    query = f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else "trashed=false"
    results = drive_service.files().list(
        q=query,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    if not items:
        print("No folders or files found in Google Drive.")
    else:
        print("Folders and files in Google Drive:")
        return items
       
def download_file(drive_service,file_id, dest_path):
    """Download a file from Google Drive."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

    print(f"File downloaded successfully to {dest_path}")
