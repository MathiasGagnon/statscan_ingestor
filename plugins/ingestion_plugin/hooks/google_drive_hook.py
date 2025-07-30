import logging
import os

from airflow.hooks.base import BaseHook

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

from base_storage_hook import BaseStorageHook

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS")
SCOPES = ['https://www.googleapis.com/auth/drive']

class GoogleDriveHook(BaseHook, BaseStorageHook):

    def __init__(self):
        self.service = self._get_service()

    def _get_service(self):
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        return build("drive", "v3", credentials=creds)

    def list_files(self, folder_id: str):
        query = f"'{folder_id}' in parents and trashed = false"
        results = self.service.files().list(
            q=query,
            pageSize=100,
            fields="nextPageToken, files(id, name)"
        ).execute()
        items = results.get('files', [])

        if not items:
            logging.info('No files found.')
        else:
            return items

    def copy_file(self, source_identifier: str, destination_identifier: str, new_name: str = None):
        body = {"parents": [destination_identifier]}
        if new_name:
            body["name"] = new_name
        return self.service.files().copy(fileId=source_identifier, body=body).execute()

    def get_or_create_folder(self, folder_name: str, parent_folder_id: str):
        query = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_folder_id}' in parents and trashed = false"
        )
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if items:
            return items[0]['id']  # Folder already exists
        else:
            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id]
            }
            folder = self.service.files().create(body=folder_metadata, fields="id").execute()
            return folder['id']

    def get_file_by_name(self, file_name: str, parent_folder_id: str):
        query = (
            f"name='{file_name}' and '{parent_folder_id}' in parents and trashed = false"
        )
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        return items[0] if items else None

    def file_exists(self, file_name: str, parent_folder_id: str):
        return self.get_file_by_name(file_name, parent_folder_id) is not None

