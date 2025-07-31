import logging
import os
from typing import Optional

from airflow.hooks.base import BaseHook

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from googleapiclient.discovery import Resource

from plugins.ingestion_plugin.hooks.base_storage_hook import BaseStorageHook

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS")
if not SERVICE_ACCOUNT_FILE:
    raise RuntimeError("Environment variable 'GOOGLE_SERVICE_ACCOUNT_CREDENTIALS' is not set.")

SCOPES = ['https://www.googleapis.com/auth/drive']

class GoogleDriveHook(BaseHook, BaseStorageHook):

    def __init__(self):
        self.service = self._get_service()

    def _get_service(self) -> Resource:
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
            return build("drive", "v3", credentials=creds)
        except Exception as e:
            logging.error(f"Failed to initialize Google Drive service: {e}")
            raise

    def list_files(self, folder_id: str) -> Optional[list[dict[str, str]]]:
        query = f"'{folder_id}' in parents and trashed = false"
        try:
            results = self.service.files().list(
                q=query,
                pageSize=100,
                fields="nextPageToken, files(id, name)"
            ).execute()
            items = results.get('files', [])
            if not items:
                logging.info(f'No files found in folder: {folder_id}')
                return None
            logging.info(f'Found {len(items)} files in folder {folder_id}')
            return items
        except HttpError as e:
            logging.error(f"Failed to list files in folder {folder_id}: {e}")
            raise

    def copy_file(self, source_identifier: str, destination_identifier: str, new_name: str = None) -> dict:
        body = {"parents": [destination_identifier]}
        if new_name:
            body["name"] = new_name
        try:
            file = self.service.files().copy(fileId=source_identifier, body=body).execute()
            logging.info(f"Copied file {source_identifier} to folder {destination_identifier} with new name '{new_name or 'unchanged'}'")
            return file
        except HttpError as e:
            logging.error(f"Failed to copy file {source_identifier} to {destination_identifier}: {e}")
            raise

    def get_or_create_folder(self, folder_name: str, parent_folder_id: str) -> str:
        query = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_folder_id}' in parents and trashed = false"
        )
        try:
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            items = results.get('files', [])

            if items:
                folder_id = items[0]['id']
                logging.info(f"Folder '{folder_name}' already exists with ID {folder_id}")
                return folder_id
            else:
                folder_metadata = {
                    "name": folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_folder_id]
                }
                folder = self.service.files().create(body=folder_metadata, fields="id").execute()
                logging.info(f"Created new folder '{folder_name}' with ID {folder['id']}")
                return folder['id']
        except HttpError as e:
            logging.error(f"Failed to get or create folder '{folder_name}': {e}")
            raise

    def get_file_by_name(self, file_name: str, parent_folder_id: str) -> Optional[dict[str, str]]:
        query = f"name='{file_name}' and '{parent_folder_id}' in parents and trashed = false"
        try:
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            items = results.get('files', [])
            if items:
                logging.info(f"Found file '{file_name}' in folder {parent_folder_id}")
                return items[0]
            logging.info(f"File '{file_name}' not found in folder {parent_folder_id}")
            return None
        except HttpError as e:
            logging.error(f"Failed to get file '{file_name}' from folder {parent_folder_id}: {e}")
            raise

    def file_exists(self, file_name: str, parent_folder_id: str) -> bool:
        try:
            return self.get_file_by_name(file_name, parent_folder_id) is not None
        except Exception as e:
            logging.error(f"Error checking if file exists: {e}")
            raise
