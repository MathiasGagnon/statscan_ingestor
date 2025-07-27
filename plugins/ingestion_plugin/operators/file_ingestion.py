import os
import logging
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.context import Context

from ingestion_plugin.hooks.google_drive_hook import GoogleDriveHook

METADATA_FILETYPE = '.yml'

class FileIngestionOperator(BaseOperator):
    def __init__(
        self,
        landing_zone_folder_id: str,
        raw_storage_root_folder_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.landing_zone_folder_id = landing_zone_folder_id
        self.raw_storage_root_folder_id = raw_storage_root_folder_id

    def _get_raw_storage_folder_id(self, base_folder_id: str, ingestion_date: datetime, hook: GoogleDriveHook) -> str:
        year = ingestion_date.strftime("%Y")
        month = ingestion_date.strftime("%m")
        day = ingestion_date.strftime("%d")

        year_id = hook.get_or_create_folder(year, base_folder_id)
        month_id = hook.get_or_create_folder(month, year_id)
        day_id = hook.get_or_create_folder(day, month_id)

        return day_id

    def execute(self) -> None:
        hook = GoogleDriveHook()
        files = hook.list_files(self.landing_zone_folder_id)
        ingestion_date = datetime.now()

        raw_folder_id = self._get_raw_storage_folder_id(self.raw_storage_root_folder_id, ingestion_date, hook)

        data_files = [file for file in files if not file['name'].endswith(f'{METADATA_FILETYPE}')]

        for file in data_files:
            data_name = file['name']
            yaml_name = f"{os.path.splitext(data_name)[0]}.{METADATA_FILETYPE}"

            if not data_name.endswith(SUPPORTED_FILE_TYPES):
                logging.info(f"Skipping {data_name}: Unsupported file type.")
                continue

            if hook.file_exists(data_name, raw_folder_id):
                logging.info(f"{data_name} already exists. Skipping.")
                continue

            if hook.file_exists(yaml_name, self.landing_zone_folder_id):
                logging.info(f"Copying {data_name} and {yaml_name} to raw storage.")

                hook.copy_file(file['id'], raw_folder_id)

                yaml_file = hook.get_file_by_name(yaml_name, self.landing_zone_folder_id)
                hook.copy_file(yaml_file['id'], raw_folder_id)
            else:
                logging.info(f"Skipping {data_name}: YAML metadata not found.")
