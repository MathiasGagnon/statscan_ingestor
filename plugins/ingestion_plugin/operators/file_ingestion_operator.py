import os
import logging
from datetime import datetime
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context

from ingestion_plugin.hooks.storage_hook_strategy_registry import STORAGE_HOOK_STRATEGY_REGISTRY
from ingestion_plugin.hooks.base_storage_hook import BaseStorageHook
from ingestion_plugin.hooks.google_drive_hook import GoogleDriveHook

METADATA_FILETYPE = '.yml'
SUPPORTED_FILE_TYPES = ['.csv', '.xlsx']

class FileIngestionOperator(BaseOperator):
    """
    Operator to ingest files from a landing zone folder to a raw storage folder.
    
    It uses a hook strategy (e.g. GoogleDriveHook) to perform storage-specific logic.
    
    Parameters:
    - landing_zone_folder_id: ID of the source folder to pull files from.
    - raw_storage_folder_id: ID of the destination folder to push files to.
    - hook_type: String key to select the hook strategy.
    - storage_hook: Optional. Directly provide a hook instance instead of resolving via registry.
    """
    
    def __init__(
        self,
        landing_zone_folder_id: str,
        raw_storage_root_folder_id: str,
        hook_type: str,
        storage_hook: Optional[BaseStorageHook] = None,
        **kwargs,
    ):
        """
        Initialize the IngestFilesOperator.

        :param landing_zone_folder_id: ID of the source folder to pull files from.
        :param raw_storage_root_folder_id: ID of the root of the destination folder to push files to.
        :param hook_type: Key used to look up a hook class from the strategy registry.
        :param storage_hook: (Optional) Hook instance to inject manually (useful for testing).
        """

        super().__init__(**kwargs)
        self.landing_zone_folder_id = landing_zone_folder_id
        self.raw_storage_root_folder_id = raw_storage_root_folder_id
        self.hook_type = hook_type

        strategy_cls = STORAGE_HOOK_STRATEGY_REGISTRY.get(hook_type)
        if not strategy_cls:
            raise ValueError(
                f"Unknown hook type: '{hook_type}'. "
                f"Available types: {list(STORAGE_HOOK_STRATEGY_REGISTRY.keys())}"
            )
        self.storage_hook = storage_hook or strategy_cls()

    def _get_raw_storage_daily_folder_id(self, raw_storage_root_folder_id: str, ingestion_date: datetime, storage_hook: BaseStorageHook) -> str:
        """
        Gets the raw storage's daily folder ID for a given ingestion date.

        :param raw_storage_root_folder_id: ID of the root of the destination folder to push files to.
        :param ingestion_date: Date of the current ingestion process
        :param storage_hook: Storage hook instance used to create or get the raw storage daily folder
        """

        year = ingestion_date.strftime("%Y")
        month = ingestion_date.strftime("%m")
        day = ingestion_date.strftime("%d")

        year_id = storage_hook.get_or_create_folder(year, raw_storage_root_folder_id)
        month_id = storage_hook.get_or_create_folder(month, year_id)
        day_id = storage_hook.get_or_create_folder(day, month_id)

        return day_id

    def execute(self, context: Context) -> None:

        files = self.storage_hook.list_files(self.landing_zone_folder_id)
        ingestion_date = datetime.now()

        raw_daily_folder_id = self._get_raw_storage_daily_folder_id(self.raw_storage_root_folder_id, ingestion_date, self.storage_hook)

        data_files = [file for file in files if not file['name'].endswith(f'{METADATA_FILETYPE}')]

        for file in data_files:
            data_name = file['name']
            yaml_name = f"{os.path.splitext(data_name)[0]}{METADATA_FILETYPE}"

            if not data_name.endswith(tuple(SUPPORTED_FILE_TYPES)):
                logging.info(f"Skipping {data_name}: Unsupported file type.")
                continue

            if self.storage_hook.file_exists(data_name, raw_daily_folder_id):
                logging.info(f"{data_name} already exists. Skipping.")
                continue

            if self.storage_hook.file_exists(yaml_name, self.landing_zone_folder_id):
                logging.info(f"Copying {data_name} and {yaml_name} to raw storage.")

                self.storage_hook.copy_file(file['id'], raw_daily_folder_id)

                yaml_file = self.storage_hook.get_file_by_name(yaml_name, self.landing_zone_folder_id)
                self.storage_hook.copy_file(yaml_file['id'], raw_daily_folder_id)
            else:
                logging.info(f"Skipping {data_name}: YAML metadata not found.")
