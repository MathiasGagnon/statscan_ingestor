from datetime import datetime

from airflow import DAG

from ingestion_plugin.operators.file_ingestion_operator import FileIngestionOperator

default_args = {
    "start_date": datetime(2023, 1, 1),
    "owner": "Mathias Gagnon",
}

with DAG(
    dag_id="process_landing_zone_gdrive",
    schedule="@hourly",
    default_args=default_args,
    catchup=False,
    description="Copy landing zone files + YAMLs to raw GDrive storage",
    tags=["gdrive", "file_ingestion"],
) as dag:

    scan_and_copy = FileIngestionOperator(
        task_id="scan_and_copy_files",
        landing_zone_folder_id="1gG1U04ROlS_cgurXVr_91hJL5lxxwpwQ",
        raw_storage_root_folder_id="1L13HvomfC3dnc_gZLRqApFxryYzND888",
        hook_type="google_drive"
    )