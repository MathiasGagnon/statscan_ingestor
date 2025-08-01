import yaml
from pathlib import Path
from datetime import datetime

from airflow.decorators import dag, task

from plugins.ingestion_plugin.operators.file_ingestion_operator import FileIngestionOperator

CONFIG_PATH = Path(__file__).parent / "config.yaml"

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

for client in config["clients"]:
    dag_id = f"ingest_{client['name']}"

    @dag(
        dag_id=dag_id,
        start_date=datetime(2023, 1, 1),
        schedule=client["schedule"],
        catchup=False,
        tags=["file_ingestion", client["hook_type"]],
    )
    def ingestion_dag():
        @task
        def run_ingestion():
            ingestion_task = FileIngestionOperator(
                task_id="scan_and_copy_files",
                landing_zone_folder_id=client["landing_zone_folder_id"],
                raw_storage_root_folder_id=client["raw_storage_root_folder_id"],
                hook_type=client["hook_type"]
            )
            return ingestion_task.execute(context={})

        run_ingestion()

    globals()[dag_id] = ingestion_dag()
