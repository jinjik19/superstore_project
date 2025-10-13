import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task


FILE_PATH = "/opt/airflow/data/input/Superstore.xls"
superstore_dataset = Dataset(FILE_PATH)

@dag(
    dag_id="file_watcher_superstore",
    start_date=datetime(2025, 10, 12),
    schedule="* * * * *",
    catchup=False,
    tags=["watcher"],
)
def file_watcher_dag() -> None:
    @task(outlets=[superstore_dataset])
    def check_for_file() -> dict[str, str]:
        if os.path.exists(FILE_PATH):
            splited_file_path = FILE_PATH.split("/")
            name, ext = os.path.splitext(os.path.basename(splited_file_path.pop()))
            processed_file = f"{name}_{int(datetime.now().timestamp())}{ext}"
            splited_file_path.append(processed_file)
            processed_path = "/".join(splited_file_path)
            print(processed_path)
            os.rename(FILE_PATH, processed_path)
            return {"processed_path": processed_path}

        raise AirflowSkipException(f"File {FILE_PATH} not found.")

    check_for_file()

file_watcher_dag()
