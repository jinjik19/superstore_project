from datetime import datetime

import pandas as pd
from airflow.datasets import Dataset
from airflow.sdk import Variable, dag, task
from clickhouse_connect import get_client

from utils import create_logger, to_snake_case


logger = create_logger()
CLICKHOUSE_URL = Variable.get("CLICKHOUSE_URL")
FILE_PATH = "/opt/airflow/data/input/Superstore.xls"
superstore_dataset = Dataset(FILE_PATH)
client = get_client(dsn=CLICKHOUSE_URL)


@dag(
    "superstore_etl",
    start_date=datetime(2025, 10, 12),
    schedule=[superstore_dataset],
    catchup=False,
    tags=["superstore", "etl", "clickhouse"],
    doc_md="""
        This DAG creates the staging layer in ClickHouse by reading data from Excel files,
        after that trigger dbt transformations to create the core and data mart layers.
    """
)
def etl_superstore() -> None:
    @task(task_id="get_file_path")
    def get_file_path(ti=None, **kwargs) -> str:
        triggering_events = kwargs.get("triggering_asset_events")
        if not triggering_events:
            raise ValueError("DAG triggered without asset event information!")

        first_event = next(iter(triggering_events.values()))[0]
        source_dag_id = first_event.source_dag_id
        source_run_id = first_event.source_run_id
        source_task_id = first_event.source_task_id

        payload = ti.xcom_pull(
            dag_id=source_dag_id,
            run_id=source_run_id,
            task_ids=source_task_id
        )

        if not payload or "processed_path" not in payload:
            raise ValueError(f"Could not find 'processed_path' in XCom from {source_dag_id}.{source_task_id}")

        file_to_process = payload["processed_path"]
        logger.info(f"Received file to process: {file_to_process}")

        return file_to_process

    @task(
        task_id="read_excel_to_clickhouse",
        doc_md="""
            This task reads data from the specified Excel sheet and loads it into the corresponding ClickHouse staging layer.
        """
    )
    def read_excel_to_clickhouse(file_to_process: str) -> None:
        map_df = dict()

        for sheet_name in ["Orders", "Returns", "People"]:
            logger.info(f"Reading sheet: {sheet_name}")
            df = pd.read_excel(file_to_process, sheet_name=sheet_name)
            df.rename(columns=to_snake_case, inplace=True)
            map_df[sheet_name.lower()] = df



        for name, df in map_df.items():
            table_name = f"stg_{name}"
            logger.info(f"Dataframe {table_name} preview:\n{df.head()}")
            client.insert_df(table=table_name, df=df)

    @task.bash()
    def run_transformation():
        # TODO: доделать вызов dbt
        return "echo 'dbt run command'"

    processed_file_path = get_file_path()
    read_excel_to_clickhouse(processed_file_path)  >> run_transformation()

etl_superstore()
