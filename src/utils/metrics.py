from datetime import datetime

import clickhouse_connect

def log_metrics(category: str, name: str, value: float, table: str, details: str = "") -> None:
    client = clickhouse_connect.get_client(dsn="clickhouse://clickhouse:8123/superstore_db")
    columns = ("timestamp", "category", "name", "value", "table_name", "details")
    raw_data = (
        datetime.now(),
        category,
        name,
        float(value),
        table,
        details,
    )
    client.insert(
        "pipeline_metrics",
        data=[raw_data],
        column_names=columns,
    )
