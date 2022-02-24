from typing import Callable, Any, Optional
from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATASET = "Test1"


def get_last_timestamp(table: str, time_key: str):
    def _get(
        timeframe: tuple[Optional[str], Optional[str]]
    ) -> tuple[datetime, datetime]:
        start, end = timeframe
        if start and end:
            return tuple([datetime.strptime(i, "%Y-%m-%d") for i in (start, end)])  # type: ignore
        else:
            rows = BQ_CLIENT.query(
                f"SELECT MAX({time_key}) AS incre FROM {DATASET}.{table}"
            ).result()
            return ([row for row in rows][0]["incre"], datetime.utcnow())

    return _get


def load(schema: list[dict[str, Any]]):
    def _load(table: str, update_fn: Callable[[str], None] = None):
        def __load(data: list[dict[str, Any]]) -> int:
            if len(data) == 0:
                return 0

            output_rows = (
                BQ_CLIENT.load_table_from_json(
                    data,
                    f"{DATASET}.{table}",
                    job_config=bigquery.LoadJobConfig(
                        create_disposition="CREATE_IF_NEEDED",
                        write_disposition="WRITE_APPEND"
                        if update_fn
                        else "WRITE_TRUNCATE",
                        schema=schema,
                    ),
                )
                .result()
                .output_rows
            )
            if update_fn:
                update_fn(table)
            return output_rows

        return __load

    return _load


def update(id_key: list[str], time_key: str):
    def _update(table: str):
        BQ_CLIENT.query(
            f"""
        CREATE OR REPLACE TABLE {DATASET}.{table} AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {','.join(id_key)} ORDER BY {time_key} DESC) AS row_num,
            FROM {DATASET}.{table}
        ) WHERE row_num = 1
        """
        ).result()

    return _update
