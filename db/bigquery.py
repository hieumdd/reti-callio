from typing import Callable, Any

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATASET = ""


def load(
    table: str,
    schema: list[dict[str, Any]],
    update_fn: Callable[[str], None] = None,
):
    def _load(data: list[dict[str, Any]]) -> int:
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                data,
                f"{DATASET}_{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND" if update_fn else "WRITE_TRUNCATE",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        if update_fn:
            update_fn(table)
        return output_rows

    return _load


def update(p_key: str, time_key: str):
    def _update(table: str):
        BQ_CLIENT.query(
            f"""
        CREATE OR REPLACE TABLE {DATASET}_{table} AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {','.join(p_key)} ORDER BY {time_key} DESC) AS row_num,
            FROM {DATASET}_{table}
        ) WHERE row_num = 1
        """
        ).result()

    return _update
