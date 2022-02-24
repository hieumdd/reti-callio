from typing import Any, Callable
from datetime import datetime

from requests import Session

from callio import callio_repo, call
from db.bigquery import get_last_timestamp, load, update
from utils import utils


def pipeline_service(
    table: str,
    get: Callable[
        [Session],
        Callable[[tuple[datetime, datetime]], list[dict[str, Any]]],
    ],
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    load: Callable[[str, Callable[[list[str], str], None]], int],
    id_key: list[str] = ["_id"],
    time_key: str = "createTime",
):
    def _svc(start: str, end: str):
        with callio_repo.get_session() as session:
            data = utils.compose(
                lambda x: {
                    "table": table,
                    "start": start,
                    "end": end,
                    "output_rows": x,
                },
                load(table, update(id_key, time_key)),
                transform,
                get(session),
                get_last_timestamp(table, time_key),
            )((start, end))
        return data

    return _svc


def call_service(table: str, direction: int):
    return pipeline_service(
        table,
        callio_repo.get_listing("call", {"direction": direction}),
        call.transform,
        load(call.schema),  # type: ignore
    )


call_inbound_service = call_service("Call_Inbound", 1)
call_outbound_service = call_service("Call_Outbound", 2)
call_internal_service = call_service("Call_Internal", 3)

