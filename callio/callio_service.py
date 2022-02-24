from typing import Any, Callable
from datetime import datetime

from callio import callio_repo, call
from db.bigquery import load, update
from utils import utils


def pipeline_service(
    table: str,
    get: Callable[[datetime, datetime], list[dict[str, Any]]],
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    load: Callable[[str, Callable[[list[str], str], None]], int],
    id_key: list[str] = ["_id"],
    time_key: str = "create_time",
):
    def _svc(start, end):
        with callio_repo.get_session() as session:
            data = utils.compose(
                load(table, update(id_key, time_key)),
                transform,
                get,
            )(session, start, end)
        return data

    return _svc


call_inbound_service = pipeline_service(
    "CallInbound",
    callio_repo.get_listing("call", {"direction": 1}),
    call.transform,
    load(call.schema),
)
