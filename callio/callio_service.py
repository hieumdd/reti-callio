from typing import Any, Callable, Union
from datetime import datetime

from callio import callio_repo, call, contact, customer
from db.bigquery import get_last_timestamp, load, update
from utils import utils


def pipeline_service(
    table: str,
    get: Callable[[tuple[datetime, datetime]], list[dict[str, Any]]],
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    load: Callable[[str, Callable[[list[str], str], None]], int],
    id_key: list[str] = ["_id"],
    time_key: str = "createTime",
):
    def _svc(start: str, end: str) -> dict[str, Union[str, int]]:
        return utils.compose(
            lambda x: {
                "table": table,
                "start": start,
                "end": end,
                "output_rows": x,
            },
            load(table, update(id_key, time_key)),
            transform,
            get,
            get_last_timestamp(table, time_key),
        )((start, end))

    return _svc


def call_service(table: str, direction: int):
    return pipeline_service(
        table,
        callio_repo.get_listing(
            "call",
            callio_repo.build_params(),
            {"direction": direction},
        ),
        call.transform,
        load(call.schema),  # type: ignore
    )


call_inbound_service = call_service("Call_Inbound", 1)
call_outbound_service = call_service("Call_Outbound", 2)
call_internal_service = call_service("Call_Internal", 3)

contact_service = pipeline_service(
    "Contact",
    callio_repo.get_listing(
        "contact",
        callio_repo.build_params(),
    ),
    contact.transform,
    load(contact.schema),  # type: ignore
)

customer_service = pipeline_service(
    "Customer",
    callio_repo.get_listing(
        "customer",
        callio_repo.build_params(True),
    ),
    customer.transform,
    load(customer.schema), # type: ignore
    time_key="updateTime"
)

services = {
    "Call_Inbound": call_inbound_service,
    "Call_Outbound": call_outbound_service,
    "Call_Internal": call_internal_service,
    "Contact": contact_service,
    "Customer": customer_service,
}
