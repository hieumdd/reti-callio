from typing import Any, Callable, Union
from enum import Enum
import os
import asyncio

from datetime import datetime

import httpx

PAGE_SIZE = 500


def get_url(uri: str) -> str:
    return f"https://clientapi.phonenet.io/{uri}"


def get_client(async_=False) -> Union[httpx.Client, httpx.AsyncClient]:
    client = httpx.AsyncClient if async_ else httpx.Client
    return client(headers={"token": os.getenv("CALLIO_TOKEN", "")}, timeout=None)


def _build_params(from_key: str, to_key: str):
    def _build(
        params: dict[str, Any],
        timeframe: tuple[datetime, datetime],
    ) -> dict[str, Union[str, int]]:
        start, end = timeframe
        return {
            **params,
            "pageSize": PAGE_SIZE,
            from_key: int(start.timestamp() * 1000),
            to_key: int(end.timestamp() * 1000),
        }

    return _build


class ListingType(Enum):
    Create = ("createTime", _build_params("from", "to"))
    Update = ("updateTime", _build_params("fromUpdateTime", "toUpdateTime"))


async def get_listing_page(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    callback_fn: Callable[[dict[str, Any]], Any],
    page: int = 1,
):
    r = await client.get(url, params={**params, "page": page})
    r.raise_for_status()
    res = r.json()
    return callback_fn(res)


def get_listing(uri: str, params: dict[str, Any] = {}):
    def _get(
        params_builder: Callable[
            [dict[str, Any], tuple[datetime, datetime]],
            dict[str, Any],
        ],
    ):
        def __get(timeframe: tuple[datetime, datetime]) -> list[dict[str, Any]]:
            async def ___get():
                url = get_url(uri)
                _params = params_builder(params, timeframe)
                async with get_client(True) as client:
                    pages = await get_listing_page(
                        client,
                        url,
                        _params,
                        lambda x: x["totalPages"],
                    )
                    tasks = [
                        asyncio.create_task(
                            get_listing_page(
                                client,
                                url,
                                _params,
                                lambda x: x["docs"],
                                page,
                            )
                        )
                        for page in range(1, pages + 1)
                    ]
                    return await asyncio.gather(*tasks)

            return [i for j in asyncio.run(___get()) for i in j]

        return __get

    return _get
