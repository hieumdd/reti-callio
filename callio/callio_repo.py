from typing import Any, Callable, Union
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


def build_params(update_time: bool = False):
    def _build(
        params: dict[str, Any],
        timeframe: tuple[datetime, datetime],
    ) -> dict[str, Union[str, int]]:
        start, end = timeframe
        return {
            **params,
            "pageSize": PAGE_SIZE,
            "fromUpdateTime" if update_time else "from": int(start.timestamp() * 1000),
            "toUpdateTime" if update_time else "to": int(end.timestamp() * 1000),
        }

    return _build


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


def get_listing(
    uri: str,
    params_builder: Callable[
        [dict[str, Any], tuple[datetime, datetime]],
        dict[str, Any],
    ],
    params: dict[str, Any] = {},
):
    def _get(timeframe: tuple[datetime, datetime]) -> list[dict[str, Any]]:
        async def __get():
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

        return [i for j in asyncio.run(__get()) for i in j]

    return _get
