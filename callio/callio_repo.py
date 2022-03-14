from typing import Any, Callable, Union
import os
import asyncio
from datetime import datetime

import httpx

from callio import callio

PAGE_SIZE = 500


def _build_params(
    page_key: callio.PageKey,
    params: dict[str, Any],
    timeframe: tuple[datetime, datetime],
) -> dict[str, Union[str, int]]:
    from_key, to_key = page_key
    start, end = timeframe
    return {
        **params,
        "pageSize": PAGE_SIZE,
        from_key: int(start.timestamp() * 1000),
        to_key: int(end.timestamp() * 1000),
    }


def get_url(uri: str) -> str:
    return f"https://clientapi.phonenet.io/{uri}"


def get_client(async_=False) -> Union[httpx.Client, httpx.AsyncClient]:
    client = httpx.AsyncClient if async_ else httpx.Client
    return client(headers={"token": os.getenv("CALLIO_TOKEN", "")}, timeout=None)


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
    def _get(page_key: callio.PageKey):
        def __get(timeframe: tuple[datetime, datetime]) -> list[dict[str, Any]]:
            async def ___get():
                url = get_url(uri)
                _params = _build_params(page_key, params, timeframe)
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
