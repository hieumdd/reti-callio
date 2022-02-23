from typing import Any
import os

from datetime import datetime

import requests

PAGE_SIZE = 10000

def get_url(uri: str) -> str:
    return f"https://clientapi.phonenet.io/{uri}"


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"token": os.getenv("CALLIO_TOKEN", "")})
    return session


def get_listing(uri: str, params: dict[str, Any]):
    def _get(session: requests.Session, start: datetime, end: datetime):
        def __get(page: int = 1) -> list[dict[str, Any]]:
            with session.get(
                get_url(uri),
                params={
                    **params,
                    "pageSize": PAGE_SIZE,
                    "from": int(start.timestamp() * 1000),
                    "to": int(end.timestamp() * 1000),
                    "page": page,
                },
            ) as r:
                r.raise_for_status()
                res = r.json()
            data = res["docs"]
            return data + __get(page + 1) if res["hasNextPage"] else data

        return __get()

    return _get
