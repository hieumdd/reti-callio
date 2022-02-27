from typing import Optional
from functools import reduce
from datetime import datetime, timezone


def parse_unix_ts(x: Optional[int]) -> Optional[str]:
    try:
        return (
            datetime.fromtimestamp(x / 1e3)
            .astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            if x
            else None
        )
    except:
        return None


def compose(*func):
    def _compose(f, g):
        return lambda x: f(g(x))

    return reduce(_compose, func, lambda x: x)
