import pytest

from callio import callio_service


@pytest.fixture(
    params=[
        # (None, None),
        ("2022-01-01", "2022-03-01"),
    ],
    ids=[
        # "auto",
        "manual",
    ],
)
def timeframe(request):
    return request.param


class TestCallio:
    @pytest.mark.parametrize(
        "service",
        callio_service.services.values(),
        ids=callio_service.services.keys(),
    )
    def test_service(self, service, timeframe):
        res = service(*timeframe)
        print(res)
        res
