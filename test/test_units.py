import pytest

from callio import callio_service

@pytest.fixture(
    params=[
        (None, None),
        ('2022-01-02', '2022-01-03'),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def timeframe(request):
    return request.param

class TestCallio:
    @pytest.mark.parametrize(
        "service",
        [
            callio_service.call_inbound_service,
            callio_service.call_outbound_service,
            callio_service.call_internal_service,
        ],
        ids=[
            "CallInbound",
            "CallOutbound",
            "CallInternal",
        ]
    )
    def test_service(self, service, timeframe):
        res = service(*timeframe)
        res
