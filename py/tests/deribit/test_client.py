import httpx
import pytest

from shawlynot.quant.deribit.client import DeribitAPIError, DeribitClient


@pytest.fixture
def captured_request() -> dict:
    return {}


async def test_get_instruments_builds_expected_request(captured_request: dict) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        captured_request["url"] = request.url
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "result": [{"instrument_name": "BTC-1-C"}]},
        )

    transport = httpx.MockTransport(handler)
    client = DeribitClient()
    client._http = httpx.AsyncClient(base_url=client._http.base_url, transport=transport)

    result = await client.get_instruments(currency="BTC", kind="option", expired=False)

    assert result == [{"instrument_name": "BTC-1-C"}]
    url = captured_request["url"]
    assert url.path == "/api/v2/public/get_instruments"
    assert dict(url.params) == {"currency": "BTC", "kind": "option", "expired": "false"}

    await client.aclose()


async def test_get_instruments_raises_on_api_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "error": {"code": 10009, "message": "not_enough_funds"}},
        )

    transport = httpx.MockTransport(handler)
    client = DeribitClient()
    client._http = httpx.AsyncClient(base_url=client._http.base_url, transport=transport)

    with pytest.raises(DeribitAPIError):
        await client.get_instruments()

    await client.aclose()
