import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from shawlynot.quant.deribit.models import FutureContract, OptionContract

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "btc_options_sample.json"


def _load_sample() -> list[dict]:
    return json.loads(FIXTURE_PATH.read_text())


def test_from_api_parses_call() -> None:
    call_payload, _put_payload = _load_sample()

    contract = OptionContract.from_api(call_payload)

    assert contract.venue_symbol == "BTC-19AUG26-56000-C"
    assert contract.venue_instrument_id == 674989
    assert contract.base_currency == "BTC"
    assert contract.quote_currency == "BTC"
    assert contract.counter_currency == "USD"
    assert contract.settlement_currency == "BTC"
    assert contract.price_index == "btc_usd"
    assert contract.tick_size == Decimal("0.0001")
    assert contract.contract_size == Decimal("1.0")
    assert contract.min_trade_amount == Decimal("0.1")
    assert contract.is_active is True
    assert contract.option_type == "call"
    assert contract.strike == Decimal("56000.0")
    assert contract.expiration_timestamp == datetime.fromtimestamp(
        1787126400000 / 1000, tz=UTC
    )
    assert contract.raw == call_payload


def test_from_api_parses_put() -> None:
    _call_payload, put_payload = _load_sample()

    contract = OptionContract.from_api(put_payload)

    assert contract.option_type == "put"
    assert contract.venue_symbol == "BTC-19AUG26-56000-P"


def test_from_api_handles_missing_optional_fields() -> None:
    payload = {
        "instrument_name": "BTC-TEST-1-C",
        "instrument_id": 1,
        "base_currency": "BTC",
        "quote_currency": "BTC",
        "settlement_currency": "BTC",
        "tick_size": 0.0005,
        "is_active": True,
        "option_type": "call",
        "strike": 100,
    }

    contract = OptionContract.from_api(payload)

    assert contract.counter_currency is None
    assert contract.price_index is None
    assert contract.contract_size is None
    assert contract.min_trade_amount is None
    assert contract.expiration_timestamp is None
    assert contract.creation_timestamp is None


FUTURES_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "btc_futures_sample.json"


def _load_futures_sample() -> list[dict]:
    return json.loads(FUTURES_FIXTURE_PATH.read_text())


def test_future_from_api_parses_dated_future() -> None:
    dated_payload, _perpetual_payload = _load_futures_sample()

    contract = FutureContract.from_api(dated_payload)

    assert contract.venue_symbol == "BTC-26JUN26"
    assert contract.venue_instrument_id == 342036
    assert contract.base_currency == "BTC"
    assert contract.quote_currency == "USD"
    assert contract.counter_currency == "USD"
    assert contract.settlement_currency == "BTC"
    assert contract.price_index == "btc_usd"
    assert contract.tick_size == Decimal("2.5")
    assert contract.contract_size == Decimal("10")
    assert contract.min_trade_amount == Decimal("10")
    assert contract.settlement_period == "month"
    assert contract.is_active is True
    assert contract.expiration_timestamp == datetime.fromtimestamp(
        1782633600000 / 1000, tz=UTC
    )
    assert contract.is_perpetual is False
    assert contract.raw == dated_payload


def test_future_from_api_flags_the_perpetual() -> None:
    _dated_payload, perpetual_payload = _load_futures_sample()

    contract = FutureContract.from_api(perpetual_payload)

    assert contract.venue_symbol == "BTC-PERPETUAL"
    assert contract.settlement_period == "perpetual"
    assert contract.is_perpetual is True


def test_future_from_api_ignores_deribit_instrument_type() -> None:
    """Deribit's `instrument_type` is the settlement convention, not our column.

    It reads "reversed" here; deriving `security_master.instrument_type` from it
    would violate the CHECK constraint, so the model does not carry it at all.
    """
    dated_payload, _ = _load_futures_sample()
    assert dated_payload["instrument_type"] == "reversed"

    contract = FutureContract.from_api(dated_payload)

    assert not hasattr(contract, "instrument_type")


def test_future_from_api_handles_missing_optional_fields() -> None:
    payload = {
        "instrument_name": "BTC-TEST",
        "instrument_id": 2,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "settlement_currency": "BTC",
        "tick_size": 2.5,
        "settlement_period": "week",
        "is_active": True,
    }

    contract = FutureContract.from_api(payload)

    assert contract.counter_currency is None
    assert contract.price_index is None
    assert contract.contract_size is None
    assert contract.min_trade_amount is None
    assert contract.expiration_timestamp is None
    assert contract.creation_timestamp is None
