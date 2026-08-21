"""Typed parsing of Deribit public API payloads."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

# `kind=future` covers dated futures and the perpetual; this is the discriminator.
PERPETUAL_SETTLEMENT_PERIOD = "perpetual"


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _decimal_or_none(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _ms_to_datetime(value: Any) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, tz=UTC)


@dataclass(frozen=True)
class OptionContract:
    """One row from `public/get_instruments` for kind=option.

    Covers both instrument-level and option-level fields in one object —
    the split into `instrument` + `option` DB rows happens at the
    repository layer, not here.
    """

    venue_symbol: str
    venue_instrument_id: int
    base_currency: str
    quote_currency: str
    counter_currency: str | None
    settlement_currency: str
    price_index: str | None
    tick_size: Decimal
    contract_size: Decimal | None
    min_trade_amount: Decimal | None
    expiration_timestamp: datetime | None
    creation_timestamp: datetime | None
    is_active: bool
    option_type: str
    strike: Decimal
    raw: dict[str, Any]

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> OptionContract:
        return cls(
            venue_symbol=payload["instrument_name"],
            venue_instrument_id=payload["instrument_id"],
            base_currency=payload["base_currency"],
            quote_currency=payload["quote_currency"],
            counter_currency=payload.get("counter_currency"),
            settlement_currency=payload["settlement_currency"],
            price_index=payload.get("price_index"),
            tick_size=_decimal(payload["tick_size"]),
            contract_size=_decimal_or_none(payload.get("contract_size")),
            min_trade_amount=_decimal_or_none(payload.get("min_trade_amount")),
            expiration_timestamp=_ms_to_datetime(payload.get("expiration_timestamp")),
            creation_timestamp=_ms_to_datetime(payload.get("creation_timestamp")),
            is_active=payload["is_active"],
            option_type=payload["option_type"],
            strike=_decimal(payload["strike"]),
            raw=payload,
        )


@dataclass(frozen=True)
class FutureContract:
    """One row from `public/get_instruments` for kind=future.

    `kind=future` returns dated futures *and* the perpetual; they are told
    apart by `settlement_period` ("month"/"week" vs "perpetual"), which is
    also what `instrument_type` is derived from at the repository layer.
    Deribit's own `instrument_type` field means the settlement convention
    ("linear"/"reversed") and is deliberately not used for that.
    """

    venue_symbol: str
    venue_instrument_id: int
    base_currency: str
    quote_currency: str
    counter_currency: str | None
    settlement_currency: str
    price_index: str | None
    tick_size: Decimal
    contract_size: Decimal | None
    min_trade_amount: Decimal | None
    settlement_period: str
    expiration_timestamp: datetime | None
    creation_timestamp: datetime | None
    is_active: bool
    raw: dict[str, Any]

    @property
    def is_perpetual(self) -> bool:
        return self.settlement_period == PERPETUAL_SETTLEMENT_PERIOD

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> FutureContract:
        return cls(
            venue_symbol=payload["instrument_name"],
            venue_instrument_id=payload["instrument_id"],
            base_currency=payload["base_currency"],
            quote_currency=payload["quote_currency"],
            counter_currency=payload.get("counter_currency"),
            settlement_currency=payload["settlement_currency"],
            price_index=payload.get("price_index"),
            tick_size=_decimal(payload["tick_size"]),
            contract_size=_decimal_or_none(payload.get("contract_size")),
            min_trade_amount=_decimal_or_none(payload.get("min_trade_amount")),
            settlement_period=payload["settlement_period"],
            expiration_timestamp=_ms_to_datetime(payload.get("expiration_timestamp")),
            creation_timestamp=_ms_to_datetime(payload.get("creation_timestamp")),
            is_active=payload["is_active"],
            raw=payload,
        )
