"""Repository functions for the `security_master` schema."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from shawlynot.quant.deribit.models import FutureContract, OptionContract

_SCHEMA_SQL_PATH = Path(__file__).with_name("schema.sql")

# `instrument` carries only what every instrument has: venue, symbol, tick size,
# type. Everything option-specific lives on `option`. tick_size is the only
# mutable column, so it is the sole DO UPDATE target — DO UPDATE rather than
# DO NOTHING so RETURNING still yields the id on a repeat run.
_UPSERT_INSTRUMENT_SQL = """
INSERT INTO security_master.instrument (
    venue_id, symbol, tick_size, instrument_type
) VALUES (
    %(venue_id)s, %(symbol)s, %(tick_size)s, %(instrument_type)s
)
ON CONFLICT (venue_id, symbol) DO UPDATE SET
    tick_size = EXCLUDED.tick_size
RETURNING instrument_id;
"""

_UPSERT_OPTION_SQL = """
INSERT INTO security_master.option (
    instrument_id, option_type, strike, expiration_timestamp,
    contract_size, creation_timestamp
) VALUES (
    %(instrument_id)s, %(option_type)s, %(strike)s, %(expiration_timestamp)s,
    %(contract_size)s, %(creation_timestamp)s
)
ON CONFLICT (instrument_id) DO UPDATE SET
    option_type = EXCLUDED.option_type,
    strike = EXCLUDED.strike,
    expiration_timestamp = EXCLUDED.expiration_timestamp,
    contract_size = EXCLUDED.contract_size,
    creation_timestamp = EXCLUDED.creation_timestamp;
"""

_UPSERT_FUTURE_SQL = """
INSERT INTO security_master.future (
    instrument_id, settlement_period, expiration_timestamp,
    contract_size, creation_timestamp
) VALUES (
    %(instrument_id)s, %(settlement_period)s, %(expiration_timestamp)s,
    %(contract_size)s, %(creation_timestamp)s
)
ON CONFLICT (instrument_id) DO UPDATE SET
    settlement_period = EXCLUDED.settlement_period,
    expiration_timestamp = EXCLUDED.expiration_timestamp,
    contract_size = EXCLUDED.contract_size,
    creation_timestamp = EXCLUDED.creation_timestamp;
"""

_ENSURE_CURRENCY_INSTRUMENT_SQL = """
INSERT INTO security_master.instrument (
    venue_id, symbol, tick_size, instrument_type
) VALUES (
    %(venue_id)s, %(symbol)s, %(tick_size)s, 'currency'
)
ON CONFLICT (venue_id, symbol) DO UPDATE SET
    tick_size = EXCLUDED.tick_size
RETURNING instrument_id;
"""

_ENSURE_CURRENCIES_ROW_SQL = """
INSERT INTO security_master.currencies (instrument_id)
VALUES (%(instrument_id)s)
ON CONFLICT (instrument_id) DO NOTHING;
"""

_GET_VENUE_ID_SQL = "SELECT venue_id FROM security_master.venue WHERE code = %s;"


def _as_int(value: Decimal | None) -> int | None:
    """Narrow a Deribit numeric to `option.contract_size`, an int column.

    Deribit sends contract_size as a float (1.0 for BTC options). Postgres
    would silently round a fractional value on assignment to an int column,
    so reject it here instead.
    """
    if value is None:
        return None
    narrowed = int(value)
    if value != narrowed:
        raise ValueError(f"non-integral contract_size: {value}")
    return narrowed


async def ensure_schema(conn: psycopg.AsyncConnection[Any]) -> None:
    """Create the security_master schema/tables (idempotent) and seed the DERIBIT venue."""
    statements = [s.strip() for s in _SCHEMA_SQL_PATH.read_text().split(";") if s.strip()]
    async with conn.cursor() as cur:
        for statement in statements:
            await cur.execute(statement)
    await conn.commit()


async def get_venue_id(conn: psycopg.AsyncConnection[Any], code: str) -> int:
    async with conn.cursor() as cur:
        await cur.execute(_GET_VENUE_ID_SQL, (code,))
        row = await cur.fetchone()
    if row is None:
        raise ValueError(f"unknown venue code: {code!r}")
    return int(row[0])


async def upsert_option_contract(
    conn: psycopg.AsyncConnection[Any], venue_id: int, contract: OptionContract
) -> int:
    """Upsert `instrument` + `option` for one Deribit option contract. Returns instrument_id."""
    async with conn.cursor() as cur:
        await cur.execute(
            _UPSERT_INSTRUMENT_SQL,
            {
                "venue_id": venue_id,
                "symbol": contract.venue_symbol,
                "tick_size": contract.tick_size,
                "instrument_type": "option",
            },
        )
        row = await cur.fetchone()
        assert row is not None
        instrument_id = int(row[0])
        await cur.execute(
            _UPSERT_OPTION_SQL,
            {
                "instrument_id": instrument_id,
                "option_type": contract.option_type,
                "strike": contract.strike,
                "expiration_timestamp": contract.expiration_timestamp,
                "contract_size": _as_int(contract.contract_size),
                "creation_timestamp": contract.creation_timestamp,
            },
        )
    return instrument_id


async def upsert_future_contract(
    conn: psycopg.AsyncConnection[Any], venue_id: int, contract: FutureContract
) -> int:
    """Upsert `instrument` + `future` for one dated Deribit future. Returns instrument_id.

    Rejects the perpetual rather than storing it: `instrument_type` is derived
    from kind + settlement_period (never from the payload's own
    `instrument_type`, which holds "linear"/"reversed"), and the perpetual is
    filtered upstream so it never reaches `security_master` at all.
    """
    if contract.is_perpetual:
        raise ValueError(f"perpetual is not ingested: {contract.venue_symbol!r}")

    async with conn.cursor() as cur:
        await cur.execute(
            _UPSERT_INSTRUMENT_SQL,
            {
                "venue_id": venue_id,
                "symbol": contract.venue_symbol,
                "tick_size": contract.tick_size,
                "instrument_type": "future",
            },
        )
        row = await cur.fetchone()
        assert row is not None
        instrument_id = int(row[0])
        await cur.execute(
            _UPSERT_FUTURE_SQL,
            {
                "instrument_id": instrument_id,
                "settlement_period": contract.settlement_period,
                "expiration_timestamp": contract.expiration_timestamp,
                "contract_size": _as_int(contract.contract_size),
                "creation_timestamp": contract.creation_timestamp,
            },
        )
    return instrument_id


async def ensure_currency_instrument(
    conn: psycopg.AsyncConnection[Any],
    venue_id: int,
    symbol: str,
    tick_size: Decimal,
) -> int:
    """Upsert the identity `instrument` + `currencies` row for a reference currency pair.

    No quote/price data — that is fetched by a separate, later process.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            _ENSURE_CURRENCY_INSTRUMENT_SQL,
            {
                "venue_id": venue_id,
                "symbol": symbol,
                "tick_size": tick_size,
            },
        )
        row = await cur.fetchone()
        assert row is not None
        instrument_id = int(row[0])
        await cur.execute(_ENSURE_CURRENCIES_ROW_SQL, {"instrument_id": instrument_id})
    return instrument_id
