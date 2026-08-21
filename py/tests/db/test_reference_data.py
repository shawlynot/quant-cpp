from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from shawlynot.quant.db import reference_data as db
from shawlynot.quant.deribit.models import FutureContract, OptionContract


def _contract(
    venue_symbol: str,
    *,
    strike: Decimal = Decimal("50000"),
    option_type: str = "call",
    is_active: bool = True,
    expiration_timestamp: datetime | None = None,
) -> OptionContract:
    return OptionContract(
        venue_symbol=venue_symbol,
        venue_instrument_id=123456,
        base_currency="BTC",
        quote_currency="BTC",
        counter_currency="USD",
        settlement_currency="BTC",
        price_index="btc_usd",
        tick_size=Decimal("0.0001"),
        contract_size=Decimal("1.0"),
        min_trade_amount=Decimal("0.1"),
        expiration_timestamp=expiration_timestamp
        or (datetime.now(tz=UTC) + timedelta(days=30)),
        creation_timestamp=datetime.now(tz=UTC),
        is_active=is_active,
        option_type=option_type,
        strike=strike,
        raw={"instrument_name": venue_symbol},
    )


def _future(
    venue_symbol: str,
    *,
    settlement_period: str = "month",
    contract_size: Decimal | None = Decimal("10"),
    expiration_timestamp: datetime | None = None,
) -> FutureContract:
    return FutureContract(
        venue_symbol=venue_symbol,
        venue_instrument_id=342036,
        base_currency="BTC",
        quote_currency="USD",
        counter_currency="USD",
        settlement_currency="BTC",
        price_index="btc_usd",
        tick_size=Decimal("2.5"),
        contract_size=contract_size,
        min_trade_amount=Decimal("10"),
        settlement_period=settlement_period,
        expiration_timestamp=expiration_timestamp
        or (datetime.now(tz=UTC) + timedelta(days=90)),
        creation_timestamp=datetime.now(tz=UTC),
        is_active=True,
        raw={"instrument_name": venue_symbol},
    )


async def test_get_venue_id_returns_seeded_deribit_venue(conn: psycopg.AsyncConnection) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    assert venue_id > 0


async def test_get_venue_id_raises_for_unknown_code(conn: psycopg.AsyncConnection) -> None:
    with pytest.raises(ValueError, match="unknown venue code"):
        await db.get_venue_id(conn, "NOT_A_VENUE")


async def test_upsert_option_contract_creates_instrument_and_option_with_venue(
    conn: psycopg.AsyncConnection,
) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _contract("TEST-DB-UPSERT-1-C")

    instrument_id = await db.upsert_option_contract(conn, venue_id, contract)

    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT i.venue_id, i.symbol, i.instrument_type, i.tick_size,
                   o.option_type, o.strike, o.expiration_timestamp,
                   o.contract_size, o.creation_timestamp
            FROM security_master.instrument i
            JOIN security_master.option o USING (instrument_id)
            WHERE i.instrument_id = %s
            """,
            (instrument_id,),
        )
        row = await cur.fetchone()

    assert row is not None
    (
        venue_id_col,
        symbol,
        instrument_type,
        tick_size,
        option_type,
        strike,
        expiration_timestamp,
        contract_size,
        creation_timestamp,
    ) = row
    assert venue_id_col == venue_id  # every subtype row's parent instrument has a venue
    assert symbol == contract.venue_symbol
    assert instrument_type == "option"
    assert tick_size == contract.tick_size
    # option-level columns now live on `option`, not `instrument`
    assert option_type == "call"
    assert strike == contract.strike
    assert expiration_timestamp == contract.expiration_timestamp
    assert contract_size == 1
    assert creation_timestamp == contract.creation_timestamp


async def test_upsert_option_contract_is_idempotent(conn: psycopg.AsyncConnection) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _contract("TEST-DB-UPSERT-IDEMPOTENT-C")

    first_id = await db.upsert_option_contract(conn, venue_id, contract)
    second_id = await db.upsert_option_contract(conn, venue_id, contract)

    assert first_id == second_id
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) FROM security_master.instrument WHERE symbol = %s",
            (contract.venue_symbol,),
        )
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 1


async def test_upsert_option_contract_rejects_fractional_contract_size(
    conn: psycopg.AsyncConnection,
) -> None:
    """`option.contract_size` is an int column; a fractional value must not round silently."""
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = replace(_contract("TEST-DB-FRACTIONAL-SIZE-C"), contract_size=Decimal("0.5"))

    with pytest.raises(ValueError, match="non-integral contract_size"):
        await db.upsert_option_contract(conn, venue_id, contract)


async def test_ensure_currency_instrument_is_idempotent_and_has_venue(
    conn: psycopg.AsyncConnection,
) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")

    first_id = await db.ensure_currency_instrument(
        conn,
        venue_id,
        "TEST-DB-btc_usd",
        tick_size=Decimal("0.01"),
    )
    second_id = await db.ensure_currency_instrument(
        conn,
        venue_id,
        "TEST-DB-btc_usd",
        tick_size=Decimal("0.01"),
    )

    assert first_id == second_id
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT i.venue_id, i.instrument_type
            FROM security_master.instrument i
            JOIN security_master.currencies c USING (instrument_id)
            WHERE i.instrument_id = %s
            """,
            (first_id,),
        )
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == venue_id
    assert row[1] == "currency"


async def test_upsert_future_contract_creates_instrument_and_future(
    conn: psycopg.AsyncConnection,
) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _future("TEST-DB-FUT-1")

    instrument_id = await db.upsert_future_contract(conn, venue_id, contract)

    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT i.venue_id, i.symbol, i.instrument_type, i.tick_size,
                   f.settlement_period, f.expiration_timestamp,
                   f.contract_size, f.creation_timestamp
            FROM security_master.instrument i
            JOIN security_master.future f USING (instrument_id)
            WHERE i.instrument_id = %s
            """,
            (instrument_id,),
        )
        row = await cur.fetchone()

    assert row is not None
    assert row[0] == venue_id
    assert row[1] == "TEST-DB-FUT-1"
    assert row[2] == "future"
    assert row[3] == Decimal("2.5")
    assert row[4] == "month"
    assert row[5] == contract.expiration_timestamp
    assert row[6] == 10
    assert row[7] == contract.creation_timestamp


async def test_upsert_future_contract_is_idempotent(conn: psycopg.AsyncConnection) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _future("TEST-DB-FUT-IDEMPOTENT")

    first_id = await db.upsert_future_contract(conn, venue_id, contract)
    second_id = await db.upsert_future_contract(conn, venue_id, contract)

    assert first_id == second_id
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) FROM security_master.future WHERE instrument_id = %s",
            (first_id,),
        )
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 1


async def test_upsert_future_contract_rejects_the_perpetual(
    conn: psycopg.AsyncConnection,
) -> None:
    """The perpetual is filtered at ingest; the repository refuses it as a backstop."""
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _future("TEST-DB-PERPETUAL", settlement_period="perpetual")

    with pytest.raises(ValueError, match="perpetual is not ingested"):
        await db.upsert_future_contract(conn, venue_id, contract)


async def test_upsert_future_contract_rejects_fractional_contract_size(
    conn: psycopg.AsyncConnection,
) -> None:
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _future("TEST-DB-FUT-FRACTIONAL", contract_size=Decimal("0.5"))

    with pytest.raises(ValueError, match="non-integral contract_size"):
        await db.upsert_future_contract(conn, venue_id, contract)


async def test_upsert_future_contract_accepts_daily_settlement_period(
    conn: psycopg.AsyncConnection,
) -> None:
    """Deribit lists 'day' futures too -- only 'perpetual' is excluded."""
    venue_id = await db.get_venue_id(conn, "DERIBIT")
    contract = _future("TEST-DB-FUT-DAILY", settlement_period="day")

    instrument_id = await db.upsert_future_contract(conn, venue_id, contract)

    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT settlement_period FROM security_master.future WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == "day"
