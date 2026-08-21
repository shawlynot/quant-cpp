"""One-shot ingestion of BTC option and dated-future reference data (+ BTC/USD currency identity).

Intended to be invoked repeatedly by an external scheduler (cron/systemd timer);
each run does a single fetch-and-upsert pass, then exits.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from decimal import Decimal

import psycopg

from shawlynot.quant.config import Settings
from shawlynot.quant.db import reference_data as db
from shawlynot.quant.deribit.client import DeribitClient
from shawlynot.quant.deribit.models import FutureContract, OptionContract

logger = logging.getLogger(__name__)

DERIBIT_VENUE_CODE = "DERIBIT"
BTC_USD_SYMBOL = "btc_usd"
# Deribit's btc_usd index has no exchange-defined tick size; instrument.tick_size
# is NOT NULL, so this is a placeholder (USD-cent granularity).
BTC_USD_PLACEHOLDER_TICK_SIZE = Decimal("0.01")
# `kind=future` returns the dated futures *and* BTC-PERPETUAL. The perpetual is
# outside the streamed universe and is deliberately not ingested, so it is
# dropped here rather than at subscribe time — that keeps it out of
# `security_master` entirely, where no later query can pull it in.


async def run(settings: Settings) -> None:
    start = time.monotonic()
    async with (
        DeribitClient(settings.deribit_base_url) as client,
        await psycopg.AsyncConnection.connect(settings.postgres_conninfo) as conn,
    ):
        await db.ensure_schema(conn)
        venue_id = await db.get_venue_id(conn, DERIBIT_VENUE_CODE)

        raw_options = await client.get_instruments(currency="BTC", kind="option", expired=False)
        contracts = [OptionContract.from_api(payload) for payload in raw_options]

        for contract in contracts:
            await db.upsert_option_contract(conn, venue_id, contract)

        raw_futures = await client.get_instruments(currency="BTC", kind="future", expired=False)
        all_futures = [FutureContract.from_api(payload) for payload in raw_futures]
        futures = [future for future in all_futures if not future.is_perpetual]
        logger.debug(
            "dropping %d perpetual instrument(s) from %d kind=future rows",
            len(all_futures) - len(futures),
            len(all_futures),
        )

        for future in futures:
            await db.upsert_future_contract(conn, venue_id, future)

        await db.ensure_currency_instrument(
            conn,
            venue_id,
            BTC_USD_SYMBOL,
            tick_size=BTC_USD_PLACEHOLDER_TICK_SIZE,
        )

        await conn.commit()

    elapsed = time.monotonic() - start
    logger.info(
        "ingested %d active BTC option contracts and %d dated futures, "
        "ensured BTC/USD currency row (%.2fs)",
        len(contracts),
        len(futures),
        elapsed,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logging"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        asyncio.run(run(Settings.from_env()))
    except Exception:
        logger.exception("reference data ingestion failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
