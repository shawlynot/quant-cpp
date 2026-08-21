from collections.abc import AsyncIterator

import psycopg
import pytest

from shawlynot.quant.config import Settings
from shawlynot.quant.db import reference_data as db


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session", autouse=True)
async def _schema(settings: Settings) -> None:
    """Ensure security_master exists once per test session (safe to leave committed)."""
    conn = await psycopg.AsyncConnection.connect(settings.postgres_conninfo)
    try:
        await db.ensure_schema(conn)
    finally:
        await conn.close()


@pytest.fixture
async def conn(settings: Settings) -> AsyncIterator[psycopg.AsyncConnection]:
    """A connection whose writes are rolled back at the end of each test."""
    connection = await psycopg.AsyncConnection.connect(settings.postgres_conninfo)
    try:
        yield connection
    finally:
        await connection.rollback()
        await connection.close()
