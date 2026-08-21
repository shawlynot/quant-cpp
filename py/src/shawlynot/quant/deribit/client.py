"""Thin async client for Deribit's public REST API."""

from __future__ import annotations

from types import TracebackType
from typing import Any

import httpx

DEFAULT_BASE_URL = "https://www.deribit.com/api/v2"


class DeribitClient:
    """Wraps the public (unauthenticated) Deribit REST endpoints this project needs."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL, *, timeout: float = 10.0) -> None:
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def __aenter__(self) -> DeribitClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _get(self, method: str, params: dict[str, Any]) -> Any:
        response = await self._http.get(f"/public/{method}", params=params)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise DeribitAPIError(method, body["error"])
        return body["result"]

    async def get_instruments(
        self,
        currency: str = "BTC",
        kind: str = "option",
        expired: bool = False,
    ) -> list[dict[str, Any]]:
        """`GET /public/get_instruments` — instrument reference data."""
        result: list[dict[str, Any]] = await self._get(
            "get_instruments",
            {"currency": currency, "kind": kind, "expired": str(expired).lower()},
        )
        return result


class DeribitAPIError(RuntimeError):
    def __init__(self, method: str, error: dict[str, Any]) -> None:
        super().__init__(f"Deribit API error calling {method}: {error}")
        self.method = method
        self.error = error
