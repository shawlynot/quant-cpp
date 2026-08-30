"""Environment-based configuration for CVP processes."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_POSTGRES_PORT = 5432
DEFAULT_DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"


@dataclass(frozen=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str
    deribit_base_url: str = DEFAULT_DERIBIT_BASE_URL

    @property
    def postgres_conninfo(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_database} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from the process environment.

        Nothing here reads a file: the environment is populated by whatever
        launches the process (shell, systemd unit, nvim-dap), mirroring the
        C++ gateway's `Config::from_env`.
        """
        host, _, port = os.environ["POSTGRES_HOST"].partition(":")
        return cls(
            postgres_host=host,
            postgres_port=int(port) if port else DEFAULT_POSTGRES_PORT,
            postgres_database=os.environ["POSTGRES_DATABASE"],
            postgres_user=os.environ["POSTGRES_USER"],
            postgres_password=os.environ["POSTGRES_PASSWORD"],
            deribit_base_url=os.environ.get("DERIBIT_BASE_URL", DEFAULT_DERIBIT_BASE_URL),
        )
