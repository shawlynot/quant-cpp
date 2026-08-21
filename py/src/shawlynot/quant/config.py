"""Environment-based configuration for CVP processes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

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
    def from_env(cls, env_file: Path | str | None = None) -> Settings:
        """Build settings from process env, loading a `.env` file first if found.

        `env_file` overrides discovery; otherwise `python-dotenv` searches
        upward from the current working directory (finds the repo-root
        `.env`). Variables already exported in the shell take precedence
        over `.env` contents.
        """
        dotenv_path = str(env_file) if env_file is not None else find_dotenv(usecwd=True)
        if dotenv_path:
            load_dotenv(dotenv_path, override=False)

        host, _, port = os.environ["POSTGRES_HOST"].partition(":")
        return cls(
            postgres_host=host,
            postgres_port=int(port) if port else DEFAULT_POSTGRES_PORT,
            postgres_database=os.environ["POSTGRES_DATABASE"],
            postgres_user=os.environ["POSTGRES_USER"],
            postgres_password=os.environ["POSTGRES_PASSWORD"],
            deribit_base_url=os.environ.get("DERIBIT_BASE_URL", DEFAULT_DERIBIT_BASE_URL),
        )
