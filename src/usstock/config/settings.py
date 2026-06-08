"""Application settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_MIGRATIONS_DIR = PROJECT_ROOT / "migrations"


def _strip_optional_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def read_env_file(path: Path = DEFAULT_ENV_FILE) -> dict[str, str]:
    """Read simple KEY=VALUE pairs from an env file."""

    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        values[key] = _strip_optional_quotes(value)

    return values


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables and .env."""

    database_url: str | None
    migrations_dir: Path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    env_file_values = read_env_file()

    database_url = os.environ.get("DATABASE_URL") or env_file_values.get("DATABASE_URL")
    migrations_dir_value = (
        os.environ.get("MIGRATIONS_DIR")
        or env_file_values.get("MIGRATIONS_DIR")
        or str(DEFAULT_MIGRATIONS_DIR)
    )

    migrations_dir = Path(migrations_dir_value).expanduser()
    if not migrations_dir.is_absolute():
        migrations_dir = PROJECT_ROOT / migrations_dir

    return Settings(
        database_url=database_url,
        migrations_dir=migrations_dir,
    )
