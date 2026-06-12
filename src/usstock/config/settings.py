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
    sec_user_agent: str | None
    sec_rate_limit_per_second: float
    sec_request_timeout_seconds: float
    sec_base_url: str
    sec_archives_base_url: str
    gdelt_doc_base_url: str
    gdelt_rate_limit_per_second: float
    gdelt_request_timeout_seconds: float
    finnhub_api_key: str | None
    finnhub_base_url: str
    finnhub_rate_limit_per_second: float
    finnhub_request_timeout_seconds: float
    reddit_client_id: str | None
    reddit_client_secret: str | None
    reddit_user_agent: str | None
    reddit_base_url: str
    reddit_oauth_url: str
    reddit_rate_limit_per_second: float
    reddit_request_timeout_seconds: float
    reddit_devvit_webhook_secret: str | None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    env_file_values = read_env_file()

    database_url = os.environ.get("DATABASE_URL") or env_file_values.get("DATABASE_URL")
    migrations_dir_value = (
        os.environ.get("MIGRATIONS_DIR")
        or env_file_values.get("MIGRATIONS_DIR")
        or str(DEFAULT_MIGRATIONS_DIR)
    )
    sec_user_agent = os.environ.get("SEC_USER_AGENT") or env_file_values.get(
        "SEC_USER_AGENT"
    )
    sec_rate_limit_per_second = float(
        os.environ.get("SEC_RATE_LIMIT_PER_SECOND")
        or env_file_values.get("SEC_RATE_LIMIT_PER_SECOND")
        or "5"
    )
    sec_request_timeout_seconds = float(
        os.environ.get("SEC_REQUEST_TIMEOUT_SECONDS")
        or env_file_values.get("SEC_REQUEST_TIMEOUT_SECONDS")
        or "30"
    )
    sec_base_url = (
        os.environ.get("SEC_BASE_URL")
        or env_file_values.get("SEC_BASE_URL")
        or "https://data.sec.gov"
    ).rstrip("/")
    sec_archives_base_url = (
        os.environ.get("SEC_ARCHIVES_BASE_URL")
        or env_file_values.get("SEC_ARCHIVES_BASE_URL")
        or "https://www.sec.gov/Archives"
    ).rstrip("/")
    gdelt_doc_base_url = (
        os.environ.get("GDELT_DOC_BASE_URL")
        or env_file_values.get("GDELT_DOC_BASE_URL")
        or "https://api.gdeltproject.org/api/v2/doc/doc"
    )
    gdelt_rate_limit_per_second = float(
        os.environ.get("GDELT_RATE_LIMIT_PER_SECOND")
        or env_file_values.get("GDELT_RATE_LIMIT_PER_SECOND")
        or "0.2"
    )
    gdelt_request_timeout_seconds = float(
        os.environ.get("GDELT_REQUEST_TIMEOUT_SECONDS")
        or env_file_values.get("GDELT_REQUEST_TIMEOUT_SECONDS")
        or "30"
    )
    finnhub_api_key = (
        os.environ.get("FINNHUB_API_KEY")
        or env_file_values.get("FINNHUB_API_KEY")
        or os.environ.get("FINNHUB_TOKEN")
        or env_file_values.get("FINNHUB_TOKEN")
    )
    finnhub_base_url = (
        os.environ.get("FINNHUB_BASE_URL")
        or env_file_values.get("FINNHUB_BASE_URL")
        or "https://finnhub.io/api/v1"
    ).rstrip("/")
    finnhub_rate_limit_per_second = float(
        os.environ.get("FINNHUB_RATE_LIMIT_PER_SECOND")
        or env_file_values.get("FINNHUB_RATE_LIMIT_PER_SECOND")
        or "1"
    )
    finnhub_request_timeout_seconds = float(
        os.environ.get("FINNHUB_REQUEST_TIMEOUT_SECONDS")
        or env_file_values.get("FINNHUB_REQUEST_TIMEOUT_SECONDS")
        or "30"
    )
    reddit_client_id = os.environ.get("REDDIT_CLIENT_ID") or env_file_values.get(
        "REDDIT_CLIENT_ID"
    )
    reddit_client_secret = os.environ.get(
        "REDDIT_CLIENT_SECRET"
    ) or env_file_values.get("REDDIT_CLIENT_SECRET")
    reddit_user_agent = os.environ.get("REDDIT_USER_AGENT") or env_file_values.get(
        "REDDIT_USER_AGENT"
    )
    reddit_base_url = (
        os.environ.get("REDDIT_BASE_URL")
        or env_file_values.get("REDDIT_BASE_URL")
        or "https://oauth.reddit.com"
    ).rstrip("/")
    reddit_oauth_url = (
        os.environ.get("REDDIT_OAUTH_URL")
        or env_file_values.get("REDDIT_OAUTH_URL")
        or "https://www.reddit.com/api/v1/access_token"
    )
    reddit_rate_limit_per_second = float(
        os.environ.get("REDDIT_RATE_LIMIT_PER_SECOND")
        or env_file_values.get("REDDIT_RATE_LIMIT_PER_SECOND")
        or "0.5"
    )
    reddit_request_timeout_seconds = float(
        os.environ.get("REDDIT_REQUEST_TIMEOUT_SECONDS")
        or env_file_values.get("REDDIT_REQUEST_TIMEOUT_SECONDS")
        or "30"
    )
    reddit_devvit_webhook_secret = os.environ.get(
        "REDDIT_DEVVIT_WEBHOOK_SECRET"
    ) or env_file_values.get("REDDIT_DEVVIT_WEBHOOK_SECRET")

    migrations_dir = Path(migrations_dir_value).expanduser()
    if not migrations_dir.is_absolute():
        migrations_dir = PROJECT_ROOT / migrations_dir

    return Settings(
        database_url=database_url,
        migrations_dir=migrations_dir,
        sec_user_agent=sec_user_agent,
        sec_rate_limit_per_second=sec_rate_limit_per_second,
        sec_request_timeout_seconds=sec_request_timeout_seconds,
        sec_base_url=sec_base_url,
        sec_archives_base_url=sec_archives_base_url,
        gdelt_doc_base_url=gdelt_doc_base_url,
        gdelt_rate_limit_per_second=gdelt_rate_limit_per_second,
        gdelt_request_timeout_seconds=gdelt_request_timeout_seconds,
        finnhub_api_key=finnhub_api_key,
        finnhub_base_url=finnhub_base_url,
        finnhub_rate_limit_per_second=finnhub_rate_limit_per_second,
        finnhub_request_timeout_seconds=finnhub_request_timeout_seconds,
        reddit_client_id=reddit_client_id,
        reddit_client_secret=reddit_client_secret,
        reddit_user_agent=reddit_user_agent,
        reddit_base_url=reddit_base_url,
        reddit_oauth_url=reddit_oauth_url,
        reddit_rate_limit_per_second=reddit_rate_limit_per_second,
        reddit_request_timeout_seconds=reddit_request_timeout_seconds,
        reddit_devvit_webhook_secret=reddit_devvit_webhook_secret,
    )
