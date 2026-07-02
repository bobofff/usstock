"""Stock universe maintenance entry points."""

from __future__ import annotations

import csv
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.db import migrations as db_migrations


DEFAULT_UNIVERSE_DATA_SOURCE = "active_universe_file"
NASDAQ_SCREENER_DATA_SOURCE = "nasdaq_screener_api"
NASDAQ_SCREENER_BASE_URL = "https://api.nasdaq.com/api/screener/stocks"
DEFAULT_NASDAQ_REQUEST_LIMIT = 25000
DEFAULT_NASDAQ_TIMEOUT_SECONDS = 30.0
DEFAULT_MIN_AVG_VOLUME_30D = Decimal("200000")
DEFAULT_MIN_MARKET_CAP_USD = Decimal("100000000")
DEFAULT_MIN_LAST_PRICE = Decimal("1")
DEFAULT_ALLOWED_EXCHANGES = ("NASDAQ", "NYSE", "AMEX")
DEFAULT_ALLOWED_ASSET_TYPES = ("equity", "adr", "reit")
NASDAQ_EXCHANGE_PARAMS = {
    "NASDAQ": "nasdaq",
    "NYSE": "nyse",
    "AMEX": "amex",
}
FILTER_VERSION = "active_us_equity_v1"
REJECT_SAMPLE_LIMIT = 20

TICKER_ALIASES = (
    "ticker",
    "symbol",
    "nasdaqsymbol",
    "actsymbol",
)
COMPANY_NAME_ALIASES = (
    "companyname",
    "securityname",
    "name",
    "issuername",
    "company",
)
EXCHANGE_ALIASES = (
    "exchange",
    "exchangename",
    "listingexchange",
    "primaryexchange",
    "market",
)
SECTOR_ALIASES = ("sector", "sectorname")
INDUSTRY_ALIASES = ("industry", "industryname")
COUNTRY_ALIASES = ("country", "countryname")
CURRENCY_ALIASES = ("currency", "currencycode")
ASSET_TYPE_ALIASES = (
    "assettype",
    "securitytype",
    "instrumenttype",
    "quotetype",
    "type",
)
MARKET_CAP_ALIASES = (
    "marketcapusd",
    "marketcap",
    "marketcapitalization",
    "marketcapintraday",
)
AVG_VOLUME_ALIASES = (
    "avgvolume30d",
    "avgvol30d",
    "averagevolume30d",
    "averagevolume",
    "averagedailyvolume",
    "averagedailyvolume3month",
    "volumeavg30d",
)
LAST_PRICE_ALIASES = (
    "lastprice",
    "lastsale",
    "price",
    "regularmarketprice",
)
DESCRIPTION_ALIASES = (
    "businessdescription",
    "description",
    "longbusinesssummary",
    "summary",
)
CIK_ALIASES = ("seccik", "cik")

FUND_SECURITY_TERMS = (
    "exchange traded fund",
    "exchange-traded fund",
    "etf",
    "etn",
    "index fund",
    "mutual fund",
    "closed end fund",
    "closed-end fund",
    "income fund",
    "municipal fund",
    "bond fund",
    "treasury fund",
    "money market fund",
    "spdr",
    "ishares",
    "proshares",
    "direxion",
    "invesco qqq",
    "vanguard",
)
NON_COMMON_SECURITY_TERMS = (
    "warrant",
    "warrants",
    "right",
    "rights",
    "unit",
    "units",
    "preferred",
    "preference",
    "senior notes",
    "subordinated notes",
    "notes due",
    "debenture",
    "bond",
)
SYMBOL_EXCLUDE_SUFFIX_PATTERN = re.compile(
    r"([.\-/](?:WS|WT|WTS|W|U|UNIT|RT|R|PR[A-Z]?|P[A-Z]))$",
    flags=re.IGNORECASE,
)


class StockUniverseError(RuntimeError):
    """Raised when stock universe ingestion fails."""


@dataclass(frozen=True)
class StockUniverseFilterConfig:
    min_avg_volume_30d: Decimal = DEFAULT_MIN_AVG_VOLUME_30D
    min_market_cap_usd: Decimal = DEFAULT_MIN_MARKET_CAP_USD
    min_last_price: Decimal = DEFAULT_MIN_LAST_PRICE
    allowed_exchanges: tuple[str, ...] = DEFAULT_ALLOWED_EXCHANGES
    allowed_asset_types: tuple[str, ...] = DEFAULT_ALLOWED_ASSET_TYPES
    allow_missing_market_cap: bool = True
    require_avg_volume_30d: bool = True


@dataclass(frozen=True)
class StockUniverseRecord:
    ticker: str
    company_name: str
    exchange: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str = "US"
    currency: str = "USD"
    asset_type: str = "equity"
    sec_cik: str | None = None
    business_description: str | None = None
    market_cap_usd: Decimal | None = None
    avg_volume_30d: Decimal | None = None
    last_price: Decimal | None = None
    data_source: str = DEFAULT_UNIVERSE_DATA_SOURCE
    source_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RejectedUniverseRow:
    row_number: int
    ticker: str | None
    company_name: str | None
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class StockUniverseImportResult:
    source_path: str
    dry_run: bool
    total_rows: int
    accepted_count: int
    rejected_count: int
    upserted_count: int
    accepted_tickers: tuple[str, ...]
    rejection_reason_counts: dict[str, int]
    rejected_samples: tuple[RejectedUniverseRow, ...]
    filter_config: StockUniverseFilterConfig


class NasdaqScreenerClient:
    """Small client for Nasdaq's public screener JSON endpoint."""

    def __init__(
        self,
        *,
        base_url: str = NASDAQ_SCREENER_BASE_URL,
        timeout_seconds: float = DEFAULT_NASDAQ_TIMEOUT_SECONDS,
        request_limit: int = DEFAULT_NASDAQ_REQUEST_LIMIT,
    ) -> None:
        if request_limit <= 0:
            raise StockUniverseError("request_limit 必须大于 0。")
        if timeout_seconds <= 0:
            raise StockUniverseError("timeout_seconds 必须大于 0。")
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.request_limit = request_limit

    def fetch_exchange_rows(
        self,
        *,
        exchange: str,
        limit: int | None = None,
    ) -> tuple[dict[str, Any], ...]:
        normalized_exchange = exchange.strip().upper()
        exchange_param = NASDAQ_EXCHANGE_PARAMS.get(normalized_exchange)
        if not exchange_param:
            raise StockUniverseError(f"Nasdaq screener 不支持交易所: {exchange}")
        request_limit = limit or self.request_limit
        if request_limit <= 0:
            raise StockUniverseError("limit 必须大于 0。")

        params = {
            "tableonly": "true",
            "download": "true",
            "exchange": exchange_param,
            "limit": str(request_limit),
            "offset": "0",
        }
        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": "Mozilla/5.0 usstock-universe-client/0.1",
                "Origin": "https://www.nasdaq.com",
                "Referer": "https://www.nasdaq.com/market-activity/stocks/screener",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise StockUniverseError(f"Nasdaq screener 请求失败: {normalized_exchange}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise StockUniverseError(f"Nasdaq screener 返回不是有效 JSON: {normalized_exchange}") from exc

        rows = extract_nasdaq_rows(payload)
        return tuple(
            {
                **row,
                "_source_exchange": normalized_exchange,
                "_source_url": url,
            }
            for row in rows[:request_limit]
        )

    def fetch_rows(
        self,
        *,
        exchanges: tuple[str, ...] = DEFAULT_ALLOWED_EXCHANGES,
        limit: int | None = None,
    ) -> tuple[dict[str, Any], ...]:
        rows: list[dict[str, Any]] = []
        for exchange in exchanges:
            remaining = None if limit is None else max(limit - len(rows), 0)
            if remaining == 0:
                break
            rows.extend(self.fetch_exchange_rows(exchange=exchange, limit=remaining))
        return tuple(rows[:limit] if limit is not None else rows)


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise StockUniverseError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def ensure_stock_universe_schema(database_url: str) -> int:
    return len(db_migrations.migrate(database_url=database_url))


def normalize_key(value: str) -> str:
    return "".join(ch for ch in value.lower().strip() if ch.isalnum())


def normalize_row_keys(row: dict[str, Any]) -> dict[str, Any]:
    return {normalize_key(str(key)): value for key, value in row.items()}


def get_first(row: dict[str, Any], aliases: tuple[str, ...]) -> Any | None:
    for alias in aliases:
        value = row.get(alias)
        if value is None:
            continue
        if str(value).strip():
            return value
    return None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().split())
    if not text or text.lower() in {"n/a", "na", "none", "null", "--", "-"}:
        return None
    return text


def normalize_ticker(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("$"):
        text = text[1:]
    text = re.sub(r"\s+", "", text)
    return text.replace("/", ".")


def normalize_exchange(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None

    compact = re.sub(r"[^A-Z0-9]+", " ", text.upper()).strip()
    if not compact:
        return None
    if any(term in compact for term in ("OTC", "PINK", "GREY", "GREY MARKET")):
        return "OTC"
    if "NASDAQ" in compact:
        return "NASDAQ"
    if "NYSE ARCA" in compact or compact == "ARCA":
        return "NYSEARCA"
    if "NYSE AMERICAN" in compact or "AMEX" in compact or "NYSE MKT" in compact:
        return "AMEX"
    if compact == "NYSE" or compact.startswith("NYSE "):
        return "NYSE"
    return compact.replace(" ", "_")


def parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float):
        return Decimal(str(value))

    text = str(value).strip()
    if not text or text.lower() in {"n/a", "na", "nan", "none", "null", "--", "-"}:
        return None
    text = text.replace("$", "").replace(",", "").replace("%", "").strip()
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]

    multiplier = Decimal("1")
    suffix = text[-1:].upper()
    if suffix in {"K", "M", "B", "T"}:
        text = text[:-1].strip()
        multiplier = {
            "K": Decimal("1000"),
            "M": Decimal("1000000"),
            "B": Decimal("1000000000"),
            "T": Decimal("1000000000000"),
        }[suffix]

    try:
        return Decimal(text) * multiplier
    except InvalidOperation:
        return None


def normalize_asset_type(raw_asset_type: Any, company_name: str | None) -> str:
    raw = clean_text(raw_asset_type) or ""
    label = f"{raw} {company_name or ''}".lower()

    if any(label_has_term(label, term) for term in FUND_SECURITY_TERMS):
        return "etf"
    if "american depositary" in label or re.search(r"\badr\b", label):
        return "adr"
    if "real estate investment trust" in label or re.search(r"\breit\b", label):
        return "reit"
    if any(label_has_term(label, term) for term in NON_COMMON_SECURITY_TERMS):
        return "other"
    if any(term in label for term in ("common stock", "common shares", "ordinary shares")):
        return "equity"
    if raw.lower() in {"stock", "equity", "common", "common stock", "ordinary share"}:
        return "equity"
    return "equity"


def label_has_term(label: str, term: str) -> bool:
    parts = re.findall(r"[a-z0-9]+", term.lower())
    if not parts:
        return False
    pattern = r"(?<![a-z0-9])" + r"[\W_]+".join(
        re.escape(part) for part in parts
    ) + r"(?![a-z0-9])"
    return re.search(pattern, label.lower()) is not None


def has_excluded_security_terms(record: StockUniverseRecord) -> bool:
    label = f" {record.company_name} {record.metadata.get('raw_asset_type') or ''} ".lower()
    if any(label_has_term(label, term) for term in FUND_SECURITY_TERMS):
        return True
    if "depositary" in label and "preferred" not in label and "american depositary" in label:
        return False
    return any(label_has_term(label, term) for term in NON_COMMON_SECURITY_TERMS)


def parse_universe_record(
    raw_row: dict[str, Any],
    *,
    row_number: int,
    data_source: str,
    source_url: str | None,
) -> StockUniverseRecord:
    row = normalize_row_keys(raw_row)
    ticker = normalize_ticker(get_first(row, TICKER_ALIASES) or "")
    company_name = clean_text(get_first(row, COMPANY_NAME_ALIASES)) or ""
    exchange = normalize_exchange(get_first(row, EXCHANGE_ALIASES))
    raw_asset_type = clean_text(get_first(row, ASSET_TYPE_ALIASES))
    asset_type = normalize_asset_type(raw_asset_type, company_name)
    sec_cik = clean_text(get_first(row, CIK_ALIASES))
    if sec_cik:
        sec_cik_digits = "".join(ch for ch in sec_cik if ch.isdigit())
        sec_cik = sec_cik_digits.zfill(10) if sec_cik_digits else None

    metadata = {
        "filter_version": FILTER_VERSION,
        "row_number": row_number,
        "raw_asset_type": raw_asset_type,
        "raw_exchange": clean_text(get_first(row, EXCHANGE_ALIASES)),
        "universe_source": data_source,
        "raw": raw_row,
    }

    return StockUniverseRecord(
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
        sector=clean_text(get_first(row, SECTOR_ALIASES)),
        industry=clean_text(get_first(row, INDUSTRY_ALIASES)),
        country=(clean_text(get_first(row, COUNTRY_ALIASES)) or "US").upper(),
        currency=(clean_text(get_first(row, CURRENCY_ALIASES)) or "USD").upper(),
        asset_type=asset_type,
        sec_cik=sec_cik or None,
        business_description=clean_text(get_first(row, DESCRIPTION_ALIASES)),
        market_cap_usd=parse_decimal(get_first(row, MARKET_CAP_ALIASES)),
        avg_volume_30d=parse_decimal(get_first(row, AVG_VOLUME_ALIASES)),
        last_price=parse_decimal(get_first(row, LAST_PRICE_ALIASES)),
        data_source=data_source,
        source_url=source_url,
        metadata=metadata,
    )


def extract_nasdaq_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    table = data.get("table")
    if isinstance(table, dict):
        rows = table.get("rows")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    rows = data.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def nasdaq_row_to_universe_record(
    raw_row: dict[str, Any],
    *,
    row_number: int,
    use_volume_as_avg_volume: bool = True,
) -> StockUniverseRecord:
    exchange = clean_text(raw_row.get("_source_exchange"))
    source_url = clean_text(raw_row.get("_source_url")) or NASDAQ_SCREENER_BASE_URL
    normalized = normalize_row_keys(raw_row)
    avg_volume = (
        get_first(normalized, AVG_VOLUME_ALIASES)
        or (raw_row.get("volume") if use_volume_as_avg_volume else None)
    )
    row = {
        "Symbol": raw_row.get("symbol"),
        "Name": raw_row.get("name"),
        "Exchange": exchange,
        "Market Cap": raw_row.get("marketCap"),
        "Avg Volume 30D": avg_volume,
        "Last Sale": raw_row.get("lastsale") or raw_row.get("lastSale"),
        "Sector": raw_row.get("sector"),
        "Industry": raw_row.get("industry"),
        "Country": raw_row.get("country"),
    }
    record = parse_universe_record(
        row,
        row_number=row_number,
        data_source=NASDAQ_SCREENER_DATA_SOURCE,
        source_url=source_url,
    )
    return replace(
        record,
        metadata={
            **record.metadata,
            "raw": raw_row,
            "nasdaq": {
                "source_exchange": exchange,
                "source_url": source_url,
                "volume": raw_row.get("volume"),
                "used_volume_as_avg_volume_30d": use_volume_as_avg_volume,
            },
        },
    )


def nasdaq_rows_to_records(
    rows: tuple[dict[str, Any], ...],
    *,
    use_volume_as_avg_volume: bool = True,
) -> tuple[StockUniverseRecord, ...]:
    return tuple(
        nasdaq_row_to_universe_record(
            row,
            row_number=index,
            use_volume_as_avg_volume=use_volume_as_avg_volume,
        )
        for index, row in enumerate(rows, start=1)
    )


def rejection_reasons(
    record: StockUniverseRecord,
    config: StockUniverseFilterConfig,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not record.ticker:
        reasons.append("missing_ticker")
    elif not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,11}", record.ticker):
        reasons.append("invalid_ticker")
    elif SYMBOL_EXCLUDE_SUFFIX_PATTERN.search(record.ticker):
        reasons.append("excluded_symbol_suffix")

    if not record.company_name:
        reasons.append("missing_company_name")

    allowed_exchanges = {exchange.upper() for exchange in config.allowed_exchanges}
    if record.exchange not in allowed_exchanges:
        reasons.append(f"unsupported_exchange:{record.exchange or 'missing'}")

    allowed_asset_types = {asset_type.lower() for asset_type in config.allowed_asset_types}
    if record.asset_type.lower() not in allowed_asset_types:
        reasons.append(f"unsupported_asset_type:{record.asset_type}")

    if has_excluded_security_terms(record):
        reasons.append("excluded_security_type")

    if record.avg_volume_30d is None:
        if config.require_avg_volume_30d:
            reasons.append("missing_avg_volume_30d")
    elif record.avg_volume_30d < config.min_avg_volume_30d:
        reasons.append("avg_volume_30d_below_min")

    if record.market_cap_usd is None:
        if not config.allow_missing_market_cap:
            reasons.append("missing_market_cap_usd")
    elif record.market_cap_usd < config.min_market_cap_usd:
        reasons.append("market_cap_usd_below_min")

    if record.last_price is not None and record.last_price < config.min_last_price:
        reasons.append("last_price_below_min")

    return tuple(reasons)


def load_stock_universe_file(
    path: Path,
    *,
    data_source: str = DEFAULT_UNIVERSE_DATA_SOURCE,
    source_url: str | None = None,
    limit: int | None = None,
) -> tuple[StockUniverseRecord, ...]:
    if not path.exists():
        raise StockUniverseError(f"股票池文件不存在: {path}")
    if not path.is_file():
        raise StockUniverseError(f"股票池路径不是文件: {path}")
    if limit is not None and limit <= 0:
        raise StockUniverseError("limit 必须大于 0。")

    records: list[StockUniverseRecord] = []
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with path.open("r", encoding="utf-8-sig") as file:
            for index, line in enumerate(file, start=1):
                if limit is not None and len(records) >= limit:
                    break
                if not line.strip():
                    continue
                try:
                    raw_row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise StockUniverseError(f"JSONL 第 {index} 行不是有效 JSON。") from exc
                if not isinstance(raw_row, dict):
                    raise StockUniverseError(f"JSONL 第 {index} 行必须是对象。")
                records.append(
                    parse_universe_record(
                        raw_row,
                        row_number=index,
                        data_source=data_source,
                        source_url=source_url,
                    )
                )
    else:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            if not reader.fieldnames:
                raise StockUniverseError(f"股票池 CSV 缺少表头: {path}")
            for row_number, raw_row in enumerate(reader, start=2):
                if limit is not None and len(records) >= limit:
                    break
                records.append(
                    parse_universe_record(
                        raw_row,
                        row_number=row_number,
                        data_source=data_source,
                        source_url=source_url,
                    )
                )

    return tuple(records)


def filter_stock_universe_records(
    records: tuple[StockUniverseRecord, ...],
    *,
    config: StockUniverseFilterConfig,
) -> tuple[tuple[StockUniverseRecord, ...], tuple[RejectedUniverseRow, ...]]:
    accepted: list[StockUniverseRecord] = []
    rejected: list[RejectedUniverseRow] = []
    seen: set[str] = set()
    for record in records:
        reasons = list(rejection_reasons(record, config))
        if record.ticker and record.ticker in seen:
            reasons.append("duplicate_ticker_in_file")
        if reasons:
            rejected.append(
                RejectedUniverseRow(
                    row_number=int(record.metadata.get("row_number") or 0),
                    ticker=record.ticker or None,
                    company_name=record.company_name or None,
                    reasons=tuple(reasons),
                )
            )
            continue
        seen.add(record.ticker)
        accepted.append(
            replace(
                record,
                metadata={
                    **record.metadata,
                    "accepted_at": datetime.now().isoformat(timespec="seconds"),
                    "filter": {
                        "min_avg_volume_30d": str(config.min_avg_volume_30d),
                        "min_market_cap_usd": str(config.min_market_cap_usd),
                        "min_last_price": str(config.min_last_price),
                        "allow_missing_market_cap": config.allow_missing_market_cap,
                        "require_avg_volume_30d": config.require_avg_volume_30d,
                    },
                },
            )
        )
    return tuple(accepted), tuple(rejected)


def upsert_stock_universe_records(
    conn: Connection,
    records: tuple[StockUniverseRecord, ...],
) -> int:
    count = 0
    for record in records:
        conn.execute(
            """
            INSERT INTO stock_universe (
                ticker,
                company_name,
                exchange,
                sector,
                industry,
                country,
                currency,
                asset_type,
                sec_cik,
                business_description,
                market_cap_usd,
                avg_volume_30d,
                last_price,
                is_active,
                data_source,
                source_url,
                last_refreshed_at,
                metadata
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, TRUE, %s, %s, now(), %s
            )
            ON CONFLICT (ticker)
            DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, stock_universe.company_name),
                exchange = COALESCE(EXCLUDED.exchange, stock_universe.exchange),
                sector = COALESCE(EXCLUDED.sector, stock_universe.sector),
                industry = COALESCE(EXCLUDED.industry, stock_universe.industry),
                country = COALESCE(EXCLUDED.country, stock_universe.country),
                currency = COALESCE(EXCLUDED.currency, stock_universe.currency),
                asset_type = EXCLUDED.asset_type,
                sec_cik = COALESCE(EXCLUDED.sec_cik, stock_universe.sec_cik),
                business_description = COALESCE(
                    EXCLUDED.business_description,
                    stock_universe.business_description
                ),
                market_cap_usd = COALESCE(
                    EXCLUDED.market_cap_usd,
                    stock_universe.market_cap_usd
                ),
                avg_volume_30d = COALESCE(
                    EXCLUDED.avg_volume_30d,
                    stock_universe.avg_volume_30d
                ),
                last_price = COALESCE(EXCLUDED.last_price, stock_universe.last_price),
                is_active = TRUE,
                is_manual_watchlist = stock_universe.is_manual_watchlist,
                data_source = EXCLUDED.data_source,
                source_url = COALESCE(EXCLUDED.source_url, stock_universe.source_url),
                last_refreshed_at = now(),
                metadata = stock_universe.metadata || EXCLUDED.metadata,
                updated_at = now()
            """,
            (
                record.ticker,
                record.company_name,
                record.exchange,
                record.sector,
                record.industry,
                record.country,
                record.currency,
                record.asset_type,
                record.sec_cik,
                record.business_description,
                record.market_cap_usd,
                record.avg_volume_30d,
                record.last_price,
                record.data_source,
                record.source_url,
                Jsonb(record.metadata),
            ),
        )
        count += 1
    return count


def build_import_result(
    *,
    source_path: Path | str,
    dry_run: bool,
    total_rows: int,
    accepted: tuple[StockUniverseRecord, ...],
    rejected: tuple[RejectedUniverseRow, ...],
    upserted_count: int,
    filter_config: StockUniverseFilterConfig,
) -> StockUniverseImportResult:
    counter: Counter[str] = Counter()
    for row in rejected:
        counter.update(row.reasons)
    return StockUniverseImportResult(
        source_path=str(source_path),
        dry_run=dry_run,
        total_rows=total_rows,
        accepted_count=len(accepted),
        rejected_count=len(rejected),
        upserted_count=upserted_count,
        accepted_tickers=tuple(record.ticker for record in accepted[:50]),
        rejection_reason_counts=dict(counter.most_common()),
        rejected_samples=tuple(rejected[:REJECT_SAMPLE_LIMIT]),
        filter_config=filter_config,
    )


def import_stock_universe_file(
    *,
    file_path: Path,
    database_url: str | None = None,
    data_source: str = DEFAULT_UNIVERSE_DATA_SOURCE,
    source_url: str | None = None,
    filter_config: StockUniverseFilterConfig | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> StockUniverseImportResult:
    filter_config = filter_config or StockUniverseFilterConfig()
    records = load_stock_universe_file(
        file_path,
        data_source=data_source,
        source_url=source_url,
        limit=limit,
    )
    accepted, rejected = filter_stock_universe_records(records, config=filter_config)
    if dry_run:
        return build_import_result(
            source_path=file_path,
            dry_run=True,
            total_rows=len(records),
            accepted=accepted,
            rejected=rejected,
            upserted_count=0,
            filter_config=filter_config,
        )

    resolved_database_url = get_database_url(database_url)
    ensure_stock_universe_schema(resolved_database_url)
    with psycopg.connect(resolved_database_url, autocommit=False) as conn:
        with conn.transaction():
            upserted_count = upsert_stock_universe_records(conn, accepted)

    return build_import_result(
        source_path=file_path,
        dry_run=False,
        total_rows=len(records),
        accepted=accepted,
        rejected=rejected,
        upserted_count=upserted_count,
        filter_config=filter_config,
    )


def sync_nasdaq_stock_universe(
    *,
    database_url: str | None = None,
    filter_config: StockUniverseFilterConfig | None = None,
    limit: int | None = None,
    dry_run: bool = True,
    exchanges: tuple[str, ...] = DEFAULT_ALLOWED_EXCHANGES,
    use_volume_as_avg_volume: bool = True,
    client: NasdaqScreenerClient | None = None,
) -> StockUniverseImportResult:
    if limit is not None and limit <= 0:
        raise StockUniverseError("limit 必须大于 0。")

    filter_config = filter_config or StockUniverseFilterConfig()
    client = client or NasdaqScreenerClient()
    rows = client.fetch_rows(exchanges=exchanges, limit=limit)
    records = nasdaq_rows_to_records(
        rows,
        use_volume_as_avg_volume=use_volume_as_avg_volume,
    )
    accepted, rejected = filter_stock_universe_records(records, config=filter_config)
    if dry_run:
        return build_import_result(
            source_path=NASDAQ_SCREENER_BASE_URL,
            dry_run=True,
            total_rows=len(records),
            accepted=accepted,
            rejected=rejected,
            upserted_count=0,
            filter_config=filter_config,
        )

    resolved_database_url = get_database_url(database_url)
    ensure_stock_universe_schema(resolved_database_url)
    with psycopg.connect(resolved_database_url, autocommit=False) as conn:
        with conn.transaction():
            upserted_count = upsert_stock_universe_records(conn, accepted)

    return build_import_result(
        source_path=NASDAQ_SCREENER_BASE_URL,
        dry_run=False,
        total_rows=len(records),
        accepted=accepted,
        rejected=rejected,
        upserted_count=upserted_count,
        filter_config=filter_config,
    )


def render_import_result(result: StockUniverseImportResult) -> str:
    mode = "预览" if result.dry_run else "写入"
    lines = [
        (
            f"[完成] 股票池扩充{mode}: 来源 {result.source_path}，"
            f"读取 {result.total_rows} 行，"
            f"通过 {result.accepted_count} 行，"
            f"剔除 {result.rejected_count} 行，"
            f"写入 {result.upserted_count} 行。"
        ),
        (
            "过滤条件: "
            f"30日均量 >= {result.filter_config.min_avg_volume_30d}, "
            f"市值 >= {result.filter_config.min_market_cap_usd}"
            + ("（缺失市值允许后续补全）" if result.filter_config.allow_missing_market_cap else "")
            + f", 价格 >= {result.filter_config.min_last_price}, "
            f"交易所={','.join(result.filter_config.allowed_exchanges)}, "
            f"资产类型={','.join(result.filter_config.allowed_asset_types)}。"
        ),
    ]
    if result.accepted_tickers:
        lines.append("通过样例: " + ", ".join(result.accepted_tickers[:20]))
    if result.rejection_reason_counts:
        reasons = ", ".join(
            f"{reason}={count}"
            for reason, count in list(result.rejection_reason_counts.items())[:10]
        )
        lines.append("剔除原因: " + reasons)
    if result.rejected_samples:
        sample = "; ".join(
            f"{row.ticker or '-'}({','.join(row.reasons[:2])})"
            for row in result.rejected_samples[:8]
        )
        lines.append("剔除样例: " + sample)
    return "\n".join(lines)
