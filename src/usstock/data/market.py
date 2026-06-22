"""Market data ingestion entry points."""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.db import migrations as db_migrations


DEFAULT_DATA_SOURCE = "manual_csv"
STOOQ_DATA_SOURCE = "stooq"
STOOQ_BASE_URL = "https://stooq.com/q/d/l/"
DEFAULT_STOOQ_REQUESTS_PER_SECOND = 1.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_STOOQ_RETRY_ATTEMPTS = 3
DEFAULT_STOOQ_RETRY_BACKOFF_SECONDS = 1.0


class MarketDataError(RuntimeError):
    """Raised when market price ingestion fails."""


@dataclass(frozen=True)
class DailyPrice:
    ticker: str
    price_date: date
    close_price: Decimal
    open_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    adjusted_close_price: Decimal | None = None
    volume: Decimal | None = None
    currency: str = "USD"
    data_source: str = DEFAULT_DATA_SOURCE
    source_uid: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketPriceSyncResult:
    provider: str
    requested_tickers: tuple[str, ...]
    synced_tickers: tuple[str, ...]
    missing_tickers: tuple[str, ...]
    price_count: int
    from_date: date | None
    to_date: date | None
    failed_tickers: tuple[str, ...] = ()
    failure_messages: tuple[str, ...] = ()


class SimpleRateLimiter:
    """Small synchronous rate limiter for free market data endpoints."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise MarketDataError("requests_per_second 必须大于 0。")
        self._min_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()


class StooqClient:
    """Minimal client for Stooq daily CSV quotes."""

    def __init__(
        self,
        *,
        base_url: str = STOOQ_BASE_URL,
        requests_per_second: float = DEFAULT_STOOQ_REQUESTS_PER_SECOND,
        timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        retry_attempts: int = DEFAULT_STOOQ_RETRY_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_STOOQ_RETRY_BACKOFF_SECONDS,
    ) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        if retry_attempts <= 0:
            raise MarketDataError("retry_attempts 必须大于 0。")
        if retry_backoff_seconds < 0:
            raise MarketDataError("retry_backoff_seconds 不能小于 0。")
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self._rate_limiter = SimpleRateLimiter(requests_per_second)

    def build_url(
        self,
        *,
        ticker: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> tuple[str, str]:
        symbol = stooq_symbol_for_ticker(ticker)
        params = {
            "s": symbol,
            "i": "d",
        }
        if from_date:
            params["d1"] = from_date.strftime("%Y%m%d")
        if to_date:
            params["d2"] = to_date.strftime("%Y%m%d")
        return symbol, f"{self.base_url}?{urllib.parse.urlencode(params)}"

    def daily_prices(
        self,
        *,
        ticker: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[DailyPrice]:
        symbol, url = self.build_url(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
        )
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "text/csv",
                "User-Agent": "usstock-market-client/0.1",
            },
        )
        last_error: urllib.error.URLError | None = None
        for attempt in range(1, self.retry_attempts + 1):
            self._rate_limiter.wait()
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    text = response.read().decode("utf-8-sig")
                break
            except urllib.error.URLError as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    raise MarketDataError(f"Stooq 日线请求失败: {ticker}: {exc}") from exc
                time.sleep(self.retry_backoff_seconds * attempt)
        else:  # pragma: no cover - retry_attempts validation keeps this unreachable.
            raise MarketDataError(f"Stooq 日线请求失败: {ticker}: {last_error}")

        return parse_stooq_csv_prices(
            text,
            ticker=normalize_ticker(ticker),
            symbol=symbol,
            request_url=url,
        )


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise MarketDataError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def ensure_market_schema(database_url: str) -> int:
    applied = db_migrations.migrate(database_url=database_url)
    return len(applied)


def normalize_ticker(value: str) -> str:
    ticker = value.strip().upper()
    if not ticker:
        raise MarketDataError("ticker/symbol 不能为空。")
    return ticker


def stooq_symbol_for_ticker(ticker: str) -> str:
    normalized = normalize_ticker(ticker)
    symbol = normalized.lower().replace(".", "-")
    if not symbol.endswith(".us"):
        symbol = f"{symbol}.us"
    return symbol


def parse_optional_date(value: str | None, *, field_name: str) -> date | None:
    if value is None or not value.strip():
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise MarketDataError(f"{field_name} 日期格式必须是 YYYY-MM-DD: {value}") from exc


def parse_ticker_list(values: list[str] | tuple[str, ...] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_values = [values] if isinstance(values, str) else list(values)
    seen: set[str] = set()
    tickers: list[str] = []
    for raw_value in raw_values:
        for chunk in str(raw_value or "").replace("\n", ",").split(","):
            text = chunk.strip()
            if not text:
                continue
            ticker = normalize_ticker(text)
            if ticker in seen:
                continue
            seen.add(ticker)
            tickers.append(ticker)
    return tuple(tickers)


def normalize_key(value: str) -> str:
    return "".join(ch for ch in value.lower().strip() if ch.isalnum())


def normalize_row_keys(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): value for key, value in row.items()}


def get_first(row: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        value = row.get(alias)
        if value is not None and value.strip():
            return value.strip()
    return None


def parse_date_value(value: str | None, *, row_number: int) -> date:
    if not value:
        raise MarketDataError(f"CSV 第 {row_number} 行缺少 date/price_date。")
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise MarketDataError(f"CSV 第 {row_number} 行日期格式无效: {value}") from exc


def parse_decimal_value(
    value: str | None,
    *,
    field_name: str,
    row_number: int,
    required: bool = False,
) -> Decimal | None:
    if value is None or not value.strip():
        if required:
            raise MarketDataError(f"CSV 第 {row_number} 行缺少 {field_name}。")
        return None
    text = value.strip().replace(",", "")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise MarketDataError(f"CSV 第 {row_number} 行 {field_name} 不是有效数字: {value}") from exc
    if parsed < 0:
        raise MarketDataError(f"CSV 第 {row_number} 行 {field_name} 不能为负数: {value}")
    return parsed


def parse_price_row(
    raw_row: dict[str, str],
    *,
    row_number: int,
    default_ticker: str | None,
    data_source: str,
    import_file: Path,
) -> DailyPrice:
    row = normalize_row_keys(raw_row)
    ticker_text = get_first(row, ("ticker", "symbol"))
    if ticker_text is None:
        ticker_text = default_ticker
    if ticker_text is None:
        raise MarketDataError(f"CSV 第 {row_number} 行缺少 ticker/symbol，且未传入 --ticker。")

    ticker = normalize_ticker(ticker_text)
    price_date = parse_date_value(
        get_first(row, ("date", "pricedate", "tradingdate")),
        row_number=row_number,
    )
    adjusted_close_text = get_first(row, ("adjclose", "adjustedclose", "adjustedcloseprice"))
    close_price = parse_decimal_value(
        get_first(row, ("close", "closeprice")) or adjusted_close_text,
        field_name="close",
        row_number=row_number,
        required=True,
    )
    assert close_price is not None

    adjusted_close_price = parse_decimal_value(
        adjusted_close_text,
        field_name="adjusted_close",
        row_number=row_number,
    )

    return DailyPrice(
        ticker=ticker,
        price_date=price_date,
        open_price=parse_decimal_value(
            get_first(row, ("open", "openprice")),
            field_name="open",
            row_number=row_number,
        ),
        high_price=parse_decimal_value(
            get_first(row, ("high", "highprice")),
            field_name="high",
            row_number=row_number,
        ),
        low_price=parse_decimal_value(
            get_first(row, ("low", "lowprice")),
            field_name="low",
            row_number=row_number,
        ),
        close_price=close_price,
        adjusted_close_price=adjusted_close_price,
        volume=parse_decimal_value(
            get_first(row, ("volume", "vol")),
            field_name="volume",
            row_number=row_number,
        ),
        currency=(get_first(row, ("currency",)) or "USD").upper(),
        data_source=data_source,
        source_uid=f"{data_source}:{ticker}:{price_date.isoformat()}",
        metadata={
            "import_file": str(import_file),
            "row_number": row_number,
        },
    )


def load_csv_prices(
    path: Path,
    *,
    default_ticker: str | None = None,
    data_source: str = DEFAULT_DATA_SOURCE,
) -> list[DailyPrice]:
    if not path.exists():
        raise MarketDataError(f"CSV 文件不存在: {path}")
    if not path.is_file():
        raise MarketDataError(f"CSV 路径不是文件: {path}")

    prices: list[DailyPrice] = []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise MarketDataError(f"CSV 文件缺少表头: {path}")
        for row_number, row in enumerate(reader, start=2):
            prices.append(
                parse_price_row(
                    row,
                    row_number=row_number,
                    default_ticker=default_ticker,
                    data_source=data_source,
                    import_file=path,
                )
            )
    if not prices:
        raise MarketDataError(f"CSV 文件没有可导入的价格行: {path}")
    return prices


def parse_stooq_csv_prices(
    csv_text: str,
    *,
    ticker: str,
    symbol: str,
    request_url: str,
) -> list[DailyPrice]:
    normalized_ticker = normalize_ticker(ticker)
    reader = csv.DictReader(io.StringIO(csv_text.strip()))
    if not reader.fieldnames or "Date" not in reader.fieldnames:
        return []

    prices: list[DailyPrice] = []
    for row_number, row in enumerate(reader, start=2):
        try:
            price_date = parse_date_value(row.get("Date"), row_number=row_number)
            close_price = parse_decimal_value(
                row.get("Close"),
                field_name="Close",
                row_number=row_number,
                required=True,
            )
            assert close_price is not None
            prices.append(
                DailyPrice(
                    ticker=normalized_ticker,
                    price_date=price_date,
                    open_price=parse_decimal_value(
                        row.get("Open"),
                        field_name="Open",
                        row_number=row_number,
                    ),
                    high_price=parse_decimal_value(
                        row.get("High"),
                        field_name="High",
                        row_number=row_number,
                    ),
                    low_price=parse_decimal_value(
                        row.get("Low"),
                        field_name="Low",
                        row_number=row_number,
                    ),
                    close_price=close_price,
                    adjusted_close_price=None,
                    volume=parse_decimal_value(
                        row.get("Volume"),
                        field_name="Volume",
                        row_number=row_number,
                    ),
                    currency="USD",
                    data_source=STOOQ_DATA_SOURCE,
                    source_uid=f"stooq:{symbol}:{price_date.isoformat()}",
                    metadata={
                        "provider": "stooq",
                        "symbol": symbol,
                        "request_url": request_url,
                    },
                )
            )
        except MarketDataError:
            raise
    return prices


def upsert_daily_prices(conn: Connection, prices: list[DailyPrice]) -> int:
    for price in prices:
        conn.execute(
            """
            INSERT INTO market_daily_prices (
                ticker,
                price_date,
                open_price,
                high_price,
                low_price,
                close_price,
                adjusted_close_price,
                volume,
                currency,
                data_source,
                source_uid,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, price_date, data_source)
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                adjusted_close_price = EXCLUDED.adjusted_close_price,
                volume = EXCLUDED.volume,
                currency = EXCLUDED.currency,
                source_uid = EXCLUDED.source_uid,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            """,
            (
                price.ticker,
                price.price_date,
                price.open_price,
                price.high_price,
                price.low_price,
                price.close_price,
                price.adjusted_close_price,
                price.volume,
                price.currency,
                price.data_source,
                price.source_uid,
                Jsonb(price.metadata),
            ),
        )
    return len(prices)


def import_prices_csv(
    *,
    csv_path: Path,
    database_url: str | None = None,
    ticker: str | None = None,
    data_source: str = DEFAULT_DATA_SOURCE,
) -> int:
    database_url = get_database_url(database_url)
    ensure_market_schema(database_url)
    prices = load_csv_prices(
        csv_path,
        default_ticker=normalize_ticker(ticker) if ticker else None,
        data_source=data_source,
    )
    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.transaction():
            return upsert_daily_prices(conn, prices)


def fetch_report_candidate_tickers(
    conn: Connection,
    *,
    start_date: date,
    end_date: date,
    profile: str,
    top_n: int,
) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (run_date, profile)
               structured_payload
        FROM daily_analysis_reports
        WHERE run_date >= %s
          AND run_date <= %s
          AND profile = %s
          AND status = 'generated'
        ORDER BY run_date, profile, generated_at DESC, id DESC
        """,
        (start_date, end_date, profile),
    ).fetchall()
    seen: set[str] = set()
    tickers: list[str] = []
    for row in rows:
        payload = row[0] if row and isinstance(row[0], dict) else {}
        candidates = payload.get("candidates")
        if not isinstance(candidates, list):
            continue
        for candidate in candidates[:top_n]:
            if not isinstance(candidate, dict) or not candidate.get("ticker"):
                continue
            ticker = normalize_ticker(str(candidate["ticker"]))
            if ticker in seen:
                continue
            seen.add(ticker)
            tickers.append(ticker)
    return tuple(tickers)


def resolve_stooq_sync_tickers(
    conn: Connection,
    *,
    tickers: tuple[str, ...],
    from_report_candidates: bool,
    start_date: date | None,
    end_date: date | None,
    profile: str,
    top_n: int,
) -> tuple[str, ...]:
    if tickers:
        return tickers
    if not from_report_candidates:
        raise MarketDataError("请传入 ticker，或启用从日报候选自动提取 ticker。")
    if start_date is None or end_date is None:
        raise MarketDataError("从日报候选提取 ticker 时必须提供开始日期和结束日期。")
    resolved = fetch_report_candidate_tickers(
        conn,
        start_date=start_date,
        end_date=end_date,
        profile=profile,
        top_n=top_n,
    )
    if not resolved:
        raise MarketDataError("没有从已生成的日报中找到候选 ticker。")
    return resolved


def sync_stooq_daily_prices(
    *,
    database_url: str | None = None,
    tickers: tuple[str, ...] = (),
    from_date: date | None = None,
    to_date: date | None = None,
    from_report_candidates: bool = False,
    profile: str = "default",
    top_n: int = 10,
    client: StooqClient | None = None,
) -> MarketPriceSyncResult:
    if from_date and to_date and to_date < from_date:
        raise MarketDataError("结束日期不能早于开始日期。")
    if top_n <= 0:
        raise MarketDataError("top_n 必须大于 0。")

    database_url = get_database_url(database_url)
    ensure_market_schema(database_url)
    client = client or StooqClient()
    synced: list[str] = []
    missing: list[str] = []
    failed: list[str] = []
    failure_messages: list[str] = []
    total_count = 0

    with psycopg.connect(database_url, autocommit=True) as conn:
        requested = resolve_stooq_sync_tickers(
            conn,
            tickers=tickers,
            from_report_candidates=from_report_candidates,
            start_date=from_date,
            end_date=to_date,
            profile=profile,
            top_n=top_n,
        )

    prices_by_ticker: list[list[DailyPrice]] = []
    for ticker in requested:
        try:
            prices = client.daily_prices(
                ticker=ticker,
                from_date=from_date,
                to_date=to_date,
            )
        except MarketDataError as exc:
            failed.append(ticker)
            failure_messages.append(f"{ticker}: {exc}")
            continue
        if not prices:
            missing.append(ticker)
            continue
        prices_by_ticker.append(prices)
        synced.append(ticker)

    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.transaction():
            for prices in prices_by_ticker:
                total_count += upsert_daily_prices(conn, prices)

    return MarketPriceSyncResult(
        provider=STOOQ_DATA_SOURCE,
        requested_tickers=requested,
        synced_tickers=tuple(synced),
        missing_tickers=tuple(missing),
        price_count=total_count,
        from_date=from_date,
        to_date=to_date,
        failed_tickers=tuple(failed),
        failure_messages=tuple(failure_messages),
    )


def render_sync_result(result: MarketPriceSyncResult) -> str:
    parts = [
        f"[完成] {result.provider} 日线同步：价格 {result.price_count} 行，"
        f"成功 {len(result.synced_tickers)} / 请求 {len(result.requested_tickers)} 个 ticker。"
    ]
    if result.missing_tickers:
        parts.append("缺少数据: " + ", ".join(result.missing_tickers[:20]))
    if result.failed_tickers:
        parts.append("请求失败: " + ", ".join(result.failed_tickers[:20]))
    return "\n".join(parts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import and manage market price data.")
    subparsers = parser.add_subparsers(dest="command")

    import_parser = subparsers.add_parser("import-prices", help="从 CSV 导入日线价格")
    import_parser.add_argument("csv_path", type=Path, help="CSV 文件路径")
    import_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    import_parser.add_argument("--ticker", help="当 CSV 没有 ticker/symbol 列时使用该股票代码")
    import_parser.add_argument(
        "--data-source",
        default=DEFAULT_DATA_SOURCE,
        help="行情来源标识，默认 manual_csv",
    )

    stooq_parser = subparsers.add_parser("sync-stooq", help="从 Stooq 免费日线接口同步价格")
    stooq_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    stooq_parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="股票代码，可重复传入，也可用逗号分隔",
    )
    stooq_parser.add_argument("--from-date", help="开始日期，格式 YYYY-MM-DD")
    stooq_parser.add_argument("--to-date", help="结束日期，格式 YYYY-MM-DD")
    stooq_parser.add_argument(
        "--from-report-candidates",
        action="store_true",
        help="从已生成日报候选中自动提取 ticker",
    )
    stooq_parser.add_argument("--profile", default="default", help="日报 profile")
    stooq_parser.add_argument("--top-n", type=int, default=10, help="每份日报取前 N 个候选")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "import-prices":
            count = import_prices_csv(
                csv_path=args.csv_path,
                database_url=args.database_url,
                ticker=args.ticker,
                data_source=args.data_source,
            )
            print(f"[完成] 已导入或更新日线价格 {count} 行。")
            return 0

        if args.command == "sync-stooq":
            result = sync_stooq_daily_prices(
                database_url=args.database_url,
                tickers=parse_ticker_list(args.ticker),
                from_date=parse_optional_date(args.from_date, field_name="from-date"),
                to_date=parse_optional_date(args.to_date, field_name="to-date"),
                from_report_candidates=args.from_report_candidates,
                profile=args.profile,
                top_n=args.top_n,
            )
            print(render_sync_result(result))
            return 0

        parser.print_help()
        return 2
    except MarketDataError as exc:
        print(f"行情数据导入失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
