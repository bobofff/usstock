"""Finnhub News API ingestion."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings


MARKET_NEWS_ENDPOINT = "market_news"
COMPANY_NEWS_ENDPOINT = "company_news"
DEFAULT_MARKET_CATEGORY = "general"
DEFAULT_COMPANY_NEWS_DAYS = 7


class FinnhubError(RuntimeError):
    """Raised when Finnhub fetching or ingestion fails."""


@dataclass(frozen=True)
class FinnhubQuery:
    query_uid: str
    endpoint: str
    category: str | None
    ticker: str | None
    from_date: date | None
    to_date: date | None
    min_id: int | None
    request_url: str
    raw_payload: Any


@dataclass(frozen=True)
class FinnhubArticle:
    article_uid: str
    finnhub_id: int | None
    article_url: str
    headline: str
    summary: str | None
    category: str | None
    source_name: str | None
    image_url: str | None
    related_tickers: list[str]
    published_at: datetime | None
    source_type: str
    query_uid: str
    endpoint: str
    request_url: str
    raw_payload: dict[str, Any]


class FinnhubRateLimiter:
    """Small synchronous rate limiter for Finnhub requests."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise FinnhubError("FINNHUB_RATE_LIMIT_PER_SECOND 必须大于 0。")

        self._min_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()


class FinnhubClient:
    """Minimal Finnhub News API client."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        requests_per_second: float = 1,
        timeout_seconds: float = 30,
        max_retries: int = 3,
    ) -> None:
        api_key = api_key.strip()
        if not api_key:
            raise FinnhubError("缺少 FINNHUB_API_KEY，请先在环境变量或 .env 中配置。")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._rate_limiter = FinnhubRateLimiter(requests_per_second)

    def build_url(
        self,
        path: str,
        params: dict[str, str],
        *,
        include_token: bool,
    ) -> str:
        query_params = dict(params)
        if include_token:
            query_params["token"] = self.api_key
        return f"{self.base_url}/{path.lstrip('/')}?{urllib.parse.urlencode(query_params)}"

    def fetch_json(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "usstock-finnhub-client/0.1",
            },
        )

        for attempt in range(self.max_retries + 1):
            self._rate_limiter.wait()
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=self.timeout_seconds,
                ) as response:
                    body = response.read()
                return json.loads(body.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if not self._should_retry(exc.code, attempt):
                    raise FinnhubError(f"Finnhub 请求失败 {exc.code}: {url}") from exc
                self._sleep_before_retry(attempt, exc)
            except urllib.error.URLError as exc:
                if attempt >= self.max_retries:
                    raise FinnhubError(f"Finnhub 请求失败: {url}: {exc}") from exc
                self._sleep_before_retry(attempt)
            except json.JSONDecodeError as exc:
                raise FinnhubError(f"Finnhub 返回不是有效 JSON: {url}") from exc

        raise FinnhubError(f"Finnhub 请求失败: {url}")

    def market_news(
        self,
        *,
        category: str = DEFAULT_MARKET_CATEGORY,
        min_id: int | None = None,
    ) -> tuple[str, Any]:
        params = {"category": normalize_category(category)}
        if min_id is not None:
            params["minId"] = str(min_id)
        request_url = self.build_url("news", params, include_token=False)
        fetch_url = self.build_url("news", params, include_token=True)
        return request_url, self.fetch_json(fetch_url)

    def company_news(
        self,
        *,
        ticker: str,
        from_date: date | str,
        to_date: date | str,
    ) -> tuple[str, Any]:
        params = {
            "symbol": normalize_ticker(ticker),
            "from": format_date(from_date),
            "to": format_date(to_date),
        }
        request_url = self.build_url("company-news", params, include_token=False)
        fetch_url = self.build_url("company-news", params, include_token=True)
        return request_url, self.fetch_json(fetch_url)

    def _should_retry(self, status_code: int, attempt: int) -> bool:
        return attempt < self.max_retries and status_code in {429, 500, 502, 503, 504}

    def _sleep_before_retry(
        self,
        attempt: int,
        exc: urllib.error.HTTPError | None = None,
    ) -> None:
        retry_after = exc.headers.get("Retry-After") if exc else None
        if retry_after:
            try:
                sleep_seconds = float(retry_after)
            except ValueError:
                sleep_seconds = 2**attempt
        else:
            sleep_seconds = 2**attempt

        time.sleep(max(1.0, sleep_seconds))


def make_finnhub_client() -> FinnhubClient:
    settings = get_settings()
    return FinnhubClient(
        api_key=settings.finnhub_api_key or "",
        base_url=settings.finnhub_base_url,
        requests_per_second=settings.finnhub_rate_limit_per_second,
        timeout_seconds=settings.finnhub_request_timeout_seconds,
    )


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise FinnhubError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def normalize_category(category: str | None) -> str:
    text = (category or DEFAULT_MARKET_CATEGORY).strip().lower()
    return text or DEFAULT_MARKET_CATEGORY


def normalize_ticker(ticker: str) -> str:
    text = ticker.strip().upper()
    if not text:
        raise FinnhubError("ticker 不能为空。")
    return text


def clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None

    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise FinnhubError(f"日期格式必须是 YYYY-MM-DD: {text}") from exc


def format_date(value: date | str) -> str:
    parsed = parse_date(value)
    if not parsed:
        raise FinnhubError("日期不能为空。")
    return parsed.isoformat()


def default_company_news_window() -> tuple[date, date]:
    to_date = date.today()
    from_date = to_date - timedelta(days=DEFAULT_COMPANY_NEWS_DAYS)
    return from_date, to_date


def parse_unix_timestamp(value: Any) -> datetime | None:
    timestamp = parse_int(value)
    if timestamp is None:
        return None
    if timestamp > 9_999_999_999:
        timestamp = timestamp // 1000
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def parse_related_tickers(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        candidates = value.split(",")
    elif isinstance(value, list):
        candidates = value
    else:
        candidates = [value]

    tickers: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        ticker = str(candidate).strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def build_query_uid(
    *,
    endpoint: str,
    category: str | None,
    ticker: str | None,
    from_date: date | str | None,
    to_date: date | str | None,
    min_id: int | None,
) -> str:
    payload = json.dumps(
        {
            "endpoint": endpoint,
            "category": normalize_category(category) if category else None,
            "ticker": normalize_ticker(ticker) if ticker else None,
            "from_date": parse_date(from_date).isoformat() if from_date else None,
            "to_date": parse_date(to_date).isoformat() if to_date else None,
            "min_id": min_id,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_article_uid(item: dict[str, Any]) -> str:
    finnhub_id = parse_int(item.get("id"))
    if finnhub_id is not None:
        return f"finnhub:{finnhub_id}"

    payload = json.dumps(
        {
            "url": clean_optional_text(item.get("url")),
            "headline": clean_optional_text(item.get("headline")),
            "datetime": item.get("datetime"),
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"finnhub:hash:{digest}"


def parse_article_payload(
    item: dict[str, Any],
    *,
    query_uid: str,
    endpoint: str,
    request_url: str,
    fallback_category: str | None = None,
) -> FinnhubArticle | None:
    article_url = clean_optional_text(item.get("url"))
    headline = clean_optional_text(item.get("headline"))
    if not article_url or not headline:
        return None

    return FinnhubArticle(
        article_uid=build_article_uid(item),
        finnhub_id=parse_int(item.get("id")),
        article_url=article_url,
        headline=headline,
        summary=clean_optional_text(item.get("summary")),
        category=clean_optional_text(item.get("category")) or fallback_category,
        source_name=clean_optional_text(item.get("source")),
        image_url=clean_optional_text(item.get("image")),
        related_tickers=parse_related_tickers(item.get("related")),
        published_at=parse_unix_timestamp(item.get("datetime")),
        source_type="financial_news",
        query_uid=query_uid,
        endpoint=endpoint,
        request_url=request_url,
        raw_payload=item,
    )


def parse_articles(
    payload: Any,
    *,
    query_uid: str,
    endpoint: str,
    request_url: str,
    fallback_category: str | None = None,
) -> list[FinnhubArticle]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        rows = payload["data"]
    else:
        rows = []

    articles: list[FinnhubArticle] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        article = parse_article_payload(
            item,
            query_uid=query_uid,
            endpoint=endpoint,
            request_url=request_url,
            fallback_category=fallback_category,
        )
        if article:
            articles.append(article)
    return articles


def upsert_news_query(conn: Connection, query: FinnhubQuery) -> None:
    conn.execute(
        """
        INSERT INTO finnhub_news_queries (
            query_uid,
            endpoint,
            category,
            ticker,
            from_date,
            to_date,
            min_id,
            request_url,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (query_uid)
        DO UPDATE SET
            endpoint = EXCLUDED.endpoint,
            category = EXCLUDED.category,
            ticker = EXCLUDED.ticker,
            from_date = EXCLUDED.from_date,
            to_date = EXCLUDED.to_date,
            min_id = EXCLUDED.min_id,
            request_url = EXCLUDED.request_url,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            query.query_uid,
            query.endpoint,
            query.category,
            query.ticker,
            query.from_date,
            query.to_date,
            query.min_id,
            query.request_url,
            Jsonb(query.raw_payload),
        ),
    )


def upsert_articles(conn: Connection, articles: list[FinnhubArticle]) -> int:
    count = 0
    for article in articles:
        conn.execute(
            """
            INSERT INTO finnhub_articles (
                article_uid,
                finnhub_id,
                article_url,
                headline,
                summary,
                category,
                source_name,
                image_url,
                related_tickers,
                published_at,
                source_type,
                query_uid,
                endpoint,
                request_url,
                raw_payload,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, now(), now()
            )
            ON CONFLICT (article_uid)
            DO UPDATE SET
                finnhub_id = EXCLUDED.finnhub_id,
                article_url = EXCLUDED.article_url,
                headline = EXCLUDED.headline,
                summary = EXCLUDED.summary,
                category = EXCLUDED.category,
                source_name = EXCLUDED.source_name,
                image_url = EXCLUDED.image_url,
                related_tickers = EXCLUDED.related_tickers,
                published_at = EXCLUDED.published_at,
                source_type = EXCLUDED.source_type,
                query_uid = EXCLUDED.query_uid,
                endpoint = EXCLUDED.endpoint,
                request_url = EXCLUDED.request_url,
                raw_payload = EXCLUDED.raw_payload,
                last_seen_at = now(),
                updated_at = now()
            """,
            (
                article.article_uid,
                article.finnhub_id,
                article.article_url,
                article.headline,
                article.summary,
                article.category,
                article.source_name,
                article.image_url,
                article.related_tickers,
                article.published_at,
                article.source_type,
                article.query_uid,
                article.endpoint,
                article.request_url,
                Jsonb(article.raw_payload),
            ),
        )
        count += 1

    return count


def sync_market_news(
    *,
    category: str = DEFAULT_MARKET_CATEGORY,
    min_id: int | None = None,
    database_url: str | None = None,
    client: FinnhubClient | None = None,
) -> int:
    normalized_category = normalize_category(category)
    client = client or make_finnhub_client()
    request_url, payload = client.market_news(
        category=normalized_category,
        min_id=min_id,
    )
    query_uid = build_query_uid(
        endpoint=MARKET_NEWS_ENDPOINT,
        category=normalized_category,
        ticker=None,
        from_date=None,
        to_date=None,
        min_id=min_id,
    )
    query = FinnhubQuery(
        query_uid=query_uid,
        endpoint=MARKET_NEWS_ENDPOINT,
        category=normalized_category,
        ticker=None,
        from_date=None,
        to_date=None,
        min_id=min_id,
        request_url=request_url,
        raw_payload=payload,
    )
    articles = parse_articles(
        payload,
        query_uid=query_uid,
        endpoint=MARKET_NEWS_ENDPOINT,
        request_url=request_url,
        fallback_category=normalized_category,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_news_query(conn, query)
            return upsert_articles(conn, articles)


def sync_company_news(
    *,
    ticker: str,
    from_date: date | str | None = None,
    to_date: date | str | None = None,
    database_url: str | None = None,
    client: FinnhubClient | None = None,
) -> int:
    normalized_ticker = normalize_ticker(ticker)
    if not from_date or not to_date:
        default_from_date, default_to_date = default_company_news_window()
        from_date = from_date or default_from_date
        to_date = to_date or default_to_date
    parsed_from_date = parse_date(from_date)
    parsed_to_date = parse_date(to_date)
    if not parsed_from_date or not parsed_to_date:
        raise FinnhubError("from_date 和 to_date 不能为空。")
    if parsed_from_date > parsed_to_date:
        raise FinnhubError("from_date 不能晚于 to_date。")

    client = client or make_finnhub_client()
    request_url, payload = client.company_news(
        ticker=normalized_ticker,
        from_date=parsed_from_date,
        to_date=parsed_to_date,
    )
    query_uid = build_query_uid(
        endpoint=COMPANY_NEWS_ENDPOINT,
        category=None,
        ticker=normalized_ticker,
        from_date=parsed_from_date,
        to_date=parsed_to_date,
        min_id=None,
    )
    query = FinnhubQuery(
        query_uid=query_uid,
        endpoint=COMPANY_NEWS_ENDPOINT,
        category=None,
        ticker=normalized_ticker,
        from_date=parsed_from_date,
        to_date=parsed_to_date,
        min_id=None,
        request_url=request_url,
        raw_payload=payload,
    )
    articles = parse_articles(
        payload,
        query_uid=query_uid,
        endpoint=COMPANY_NEWS_ENDPOINT,
        request_url=request_url,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_news_query(conn, query)
            return upsert_articles(conn, articles)


def add_common_database_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Finnhub News API data.")
    subparsers = parser.add_subparsers(dest="command")

    market_parser = subparsers.add_parser(
        "sync-market",
        help="同步 Finnhub market news",
    )
    market_parser.add_argument(
        "--category",
        default=DEFAULT_MARKET_CATEGORY,
        help="新闻分类，例如 general、forex、crypto、merger",
    )
    market_parser.add_argument("--min-id", type=int, help="仅抓取大于该 ID 的新闻")
    add_common_database_arg(market_parser)

    company_parser = subparsers.add_parser(
        "sync-company",
        aliases=["sync-ticker"],
        help="同步 Finnhub company news",
    )
    company_parser.add_argument("ticker", help="股票 ticker，例如 AAPL")
    company_parser.add_argument("--from-date", help="开始日期，格式 YYYY-MM-DD")
    company_parser.add_argument("--to-date", help="结束日期，格式 YYYY-MM-DD")
    add_common_database_arg(company_parser)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "sync-market":
            count = sync_market_news(
                category=args.category,
                min_id=args.min_id,
                database_url=args.database_url,
            )
            print(f"[完成] Finnhub market news 同步 {count} 条")
            return 0

        if args.command in {"sync-company", "sync-ticker"}:
            count = sync_company_news(
                ticker=args.ticker,
                from_date=args.from_date,
                to_date=args.to_date,
                database_url=args.database_url,
            )
            print(f"[完成] Finnhub company news 同步 {count} 条")
            return 0

        parser.print_help()
        return 2
    except FinnhubError as exc:
        print(f"Finnhub 同步失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
