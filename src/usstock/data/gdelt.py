"""GDELT DOC API ingestion."""

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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.runtime_logs import log_sync_operation


ARTLIST_MODE = "artlist"
TIMELINE_VOL_RAW_MODE = "timelinevolraw"
DEFAULT_SORT = "datedesc"
DEFAULT_TIMESPAN = "24h"
DEFAULT_MAX_RECORDS = 75


class GdeltError(RuntimeError):
    """Raised when GDELT fetching or ingestion fails."""


@dataclass(frozen=True)
class GdeltQuery:
    query_uid: str
    query_text: str
    mode: str
    format: str
    timespan: str | None
    start_datetime: datetime | None
    end_datetime: datetime | None
    sort: str | None
    max_records: int | None
    request_url: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class GdeltArticle:
    article_url: str
    mobile_url: str | None
    title: str
    seen_at: datetime | None
    domain: str | None
    language: str | None
    source_country: str | None
    social_image_url: str | None
    tone: Decimal | None
    query_uid: str
    query_text: str
    request_url: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class GdeltTimelinePoint:
    point_uid: str
    query_uid: str
    query_text: str
    mode: str
    bucket_start_at: datetime
    article_count: Decimal | None
    norm_count: Decimal | None
    volume_share: Decimal | None
    raw_payload: dict[str, Any]


class GdeltRateLimiter:
    """Small synchronous rate limiter for GDELT requests."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise GdeltError("GDELT_RATE_LIMIT_PER_SECOND 必须大于 0。")

        self._min_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()


class GdeltDocClient:
    """Minimal GDELT DOC API client."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.gdeltproject.org/api/v2/doc/doc",
        requests_per_second: float = 0.2,
        timeout_seconds: float = 30,
        max_retries: int = 5,
    ) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._rate_limiter = GdeltRateLimiter(requests_per_second)

    def build_url(
        self,
        *,
        query: str,
        mode: str,
        format_: str = "json",
        timespan: str | None = None,
        start_datetime: datetime | str | None = None,
        end_datetime: datetime | str | None = None,
        sort: str | None = None,
        max_records: int | None = None,
    ) -> str:
        params: dict[str, str] = {
            "query": query,
            "mode": mode,
            "format": format_,
        }
        if timespan:
            params["timespan"] = timespan
        if start_datetime:
            params["startdatetime"] = format_gdelt_datetime(start_datetime)
        if end_datetime:
            params["enddatetime"] = format_gdelt_datetime(end_datetime)
        if sort:
            params["sort"] = sort
        if max_records:
            params["maxrecords"] = str(max_records)

        return f"{self.base_url}?{urllib.parse.urlencode(params)}"

    def fetch_json(self, url: str) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "usstock-gdelt-client/0.1",
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
                return json.loads(body.decode("utf-8-sig"))
            except urllib.error.HTTPError as exc:
                if not self._should_retry(exc.code, attempt):
                    raise GdeltError(f"GDELT 请求失败 {exc.code}: {url}") from exc
                self._sleep_before_retry(attempt, exc)
            except urllib.error.URLError as exc:
                if attempt >= self.max_retries:
                    raise GdeltError(f"GDELT 请求失败: {url}: {exc}") from exc
                self._sleep_before_retry(attempt)
            except json.JSONDecodeError as exc:
                raise GdeltError(f"GDELT 返回不是有效 JSON: {url}") from exc

        raise GdeltError(f"GDELT 请求失败: {url}")

    def search_articles(
        self,
        *,
        query: str,
        timespan: str | None = DEFAULT_TIMESPAN,
        start_datetime: datetime | str | None = None,
        end_datetime: datetime | str | None = None,
        sort: str | None = DEFAULT_SORT,
        max_records: int | None = DEFAULT_MAX_RECORDS,
    ) -> tuple[str, dict[str, Any]]:
        url = self.build_url(
            query=query,
            mode=ARTLIST_MODE,
            timespan=timespan,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            sort=sort,
            max_records=max_records,
        )
        return url, self.fetch_json(url)

    def search_timeline_vol_raw(
        self,
        *,
        query: str,
        timespan: str | None = DEFAULT_TIMESPAN,
        start_datetime: datetime | str | None = None,
        end_datetime: datetime | str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        url = self.build_url(
            query=query,
            mode=TIMELINE_VOL_RAW_MODE,
            timespan=timespan,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        return url, self.fetch_json(url)

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

        time.sleep(max(2.0, sleep_seconds))


def make_gdelt_client() -> GdeltDocClient:
    settings = get_settings()
    return GdeltDocClient(
        base_url=settings.gdelt_doc_base_url,
        requests_per_second=settings.gdelt_rate_limit_per_second,
        timeout_seconds=settings.gdelt_request_timeout_seconds,
    )


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise GdeltError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def _count_log_result(count: int) -> dict[str, int]:
    return {"count": count}


def _articles_log_details(
    *,
    query: str,
    timespan: str | None = DEFAULT_TIMESPAN,
    start_datetime: datetime | str | None = None,
    end_datetime: datetime | str | None = None,
    sort: str | None = DEFAULT_SORT,
    max_records: int | None = DEFAULT_MAX_RECORDS,
    database_url: str | None = None,
    client: GdeltDocClient | None = None,
) -> dict[str, Any]:
    return {
        "query": query,
        "timespan": timespan,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "sort": sort,
        "max_records": max_records,
    }


def _timeline_log_details(
    *,
    query: str,
    timespan: str | None = DEFAULT_TIMESPAN,
    start_datetime: datetime | str | None = None,
    end_datetime: datetime | str | None = None,
    database_url: str | None = None,
    client: GdeltDocClient | None = None,
) -> dict[str, Any]:
    return {
        "query": query,
        "timespan": timespan,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }


def build_query_uid(
    *,
    query: str,
    mode: str,
    timespan: str | None,
    start_datetime: datetime | str | None,
    end_datetime: datetime | str | None,
    sort: str | None,
    max_records: int | None,
) -> str:
    payload = json.dumps(
        {
            "query": query,
            "mode": mode,
            "timespan": timespan,
            "start_datetime": format_gdelt_datetime(start_datetime)
            if start_datetime
            else None,
            "end_datetime": format_gdelt_datetime(end_datetime) if end_datetime else None,
            "sort": sort,
            "max_records": max_records,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_point_uid(
    *,
    query_uid: str,
    mode: str,
    bucket_start_at: datetime,
) -> str:
    return f"{query_uid}|{mode}|{bucket_start_at.isoformat()}"


def clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_gdelt_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%Y%m%dT%H%M%SZ",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y%m%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def format_gdelt_datetime(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value.strftime("%Y%m%d%H%M%S")


def parse_article_payload(
    item: dict[str, Any],
    *,
    query_uid: str,
    query_text: str,
    request_url: str,
) -> GdeltArticle | None:
    article_url = clean_optional_text(item.get("url"))
    title = clean_optional_text(item.get("title"))
    if not article_url or not title:
        return None

    return GdeltArticle(
        article_url=article_url,
        mobile_url=clean_optional_text(
            item.get("url_mobile")
            or item.get("urlmobile")
            or item.get("mobileurl")
            or item.get("ampurl")
        ),
        title=title,
        seen_at=parse_gdelt_datetime(item.get("seendate") or item.get("date")),
        domain=clean_optional_text(item.get("domain")),
        language=clean_optional_text(item.get("language")),
        source_country=clean_optional_text(
            item.get("sourcecountry") or item.get("sourceCountry")
        ),
        social_image_url=clean_optional_text(
            item.get("socialimage")
            or item.get("socialImage")
            or item.get("image")
        ),
        tone=parse_decimal(item.get("tone")),
        query_uid=query_uid,
        query_text=query_text,
        request_url=request_url,
        raw_payload=item,
    )


def parse_articles(
    payload: dict[str, Any],
    *,
    query_uid: str,
    query_text: str,
    request_url: str,
) -> list[GdeltArticle]:
    articles = payload.get("articles")
    if not isinstance(articles, list):
        return []

    parsed: list[GdeltArticle] = []
    for item in articles:
        if not isinstance(item, dict):
            continue
        article = parse_article_payload(
            item,
            query_uid=query_uid,
            query_text=query_text,
            request_url=request_url,
        )
        if article:
            parsed.append(article)

    return parsed


def extract_timeline_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("timeline")
    if isinstance(rows, list):
        flattened: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            nested_rows = row.get("data")
            if isinstance(nested_rows, list):
                flattened.extend(
                    nested_row
                    for nested_row in nested_rows
                    if isinstance(nested_row, dict)
                )
            else:
                flattened.append(row)
        return flattened

    for key in ("timelinevolraw", "data"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]

    return []


def parse_timeline_points(
    payload: dict[str, Any],
    *,
    query_uid: str,
    query_text: str,
    mode: str,
) -> list[GdeltTimelinePoint]:
    points: list[GdeltTimelinePoint] = []
    for row in extract_timeline_rows(payload):
        bucket_start_at = parse_gdelt_datetime(
            row.get("date") or row.get("datetime") or row.get("timestamp")
        )
        if not bucket_start_at:
            continue

        article_count = parse_decimal(
            row.get("value")
            or row.get("count")
            or row.get("articles")
            or row.get("article_count")
        )
        norm_count = parse_decimal(row.get("norm") or row.get("norm_count"))
        volume_share = None
        if article_count is not None and norm_count not in {None, Decimal("0")}:
            volume_share = article_count / norm_count

        points.append(
            GdeltTimelinePoint(
                point_uid=build_point_uid(
                    query_uid=query_uid,
                    mode=mode,
                    bucket_start_at=bucket_start_at,
                ),
                query_uid=query_uid,
                query_text=query_text,
                mode=mode,
                bucket_start_at=bucket_start_at,
                article_count=article_count,
                norm_count=norm_count,
                volume_share=volume_share,
                raw_payload=row,
            )
        )

    return points


def upsert_doc_query(conn: Connection, query: GdeltQuery) -> None:
    conn.execute(
        """
        INSERT INTO gdelt_doc_queries (
            query_uid,
            query_text,
            mode,
            format,
            timespan,
            start_datetime,
            end_datetime,
            sort,
            max_records,
            request_url,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (query_uid)
        DO UPDATE SET
            query_text = EXCLUDED.query_text,
            mode = EXCLUDED.mode,
            format = EXCLUDED.format,
            timespan = EXCLUDED.timespan,
            start_datetime = EXCLUDED.start_datetime,
            end_datetime = EXCLUDED.end_datetime,
            sort = EXCLUDED.sort,
            max_records = EXCLUDED.max_records,
            request_url = EXCLUDED.request_url,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            query.query_uid,
            query.query_text,
            query.mode,
            query.format,
            query.timespan,
            query.start_datetime,
            query.end_datetime,
            query.sort,
            query.max_records,
            query.request_url,
            Jsonb(query.raw_payload),
        ),
    )


def upsert_articles(conn: Connection, articles: list[GdeltArticle]) -> int:
    count = 0
    for article in articles:
        conn.execute(
            """
            INSERT INTO gdelt_articles (
                article_url,
                mobile_url,
                title,
                seen_at,
                domain,
                language,
                source_country,
                social_image_url,
                tone,
                source_type,
                query_uid,
                query_text,
                request_url,
                raw_payload,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'global_news', %s, %s, %s, %s, now(), now()
            )
            ON CONFLICT (article_url)
            DO UPDATE SET
                mobile_url = EXCLUDED.mobile_url,
                title = EXCLUDED.title,
                seen_at = EXCLUDED.seen_at,
                domain = EXCLUDED.domain,
                language = EXCLUDED.language,
                source_country = EXCLUDED.source_country,
                social_image_url = EXCLUDED.social_image_url,
                tone = EXCLUDED.tone,
                query_uid = EXCLUDED.query_uid,
                query_text = EXCLUDED.query_text,
                request_url = EXCLUDED.request_url,
                raw_payload = EXCLUDED.raw_payload,
                last_seen_at = now(),
                updated_at = now()
            """,
            (
                article.article_url,
                article.mobile_url,
                article.title,
                article.seen_at,
                article.domain,
                article.language,
                article.source_country,
                article.social_image_url,
                article.tone,
                article.query_uid,
                article.query_text,
                article.request_url,
                Jsonb(article.raw_payload),
            ),
        )
        count += 1

    return count


def upsert_timeline_points(
    conn: Connection,
    points: list[GdeltTimelinePoint],
) -> int:
    count = 0
    for point in points:
        conn.execute(
            """
            INSERT INTO gdelt_timeline_points (
                point_uid,
                query_uid,
                query_text,
                mode,
                bucket_start_at,
                article_count,
                norm_count,
                volume_share,
                raw_payload,
                fetched_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (point_uid)
            DO UPDATE SET
                query_uid = EXCLUDED.query_uid,
                query_text = EXCLUDED.query_text,
                mode = EXCLUDED.mode,
                bucket_start_at = EXCLUDED.bucket_start_at,
                article_count = EXCLUDED.article_count,
                norm_count = EXCLUDED.norm_count,
                volume_share = EXCLUDED.volume_share,
                raw_payload = EXCLUDED.raw_payload,
                fetched_at = now(),
                updated_at = now()
            """,
            (
                point.point_uid,
                point.query_uid,
                point.query_text,
                point.mode,
                point.bucket_start_at,
                point.article_count,
                point.norm_count,
                point.volume_share,
                Jsonb(point.raw_payload),
            ),
        )
        count += 1

    return count


@log_sync_operation(
    source="GDELT",
    action="sync_articles",
    details_builder=_articles_log_details,
    result_builder=_count_log_result,
)
def sync_articles(
    *,
    query: str,
    timespan: str | None = DEFAULT_TIMESPAN,
    start_datetime: datetime | str | None = None,
    end_datetime: datetime | str | None = None,
    sort: str | None = DEFAULT_SORT,
    max_records: int | None = DEFAULT_MAX_RECORDS,
    database_url: str | None = None,
    client: GdeltDocClient | None = None,
) -> int:
    client = client or make_gdelt_client()
    request_url, payload = client.search_articles(
        query=query,
        timespan=timespan,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        sort=sort,
        max_records=max_records,
    )
    query_uid = build_query_uid(
        query=query,
        mode=ARTLIST_MODE,
        timespan=timespan,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        sort=sort,
        max_records=max_records,
    )
    gdelt_query = GdeltQuery(
        query_uid=query_uid,
        query_text=query,
        mode=ARTLIST_MODE,
        format="json",
        timespan=timespan,
        start_datetime=parse_gdelt_datetime(start_datetime),
        end_datetime=parse_gdelt_datetime(end_datetime),
        sort=sort,
        max_records=max_records,
        request_url=request_url,
        raw_payload=payload,
    )
    articles = parse_articles(
        payload,
        query_uid=query_uid,
        query_text=query,
        request_url=request_url,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_doc_query(conn, gdelt_query)
            return upsert_articles(conn, articles)


@log_sync_operation(
    source="GDELT",
    action="sync_timeline",
    details_builder=_timeline_log_details,
    result_builder=_count_log_result,
)
def sync_timeline(
    *,
    query: str,
    timespan: str | None = DEFAULT_TIMESPAN,
    start_datetime: datetime | str | None = None,
    end_datetime: datetime | str | None = None,
    database_url: str | None = None,
    client: GdeltDocClient | None = None,
) -> int:
    client = client or make_gdelt_client()
    request_url, payload = client.search_timeline_vol_raw(
        query=query,
        timespan=timespan,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    query_uid = build_query_uid(
        query=query,
        mode=TIMELINE_VOL_RAW_MODE,
        timespan=timespan,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        sort=None,
        max_records=None,
    )
    gdelt_query = GdeltQuery(
        query_uid=query_uid,
        query_text=query,
        mode=TIMELINE_VOL_RAW_MODE,
        format="json",
        timespan=timespan,
        start_datetime=parse_gdelt_datetime(start_datetime),
        end_datetime=parse_gdelt_datetime(end_datetime),
        sort=None,
        max_records=None,
        request_url=request_url,
        raw_payload=payload,
    )
    points = parse_timeline_points(
        payload,
        query_uid=query_uid,
        query_text=query,
        mode=TIMELINE_VOL_RAW_MODE,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        with conn.transaction():
            upsert_doc_query(conn, gdelt_query)
            return upsert_timeline_points(conn, points)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    parser.add_argument(
        "--timespan",
        default=DEFAULT_TIMESPAN,
        help="GDELT timespan，例如 1h、24h、7d",
    )
    parser.add_argument(
        "--start-datetime",
        help="GDELT startdatetime，格式 YYYYMMDDHHMMSS",
    )
    parser.add_argument(
        "--end-datetime",
        help="GDELT enddatetime，格式 YYYYMMDDHHMMSS",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync GDELT DOC API data.")
    subparsers = parser.add_subparsers(dest="command")

    articles_parser = subparsers.add_parser(
        "sync-articles",
        help="同步 GDELT ArticleList 文章列表",
    )
    articles_parser.add_argument("query", help="GDELT query 参数")
    add_common_args(articles_parser)
    articles_parser.add_argument("--sort", default=DEFAULT_SORT, help="排序方式")
    articles_parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="最多返回多少条文章，GDELT ArticleList 最大通常为 250",
    )

    timeline_parser = subparsers.add_parser(
        "sync-timeline",
        help="同步 GDELT timelinevolraw 时间线",
    )
    timeline_parser.add_argument("query", help="GDELT query 参数")
    add_common_args(timeline_parser)

    query_parser = subparsers.add_parser(
        "sync-query",
        help="同时同步文章列表和 timelinevolraw 时间线",
    )
    query_parser.add_argument("query", help="GDELT query 参数")
    add_common_args(query_parser)
    query_parser.add_argument("--sort", default=DEFAULT_SORT, help="排序方式")
    query_parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="最多返回多少条文章，GDELT ArticleList 最大通常为 250",
    )

    return parser


def _normalize_time_window_args(args: argparse.Namespace) -> tuple[str | None, str | None, str | None]:
    timespan = args.timespan
    start_datetime = args.start_datetime
    end_datetime = args.end_datetime
    if start_datetime or end_datetime:
        timespan = None

    return timespan, start_datetime, end_datetime


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "sync-articles":
            timespan, start_datetime, end_datetime = _normalize_time_window_args(args)
            count = sync_articles(
                query=args.query,
                timespan=timespan,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                sort=args.sort,
                max_records=args.max_records,
                database_url=args.database_url,
            )
            print(f"[完成] GDELT 文章同步 {count} 条")
            return 0

        if args.command == "sync-timeline":
            timespan, start_datetime, end_datetime = _normalize_time_window_args(args)
            count = sync_timeline(
                query=args.query,
                timespan=timespan,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                database_url=args.database_url,
            )
            print(f"[完成] GDELT 时间线同步 {count} 个点位")
            return 0

        if args.command == "sync-query":
            timespan, start_datetime, end_datetime = _normalize_time_window_args(args)
            client = make_gdelt_client()
            article_count = sync_articles(
                query=args.query,
                timespan=timespan,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                sort=args.sort,
                max_records=args.max_records,
                database_url=args.database_url,
                client=client,
            )
            timeline_count = sync_timeline(
                query=args.query,
                timespan=timespan,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                database_url=args.database_url,
                client=client,
            )
            print(
                f"[完成] GDELT query 同步 article={article_count} "
                f"timeline={timeline_count}"
            )
            return 0

        parser.print_help()
        return 2
    except GdeltError as exc:
        print(f"GDELT 同步失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
