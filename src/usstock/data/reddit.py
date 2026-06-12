"""Reddit API ingestion for community discussion signals."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
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


DEFAULT_SUBREDDITS = ("stocks", "investing", "wallstreetbets", "SecurityAnalysis")
DEFAULT_LISTING = "new"
DEFAULT_LIMIT = 100
DEFAULT_TIME_FILTER = "day"
VALID_LISTINGS = {"new", "hot", "top", "rising", "controversial"}
VALID_TIME_FILTERS = {"hour", "day", "week", "month", "year", "all"}
REDDIT_WEB_BASE_URL = "https://www.reddit.com"
DEVVIT_REQUEST_BASE_URL = "devvit://reddit"
SUBREDDIT_POSTS_ENDPOINT = "subreddit_posts"

TICKER_STOPWORDS = {
    "AI",
    "API",
    "ATH",
    "CEO",
    "CFO",
    "CPI",
    "DD",
    "EPS",
    "ETF",
    "FED",
    "FOMC",
    "GDP",
    "IPO",
    "IRA",
    "IRS",
    "LOL",
    "SEC",
    "USA",
    "USD",
    "WSB",
    "YOLO",
}

KEYWORD_STOPWORDS = {
    "about",
    "after",
    "against",
    "all",
    "and",
    "are",
    "been",
    "but",
    "can",
    "company",
    "from",
    "have",
    "into",
    "market",
    "more",
    "new",
    "news",
    "not",
    "over",
    "post",
    "reddit",
    "share",
    "shares",
    "stock",
    "stocks",
    "that",
    "the",
    "this",
    "with",
    "would",
}


class RedditError(RuntimeError):
    """Raised when Reddit fetching or ingestion fails."""


@dataclass(frozen=True)
class RedditQuery:
    query_uid: str
    subreddit: str
    listing: str
    time_filter: str | None
    limit_count: int
    after_token: str | None
    request_url: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class RedditPost:
    post_uid: str
    reddit_id: str
    fullname: str
    subreddit: str
    title: str
    selftext: str | None
    author_name: str | None
    permalink_url: str
    external_url: str | None
    score: int | None
    upvote_ratio: Decimal | None
    comment_count: int
    over_18: bool
    spoiler: bool
    stickied: bool
    is_video: bool
    link_flair_text: str | None
    candidate_tickers: list[str]
    candidate_keywords: list[str]
    created_utc: datetime | None
    source_type: str
    query_uid: str
    request_url: str
    raw_payload: dict[str, Any]


class RedditRateLimiter:
    """Small synchronous rate limiter for Reddit requests."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise RedditError("REDDIT_RATE_LIMIT_PER_SECOND 必须大于 0。")

        self._min_interval = 1.0 / requests_per_second
        self._last_request_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()


class RedditClient:
    """Minimal Reddit OAuth API client for app-only listing reads."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        user_agent: str,
        base_url: str = "https://oauth.reddit.com",
        oauth_url: str = "https://www.reddit.com/api/v1/access_token",
        requests_per_second: float = 0.5,
        timeout_seconds: float = 30,
        max_retries: int = 3,
    ) -> None:
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.user_agent = user_agent.strip()
        if not self.client_id:
            raise RedditError("缺少 REDDIT_CLIENT_ID，请先在环境变量或 .env 中配置。")
        if not self.client_secret:
            raise RedditError("缺少 REDDIT_CLIENT_SECRET，请先在环境变量或 .env 中配置。")
        if not self.user_agent:
            raise RedditError("缺少 REDDIT_USER_AGENT，请配置包含应用名和联系方式的 User-Agent。")

        self.base_url = base_url.rstrip("/")
        self.oauth_url = oauth_url
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._rate_limiter = RedditRateLimiter(requests_per_second)
        self._access_token: str | None = None
        self._access_token_expires_at = 0.0

    def build_url(self, path: str, params: dict[str, str]) -> str:
        query = urllib.parse.urlencode(params)
        return f"{self.base_url}/{path.lstrip('/')}?{query}"

    def get_access_token(self) -> str:
        now = time.monotonic()
        if self._access_token and now < self._access_token_expires_at - 60:
            return self._access_token

        auth_token = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("ascii")
        request = urllib.request.Request(
            self.oauth_url,
            data=urllib.parse.urlencode({"grant_type": "client_credentials"}).encode(
                "utf-8"
            ),
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {auth_token}",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": self.user_agent,
            },
            method="POST",
        )

        for attempt in range(self.max_retries + 1):
            self._rate_limiter.wait()
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=self.timeout_seconds,
                ) as response:
                    body = response.read()
                payload = json.loads(body.decode("utf-8"))
                access_token = clean_optional_text(payload.get("access_token"))
                if not access_token:
                    raise RedditError("Reddit OAuth 响应缺少 access_token。")
                expires_in = parse_int(payload.get("expires_in")) or 3600
                self._access_token = access_token
                self._access_token_expires_at = time.monotonic() + max(60, expires_in)
                return access_token
            except urllib.error.HTTPError as exc:
                if not self._should_retry(exc.code, attempt):
                    raise RedditError(f"Reddit OAuth 请求失败 {exc.code}") from exc
                self._sleep_before_retry(attempt, exc)
            except urllib.error.URLError as exc:
                if attempt >= self.max_retries:
                    raise RedditError(f"Reddit OAuth 请求失败: {exc}") from exc
                self._sleep_before_retry(attempt)
            except json.JSONDecodeError as exc:
                raise RedditError("Reddit OAuth 返回不是有效 JSON。") from exc

        raise RedditError("Reddit OAuth 请求失败。")

    def fetch_json(self, url: str) -> dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            token = self.get_access_token()
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"bearer {token}",
                    "User-Agent": self.user_agent,
                },
            )
            self._rate_limiter.wait()
            try:
                with urllib.request.urlopen(
                    request,
                    timeout=self.timeout_seconds,
                ) as response:
                    body = response.read()
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise RedditError(f"Reddit 返回的 JSON 不是对象: {url}")
                return payload
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    self._access_token = None
                if not self._should_retry(exc.code, attempt):
                    raise RedditError(f"Reddit 请求失败 {exc.code}: {url}") from exc
                self._sleep_before_retry(attempt, exc)
            except urllib.error.URLError as exc:
                if attempt >= self.max_retries:
                    raise RedditError(f"Reddit 请求失败: {url}: {exc}") from exc
                self._sleep_before_retry(attempt)
            except json.JSONDecodeError as exc:
                raise RedditError(f"Reddit 返回不是有效 JSON: {url}") from exc

        raise RedditError(f"Reddit 请求失败: {url}")

    def subreddit_posts(
        self,
        *,
        subreddit: str,
        listing: str = DEFAULT_LISTING,
        limit: int = DEFAULT_LIMIT,
        time_filter: str | None = None,
        after: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        normalized_subreddit = normalize_subreddit(subreddit)
        normalized_listing = normalize_listing(listing)
        normalized_limit = normalize_limit(limit)
        normalized_time_filter = normalize_time_filter(time_filter)
        params = {
            "limit": str(normalized_limit),
            "raw_json": "1",
        }
        if after:
            params["after"] = after.strip()
        if normalized_listing in {"top", "controversial"}:
            params["t"] = normalized_time_filter or DEFAULT_TIME_FILTER

        path = f"r/{normalized_subreddit}/{normalized_listing}.json"
        url = self.build_url(path, params)
        return url, self.fetch_json(url)

    def _should_retry(self, status_code: int, attempt: int) -> bool:
        return attempt < self.max_retries and status_code in {401, 429, 500, 502, 503, 504}

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


def make_reddit_client() -> RedditClient:
    settings = get_settings()
    return RedditClient(
        client_id=settings.reddit_client_id or "",
        client_secret=settings.reddit_client_secret or "",
        user_agent=settings.reddit_user_agent or "",
        base_url=settings.reddit_base_url,
        oauth_url=settings.reddit_oauth_url,
        requests_per_second=settings.reddit_rate_limit_per_second,
        timeout_seconds=settings.reddit_request_timeout_seconds,
    )


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise RedditError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def normalize_subreddit(subreddit: str) -> str:
    text = subreddit.strip()
    if text.startswith("/r/"):
        text = text[3:]
    elif text.lower().startswith("r/"):
        text = text[2:]
    text = text.strip("/")
    if not text:
        raise RedditError("subreddit 不能为空。")
    if not re.fullmatch(r"[A-Za-z0-9_]+", text):
        raise RedditError(f"subreddit 格式无效: {subreddit}")
    return text


def normalize_listing(listing: str | None) -> str:
    text = (listing or DEFAULT_LISTING).strip().lower()
    if text not in VALID_LISTINGS:
        valid = ", ".join(sorted(VALID_LISTINGS))
        raise RedditError(f"listing 必须是以下值之一: {valid}")
    return text


def normalize_time_filter(time_filter: str | None) -> str | None:
    if time_filter is None:
        return None
    text = time_filter.strip().lower()
    if not text:
        return None
    if text not in VALID_TIME_FILTERS:
        valid = ", ".join(sorted(VALID_TIME_FILTERS))
        raise RedditError(f"time_filter 必须是以下值之一: {valid}")
    return text


def normalize_limit(limit: int | str | None) -> int:
    parsed = parse_int(limit) if limit is not None else DEFAULT_LIMIT
    if parsed is None or parsed <= 0:
        raise RedditError("limit 必须大于 0。")
    return min(parsed, 100)


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


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_utc_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        if isinstance(value, str):
            text = value.strip()
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None
    if timestamp > 9_999_999_999:
        timestamp = timestamp / 1000
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def normalize_permalink(value: Any) -> str | None:
    text = clean_optional_text(value)
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if text.startswith("/"):
        return f"{REDDIT_WEB_BASE_URL}{text}"
    return f"{REDDIT_WEB_BASE_URL}/{text.lstrip('/')}"


def normalize_external_url(value: Any, permalink_url: str) -> str | None:
    text = clean_optional_text(value)
    if not text:
        return None
    if text == permalink_url:
        return None
    if text.startswith(f"{REDDIT_WEB_BASE_URL}/r/") and "/comments/" in text:
        return None
    return text


def build_query_uid(
    *,
    subreddit: str,
    listing: str,
    time_filter: str | None,
    limit: int,
    after: str | None,
) -> str:
    payload = json.dumps(
        {
            "endpoint": SUBREDDIT_POSTS_ENDPOINT,
            "subreddit": normalize_subreddit(subreddit).lower(),
            "listing": normalize_listing(listing),
            "time_filter": normalize_time_filter(time_filter),
            "limit": normalize_limit(limit),
            "after": clean_optional_text(after),
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_devvit_request_url(
    *,
    subreddit: str,
    listing: str,
    time_filter: str | None,
    limit: int,
    after: str | None,
) -> str:
    params = {"limit": str(normalize_limit(limit))}
    normalized_listing = normalize_listing(listing)
    normalized_time_filter = normalize_time_filter(time_filter)
    if clean_optional_text(after):
        params["after"] = clean_optional_text(after) or ""
    if normalized_listing in {"top", "controversial"}:
        params["t"] = normalized_time_filter or DEFAULT_TIME_FILTER
    query = urllib.parse.urlencode(params)
    return (
        f"{DEVVIT_REQUEST_BASE_URL}/r/{normalize_subreddit(subreddit)}/"
        f"{normalized_listing}?{query}"
    )


def _clean_fullname(value: Any) -> str | None:
    text = clean_optional_text(value)
    if not text:
        return None
    if text.startswith("t3_"):
        return text
    return f"t3_{text}"


def _extract_flair_text(value: Any) -> str | None:
    if isinstance(value, dict):
        return clean_optional_text(
            value.get("text") or value.get("flairText") or value.get("label")
        )
    return clean_optional_text(value)


def normalize_devvit_post_payload(
    item: dict[str, Any],
    *,
    fallback_subreddit: str | None = None,
) -> dict[str, Any]:
    """Convert a Devvit Post-shaped payload to Reddit listing-style fields."""

    payload = item.get("data") if isinstance(item.get("data"), dict) else item
    fullname = (
        _clean_fullname(payload.get("name"))
        or _clean_fullname(payload.get("fullname"))
        or _clean_fullname(payload.get("id"))
        or _clean_fullname(payload.get("postId"))
    )
    reddit_id = clean_optional_text(payload.get("reddit_id") or payload.get("redditId"))
    if not reddit_id and fullname:
        reddit_id = fullname.removeprefix("t3_")

    subreddit = (
        clean_optional_text(payload.get("subreddit"))
        or clean_optional_text(payload.get("subredditName"))
        or fallback_subreddit
    )
    created_utc = (
        payload.get("created_utc")
        if payload.get("created_utc") is not None
        else payload.get("createdAt", payload.get("created_at"))
    )
    flair_text = (
        clean_optional_text(payload.get("link_flair_text"))
        or clean_optional_text(payload.get("flairText"))
        or _extract_flair_text(payload.get("flair"))
    )

    return {
        "id": reddit_id,
        "name": fullname,
        "subreddit": subreddit,
        "title": clean_optional_text(payload.get("title")),
        "selftext": clean_optional_text(payload.get("selftext") or payload.get("body")),
        "author": clean_optional_text(
            payload.get("author")
            or payload.get("authorName")
            or payload.get("author_name")
        ),
        "permalink": clean_optional_text(
            payload.get("permalink") or payload.get("permalink_url")
        ),
        "url": clean_optional_text(payload.get("url") or payload.get("external_url")),
        "score": payload.get("score"),
        "upvote_ratio": payload.get("upvote_ratio") or payload.get("upvoteRatio"),
        "num_comments": (
            payload.get("num_comments")
            if payload.get("num_comments") is not None
            else payload.get("numberOfComments", payload.get("comment_count"))
        ),
        "created_utc": created_utc,
        "over_18": (
            payload.get("over_18")
            if payload.get("over_18") is not None
            else payload.get("nsfw", False)
        ),
        "spoiler": payload.get("spoiler", False),
        "stickied": payload.get("stickied", False),
        "is_video": payload.get("is_video") or payload.get("isVideo") or False,
        "link_flair_text": flair_text,
        "raw_devvit_payload": payload,
    }


def build_post_uid(item: dict[str, Any]) -> str:
    fullname = clean_optional_text(item.get("name"))
    if fullname:
        return f"reddit:{fullname}"

    reddit_id = clean_optional_text(item.get("id"))
    if reddit_id:
        return f"reddit:t3_{reddit_id}"

    payload = json.dumps(
        {
            "title": clean_optional_text(item.get("title")),
            "permalink": clean_optional_text(item.get("permalink")),
            "created_utc": item.get("created_utc"),
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"reddit:hash:{digest}"


def extract_candidate_tickers(
    text: str,
    *,
    known_tickers: set[str] | None = None,
    limit: int = 16,
) -> list[str]:
    known_tickers = known_tickers or set()
    found: list[str] = []
    seen: set[str] = set()

    def add(candidate: str, *, from_cashtag: bool) -> None:
        ticker = candidate.strip().upper().replace(".", "-")
        if not ticker or ticker in seen:
            return
        if ticker in TICKER_STOPWORDS and ticker not in known_tickers and not from_cashtag:
            return
        if not from_cashtag and known_tickers and ticker not in known_tickers:
            return
        if not re.fullmatch(r"[A-Z][A-Z0-9\-]{0,5}", ticker):
            return
        seen.add(ticker)
        found.append(ticker)

    for match in re.finditer(r"(?<![A-Za-z0-9])\$([A-Za-z][A-Za-z0-9\.\-]{0,5})", text):
        add(match.group(1), from_cashtag=True)

    for match in re.finditer(r"\b[A-Z][A-Z0-9\.\-]{1,5}\b", text):
        add(match.group(0), from_cashtag=False)

    return found[:limit]


def extract_keywords(text: str, *, limit: int = 12) -> list[str]:
    words = re.findall(r"[A-Za-z][A-Za-z0-9\-]{2,}", text.lower())
    counts: dict[str, int] = {}
    for word in words:
        if word in KEYWORD_STOPWORDS:
            continue
        counts[word] = counts.get(word, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [word for word, _count in ranked[:limit]]


def parse_post_payload(
    item: dict[str, Any],
    *,
    query_uid: str,
    request_url: str,
    fallback_subreddit: str | None = None,
    known_tickers: set[str] | None = None,
) -> RedditPost | None:
    payload = item.get("data") if isinstance(item.get("data"), dict) else item
    reddit_id = clean_optional_text(payload.get("id"))
    fullname = clean_optional_text(payload.get("name")) or (
        f"t3_{reddit_id}" if reddit_id else None
    )
    title = clean_optional_text(payload.get("title"))
    subreddit = clean_optional_text(payload.get("subreddit")) or fallback_subreddit
    if not reddit_id or not fullname or not title or not subreddit:
        return None

    permalink_url = normalize_permalink(payload.get("permalink"))
    if not permalink_url:
        return None

    selftext = clean_optional_text(payload.get("selftext"))
    flair = clean_optional_text(payload.get("link_flair_text"))
    text = " ".join(part for part in (title, selftext, flair) if part)
    comment_count = parse_int(payload.get("num_comments")) or 0

    return RedditPost(
        post_uid=build_post_uid(payload),
        reddit_id=reddit_id,
        fullname=fullname,
        subreddit=normalize_subreddit(subreddit),
        title=title,
        selftext=selftext,
        author_name=clean_optional_text(payload.get("author")),
        permalink_url=permalink_url,
        external_url=normalize_external_url(payload.get("url"), permalink_url),
        score=parse_int(payload.get("score")),
        upvote_ratio=parse_decimal(payload.get("upvote_ratio")),
        comment_count=max(0, comment_count),
        over_18=parse_bool(payload.get("over_18")),
        spoiler=parse_bool(payload.get("spoiler")),
        stickied=parse_bool(payload.get("stickied")),
        is_video=parse_bool(payload.get("is_video")),
        link_flair_text=flair,
        candidate_tickers=extract_candidate_tickers(
            text,
            known_tickers=known_tickers,
        ),
        candidate_keywords=extract_keywords(text),
        created_utc=parse_utc_timestamp(payload.get("created_utc")),
        source_type="reddit_post",
        query_uid=query_uid,
        request_url=request_url,
        raw_payload=payload,
    )


def parse_posts(
    payload: dict[str, Any],
    *,
    query_uid: str,
    request_url: str,
    fallback_subreddit: str | None = None,
    known_tickers: set[str] | None = None,
) -> list[RedditPost]:
    data = payload.get("data") if isinstance(payload, dict) else None
    children = data.get("children") if isinstance(data, dict) else None
    if not isinstance(children, list):
        children = []

    posts: list[RedditPost] = []
    for child in children:
        if not isinstance(child, dict):
            continue
        post = parse_post_payload(
            child,
            query_uid=query_uid,
            request_url=request_url,
            fallback_subreddit=fallback_subreddit,
            known_tickers=known_tickers,
        )
        if post:
            posts.append(post)
    return posts


def parse_devvit_posts(
    payload: dict[str, Any],
    *,
    query_uid: str,
    request_url: str,
    fallback_subreddit: str | None = None,
    known_tickers: set[str] | None = None,
) -> list[RedditPost]:
    items = payload.get("posts")
    if not isinstance(items, list):
        data = payload.get("data") if isinstance(payload.get("data"), dict) else None
        children = data.get("children") if isinstance(data, dict) else None
        items = children if isinstance(children, list) else []

    posts: list[RedditPost] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = normalize_devvit_post_payload(
            item,
            fallback_subreddit=fallback_subreddit,
        )
        post = parse_post_payload(
            normalized,
            query_uid=query_uid,
            request_url=request_url,
            fallback_subreddit=fallback_subreddit,
            known_tickers=known_tickers,
        )
        if post:
            posts.append(post)
    return posts


def fetch_known_tickers(conn: Connection) -> set[str]:
    table_row = conn.execute("SELECT to_regclass('public.stock_universe')").fetchone()
    if not table_row or table_row[0] is None:
        return set()

    rows = conn.execute(
        """
        SELECT upper(ticker)
        FROM stock_universe
        WHERE is_active
        """
    ).fetchall()
    return {row[0] for row in rows if row[0]}


def upsert_post_query(conn: Connection, query: RedditQuery) -> None:
    conn.execute(
        """
        INSERT INTO reddit_post_queries (
            query_uid,
            subreddit,
            listing,
            time_filter,
            limit_count,
            after_token,
            request_url,
            raw_payload,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (query_uid)
        DO UPDATE SET
            subreddit = EXCLUDED.subreddit,
            listing = EXCLUDED.listing,
            time_filter = EXCLUDED.time_filter,
            limit_count = EXCLUDED.limit_count,
            after_token = EXCLUDED.after_token,
            request_url = EXCLUDED.request_url,
            raw_payload = EXCLUDED.raw_payload,
            fetched_at = now(),
            updated_at = now()
        """,
        (
            query.query_uid,
            query.subreddit,
            query.listing,
            query.time_filter,
            query.limit_count,
            query.after_token,
            query.request_url,
            Jsonb(query.raw_payload),
        ),
    )


def upsert_posts(conn: Connection, posts: list[RedditPost]) -> int:
    count = 0
    for post in posts:
        conn.execute(
            """
            INSERT INTO reddit_posts (
                post_uid,
                reddit_id,
                fullname,
                subreddit,
                title,
                selftext,
                author_name,
                permalink_url,
                external_url,
                score,
                upvote_ratio,
                comment_count,
                over_18,
                spoiler,
                stickied,
                is_video,
                link_flair_text,
                candidate_tickers,
                candidate_keywords,
                created_utc,
                source_type,
                query_uid,
                request_url,
                raw_payload,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, now(), now()
            )
            ON CONFLICT (post_uid)
            DO UPDATE SET
                reddit_id = EXCLUDED.reddit_id,
                fullname = EXCLUDED.fullname,
                subreddit = EXCLUDED.subreddit,
                title = EXCLUDED.title,
                selftext = EXCLUDED.selftext,
                author_name = EXCLUDED.author_name,
                permalink_url = EXCLUDED.permalink_url,
                external_url = EXCLUDED.external_url,
                score = EXCLUDED.score,
                upvote_ratio = EXCLUDED.upvote_ratio,
                comment_count = EXCLUDED.comment_count,
                over_18 = EXCLUDED.over_18,
                spoiler = EXCLUDED.spoiler,
                stickied = EXCLUDED.stickied,
                is_video = EXCLUDED.is_video,
                link_flair_text = EXCLUDED.link_flair_text,
                candidate_tickers = EXCLUDED.candidate_tickers,
                candidate_keywords = EXCLUDED.candidate_keywords,
                created_utc = EXCLUDED.created_utc,
                source_type = EXCLUDED.source_type,
                query_uid = EXCLUDED.query_uid,
                request_url = EXCLUDED.request_url,
                raw_payload = EXCLUDED.raw_payload,
                last_seen_at = now(),
                updated_at = now()
            """,
            (
                post.post_uid,
                post.reddit_id,
                post.fullname,
                post.subreddit,
                post.title,
                post.selftext,
                post.author_name,
                post.permalink_url,
                post.external_url,
                post.score,
                post.upvote_ratio,
                post.comment_count,
                post.over_18,
                post.spoiler,
                post.stickied,
                post.is_video,
                post.link_flair_text,
                post.candidate_tickers,
                post.candidate_keywords,
                post.created_utc,
                post.source_type,
                post.query_uid,
                post.request_url,
                Jsonb(post.raw_payload),
            ),
        )
        count += 1
    return count


def sync_subreddit_posts(
    *,
    subreddit: str,
    listing: str = DEFAULT_LISTING,
    limit: int = DEFAULT_LIMIT,
    time_filter: str | None = None,
    after: str | None = None,
    database_url: str | None = None,
    client: RedditClient | None = None,
) -> int:
    normalized_subreddit = normalize_subreddit(subreddit)
    normalized_listing = normalize_listing(listing)
    normalized_limit = normalize_limit(limit)
    normalized_time_filter = normalize_time_filter(time_filter)
    normalized_after = clean_optional_text(after)
    client = client or make_reddit_client()
    request_url, payload = client.subreddit_posts(
        subreddit=normalized_subreddit,
        listing=normalized_listing,
        limit=normalized_limit,
        time_filter=normalized_time_filter,
        after=normalized_after,
    )
    query_uid = build_query_uid(
        subreddit=normalized_subreddit,
        listing=normalized_listing,
        time_filter=normalized_time_filter,
        limit=normalized_limit,
        after=normalized_after,
    )
    query = RedditQuery(
        query_uid=query_uid,
        subreddit=normalized_subreddit,
        listing=normalized_listing,
        time_filter=normalized_time_filter
        if normalized_listing in {"top", "controversial"}
        else None,
        limit_count=normalized_limit,
        after_token=normalized_after,
        request_url=request_url,
        raw_payload=payload,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        known_tickers = fetch_known_tickers(conn)
        posts = parse_posts(
            payload,
            query_uid=query_uid,
            request_url=request_url,
            fallback_subreddit=normalized_subreddit,
            known_tickers=known_tickers,
        )
        with conn.transaction():
            upsert_post_query(conn, query)
            return upsert_posts(conn, posts)


def sync_default_subreddits(
    *,
    subreddits: tuple[str, ...] = DEFAULT_SUBREDDITS,
    listing: str = DEFAULT_LISTING,
    limit: int = DEFAULT_LIMIT,
    time_filter: str | None = None,
    database_url: str | None = None,
    client: RedditClient | None = None,
) -> dict[str, int]:
    client = client or make_reddit_client()
    counts: dict[str, int] = {}
    for subreddit in subreddits:
        counts[normalize_subreddit(subreddit)] = sync_subreddit_posts(
            subreddit=subreddit,
            listing=listing,
            limit=limit,
            time_filter=time_filter,
            database_url=database_url,
            client=client,
        )
    return counts


def ingest_devvit_payload(
    payload: dict[str, Any],
    *,
    database_url: str | None = None,
) -> int:
    subreddit = normalize_subreddit(clean_optional_text(payload.get("subreddit")) or "")
    listing = normalize_listing(clean_optional_text(payload.get("listing")))
    limit = normalize_limit(payload.get("limit") or payload.get("limit_count"))
    time_filter = normalize_time_filter(payload.get("time_filter"))
    after = clean_optional_text(payload.get("after") or payload.get("after_token"))
    request_url = clean_optional_text(payload.get("request_url")) or build_devvit_request_url(
        subreddit=subreddit,
        listing=listing,
        time_filter=time_filter,
        limit=limit,
        after=after,
    )
    query_uid = build_query_uid(
        subreddit=subreddit,
        listing=listing,
        time_filter=time_filter,
        limit=limit,
        after=after,
    )
    query = RedditQuery(
        query_uid=query_uid,
        subreddit=subreddit,
        listing=listing,
        time_filter=time_filter if listing in {"top", "controversial"} else None,
        limit_count=limit,
        after_token=after,
        request_url=request_url,
        raw_payload=payload,
    )

    with psycopg.connect(get_database_url(database_url), autocommit=False) as conn:
        known_tickers = fetch_known_tickers(conn)
        posts = parse_devvit_posts(
            payload,
            query_uid=query_uid,
            request_url=request_url,
            fallback_subreddit=subreddit,
            known_tickers=known_tickers,
        )
        with conn.transaction():
            upsert_post_query(conn, query)
            return upsert_posts(conn, posts)


def add_common_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--listing",
        default=DEFAULT_LISTING,
        choices=sorted(VALID_LISTINGS),
        help="Reddit listing 类型",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="最多同步帖子数，最大 100")
    parser.add_argument(
        "--time-filter",
        choices=sorted(VALID_TIME_FILTERS),
        help="top/controversial 使用的时间窗口",
    )
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Reddit API community posts.")
    subparsers = parser.add_subparsers(dest="command")

    subreddit_parser = subparsers.add_parser(
        "sync-subreddit",
        aliases=["sync-sub"],
        help="同步单个 subreddit 的帖子 listing",
    )
    subreddit_parser.add_argument("subreddit", help="subreddit 名称，例如 stocks")
    subreddit_parser.add_argument("--after", help="Reddit listing after 分页游标")
    add_common_sync_args(subreddit_parser)

    defaults_parser = subparsers.add_parser(
        "sync-defaults",
        help="同步 README 建议的默认投资社区",
    )
    defaults_parser.add_argument(
        "--subreddit",
        action="append",
        dest="subreddits",
        help="覆盖默认 subreddit，可重复传入",
    )
    add_common_sync_args(defaults_parser)

    devvit_parser = subparsers.add_parser(
        "import-devvit",
        help="导入 Devvit bridge 推送的 Reddit 帖子 JSON",
    )
    devvit_parser.add_argument(
        "--payload-file",
        help="Devvit JSON 文件路径；不传时从 stdin 读取",
    )
    devvit_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")

    return parser


def read_json_payload(path: str | None) -> dict[str, Any]:
    if path:
        with open(path, encoding="utf-8") as payload_file:
            payload = json.load(payload_file)
    else:
        payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise RedditError("Devvit payload 必须是 JSON 对象。")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command in {"sync-subreddit", "sync-sub"}:
            count = sync_subreddit_posts(
                subreddit=args.subreddit,
                listing=args.listing,
                limit=args.limit,
                time_filter=args.time_filter,
                after=args.after,
                database_url=args.database_url,
            )
            print(f"[完成] Reddit r/{normalize_subreddit(args.subreddit)} 同步 {count} 条")
            return 0

        if args.command == "sync-defaults":
            subreddits = tuple(args.subreddits or DEFAULT_SUBREDDITS)
            counts = sync_default_subreddits(
                subreddits=subreddits,
                listing=args.listing,
                limit=args.limit,
                time_filter=args.time_filter,
                database_url=args.database_url,
            )
            total = sum(counts.values())
            detail = ", ".join(f"r/{name}={count}" for name, count in counts.items())
            print(f"[完成] Reddit 默认社区同步 {total} 条：{detail}")
            return 0

        if args.command == "import-devvit":
            payload = read_json_payload(args.payload_file)
            count = ingest_devvit_payload(
                payload,
                database_url=args.database_url,
            )
            print(f"[完成] Devvit Reddit payload 导入 {count} 条")
            return 0

        parser.print_help()
        return 2
    except RedditError as exc:
        print(f"Reddit 同步失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
