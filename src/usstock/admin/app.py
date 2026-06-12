"""A small local admin panel for self-hosted research workflows."""

from __future__ import annotations

import argparse
import hmac
import html
import json
import sys
import threading
import traceback
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from usstock.config.settings import get_settings
from usstock.data import finnhub
from usstock.data import gdelt, reddit, sec
from usstock.discovery import daily as discovery
from usstock.discovery import topic_candidates


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7878
PAGE_SIZE = 100
MAX_POST_BYTES = 64 * 1024
MAX_JSON_POST_BYTES = 4 * 1024 * 1024
DEVVIT_JSON_PATHS = {
    "/api/reddit/devvit/posts",
    "/api/reddit/devvit/matches",
}
PICO_CSS_URL = "https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css"
HTMX_JS_URL = "https://cdn.jsdelivr.net/npm/htmx.org@2.0.10/dist/htmx.min.js"
ALPINE_JS_URL = "https://cdn.jsdelivr.net/npm/alpinejs@3/dist/cdn.min.js"


@dataclass(frozen=True)
class DiscoveryRunConfig:
    top_n: int
    lookback_hours: int
    gdelt_max_records: int
    max_sec_tickers: int
    sec_filing_limit: int
    include_company_facts: bool
    skip_finnhub_sync: bool
    skip_gdelt_sync: bool
    skip_reddit_sync: bool
    skip_sec_sync: bool


class DiscoveryScheduler:
    """Small in-process scheduler controlled from the local admin panel."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._interval_minutes: int | None = None
        self._config: DiscoveryRunConfig | None = None
        self._started_at: datetime | None = None
        self._last_run_started_at: datetime | None = None
        self._last_run_finished_at: datetime | None = None
        self._last_message = "尚未启动。"
        self._last_error: str | None = None
        self._run_count = 0
        self._is_running_once = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "active": self._thread is not None and self._thread.is_alive(),
                "interval_minutes": self._interval_minutes,
                "config": self._config,
                "started_at": self._started_at,
                "last_run_started_at": self._last_run_started_at,
                "last_run_finished_at": self._last_run_finished_at,
                "last_message": self._last_message,
                "last_error": self._last_error,
                "run_count": self._run_count,
                "is_running_once": self._is_running_once,
            }

    def start(
        self,
        *,
        database_url: str,
        interval_minutes: int,
        config: DiscoveryRunConfig,
    ) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False

            self._stop_event.clear()
            self._interval_minutes = interval_minutes
            self._config = config
            self._started_at = datetime.now()
            self._last_message = "定时任务已启动，正在等待首次运行。"
            self._last_error = None
            self._thread = threading.Thread(
                target=self._run_loop,
                args=(database_url, interval_minutes, config),
                daemon=True,
                name="usstock-discovery-scheduler",
            )
            self._thread.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._last_message = "定时任务当前未运行。"
                return False

            self._stop_event.set()
            self._last_message = "已请求停止定时任务。"
            return True

    def _run_loop(
        self,
        database_url: str,
        interval_minutes: int,
        config: DiscoveryRunConfig,
    ) -> None:
        try:
            while not self._stop_event.is_set():
                self._run_once(database_url, config)
                if self._stop_event.wait(max(1, interval_minutes) * 60):
                    break
        finally:
            with self._lock:
                self._thread = None
                if self._last_message == "已请求停止定时任务。":
                    self._last_message = "定时任务已停止。"

    def _run_once(self, database_url: str, config: DiscoveryRunConfig) -> None:
        with self._lock:
            self._is_running_once = True
            self._last_run_started_at = datetime.now()
            self._last_error = None

        try:
            result = run_discovery_daily(database_url, config)
            message = (
                f"{result.run_date.isoformat()} 自动发现完成："
                f"候选 {len(result.candidates)} 个，"
                f"警告 {len(result.warnings)} 条。"
            )
            with self._lock:
                self._run_count += 1
                self._last_message = message
        except Exception as exc:  # pragma: no cover - background safety net.
            with self._lock:
                self._last_error = str(exc)
                self._last_message = f"自动发现失败：{exc}"
        finally:
            with self._lock:
                self._last_run_finished_at = datetime.now()
                self._is_running_once = False


DISCOVERY_SCHEDULER = DiscoveryScheduler()


class AdminPanelError(RuntimeError):
    """Raised when the local admin panel cannot complete a request."""


class AdminHTTPServer(ThreadingHTTPServer):
    """HTTP server carrying runtime settings."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        database_url: str | None,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.database_url = database_url


class AdminRequestHandler(BaseHTTPRequestHandler):
    """Request handler for the minimal local panel."""

    server: AdminHTTPServer

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/partials/discovery-status":
            self.send_html(
                HTTPStatus.OK,
                render_discovery_scheduler_status(DISCOVERY_SCHEDULER.snapshot()),
            )
            return

        routes = {
            "/": render_dashboard,
            "/stocks": render_stocks,
            "/sec": render_sec_filings,
            "/gdelt": render_gdelt,
            "/finnhub": render_finnhub,
            "/reddit": render_reddit,
            "/topics": render_topics,
            "/tasks": render_tasks,
        }
        renderer = routes.get(parsed.path)
        if not renderer:
            self.send_html(HTTPStatus.NOT_FOUND, render_not_found())
            return

        self.send_html(
            HTTPStatus.OK,
            renderer(
                database_url=self.resolve_database_url(),
                query=query,
            ),
        )

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0") or "0")
        max_post_bytes = (
            MAX_JSON_POST_BYTES
            if parsed.path in DEVVIT_JSON_PATHS
            else MAX_POST_BYTES
        )
        if length > max_post_bytes:
            if parsed.path in DEVVIT_JSON_PATHS:
                self.send_json(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    {"ok": False, "error": "请求内容过大。"},
                )
                return
            self.redirect("/tasks", "请求内容过大。", ok=False)
            return

        raw_bytes = self.rfile.read(length)
        if parsed.path == "/api/reddit/devvit/posts":
            self.handle_devvit_reddit_posts(raw_bytes)
            return
        if parsed.path == "/api/reddit/devvit/matches":
            self.handle_devvit_reddit_matches(raw_bytes)
            return

        raw_body = raw_bytes.decode("utf-8")
        form = urllib.parse.parse_qs(raw_body)

        try:
            database_url = self.resolve_database_url()
            if parsed.path == "/actions/stock-upsert":
                message = upsert_stock(database_url, form)
                self.redirect("/stocks", message, ok=True)
                return

            if parsed.path == "/actions/sec-registry":
                count = sec.sync_company_registry(database_url=database_url)
                self.redirect("/tasks", f"SEC 公司映射同步完成：{count} 条。", ok=True)
                return

            if parsed.path == "/actions/sec-ticker":
                ticker = form_value(form, "ticker").upper()
                if not ticker:
                    raise AdminPanelError("ticker 不能为空。")

                filing_limit = parse_positive_int(form_value(form, "filing_limit"))
                fact_limit = parse_positive_int(form_value(form, "fact_limit"))
                include_facts = form_bool(form, "include_company_facts")
                cik, filing_count, fact_count = sec.sync_ticker(
                    ticker=ticker,
                    include_company_facts=include_facts,
                    filing_limit=filing_limit,
                    fact_limit=fact_limit,
                    database_url=database_url,
                )
                self.redirect(
                    "/tasks",
                    (
                        f"{ticker} 同步完成：CIK={cik}，"
                        f"filing={filing_count}，fact={fact_count}。"
                    ),
                    ok=True,
                )
                return

            if parsed.path == "/actions/gdelt-query":
                query_text = form_value(form, "query")
                if not query_text:
                    raise AdminPanelError("GDELT query 不能为空。")

                timespan = form_value(form, "timespan") or gdelt.DEFAULT_TIMESPAN
                max_records = parse_positive_int(form_value(form, "max_records"))
                client = gdelt.make_gdelt_client()
                article_count = gdelt.sync_articles(
                    query=query_text,
                    timespan=timespan,
                    max_records=max_records or gdelt.DEFAULT_MAX_RECORDS,
                    database_url=database_url,
                    client=client,
                )
                timeline_count = gdelt.sync_timeline(
                    query=query_text,
                    timespan=timespan,
                    database_url=database_url,
                    client=client,
                )
                self.redirect(
                    "/tasks",
                    (
                        "GDELT 同步完成："
                        f"article={article_count}，timeline={timeline_count}。"
                    ),
                    ok=True,
                )
                return

            if parsed.path == "/actions/finnhub-market":
                category = (
                    form_value(form, "category")
                    or finnhub.DEFAULT_MARKET_CATEGORY
                )
                min_id = parse_positive_int(form_value(form, "min_id"))
                count = finnhub.sync_market_news(
                    category=category,
                    min_id=min_id,
                    database_url=database_url,
                )
                self.redirect(
                    "/tasks",
                    f"Finnhub market news 同步完成：{count} 条。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/finnhub-company":
                ticker = form_value(form, "ticker").upper()
                if not ticker:
                    raise AdminPanelError("ticker 不能为空。")

                count = finnhub.sync_company_news(
                    ticker=ticker,
                    from_date=form_value(form, "from_date") or None,
                    to_date=form_value(form, "to_date") or None,
                    database_url=database_url,
                )
                self.redirect(
                    "/tasks",
                    f"{ticker} Finnhub company news 同步完成：{count} 条。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/reddit-defaults":
                listing = form_value(form, "listing") or reddit.DEFAULT_LISTING
                limit = parse_positive_int(form_value(form, "limit")) or 50
                counts = reddit.sync_default_subreddits(
                    listing=listing,
                    limit=limit,
                    database_url=database_url,
                )
                total = sum(counts.values())
                detail = "，".join(f"r/{name}={count}" for name, count in counts.items())
                self.redirect(
                    "/reddit",
                    f"Reddit 默认社区同步完成：{total} 条。{detail}",
                    ok=True,
                )
                return

            if parsed.path == "/actions/reddit-subreddit":
                subreddit = form_value(form, "subreddit")
                if not subreddit:
                    raise AdminPanelError("subreddit 不能为空。")

                listing = form_value(form, "listing") or reddit.DEFAULT_LISTING
                limit = parse_positive_int(form_value(form, "limit")) or 50
                time_filter = form_value(form, "time_filter") or None
                count = reddit.sync_subreddit_posts(
                    subreddit=subreddit,
                    listing=listing,
                    limit=limit,
                    time_filter=time_filter,
                    database_url=database_url,
                )
                self.redirect(
                    "/reddit",
                    f"Reddit r/{reddit.normalize_subreddit(subreddit)} 同步完成：{count} 条。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/discovery-daily":
                config = parse_discovery_run_config(form)
                result = run_discovery_daily(database_url, config)
                self.redirect(
                    "/tasks",
                    (
                        "自动热点发现完成："
                        f"候选={len(result.candidates)}，"
                        f"警告={len(result.warnings)}。"
                    ),
                    ok=True,
                )
                return

            if parsed.path == "/actions/topic-extract":
                result = topic_candidates.run_topic_extraction(
                    database_url=database_url,
                    lookback_hours=(
                        parse_positive_int(form_value(form, "lookback_hours"))
                        or topic_candidates.DEFAULT_TOPIC_EXTRACTION_LOOKBACK_HOURS
                    ),
                    max_candidates=(
                        parse_positive_int(form_value(form, "max_candidates"))
                        or topic_candidates.DEFAULT_MAX_CANDIDATES
                    ),
                    min_articles=(
                        parse_positive_int(form_value(form, "min_articles"))
                        or topic_candidates.DEFAULT_MIN_ARTICLES
                    ),
                    min_score=form_value(form, "min_score")
                    or str(topic_candidates.DEFAULT_MIN_SCORE),
                    include_existing_matches=form_bool(form, "include_existing_matches"),
                )
                self.redirect(
                    "/topics",
                    f"候选主题抽取完成：新增或刷新 {len(result.candidates)} 个候选主题。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/discovery-schedule-start":
                config = parse_discovery_run_config(form)
                interval_minutes = (
                    parse_positive_int(form_value(form, "interval_minutes")) or 60
                )
                started = DISCOVERY_SCHEDULER.start(
                    database_url=get_database_url(database_url),
                    interval_minutes=interval_minutes,
                    config=config,
                )
                self.redirect(
                    "/tasks",
                    (
                        f"自动发现定时任务已启动，每 {interval_minutes} 分钟运行一次。"
                        if started
                        else "自动发现定时任务已经在运行。"
                    ),
                    ok=True,
                )
                return

            if parsed.path == "/actions/discovery-schedule-stop":
                stopped = DISCOVERY_SCHEDULER.stop()
                self.redirect(
                    "/tasks",
                    "已请求停止自动发现定时任务。" if stopped else "自动发现定时任务当前未运行。",
                    ok=True,
                )
                return

            self.redirect("/tasks", "未知操作。", ok=False)
        except Exception as exc:  # pragma: no cover - keeps the panel usable.
            traceback.print_exc()
            self.redirect("/tasks", f"操作失败：{exc}", ok=False)

    def handle_devvit_reddit_posts(self, raw_body: bytes) -> None:
        try:
            self.authorize_devvit_webhook()
            payload = json.loads(raw_body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise AdminPanelError("Devvit payload 必须是 JSON 对象。")
            count = reddit.ingest_devvit_payload(
                payload,
                database_url=self.resolve_database_url(),
            )
            self.send_json(HTTPStatus.OK, {"ok": True, "imported": count})
        except json.JSONDecodeError as exc:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {"ok": False, "error": f"JSON 解析失败：{exc}"},
            )
        except PermissionError as exc:
            self.send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": str(exc)})
        except Exception as exc:  # pragma: no cover - API safety net.
            traceback.print_exc()
            self.send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": str(exc)},
            )

    def handle_devvit_reddit_matches(self, raw_body: bytes) -> None:
        try:
            self.authorize_devvit_webhook()
            payload = json.loads(raw_body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise AdminPanelError("Devvit match payload 必须是 JSON 对象。")
            counts = reddit.ingest_devvit_match_payload(
                payload,
                database_url=self.resolve_database_url(),
            )
            self.send_json(HTTPStatus.OK, {"ok": True, **counts})
        except json.JSONDecodeError as exc:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {"ok": False, "error": f"JSON 解析失败：{exc}"},
            )
        except PermissionError as exc:
            self.send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": str(exc)})
        except Exception as exc:  # pragma: no cover - API safety net.
            traceback.print_exc()
            self.send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": str(exc)},
            )

    def authorize_devvit_webhook(self) -> None:
        secret = get_settings().reddit_devvit_webhook_secret
        if not secret:
            raise PermissionError("缺少 REDDIT_DEVVIT_WEBHOOK_SECRET，拒绝 Devvit webhook。")

        auth_header = self.headers.get("Authorization", "")
        token = ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
        token = token or self.headers.get("X-Usstock-Webhook-Secret", "").strip()
        if not hmac.compare_digest(token, secret):
            raise PermissionError("Devvit webhook 密钥无效。")

    def resolve_database_url(self) -> str | None:
        return self.server.database_url

    def redirect(self, path: str, message: str, *, ok: bool) -> None:
        params = urllib.parse.urlencode(
            {
                "notice": clip_text(message, 600),
                "notice_type": "ok" if ok else "error",
            }
        )
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", f"{path}?{params}")
        self.end_headers()

    def send_html(self, status: HTTPStatus, body: str) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_json(self, status: HTTPStatus, body: dict[str, Any]) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[admin] " + fmt % args + "\n")


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise AdminPanelError("缺少 DATABASE_URL，请先配置 .env 或环境变量。")
    return database_url


def connect_database(database_url: str | None) -> Connection[dict[str, Any]]:
    return psycopg.connect(
        get_database_url(database_url),
        autocommit=True,
        row_factory=dict_row,
    )


def table_exists(conn: Connection[dict[str, Any]], table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) AS table_name", (table_name,)).fetchone()
    return bool(row and row["table_name"])


def column_exists(
    conn: Connection[dict[str, Any]],
    table_name: str,
    column_name: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    ).fetchone()
    return bool(row)


def fetch_one(
    conn: Connection[dict[str, Any]],
    sql: str,
    params: tuple[Any, ...] = (),
) -> dict[str, Any]:
    row = conn.execute(sql, params).fetchone()
    return dict(row or {})


def fetch_all(
    conn: Connection[dict[str, Any]],
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def render_dashboard(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    try:
        with connect_database(database_url) as conn:
            tables = {
                "stock_universe": table_exists(conn, "stock_universe"),
                "sec_filings": table_exists(conn, "sec_filings"),
                "sec_financial_facts": table_exists(conn, "sec_financial_facts"),
                "gdelt_articles": table_exists(conn, "gdelt_articles"),
                "gdelt_doc_queries": table_exists(conn, "gdelt_doc_queries"),
                "finnhub_articles": table_exists(conn, "finnhub_articles"),
                "finnhub_news_queries": table_exists(conn, "finnhub_news_queries"),
                "reddit_posts": table_exists(conn, "reddit_posts"),
                "reddit_post_queries": table_exists(conn, "reddit_post_queries"),
                "market_topics": table_exists(conn, "market_topics"),
                "market_topic_candidates": table_exists(conn, "market_topic_candidates"),
                "topic_mentions": table_exists(conn, "topic_mentions"),
                "schema_migrations": table_exists(conn, "schema_migrations"),
            }
            stock_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE is_active) AS active,
                        count(*) FILTER (WHERE is_manual_watchlist) AS manual
                    FROM stock_universe
                    """,
                )
                if tables["stock_universe"]
                else {}
            )
            sec_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE filing_date >= current_date - interval '7 days'
                        ) AS recent,
                        max(fetched_at) AS last_fetched_at
                    FROM sec_filings
                    """,
                )
                if tables["sec_filings"]
                else {}
            )
            fact_summary = (
                fetch_one(conn, "SELECT count(*) AS total FROM sec_financial_facts")
                if tables["sec_financial_facts"]
                else {}
            )
            gdelt_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE seen_at >= now() - interval '24 hours'
                        ) AS recent,
                        max(last_seen_at) AS last_seen_at
                    FROM gdelt_articles
                    """,
                )
                if tables["gdelt_articles"]
                else {}
            )
            query_summary = (
                fetch_one(conn, "SELECT count(*) AS total FROM gdelt_doc_queries")
                if tables["gdelt_doc_queries"]
                else {}
            )
            finnhub_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE published_at >= now() - interval '24 hours'
                        ) AS recent,
                        max(last_seen_at) AS last_seen_at
                    FROM finnhub_articles
                    """,
                )
                if tables["finnhub_articles"]
                else {}
            )
            finnhub_query_summary = (
                fetch_one(conn, "SELECT count(*) AS total FROM finnhub_news_queries")
                if tables["finnhub_news_queries"]
                else {}
            )
            reddit_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE created_utc >= now() - interval '24 hours'
                        ) AS recent,
                        count(DISTINCT subreddit) AS subreddits,
                        max(created_utc) AS latest_created_at
                    FROM reddit_posts
                    """,
                )
                if tables["reddit_posts"]
                else {}
            )
            reddit_query_summary = (
                fetch_one(conn, "SELECT count(*) AS total FROM reddit_post_queries")
                if tables["reddit_post_queries"]
                else {}
            )
            topic_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE is_active) AS active
                    FROM market_topics
                    """,
                )
                if tables["market_topics"]
                else {}
            )
            topic_mention_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE detected_at >= now() - interval '24 hours'
                        ) AS recent
                    FROM topic_mentions
                    """,
                )
                if tables["topic_mentions"]
                else {}
            )
            topic_candidate_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE status = 'pending') AS pending,
                        count(*) FILTER (
                            WHERE last_seen_at >= now() - interval '24 hours'
                        ) AS recent,
                        max(last_seen_at) AS last_seen_at
                    FROM market_topic_candidates
                    """,
                )
                if tables["market_topic_candidates"]
                else {}
            )
            migrations = (
                fetch_all(
                    conn,
                    """
                    SELECT version, name, applied_at, execution_time_ms
                    FROM schema_migrations
                    ORDER BY version DESC
                    LIMIT 6
                    """,
                )
                if tables["schema_migrations"]
                else []
            )
            recent_filings = (
                fetch_all(
                    conn,
                    """
                    SELECT ticker, company_name, form_type, filing_date,
                           report_date, primary_document_url
                    FROM sec_filings
                    ORDER BY filing_date DESC, acceptance_datetime DESC NULLS LAST
                    LIMIT 8
                    """,
                )
                if tables["sec_filings"]
                else []
            )
            recent_articles = (
                fetch_all(
                    conn,
                    """
                    SELECT title, domain, language, seen_at, article_url
                    FROM gdelt_articles
                    ORDER BY coalesce(seen_at, last_seen_at) DESC
                    LIMIT 8
                    """,
                )
                if tables["gdelt_articles"]
                else []
            )
            recent_finnhub_articles = (
                fetch_all(
                    conn,
                    """
                    SELECT headline, source_name, category, related_tickers,
                           published_at, article_url
                    FROM finnhub_articles
                    ORDER BY coalesce(published_at, last_seen_at) DESC
                    LIMIT 8
                    """,
                )
                if tables["finnhub_articles"]
                else []
            )
            recent_reddit_posts = (
                fetch_all(
                    conn,
                    """
                    SELECT title, subreddit, candidate_tickers, score,
                           comment_count, created_utc, permalink_url
                    FROM reddit_posts
                    ORDER BY coalesce(created_utc, last_seen_at) DESC
                    LIMIT 8
                    """,
                )
                if tables["reddit_posts"]
                else []
            )
    except Exception as exc:
        return render_database_error(exc, active="/")

    metrics = [
        metric_box(
            "股票池",
            stock_summary.get("total"),
            f"启用 {fmt(stock_summary.get('active'))} / 手工 {fmt(stock_summary.get('manual'))}",
        ),
        metric_box(
            "SEC 公告",
            sec_summary.get("total"),
            f"近 7 天 {fmt(sec_summary.get('recent'))}",
        ),
        metric_box("财务事实", fact_summary.get("total"), "SEC company facts"),
        metric_box(
            "GDELT 文章",
            gdelt_summary.get("total"),
            f"近 24 小时 {fmt(gdelt_summary.get('recent'))}",
        ),
        metric_box("GDELT 查询", query_summary.get("total"), "artlist / timeline"),
        metric_box(
            "Finnhub 新闻",
            finnhub_summary.get("total"),
            f"近 24 小时 {fmt(finnhub_summary.get('recent'))}",
        ),
        metric_box(
            "Finnhub 查询",
            finnhub_query_summary.get("total"),
            "market / company",
        ),
        metric_box(
            "Reddit 帖子",
            reddit_summary.get("total"),
            f"近 24 小时 {fmt(reddit_summary.get('recent'))}",
        ),
        metric_box(
            "Reddit 查询",
            reddit_query_summary.get("total"),
            f"社区 {fmt(reddit_summary.get('subreddits'))}",
        ),
        metric_box(
            "主题",
            topic_summary.get("total"),
            f"启用 {fmt(topic_summary.get('active'))} / 24h 提及 {fmt(topic_mention_summary.get('recent'))}",
        ),
        metric_box(
            "候选主题",
            topic_candidate_summary.get("total"),
            f"待审核 {fmt(topic_candidate_summary.get('pending'))} / 24h {fmt(topic_candidate_summary.get('recent'))}",
        ),
    ]

    body = f"""
    <section class="toolbar">
      <h1>控制台</h1>
      <a class="button primary" href="/tasks">同步数据</a>
    </section>
    <section class="metrics">{"".join(metrics)}</section>
    <section class="split">
      <div>
        <h2>最近 SEC 公告</h2>
        {render_filings_table(recent_filings, compact=True)}
      </div>
      <div>
        <h2>最近 GDELT 文章</h2>
        {render_articles_table(recent_articles, compact=True)}
      </div>
      <div>
        <h2>最近 Finnhub 新闻</h2>
        {render_finnhub_articles_table(recent_finnhub_articles, compact=True)}
      </div>
      <div>
        <h2>最近 Reddit 帖子</h2>
        {render_reddit_posts_table(recent_reddit_posts, compact=True)}
      </div>
    </section>
    <section>
      <h2>已执行迁移</h2>
      {render_migrations_table(migrations)}
    </section>
    """
    return layout("控制台", body, active="/", query=query)


def render_stocks(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    q = form_value(query, "q")
    manual_only = form_value(query, "manual") == "1"
    active_only = form_value(query, "active") == "1"

    try:
        with connect_database(database_url) as conn:
            if not table_exists(conn, "stock_universe"):
                rows: list[dict[str, Any]] = []
            else:
                conditions: list[str] = []
                params: list[Any] = []
                if q:
                    conditions.append(
                        """
                        (
                            ticker ILIKE %s OR company_name ILIKE %s
                            OR coalesce(sector, '') ILIKE %s
                            OR coalesce(industry, '') ILIKE %s
                        )
                        """
                    )
                    like = f"%{q}%"
                    params.extend([like, like, like, like])
                if manual_only:
                    conditions.append("is_manual_watchlist = TRUE")
                if active_only:
                    conditions.append("is_active = TRUE")

                where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                rows = fetch_all(
                    conn,
                    f"""
                    SELECT ticker, company_name, exchange, sector, industry,
                           is_active, is_manual_watchlist, sec_cik,
                           last_price, last_refreshed_at, notes
                    FROM stock_universe
                    {where}
                    ORDER BY is_manual_watchlist DESC, is_active DESC, ticker
                    LIMIT %s
                    """,
                    (*params, PAGE_SIZE),
                )
    except Exception as exc:
        return render_database_error(exc, active="/stocks")

    body = f"""
    <section class="toolbar">
      <h1>股票池</h1>
      <form method="get" class="filters">
        <input name="q" value="{e(q)}" placeholder="ticker / 公司 / 行业">
        <label><input type="checkbox" name="manual" value="1" {checked(manual_only)}> 手工观察</label>
        <label><input type="checkbox" name="active" value="1" {checked(active_only)}> 仅启用</label>
        <button type="submit">筛选</button>
      </form>
    </section>
    <section>
      <h2>新增或更新标的</h2>
      <form method="post" action="/actions/stock-upsert" class="grid-form">
        <label>ticker <input name="ticker" required placeholder="AAPL"></label>
        <label>公司名称 <input name="company_name" required placeholder="Apple Inc."></label>
        <label>交易所 <input name="exchange" placeholder="NASDAQ"></label>
        <label>板块 <input name="sector" placeholder="Technology"></label>
        <label>行业 <input name="industry" placeholder="Consumer Electronics"></label>
        <label class="span-2">业务描述 <textarea name="business_description" rows="3"></textarea></label>
        <label><input type="checkbox" name="is_active" value="1" checked> 启用</label>
        <label><input type="checkbox" name="is_manual_watchlist" value="1" checked> 手工观察</label>
        <button class="primary" type="submit">保存</button>
      </form>
    </section>
    <section>
      <h2>标的列表</h2>
      {render_stocks_table(rows)}
    </section>
    """
    return layout("股票池", body, active="/stocks", query=query)


def render_sec_filings(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    ticker = form_value(query, "ticker").upper()
    form_type = form_value(query, "form_type").upper()

    try:
        with connect_database(database_url) as conn:
            if not table_exists(conn, "sec_filings"):
                rows: list[dict[str, Any]] = []
            else:
                conditions: list[str] = []
                params: list[Any] = []
                if ticker:
                    conditions.append("upper(ticker) = upper(%s)")
                    params.append(ticker)
                if form_type:
                    conditions.append("upper(form_type) = upper(%s)")
                    params.append(form_type)
                where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                rows = fetch_all(
                    conn,
                    f"""
                    SELECT ticker, company_name, form_type, filing_date,
                           report_date, items, is_amendment,
                           primary_document_url, filing_detail_url, fetched_at
                    FROM sec_filings
                    {where}
                    ORDER BY filing_date DESC, acceptance_datetime DESC NULLS LAST
                    LIMIT %s
                    """,
                    (*params, PAGE_SIZE),
                )
    except Exception as exc:
        return render_database_error(exc, active="/sec")

    body = f"""
    <section class="toolbar">
      <h1>SEC 公告</h1>
      <form method="get" class="filters">
        <input name="ticker" value="{e(ticker)}" placeholder="ticker">
        <input name="form_type" value="{e(form_type)}" placeholder="10-K / 10-Q / 8-K">
        <button type="submit">筛选</button>
      </form>
    </section>
    <section>{render_filings_table(rows)}</section>
    """
    return layout("SEC 公告", body, active="/sec", query=query)


def render_gdelt(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    q = form_value(query, "q")

    try:
        with connect_database(database_url) as conn:
            has_articles = table_exists(conn, "gdelt_articles")
            has_queries = table_exists(conn, "gdelt_doc_queries")
            article_conditions: list[str] = []
            article_params: list[Any] = []
            query_conditions: list[str] = []
            query_params: list[Any] = []
            if q:
                like = f"%{q}%"
                article_conditions.append(
                    """
                    (
                        title ILIKE %s OR query_text ILIKE %s
                        OR coalesce(domain, '') ILIKE %s
                    )
                    """
                )
                article_params.extend([like, like, like])
                query_conditions.append("query_text ILIKE %s")
                query_params.append(like)

            article_where = (
                f"WHERE {' AND '.join(article_conditions)}"
                if article_conditions
                else ""
            )
            query_where = (
                f"WHERE {' AND '.join(query_conditions)}" if query_conditions else ""
            )
            articles = (
                fetch_all(
                    conn,
                    f"""
                    SELECT title, domain, language, source_country, tone,
                           seen_at, article_url, query_text
                    FROM gdelt_articles
                    {article_where}
                    ORDER BY coalesce(seen_at, last_seen_at) DESC
                    LIMIT %s
                    """,
                    (*article_params, PAGE_SIZE),
                )
                if has_articles
                else []
            )
            queries = (
                fetch_all(
                    conn,
                    f"""
                    SELECT query_text, mode, timespan, sort, max_records, fetched_at
                    FROM gdelt_doc_queries
                    {query_where}
                    ORDER BY fetched_at DESC
                    LIMIT 20
                    """,
                    tuple(query_params),
                )
                if has_queries
                else []
            )
    except Exception as exc:
        return render_database_error(exc, active="/gdelt")

    body = f"""
    <section class="toolbar">
      <h1>GDELT</h1>
      <form method="get" class="filters">
        <input name="q" value="{e(q)}" placeholder="关键词 / 域名 / query">
        <button type="submit">筛选</button>
      </form>
    </section>
    <section>
      <h2>查询记录</h2>
      {render_gdelt_queries_table(queries)}
    </section>
    <section>
      <h2>文章</h2>
      {render_articles_table(articles)}
    </section>
    """
    return layout("GDELT", body, active="/gdelt", query=query)


def render_finnhub(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    q = form_value(query, "q")
    ticker = form_value(query, "ticker").upper()
    category = form_value(query, "category").lower()
    settings = get_settings()
    today = date.today()
    week_ago = today - timedelta(days=finnhub.DEFAULT_COMPANY_NEWS_DAYS)

    try:
        with connect_database(database_url) as conn:
            has_articles = table_exists(conn, "finnhub_articles")
            has_queries = table_exists(conn, "finnhub_news_queries")
            summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE published_at >= now() - interval '24 hours'
                        ) AS recent,
                        count(*) FILTER (WHERE endpoint = 'market_news') AS market,
                        count(*) FILTER (WHERE endpoint = 'company_news') AS company,
                        count(DISTINCT nullif(source_name, '')) AS sources,
                        max(published_at) AS latest_published_at
                    FROM finnhub_articles
                    """,
                )
                if has_articles
                else {}
            )
            category_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT coalesce(category, '-') AS label,
                           count(*) AS total,
                           max(published_at) AS latest_at
                    FROM finnhub_articles
                    GROUP BY coalesce(category, '-')
                    ORDER BY total DESC, label
                    LIMIT 8
                    """,
                )
                if has_articles
                else []
            )
            source_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT coalesce(source_name, '-') AS label,
                           count(*) AS total,
                           max(published_at) AS latest_at
                    FROM finnhub_articles
                    GROUP BY coalesce(source_name, '-')
                    ORDER BY total DESC, label
                    LIMIT 8
                    """,
                )
                if has_articles
                else []
            )
            article_conditions: list[str] = []
            article_params: list[Any] = []
            query_conditions: list[str] = []
            query_params: list[Any] = []
            if q:
                like = f"%{q}%"
                article_conditions.append(
                    """
                    (
                        headline ILIKE %s OR coalesce(summary, '') ILIKE %s
                        OR coalesce(source_name, '') ILIKE %s
                    )
                    """
                )
                article_params.extend([like, like, like])
            if ticker:
                article_conditions.append("%s = ANY(related_tickers)")
                article_params.append(ticker)
                query_conditions.append("upper(coalesce(ticker, '')) = upper(%s)")
                query_params.append(ticker)
            if category:
                article_conditions.append("lower(coalesce(category, '')) = lower(%s)")
                article_params.append(category)
                query_conditions.append("lower(coalesce(category, '')) = lower(%s)")
                query_params.append(category)

            article_where = (
                f"WHERE {' AND '.join(article_conditions)}"
                if article_conditions
                else ""
            )
            query_where = (
                f"WHERE {' AND '.join(query_conditions)}" if query_conditions else ""
            )
            articles = (
                fetch_all(
                    conn,
                    f"""
                    SELECT headline, source_name, category, related_tickers,
                           published_at, article_url, endpoint
                    FROM finnhub_articles
                    {article_where}
                    ORDER BY coalesce(published_at, last_seen_at) DESC
                    LIMIT %s
                    """,
                    (*article_params, PAGE_SIZE),
                )
                if has_articles
                else []
            )
            queries = (
                fetch_all(
                    conn,
                    f"""
                    SELECT endpoint, category, ticker, from_date, to_date,
                           min_id, fetched_at
                    FROM finnhub_news_queries
                    {query_where}
                    ORDER BY fetched_at DESC
                    LIMIT 20
                    """,
                    tuple(query_params),
                )
                if has_queries
                else []
            )
    except Exception as exc:
        return render_database_error(exc, active="/finnhub")

    metrics = [
        metric_box(
            "API Key",
            "已配置" if settings.finnhub_api_key else "未配置",
            "FINNHUB_API_KEY",
        ),
        metric_box("新闻总数", summary.get("total"), "financial_news"),
        metric_box("近 24 小时", summary.get("recent"), "published_at"),
        metric_box(
            "来源数",
            summary.get("sources"),
            f"最近 {fmt(summary.get('latest_published_at'))}",
        ),
        metric_box(
            "Market / Company",
            f"{fmt(summary.get('market'))} / {fmt(summary.get('company'))}",
            "endpoint",
        ),
    ]
    body = f"""
    <section class="toolbar">
      <h1>Finnhub</h1>
      <form method="get" class="filters">
        <input name="q" value="{e(q)}" placeholder="关键词 / 来源">
        <input name="ticker" value="{e(ticker)}" placeholder="ticker">
        <input name="category" value="{e(category)}" placeholder="category">
        <button type="submit">筛选</button>
      </form>
    </section>
    <section class="metrics">{"".join(metrics)}</section>
    <section class="task-grid">
      <form method="post" action="/actions/finnhub-market">
        <h2>市场新闻</h2>
        <label>category <input name="category" placeholder="general" value="{e(category or finnhub.DEFAULT_MARKET_CATEGORY)}"></label>
        <label>minId <input name="min_id" type="number" min="1" placeholder="增量 ID，可空"></label>
        <button class="primary" type="submit">同步市场新闻</button>
      </form>
      <form method="post" action="/actions/finnhub-company">
        <h2>个股新闻</h2>
        <label>ticker <input name="ticker" required placeholder="AAPL" value="{e(ticker)}"></label>
        <label>开始日期 <input name="from_date" type="date" value="{week_ago.isoformat()}"></label>
        <label>结束日期 <input name="to_date" type="date" value="{today.isoformat()}"></label>
        <button class="primary" type="submit">同步个股新闻</button>
      </form>
    </section>
    <section class="split">
      <div>
        <h2>分类概览</h2>
        {render_finnhub_breakdown_table(category_rows, empty_message="暂无 Finnhub 分类统计。")}
      </div>
      <div>
        <h2>来源概览</h2>
        {render_finnhub_breakdown_table(source_rows, empty_message="暂无 Finnhub 来源统计。")}
      </div>
    </section>
    <section>
      <h2>查询记录</h2>
      {render_finnhub_queries_table(queries)}
    </section>
    <section>
      <h2>金融新闻</h2>
      {render_finnhub_articles_table(articles)}
    </section>
    """
    return layout("Finnhub", body, active="/finnhub", query=query)


def render_reddit(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    q = form_value(query, "q")
    subreddit_filter = form_value(query, "subreddit")
    ticker = form_value(query, "ticker").upper()
    settings = get_settings()

    try:
        with connect_database(database_url) as conn:
            has_posts = table_exists(conn, "reddit_posts")
            has_queries = table_exists(conn, "reddit_post_queries")
            has_comments = table_exists(conn, "reddit_comments")
            summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE created_utc >= now() - interval '24 hours'
                        ) AS recent,
                        count(DISTINCT subreddit) AS subreddits,
                        sum(comment_count) AS comments,
                        max(created_utc) AS latest_created_at
                    FROM reddit_posts
                    """,
                )
                if has_posts
                else {}
            )
            comment_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE coalesce(created_utc, last_seen_at, created_at)
                                  >= now() - interval '24 hours'
                        ) AS recent,
                        max(coalesce(created_utc, last_seen_at, created_at)) AS latest_at
                    FROM reddit_comments
                    """,
                )
                if has_comments
                else {}
            )
            subreddit_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT subreddit AS label,
                           count(*) AS total,
                           max(created_utc) AS latest_at
                    FROM reddit_posts
                    GROUP BY subreddit
                    ORDER BY total DESC, label
                    LIMIT 12
                    """,
                )
                if has_posts
                else []
            )
            ticker_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT ticker AS label,
                           count(*) AS total,
                           max(created_utc) AS latest_at
                    FROM reddit_posts
                    CROSS JOIN LATERAL unnest(candidate_tickers) AS ticker
                    GROUP BY ticker
                    ORDER BY total DESC, label
                    LIMIT 12
                    """,
                )
                if has_posts
                else []
            )
            post_conditions: list[str] = []
            post_params: list[Any] = []
            comment_conditions: list[str] = []
            comment_params: list[Any] = []
            query_conditions: list[str] = []
            query_params: list[Any] = []
            if q:
                like = f"%{q}%"
                post_conditions.append(
                    """
                    (
                        title ILIKE %s OR coalesce(selftext, '') ILIKE %s
                        OR coalesce(link_flair_text, '') ILIKE %s
                    )
                    """
                )
                post_params.extend([like, like, like])
                comment_conditions.append(
                    """
                    (
                        body ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM unnest(matched_keywords) AS keyword
                            WHERE keyword ILIKE %s
                        )
                    )
                    """
                )
                comment_params.extend([like, like])
            if subreddit_filter:
                post_conditions.append("lower(subreddit) = lower(%s)")
                post_params.append(subreddit_filter)
                comment_conditions.append("lower(subreddit) = lower(%s)")
                comment_params.append(subreddit_filter)
                query_conditions.append("lower(subreddit) = lower(%s)")
                query_params.append(subreddit_filter)
            if ticker:
                post_conditions.append("%s = ANY(candidate_tickers)")
                post_params.append(ticker)
                comment_conditions.append("%s = ANY(candidate_tickers)")
                comment_params.append(ticker)

            post_where = (
                f"WHERE {' AND '.join(post_conditions)}"
                if post_conditions
                else ""
            )
            comment_where = (
                f"WHERE {' AND '.join(comment_conditions)}"
                if comment_conditions
                else ""
            )
            query_where = (
                f"WHERE {' AND '.join(query_conditions)}" if query_conditions else ""
            )
            posts = (
                fetch_all(
                    conn,
                    f"""
                    SELECT title, subreddit, candidate_tickers, candidate_keywords,
                           score, upvote_ratio, comment_count, created_utc,
                           permalink_url, link_flair_text
                    FROM reddit_posts
                    {post_where}
                    ORDER BY coalesce(created_utc, last_seen_at) DESC
                    LIMIT %s
                    """,
                    (*post_params, PAGE_SIZE),
                )
                if has_posts
                else []
            )
            comments = (
                fetch_all(
                    conn,
                    f"""
                    SELECT body, subreddit, candidate_tickers, candidate_keywords,
                           matched_keywords, score, created_utc, permalink_url,
                           author_name, post_fullname
                    FROM reddit_comments
                    {comment_where}
                    ORDER BY coalesce(created_utc, last_seen_at) DESC
                    LIMIT %s
                    """,
                    (*comment_params, PAGE_SIZE),
                )
                if has_comments
                else []
            )
            queries = (
                fetch_all(
                    conn,
                    f"""
                    SELECT subreddit, listing, time_filter, limit_count,
                           after_token, fetched_at
                    FROM reddit_post_queries
                    {query_where}
                    ORDER BY fetched_at DESC
                    LIMIT 20
                    """,
                    tuple(query_params),
                )
                if has_queries
                else []
            )
    except Exception as exc:
        return render_database_error(exc, active="/reddit")

    metrics = [
        metric_box(
            "Client ID",
            "已配置" if settings.reddit_client_id else "未配置",
            "REDDIT_CLIENT_ID",
        ),
        metric_box(
            "User-Agent",
            "已配置" if settings.reddit_user_agent else "未配置",
            "REDDIT_USER_AGENT",
        ),
        metric_box("帖子总数", summary.get("total"), "reddit_post"),
        metric_box("近 24 小时", summary.get("recent"), "created_utc"),
        metric_box(
            "社区 / 评论",
            f"{fmt(summary.get('subreddits'))} / {fmt(summary.get('comments'))}",
            f"最近 {fmt(summary.get('latest_created_at'))}",
        ),
        metric_box(
            "实时评论命中",
            comment_summary.get("total"),
            f"近 24 小时 {fmt(comment_summary.get('recent'))}",
        ),
    ]
    listing_options = render_select_options(
        sorted(reddit.VALID_LISTINGS),
        reddit.DEFAULT_LISTING,
    )
    time_filter_options = render_select_options(
        sorted(reddit.VALID_TIME_FILTERS),
        reddit.DEFAULT_TIME_FILTER,
    )
    body = f"""
    <section class="toolbar">
      <h1>Reddit</h1>
      <form method="get" class="filters">
        <input name="q" value="{e(q)}" placeholder="关键词 / flair">
        <input name="subreddit" value="{e(subreddit_filter)}" placeholder="subreddit">
        <input name="ticker" value="{e(ticker)}" placeholder="ticker">
        <button type="submit">筛选</button>
      </form>
    </section>
    <section class="metrics">{"".join(metrics)}</section>
    <section class="task-grid">
      <form method="post" action="/actions/reddit-defaults">
        <h2>默认投资社区</h2>
        <p>同步 stocks、investing、wallstreetbets、SecurityAnalysis，用作低权重社区讨论信号。</p>
        <label>listing <select name="listing">{listing_options}</select></label>
        <label>帖子数量 <input name="limit" type="number" min="1" max="100" value="50"></label>
        <button class="primary" type="submit">同步默认社区</button>
      </form>
      <form method="post" action="/actions/reddit-subreddit">
        <h2>单个 subreddit</h2>
        <label>subreddit <input name="subreddit" required placeholder="stocks" value="{e(subreddit_filter)}"></label>
        <label>listing <select name="listing">{listing_options}</select></label>
        <label>time filter <select name="time_filter">{time_filter_options}</select></label>
        <label>帖子数量 <input name="limit" type="number" min="1" max="100" value="50"></label>
        <button class="primary" type="submit">同步社区</button>
      </form>
    </section>
    <section class="split">
      <div>
        <h2>社区概览</h2>
        {render_reddit_breakdown_table(subreddit_rows, empty_message="暂无 Reddit 社区统计。")}
      </div>
      <div>
        <h2>候选 ticker</h2>
        {render_reddit_breakdown_table(ticker_rows, empty_message="暂无 Reddit ticker 统计。")}
      </div>
    </section>
    <section>
      <h2>查询记录</h2>
      {render_reddit_queries_table(queries)}
    </section>
    <section>
      <h2>社区帖子</h2>
      {render_reddit_posts_table(posts)}
    </section>
    <section>
      <h2>实时命中评论</h2>
      {render_reddit_comments_table(comments)}
    </section>
    """
    return layout("Reddit", body, active="/reddit", query=query)


def render_topics(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    topic_filter = form_value(query, "topic")
    ticker_filter = form_value(query, "ticker").upper()
    candidate_status_filter = form_value(query, "candidate_status")

    try:
        with connect_database(database_url) as conn:
            has_topics = table_exists(conn, "market_topics")
            has_topic_candidates = table_exists(conn, "market_topic_candidates")
            has_mentions = table_exists(conn, "topic_mentions")
            has_scores = table_exists(conn, "daily_candidate_scores")
            has_reddit_score_columns = has_scores and column_exists(
                conn,
                "daily_candidate_scores",
                "reddit_post_count",
            )

            summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE is_active) AS active,
                        max(last_refreshed_at) AS last_refreshed_at
                    FROM market_topics
                    """,
                )
                if has_topics
                else {}
            )
            mention_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (
                            WHERE detected_at >= now() - interval '24 hours'
                        ) AS recent,
                        count(DISTINCT ticker) FILTER (WHERE ticker IS NOT NULL) AS tickers,
                        max(detected_at) AS last_detected_at
                    FROM topic_mentions
                    """,
                )
                if has_mentions
                else {}
            )
            candidate_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE run_date = current_date) AS today,
                        max(run_date) AS last_run_date
                    FROM daily_candidate_scores
                    """,
                )
                if has_scores
                else {}
            )
            topic_candidate_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE status = 'pending') AS pending,
                        count(*) FILTER (WHERE status = 'promoted') AS promoted,
                        count(*) FILTER (WHERE status IN ('rejected', 'ignored')) AS closed,
                        count(*) FILTER (
                            WHERE last_seen_at >= now() - interval '24 hours'
                        ) AS recent,
                        max(last_seen_at) AS last_seen_at
                    FROM market_topic_candidates
                    """,
                )
                if has_topic_candidates
                else {}
            )

            if has_topics and has_mentions:
                topic_rows = fetch_all(
                    conn,
                    """
                    SELECT
                        mt.topic_slug,
                        mt.topic_name,
                        mt.gdelt_query,
                        mt.keywords,
                        mt.ticker_hints,
                        mt.priority,
                        mt.is_active,
                        mt.last_refreshed_at,
                        count(tm.id) AS mention_count,
                        count(tm.id) FILTER (
                            WHERE tm.detected_at >= now() - interval '24 hours'
                        ) AS recent_mentions,
                        count(DISTINCT tm.ticker) FILTER (
                            WHERE tm.ticker IS NOT NULL
                        ) AS ticker_count,
                        max(tm.detected_at) AS last_detected_at
                    FROM market_topics mt
                    LEFT JOIN topic_mentions tm
                      ON tm.topic_slug = mt.topic_slug
                    GROUP BY mt.topic_slug, mt.topic_name, mt.gdelt_query,
                             mt.keywords, mt.ticker_hints, mt.priority,
                             mt.is_active, mt.last_refreshed_at
                    ORDER BY mt.is_active DESC, mt.priority, mt.topic_slug
                    LIMIT %s
                    """,
                    (PAGE_SIZE,),
                )
            elif has_topics:
                topic_rows = fetch_all(
                    conn,
                    """
                    SELECT
                        topic_slug,
                        topic_name,
                        gdelt_query,
                        keywords,
                        ticker_hints,
                        priority,
                        is_active,
                        last_refreshed_at,
                        0 AS mention_count,
                        0 AS recent_mentions,
                        0 AS ticker_count,
                        NULL AS last_detected_at
                    FROM market_topics
                    ORDER BY is_active DESC, priority, topic_slug
                    LIMIT %s
                    """,
                    (PAGE_SIZE,),
                )
            else:
                topic_rows = []

            candidate_conditions: list[str] = []
            candidate_params: list[Any] = []
            if candidate_status_filter:
                candidate_conditions.append("status = %s")
                candidate_params.append(candidate_status_filter)
            if topic_filter:
                candidate_conditions.append(
                    """
                    (
                        candidate_slug ILIKE %s
                        OR coalesce(matched_topic_slug, '') = %s
                        OR %s = ANY(keywords)
                    )
                    """
                )
                candidate_params.extend([f"%{topic_filter}%", topic_filter, topic_filter])
            if ticker_filter:
                candidate_conditions.append("%s = ANY(ticker_hints)")
                candidate_params.append(ticker_filter)
            candidate_where = (
                f"WHERE {' AND '.join(candidate_conditions)}"
                if candidate_conditions
                else ""
            )
            topic_candidate_rows = (
                fetch_all(
                    conn,
                    f"""
                    SELECT candidate_slug, topic_name, gdelt_query, keywords,
                           ticker_hints, source_types, article_count, source_count,
                           ticker_count, trend_score, novelty_score, status,
                           matched_topic_slug, evidence, last_seen_at
                    FROM market_topic_candidates
                    {candidate_where}
                    ORDER BY
                        CASE status
                            WHEN 'pending' THEN 0
                            WHEN 'promoted' THEN 1
                            ELSE 2
                        END,
                        trend_score DESC,
                        last_seen_at DESC
                    LIMIT %s
                    """,
                    (*candidate_params, PAGE_SIZE),
                )
                if has_topic_candidates
                else []
            )

            mention_conditions: list[str] = []
            mention_params: list[Any] = []
            if topic_filter:
                mention_conditions.append("topic_slug = %s")
                mention_params.append(topic_filter)
            if ticker_filter:
                mention_conditions.append("upper(coalesce(ticker, '')) = upper(%s)")
                mention_params.append(ticker_filter)
            mention_where = (
                f"WHERE {' AND '.join(mention_conditions)}"
                if mention_conditions
                else ""
            )
            mention_rows = (
                fetch_all(
                    conn,
                    f"""
                    SELECT topic_slug, ticker, source_type, source_title,
                           source_url, published_at, relevance_score, detected_at
                    FROM topic_mentions
                    {mention_where}
                    ORDER BY detected_at DESC
                    LIMIT %s
                    """,
                    (*mention_params, PAGE_SIZE),
                )
                if has_mentions
                else []
            )

            score_conditions: list[str] = []
            score_params: list[Any] = []
            if topic_filter:
                score_conditions.append("%s = ANY(topic_slugs)")
                score_params.append(topic_filter)
            if ticker_filter:
                score_conditions.append("upper(ticker) = upper(%s)")
                score_params.append(ticker_filter)
            score_where = (
                f"WHERE {' AND '.join(score_conditions)}" if score_conditions else ""
            )
            reddit_count_expr = (
                "reddit_post_count"
                if has_reddit_score_columns
                else "0 AS reddit_post_count"
            )
            score_rows = (
                fetch_all(
                    conn,
                    f"""
                    SELECT run_date, rank, ticker, company_name, score,
                           action_bias, primary_topic_slug, topic_slugs,
                           finnhub_article_count, gdelt_article_count,
                           sec_filing_count, {reddit_count_expr}
                    FROM daily_candidate_scores
                    {score_where}
                    ORDER BY run_date DESC, rank NULLS LAST, score DESC
                    LIMIT %s
                    """,
                    (*score_params, PAGE_SIZE),
                )
                if has_scores
                else []
            )
    except Exception as exc:
        return render_database_error(exc, active="/topics")

    metrics = [
        metric_box(
            "主题库",
            summary.get("total"),
            f"启用 {fmt(summary.get('active'))}",
        ),
        metric_box(
            "主题提及",
            mention_summary.get("total"),
            f"近 24 小时 {fmt(mention_summary.get('recent'))}",
        ),
        metric_box(
            "候选主题",
            topic_candidate_summary.get("total"),
            f"待审核 {fmt(topic_candidate_summary.get('pending'))} / 已晋升 {fmt(topic_candidate_summary.get('promoted'))}",
        ),
        metric_box(
            "关联标的",
            mention_summary.get("tickers"),
            f"最近 {fmt(mention_summary.get('last_detected_at'))}",
        ),
        metric_box(
            "候选评分",
            candidate_summary.get("total"),
            f"今日 {fmt(candidate_summary.get('today'))}",
        ),
    ]
    migration_hint = ""
    if not summary and not topic_candidate_summary:
        migration_hint = """
        <section>
          <div class="empty">暂无主题数据。请先执行数据库迁移，并在同步页运行自动热点发现。</div>
        </section>
        """

    body = f"""
    <section class="toolbar">
      <h1>主题</h1>
      <form method="get" class="filters">
        <input name="topic" value="{e(topic_filter)}" placeholder="topic_slug">
        <input name="ticker" value="{e(ticker_filter)}" placeholder="ticker">
        <select name="candidate_status">
          {render_candidate_status_options(candidate_status_filter)}
        </select>
        <button type="submit">筛选</button>
        <a class="button primary" href="/tasks">运行自动发现</a>
      </form>
    </section>
    <section class="metrics">{"".join(metrics)}</section>
    {migration_hint}
    <section>
      <h2>主题库</h2>
      {render_topics_table(topic_rows)}
    </section>
    <section>
      <h2>新闻候选主题</h2>
      {render_topic_candidates_table(topic_candidate_rows)}
    </section>
    <section>
      <h2>最近主题提及</h2>
      {render_topic_mentions_table(mention_rows)}
    </section>
    <section>
      <h2>每日候选评分</h2>
      {render_candidate_scores_table(score_rows)}
    </section>
    """
    return layout("主题", body, active="/topics", query=query)


def render_tasks(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    today = date.today()
    week_ago = today - timedelta(days=finnhub.DEFAULT_COMPANY_NEWS_DAYS)
    discovery_status = render_discovery_scheduler_status(
        DISCOVERY_SCHEDULER.snapshot()
    )
    body = f"""
    <section class="toolbar">
      <h1>同步任务</h1>
    </section>
    {discovery_status}
    <section class="task-grid">
      <form method="post" action="/actions/discovery-daily">
        <h2>自动热点发现</h2>
        <p>按主题库同步 GDELT，拉取 Finnhub 市场新闻，读取 Reddit 社区信号，扫描 SEC filings，并生成每日候选股评分。</p>
        <label>候选数量 <input name="top_n" type="number" min="1" value="{discovery.DEFAULT_TOP_N}"></label>
        <label>回看小时 <input name="lookback_hours" type="number" min="1" value="{discovery.DEFAULT_LOOKBACK_HOURS}"></label>
        <label>GDELT 文章数 <input name="gdelt_max_records" type="number" min="1" max="250" value="{gdelt.DEFAULT_MAX_RECORDS}"></label>
        <label>SEC 扫描标的数 <input name="max_sec_tickers" type="number" min="1" value="{discovery.DEFAULT_MAX_SEC_TICKERS}"></label>
        <label>SEC filing 数量 <input name="sec_filing_limit" type="number" min="1" value="{discovery.DEFAULT_SEC_FILING_LIMIT}"></label>
        <label>定时间隔分钟 <input name="interval_minutes" type="number" min="1" value="60"></label>
        <label><input type="checkbox" name="include_company_facts" value="1"> 同步 company facts</label>
        <label><input type="checkbox" name="skip_finnhub_sync" value="1"> 跳过 Finnhub</label>
        <label><input type="checkbox" name="skip_gdelt_sync" value="1"> 跳过 GDELT</label>
        <label><input type="checkbox" name="skip_reddit_sync" value="1"> 跳过 Reddit</label>
        <label><input type="checkbox" name="skip_sec_sync" value="1"> 跳过 SEC</label>
        <div class="button-row">
          <button class="primary" type="submit" formaction="/actions/discovery-daily">运行一次</button>
          <button type="submit" formaction="/actions/discovery-schedule-start">启动定时</button>
          <button type="submit" formaction="/actions/discovery-schedule-stop">停止定时</button>
        </div>
      </form>
      <form method="post" action="/actions/topic-extract">
        <h2>新闻候选主题</h2>
        <p>从已入库的 Finnhub 和 GDELT 新闻中抽取候选主题，先进入候选表等待审核。</p>
        <label>回看小时 <input name="lookback_hours" type="number" min="1" value="{topic_candidates.DEFAULT_TOPIC_EXTRACTION_LOOKBACK_HOURS}"></label>
        <label>候选数量 <input name="max_candidates" type="number" min="1" value="{topic_candidates.DEFAULT_MAX_CANDIDATES}"></label>
        <label>最少文章数 <input name="min_articles" type="number" min="1" value="{topic_candidates.DEFAULT_MIN_ARTICLES}"></label>
        <label>最低分数 <input name="min_score" type="number" min="0" step="0.1" value="{topic_candidates.DEFAULT_MIN_SCORE}"></label>
        <label><input type="checkbox" name="include_existing_matches" value="1"> 保留已匹配正式主题的候选</label>
        <button class="primary" type="submit">抽取候选主题</button>
      </form>
      <form method="post" action="/actions/sec-registry">
        <h2>SEC 公司映射</h2>
        <p>刷新 SEC ticker / CIK 映射，并回填股票池里的 CIK。</p>
        <button class="primary" type="submit">同步映射</button>
      </form>
      <form method="post" action="/actions/sec-ticker">
        <h2>SEC 单只标的</h2>
        <label>ticker <input name="ticker" required placeholder="AAPL"></label>
        <label>filing 数量 <input name="filing_limit" type="number" min="1" placeholder="20"></label>
        <label>fact 数量 <input name="fact_limit" type="number" min="1" placeholder="500"></label>
        <label><input type="checkbox" name="include_company_facts" value="1"> 同步 company facts</label>
        <button class="primary" type="submit">同步标的</button>
      </form>
      <form method="post" action="/actions/gdelt-query">
        <h2>GDELT 查询</h2>
        <label>query <input name="query" required placeholder="&quot;artificial intelligence&quot; semiconductor"></label>
        <label>timespan <input name="timespan" placeholder="24h" value="24h"></label>
        <label>文章数量 <input name="max_records" type="number" min="1" max="250" value="75"></label>
        <button class="primary" type="submit">同步新闻</button>
      </form>
      <form method="post" action="/actions/finnhub-market">
        <h2>Finnhub 市场新闻</h2>
        <p>同步 market news，用作金融新闻主源。</p>
        <label>category <input name="category" placeholder="general" value="{e(finnhub.DEFAULT_MARKET_CATEGORY)}"></label>
        <label>minId <input name="min_id" type="number" min="1" placeholder="增量 ID，可空"></label>
        <button class="primary" type="submit">同步市场新闻</button>
      </form>
      <form method="post" action="/actions/finnhub-company">
        <h2>Finnhub 个股新闻</h2>
        <label>ticker <input name="ticker" required placeholder="AAPL"></label>
        <label>开始日期 <input name="from_date" type="date" value="{week_ago.isoformat()}"></label>
        <label>结束日期 <input name="to_date" type="date" value="{today.isoformat()}"></label>
        <button class="primary" type="submit">同步个股新闻</button>
      </form>
      <form method="post" action="/actions/reddit-defaults">
        <h2>Reddit 默认社区</h2>
        <p>同步投资社区帖子，作为候选股评分中的低权重讨论热度信号。</p>
        <label>listing <select name="listing">{render_select_options(sorted(reddit.VALID_LISTINGS), reddit.DEFAULT_LISTING)}</select></label>
        <label>帖子数量 <input name="limit" type="number" min="1" max="100" value="50"></label>
        <button class="primary" type="submit">同步默认社区</button>
      </form>
      <form method="post" action="/actions/reddit-subreddit">
        <h2>Reddit 单个社区</h2>
        <label>subreddit <input name="subreddit" required placeholder="stocks"></label>
        <label>listing <select name="listing">{render_select_options(sorted(reddit.VALID_LISTINGS), reddit.DEFAULT_LISTING)}</select></label>
        <label>time filter <select name="time_filter">{render_select_options(sorted(reddit.VALID_TIME_FILTERS), reddit.DEFAULT_TIME_FILTER)}</select></label>
        <label>帖子数量 <input name="limit" type="number" min="1" max="100" value="50"></label>
        <button class="primary" type="submit">同步社区</button>
      </form>
    </section>
    """
    return layout("同步任务", body, active="/tasks", query=query)


def render_discovery_scheduler_status(snapshot: dict[str, Any]) -> str:
    active = bool(snapshot.get("active"))
    badge = (
        '<span class="badge ok">定时运行中</span>'
        if active
        else '<span class="badge muted">定时未运行</span>'
    )
    running_once = (
        '<span class="badge watch">本轮执行中</span>'
        if snapshot.get("is_running_once")
        else ""
    )
    error = snapshot.get("last_error")
    error_html = f'<p class="error-text">{e(error)}</p>' if error else ""
    config = snapshot.get("config")
    config_hint = ""
    if config:
        config_hint = (
            f"top_n={config.top_n}，"
            f"回看={config.lookback_hours}h，"
            f"SEC={config.max_sec_tickers} 个标的"
        )

    return f"""
    <section class="status-panel" hx-get="/partials/discovery-status" hx-trigger="every 10s" hx-target="this" hx-select=".status-panel" hx-swap="outerHTML">
      <div class="toolbar">
        <h2>自动发现定时任务</h2>
        <div>{badge} {running_once}</div>
      </div>
      <div class="metrics">
        {metric_box("运行次数", snapshot.get("run_count"), "本次面板进程内")}
        {metric_box("间隔分钟", snapshot.get("interval_minutes"), config_hint or "未设置")}
        {metric_box("上次开始", snapshot.get("last_run_started_at"), "自动发现")}
        {metric_box("上次完成", snapshot.get("last_run_finished_at"), "自动发现")}
      </div>
      <p class="subtle">{e(snapshot.get("last_message"))}</p>
      {error_html}
    </section>
    """


def parse_discovery_run_config(
    form: Mapping[str, list[str]],
) -> DiscoveryRunConfig:
    return DiscoveryRunConfig(
        top_n=parse_positive_int(form_value(form, "top_n")) or discovery.DEFAULT_TOP_N,
        lookback_hours=(
            parse_positive_int(form_value(form, "lookback_hours"))
            or discovery.DEFAULT_LOOKBACK_HOURS
        ),
        gdelt_max_records=(
            parse_positive_int(form_value(form, "gdelt_max_records"))
            or gdelt.DEFAULT_MAX_RECORDS
        ),
        max_sec_tickers=(
            parse_positive_int(form_value(form, "max_sec_tickers"))
            or discovery.DEFAULT_MAX_SEC_TICKERS
        ),
        sec_filing_limit=(
            parse_positive_int(form_value(form, "sec_filing_limit"))
            or discovery.DEFAULT_SEC_FILING_LIMIT
        ),
        include_company_facts=form_bool(form, "include_company_facts"),
        skip_finnhub_sync=form_bool(form, "skip_finnhub_sync"),
        skip_gdelt_sync=form_bool(form, "skip_gdelt_sync"),
        skip_reddit_sync=form_bool(form, "skip_reddit_sync"),
        skip_sec_sync=form_bool(form, "skip_sec_sync"),
    )


def run_discovery_daily(
    database_url: str | None,
    config: DiscoveryRunConfig,
) -> discovery.DailyDiscoveryResult:
    return discovery.run_daily_discovery(
        database_url=database_url,
        top_n=config.top_n,
        lookback_hours=config.lookback_hours,
        gdelt_max_records=config.gdelt_max_records,
        max_sec_tickers=config.max_sec_tickers,
        sec_filing_limit=config.sec_filing_limit,
        include_company_facts=config.include_company_facts,
        skip_finnhub_sync=config.skip_finnhub_sync,
        skip_gdelt_sync=config.skip_gdelt_sync,
        skip_reddit_sync=config.skip_reddit_sync,
        skip_sec_sync=config.skip_sec_sync,
    )


def upsert_stock(database_url: str | None, form: Mapping[str, list[str]]) -> str:
    ticker = form_value(form, "ticker").upper()
    company_name = form_value(form, "company_name")
    if not ticker:
        raise AdminPanelError("ticker 不能为空。")
    if not company_name:
        raise AdminPanelError("公司名称不能为空。")

    with connect_database(database_url) as conn:
        conn.execute(
            """
            INSERT INTO stock_universe (
                ticker,
                company_name,
                exchange,
                sector,
                industry,
                business_description,
                is_active,
                is_manual_watchlist,
                data_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'manual')
            ON CONFLICT (ticker)
            DO UPDATE SET
                company_name = EXCLUDED.company_name,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                business_description = EXCLUDED.business_description,
                is_active = EXCLUDED.is_active,
                is_manual_watchlist = EXCLUDED.is_manual_watchlist,
                data_source = 'manual',
                updated_at = now()
            """,
            (
                ticker,
                company_name,
                clean_blank(form_value(form, "exchange")),
                clean_blank(form_value(form, "sector")),
                clean_blank(form_value(form, "industry")),
                clean_blank(form_value(form, "business_description")),
                form_bool(form, "is_active"),
                form_bool(form, "is_manual_watchlist"),
            ),
        )

    return f"{ticker} 已保存到股票池。"


def render_stocks_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无股票池记录。")

    body = []
    for row in rows:
        badges = []
        if row.get("is_active"):
            badges.append('<span class="badge ok">启用</span>')
        else:
            badges.append('<span class="badge muted">停用</span>')
        if row.get("is_manual_watchlist"):
            badges.append('<span class="badge watch">观察</span>')
        body.append(
            f"""
            <tr>
              <td><strong>{e(row.get("ticker"))}</strong></td>
              <td>{e(row.get("company_name"))}<div class="subtle">{e(row.get("notes"))}</div></td>
              <td>{e(row.get("exchange"))}</td>
              <td>{e(row.get("sector"))}</td>
              <td>{e(row.get("industry"))}</td>
              <td>{e(row.get("sec_cik"))}</td>
              <td>{fmt(row.get("last_price"))}</td>
              <td>{" ".join(badges)}</td>
              <td>{fmt(row.get("last_refreshed_at"))}</td>
            </tr>
            """
        )

    return table(
        """
        <tr>
          <th>ticker</th><th>公司</th><th>交易所</th><th>板块</th>
          <th>行业</th><th>CIK</th><th>价格</th><th>状态</th><th>刷新时间</th>
        </tr>
        """,
        "".join(body),
    )


def render_filings_table(rows: list[dict[str, Any]], *, compact: bool = False) -> str:
    if not rows:
        return empty_state("暂无 SEC 公告。")

    body = []
    for row in rows:
        url = row.get("primary_document_url") or row.get("filing_detail_url")
        title = e(row.get("company_name"))
        if url:
            title = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{title}</a>'
        amendment = '<span class="badge muted">修订</span>' if row.get("is_amendment") else ""
        extra = "" if compact else f"<td>{e(row.get('items'))}</td><td>{fmt(row.get('fetched_at'))}</td>"
        body.append(
            f"""
            <tr>
              <td><strong>{e(row.get("ticker"))}</strong></td>
              <td>{title}</td>
              <td>{e(row.get("form_type"))} {amendment}</td>
              <td>{fmt(row.get("filing_date"))}</td>
              <td>{fmt(row.get("report_date"))}</td>
              {extra}
            </tr>
            """
        )

    if compact:
        head = "<tr><th>ticker</th><th>公司</th><th>表单</th><th>提交日</th><th>报告期</th></tr>"
    else:
        head = (
            "<tr><th>ticker</th><th>公司</th><th>表单</th><th>提交日</th>"
            "<th>报告期</th><th>事项</th><th>抓取时间</th></tr>"
        )
    return table(head, "".join(body))


def render_articles_table(rows: list[dict[str, Any]], *, compact: bool = False) -> str:
    if not rows:
        return empty_state("暂无 GDELT 文章。")

    body = []
    for row in rows:
        title = e(row.get("title"))
        url = row.get("article_url")
        if url:
            title = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{title}</a>'
        extra = "" if compact else f"<td>{e(row.get('query_text'))}</td><td>{fmt(row.get('tone'))}</td>"
        body.append(
            f"""
            <tr>
              <td>{title}<div class="subtle">{e(row.get("domain"))}</div></td>
              <td>{e(row.get("language"))}</td>
              <td>{e(row.get("source_country"))}</td>
              <td>{fmt(row.get("seen_at"))}</td>
              {extra}
            </tr>
            """
        )

    if compact:
        head = "<tr><th>标题</th><th>语言</th><th>地区</th><th>时间</th></tr>"
    else:
        head = "<tr><th>标题</th><th>语言</th><th>地区</th><th>时间</th><th>query</th><th>tone</th></tr>"
    return table(head, "".join(body))


def render_finnhub_articles_table(
    rows: list[dict[str, Any]],
    *,
    compact: bool = False,
) -> str:
    if not rows:
        return empty_state("暂无 Finnhub 新闻。")

    body = []
    for row in rows:
        title = e(row.get("headline"))
        url = row.get("article_url")
        if url:
            title = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{title}</a>'
        related = format_ticker_list(row.get("related_tickers"))
        source = row.get("source_name") or "-"
        extra = (
            ""
            if compact
            else (
                f"<td>{e(row.get('category'))}</td>"
                f"<td>{related}</td>"
                f"<td>{e(row.get('endpoint'))}</td>"
            )
        )
        if compact:
            body.append(
                f"""
                <tr>
                  <td>{title}<div class="subtle">{e(row.get("category"))}</div></td>
                  <td>{e(source)}</td>
                  <td>{related}</td>
                  <td>{fmt(row.get("published_at"))}</td>
                </tr>
                """
            )
        else:
            body.append(
                f"""
                <tr>
                  <td>{title}</td>
                  <td>{e(source)}</td>
                  {extra}
                  <td>{fmt(row.get("published_at"))}</td>
                </tr>
                """
            )

    if compact:
        head = "<tr><th>标题</th><th>来源</th><th>相关</th><th>时间</th></tr>"
    else:
        head = (
            "<tr><th>标题</th><th>来源</th><th>分类</th><th>相关</th>"
            "<th>接口</th><th>时间</th></tr>"
        )
    return table(head, "".join(body))


def render_reddit_posts_table(
    rows: list[dict[str, Any]],
    *,
    compact: bool = False,
) -> str:
    if not rows:
        return empty_state("暂无 Reddit 帖子。")

    body = []
    for row in rows:
        title = e(clip_text(str(row.get("title") or ""), 120))
        url = row.get("permalink_url")
        if url:
            title = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{title}</a>'
        tickers = format_ticker_list(row.get("candidate_tickers"))
        metrics = (
            f"score {fmt(row.get('score'))}"
            f" / 评论 {fmt(row.get('comment_count'))}"
        )
        if compact:
            body.append(
                f"""
                <tr>
                  <td>{title}<div class="subtle">r/{e(row.get("subreddit"))}</div></td>
                  <td>{tickers}</td>
                  <td>{metrics}</td>
                  <td>{fmt(row.get("created_utc"))}</td>
                </tr>
                """
            )
        else:
            body.append(
                f"""
                <tr>
                  <td>{title}<div class="subtle">{e(row.get("link_flair_text"))}</div></td>
                  <td>r/{e(row.get("subreddit"))}</td>
                  <td>{tickers}</td>
                  <td>{format_list(row.get("candidate_keywords"), limit=6)}</td>
                  <td>{fmt(row.get("score"))}</td>
                  <td>{fmt(row.get("upvote_ratio"))}</td>
                  <td>{fmt(row.get("comment_count"))}</td>
                  <td>{fmt(row.get("created_utc"))}</td>
                </tr>
                """
            )

    if compact:
        head = "<tr><th>标题</th><th>ticker</th><th>热度</th><th>时间</th></tr>"
    else:
        head = (
            "<tr><th>标题</th><th>社区</th><th>ticker</th><th>关键词</th>"
            "<th>score</th><th>赞同比</th><th>评论</th><th>时间</th></tr>"
        )
    return table(head, "".join(body))


def render_reddit_comments_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无 Devvit 实时命中评论。")

    body = []
    for row in rows:
        comment = e(clip_text(str(row.get("body") or ""), 160))
        url = row.get("permalink_url")
        if url:
            comment = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{comment}</a>'
        body.append(
            f"""
            <tr>
              <td>
                {comment}
                <div class="subtle">
                  {e(row.get("author_name"))} / {e(row.get("post_fullname"))}
                </div>
              </td>
              <td>r/{e(row.get("subreddit"))}</td>
              <td>{format_list(row.get("matched_keywords"), limit=8)}</td>
              <td>{format_ticker_list(row.get("candidate_tickers"))}</td>
              <td>{format_list(row.get("candidate_keywords"), limit=6)}</td>
              <td>{fmt(row.get("score"))}</td>
              <td>{fmt(row.get("created_utc"))}</td>
            </tr>
            """
        )

    head = (
        "<tr><th>评论</th><th>社区</th><th>命中词</th><th>ticker</th>"
        "<th>关键词</th><th>score</th><th>时间</th></tr>"
    )
    return table(head, "".join(body))


def render_topics_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无主题库记录。")

    body = []
    for row in rows:
        status = (
            '<span class="badge ok">启用</span>'
            if row.get("is_active")
            else '<span class="badge muted">停用</span>'
        )
        body.append(
            f"""
            <tr>
              <td>
                <strong>{e(row.get("topic_slug"))}</strong>
                <div class="subtle">{e(row.get("topic_name"))}</div>
              </td>
              <td>{status}</td>
              <td>{fmt(row.get("priority"))}</td>
              <td>{e(clip_text(row.get("gdelt_query") or "", 140))}</td>
              <td>{format_list(row.get("keywords"), limit=8)}</td>
              <td>{format_list(row.get("ticker_hints"), limit=8)}</td>
              <td>{fmt(row.get("recent_mentions"))}</td>
              <td>{fmt(row.get("ticker_count"))}</td>
              <td>{fmt(row.get("last_detected_at") or row.get("last_refreshed_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>主题</th><th>状态</th><th>优先级</th><th>GDELT query</th>"
            "<th>关键词</th><th>种子标的</th><th>24h 提及</th>"
            "<th>标的数</th><th>最近时间</th></tr>"
        ),
        "".join(body),
    )


def render_candidate_status_options(selected: str) -> str:
    options = [
        ("", "全部候选状态"),
        ("pending", "待审核"),
        ("promoted", "已晋升"),
        ("rejected", "已拒绝"),
        ("ignored", "已忽略"),
    ]
    return "".join(
        (
            f'<option value="{e(value)}"'
            f'{" selected" if value == selected else ""}>{e(label)}</option>'
        )
        for value, label in options
    )


def render_topic_candidates_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无新闻候选主题。可以在同步页运行候选主题抽取。")

    body = []
    for row in rows:
        status = render_topic_candidate_status(row.get("status"))
        coverage = (
            f"文章 {fmt(row.get('article_count'))}"
            f"<br>来源 {fmt(row.get('source_count'))}"
            f"<br>标的 {fmt(row.get('ticker_count'))}"
        )
        score = (
            f"<strong>{fmt(row.get('trend_score'))}</strong>"
            f"<div class=\"subtle\">新颖 {fmt(row.get('novelty_score'))}</div>"
        )
        matched = row.get("matched_topic_slug") or "-"
        body.append(
            f"""
            <tr>
              <td>
                <strong>{e(row.get("candidate_slug"))}</strong>
                <div class="subtle">{e(row.get("topic_name"))}</div>
              </td>
              <td>{status}</td>
              <td>{score}</td>
              <td>{coverage}</td>
              <td>{e(clip_text(row.get("gdelt_query") or "", 120))}</td>
              <td>{format_list(row.get("keywords"), limit=6)}</td>
              <td>{format_list(row.get("ticker_hints"), limit=6)}</td>
              <td>{format_list(row.get("source_types"), limit=4)}</td>
              <td>{e(matched)}</td>
              <td>{format_topic_candidate_evidence(row.get("evidence"))}</td>
              <td>{fmt(row.get("last_seen_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>候选主题</th><th>状态</th><th>分数</th><th>覆盖</th>"
            "<th>GDELT query</th><th>关键词</th><th>相关标的</th>"
            "<th>来源</th><th>匹配正式主题</th><th>证据</th><th>最近发现</th></tr>"
        ),
        "".join(body),
    )


def render_topic_candidate_status(status: Any) -> str:
    text = str(status or "pending")
    if text == "pending":
        return '<span class="badge watch">待审核</span>'
    if text == "promoted":
        return '<span class="badge ok">已晋升</span>'
    if text == "rejected":
        return '<span class="badge muted">已拒绝</span>'
    if text == "ignored":
        return '<span class="badge muted">已忽略</span>'
    return f'<span class="badge muted">{e(text)}</span>'


def format_topic_candidate_evidence(value: Any) -> str:
    if not isinstance(value, list | tuple) or not value:
        return "-"

    items = []
    for item in value[:2]:
        if not isinstance(item, dict):
            continue
        title = clip_text(str(item.get("title") or item.get("source_uid") or ""), 86)
        source_name = item.get("source_name") or item.get("source_type") or "-"
        url = item.get("url")
        title_html = e(title)
        if url:
            title_html = f'<a href="{e(url)}" target="_blank" rel="noreferrer">{title_html}</a>'
        items.append(f'{title_html}<div class="subtle">{e(source_name)}</div>')

    return "<br>".join(items) if items else "-"


def render_topic_mentions_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无主题提及。")

    body = []
    for row in rows:
        title = e(row.get("source_title"))
        if row.get("source_url"):
            title = f'<a href="{e(row.get("source_url"))}" target="_blank" rel="noreferrer">{title}</a>'
        body.append(
            f"""
            <tr>
              <td><strong>{e(row.get("topic_slug"))}</strong></td>
              <td>{e(row.get("ticker"))}</td>
              <td>{e(row.get("source_type"))}</td>
              <td>{title}</td>
              <td>{fmt(row.get("relevance_score"))}</td>
              <td>{fmt(row.get("published_at"))}</td>
              <td>{fmt(row.get("detected_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>主题</th><th>ticker</th><th>来源</th><th>标题</th>"
            "<th>相关性</th><th>发布时间</th><th>检测时间</th></tr>"
        ),
        "".join(body),
    )


def render_candidate_scores_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无每日候选评分。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{fmt(row.get("run_date"))}</td>
              <td>{fmt(row.get("rank"))}</td>
              <td><strong>{e(row.get("ticker"))}</strong><div class="subtle">{e(row.get("company_name"))}</div></td>
              <td>{fmt(row.get("score"))}</td>
              <td>{e(row.get("action_bias"))}</td>
              <td>{e(row.get("primary_topic_slug"))}</td>
              <td>{format_list(row.get("topic_slugs"), limit=5)}</td>
              <td>{fmt(row.get("finnhub_article_count"))}</td>
              <td>{fmt(row.get("gdelt_article_count"))}</td>
              <td>{fmt(row.get("reddit_post_count"))}</td>
              <td>{fmt(row.get("sec_filing_count"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>日期</th><th>排名</th><th>ticker</th><th>评分</th>"
            "<th>动作</th><th>主主题</th><th>主题</th>"
            "<th>Finnhub</th><th>GDELT</th><th>Reddit</th><th>SEC</th></tr>"
        ),
        "".join(body),
    )


def format_ticker_list(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, str):
        tickers = [item.strip() for item in value.split(",")]
    else:
        tickers = [str(item).strip() for item in value]
    tickers = [ticker for ticker in tickers if ticker]
    if not tickers:
        return "-"
    return e(", ".join(tickers[:8]))


def format_list(value: Any, *, limit: int = 8) -> str:
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    elif isinstance(value, list | tuple):
        items = [str(item).strip() for item in value]
    else:
        items = []
    items = [item for item in items if item]
    if not items:
        return "-"
    clipped = items[:limit]
    suffix = f" +{len(items) - limit}" if len(items) > limit else ""
    return e(", ".join(clipped) + suffix)


def render_finnhub_breakdown_table(
    rows: list[dict[str, Any]],
    *,
    empty_message: str,
) -> str:
    if not rows:
        return empty_state(empty_message)

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{e(row.get("label"))}</td>
              <td>{fmt(row.get("total"))}</td>
              <td>{fmt(row.get("latest_at"))}</td>
            </tr>
            """
        )
    return table(
        "<tr><th>名称</th><th>数量</th><th>最近发布时间</th></tr>",
        "".join(body),
    )


def render_reddit_breakdown_table(
    rows: list[dict[str, Any]],
    *,
    empty_message: str,
) -> str:
    if not rows:
        return empty_state(empty_message)

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{e(row.get("label"))}</td>
              <td>{fmt(row.get("total"))}</td>
              <td>{fmt(row.get("latest_at"))}</td>
            </tr>
            """
        )
    return table(
        "<tr><th>名称</th><th>数量</th><th>最近帖子</th></tr>",
        "".join(body),
    )


def render_gdelt_queries_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无 GDELT 查询记录。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{e(row.get("query_text"))}</td>
              <td>{e(row.get("mode"))}</td>
              <td>{e(row.get("timespan"))}</td>
              <td>{e(row.get("sort"))}</td>
              <td>{fmt(row.get("max_records"))}</td>
              <td>{fmt(row.get("fetched_at"))}</td>
            </tr>
            """
        )
    return table(
        "<tr><th>query</th><th>mode</th><th>timespan</th><th>sort</th><th>数量</th><th>抓取时间</th></tr>",
        "".join(body),
    )


def render_finnhub_queries_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无 Finnhub 查询记录。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{e(row.get("endpoint"))}</td>
              <td>{e(row.get("category"))}</td>
              <td>{e(row.get("ticker"))}</td>
              <td>{fmt(row.get("from_date"))}</td>
              <td>{fmt(row.get("to_date"))}</td>
              <td>{fmt(row.get("min_id"))}</td>
              <td>{fmt(row.get("fetched_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>endpoint</th><th>category</th><th>ticker</th>"
            "<th>开始</th><th>结束</th><th>minId</th><th>抓取时间</th></tr>"
        ),
        "".join(body),
    )


def render_reddit_queries_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无 Reddit 查询记录。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>r/{e(row.get("subreddit"))}</td>
              <td>{e(row.get("listing"))}</td>
              <td>{e(row.get("time_filter"))}</td>
              <td>{fmt(row.get("limit_count"))}</td>
              <td>{e(row.get("after_token"))}</td>
              <td>{fmt(row.get("fetched_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>subreddit</th><th>listing</th><th>时间窗口</th>"
            "<th>数量</th><th>after</th><th>抓取时间</th></tr>"
        ),
        "".join(body),
    )


def render_migrations_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无迁移记录。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{e(row.get("version"))}</td>
              <td>{e(row.get("name"))}</td>
              <td>{fmt(row.get("applied_at"))}</td>
              <td>{fmt(row.get("execution_time_ms"))} ms</td>
            </tr>
            """
        )
    return table("<tr><th>版本</th><th>名称</th><th>执行时间</th><th>耗时</th></tr>", "".join(body))


def table(head: str, body: str) -> str:
    return (
        '<div class="table-panel"><div class="table-wrap">'
        f"<table><thead>{head}</thead><tbody>{body}</tbody></table>"
        "</div></div>"
    )


def metric_box(label: str, value: Any, hint: str) -> str:
    return f"""
    <article class="metric">
      <div class="metric-label">{e(label)}</div>
      <div class="metric-value">{fmt(value)}</div>
      <div class="metric-hint">{e(hint)}</div>
    </article>
    """


def empty_state(message: str) -> str:
    return f'<div class="empty" role="status">{e(message)}</div>'


def render_database_error(exc: Exception, *, active: str) -> str:
    body = f"""
    <section class="toolbar">
      <h1>数据库不可用</h1>
    </section>
    <section>
      <p class="error-text">{e(exc)}</p>
      <p class="subtle">确认 .env 中 DATABASE_URL 正确，并且 PostgreSQL 与迁移已经准备好。</p>
    </section>
    """
    return layout("数据库不可用", body, active=active, query={})


def render_not_found() -> str:
    body = """
    <section class="toolbar">
      <h1>页面不存在</h1>
      <a class="button primary" href="/">回到控制台</a>
    </section>
    """
    return layout("页面不存在", body, active="", query={})


def layout(
    title: str,
    body: str,
    *,
    active: str,
    query: Mapping[str, list[str]],
) -> str:
    notice = form_value(query, "notice")
    notice_type = form_value(query, "notice_type") or "ok"
    notice_html = (
        f"""
        <div class="notice {e(notice_type)}" role="status" x-data="{{show: true}}" x-show="show">
          <span>{e(notice)}</span>
          <button class="notice-close" type="button" aria-label="关闭提示" @click="show = false">×</button>
        </div>
        """
        if notice
        else ""
    )
    nav_items = [
        ("/", "控制台", "总览"),
        ("/stocks", "股票池", "标的维护"),
        ("/sec", "SEC", "公告与事实"),
        ("/gdelt", "GDELT", "全球新闻"),
        ("/finnhub", "Finnhub", "金融新闻"),
        ("/reddit", "Reddit", "社区情绪"),
        ("/topics", "主题", "热点与评分"),
        ("/tasks", "同步", "数据任务"),
    ]
    nav_parts = []
    for href, label, hint in nav_items:
        active_class = " active" if href == active else ""
        aria_current = ' aria-current="page"' if href == active else ""
        nav_parts.append(
            f"""
            <a class="nav-link{active_class}" href="{href}"{aria_current} @click="navOpen = false">
              <span class="nav-mark"></span>
              <span class="nav-text">
                <strong>{e(label)}</strong>
                <small>{e(hint)}</small>
              </span>
            </a>
            """
        )
    nav = "".join(nav_parts)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{e(title)} · USStock</title>
  <link rel="stylesheet" href="{PICO_CSS_URL}">
  <script src="{HTMX_JS_URL}" defer></script>
  <script src="{ALPINE_JS_URL}" defer></script>
  <style>
    :root {{
      color-scheme: light;
      --pico-font-family-sans-serif: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      --pico-border-radius: 0.5rem;
      --pico-form-element-spacing-vertical: 0.55rem;
      --pico-form-element-spacing-horizontal: 0.7rem;
      --bg: #f6f7f8;
      --surface: #ffffff;
      --surface-soft: #f8faf9;
      --ink: #202421;
      --muted: #68716d;
      --line: #dfe4e1;
      --line-strong: #c8d0cb;
      --nav: #171b19;
      --nav-soft: #212722;
      --primary: #13795b;
      --primary-hover: #0f6049;
      --accent: #a15c10;
      --ok: #237847;
      --watch: #996600;
      --error: #b3261e;
      --shadow: 0 18px 50px rgba(31, 37, 35, 0.08);
    }}
    * {{
      box-sizing: border-box;
    }}
    [x-cloak] {{
      display: none !important;
    }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--pico-font-family-sans-serif);
      font-size: 14px;
      letter-spacing: 0;
    }}
    body, input, textarea, select, button {{
      letter-spacing: 0;
    }}
    a {{
      color: var(--primary);
      text-decoration-thickness: 1px;
      text-underline-offset: 3px;
    }}
    .app-shell {{
      min-height: 100vh;
      display: grid;
      grid-template-columns: 248px minmax(0, 1fr);
    }}
    .sidebar {{
      position: sticky;
      top: 0;
      height: 100vh;
      display: flex;
      flex-direction: column;
      gap: 18px;
      padding: 22px 16px;
      background: var(--nav);
      color: #eef4f1;
    }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 11px;
      min-height: 44px;
      padding: 0 8px;
    }}
    .brand-mark {{
      display: inline-grid;
      place-items: center;
      width: 36px;
      height: 36px;
      border-radius: 8px;
      background: #dfeee8;
      color: var(--primary);
      font-weight: 800;
    }}
    .brand-title {{
      display: grid;
      line-height: 1.2;
    }}
    .brand-title strong {{
      color: #ffffff;
      font-size: 16px;
    }}
    .brand-title span {{
      color: #a9b7b1;
      font-size: 12px;
    }}
    .nav-list {{
      display: grid;
      gap: 6px;
      margin-top: 4px;
    }}
    .nav-link {{
      display: grid;
      grid-template-columns: 8px minmax(0, 1fr);
      gap: 10px;
      align-items: center;
      min-height: 54px;
      padding: 8px 10px;
      border-radius: 8px;
      color: #cbd7d2;
      text-decoration: none;
      transition: background 140ms ease, color 140ms ease;
    }}
    .nav-link:hover, .nav-link.active {{
      background: var(--nav-soft);
      color: #ffffff;
    }}
    .nav-mark {{
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: transparent;
    }}
    .nav-link.active .nav-mark {{
      background: #5bd2a4;
    }}
    .nav-text {{
      display: grid;
      min-width: 0;
      line-height: 1.2;
    }}
    .nav-text strong {{
      overflow: hidden;
      color: inherit;
      font-size: 14px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .nav-text small {{
      margin-top: 4px;
      overflow: hidden;
      color: #98a8a1;
      font-size: 12px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .sidebar-note {{
      margin-top: auto;
      padding: 12px;
      border: 1px solid rgba(255, 255, 255, 0.09);
      border-radius: 8px;
      color: #b7c5bf;
      background: rgba(255, 255, 255, 0.04);
      font-size: 12px;
      line-height: 1.55;
    }}
    .content-shell {{
      min-width: 0;
    }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 4;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 64px;
      padding: 0 28px;
      border-bottom: 1px solid var(--line);
      background: rgba(246, 247, 248, 0.92);
      backdrop-filter: blur(12px);
    }}
    .topbar-title {{
      display: grid;
      gap: 2px;
      min-width: 0;
    }}
    .topbar-title span {{
      color: var(--muted);
      font-size: 12px;
    }}
    .topbar-title strong {{
      overflow: hidden;
      color: var(--ink);
      font-size: 16px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .topbar-actions {{
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    button.nav-toggle {{
      display: none;
      width: 40px;
      min-width: 40px;
      height: 40px;
      padding: 0;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
    }}
    .nav-toggle-lines {{
      display: grid;
      gap: 4px;
      width: 16px;
      margin: auto;
    }}
    .nav-toggle-lines span {{
      display: block;
      height: 2px;
      border-radius: 999px;
      background: currentColor;
    }}
    main.content {{
      width: min(1260px, calc(100vw - 312px));
      margin: 0 auto;
      padding: 26px 28px 64px;
    }}
    section {{
      margin-top: 24px;
      padding: 0;
      border: 0;
      background: transparent;
    }}
    section:first-child {{
      margin-top: 0;
    }}
    h1, h2 {{
      margin: 0;
      color: var(--ink);
      line-height: 1.25;
    }}
    h1 {{
      font-size: 24px;
    }}
    h2 {{
      margin-bottom: 12px;
      font-size: 16px;
    }}
    p {{
      margin: 0 0 14px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .toolbar, section.toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }}
    section.toolbar {{
      min-height: 48px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--line);
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 12px;
    }}
    .metric {{
      min-height: 112px;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 0 rgba(31, 37, 35, 0.02);
    }}
    .metric-label, .metric-hint, .subtle {{
      color: var(--muted);
    }}
    .metric-label {{
      font-size: 12px;
      font-weight: 700;
    }}
    .metric-value {{
      margin: 8px 0 6px;
      color: var(--ink);
      font-size: 26px;
      font-weight: 760;
      line-height: 1.05;
      overflow-wrap: anywhere;
    }}
    .metric-hint {{
      font-size: 12px;
      line-height: 1.4;
    }}
    .split {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 18px;
    }}
    .split > div {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      min-width: 0;
    }}
    .task-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(270px, 1fr));
      gap: 18px;
      align-items: flex-start;
    }}
    .task-grid form {{
      display: grid;
      gap: 12px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 0 rgba(31, 37, 35, 0.02);
    }}
    .button-row {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .filters {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      margin: 0;
    }}
    .grid-form {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 12px;
      align-items: end;
      margin: 0;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }}
    .span-2 {{
      grid-column: 1 / -1;
    }}
    label {{
      display: grid;
      gap: 6px;
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
    }}
    input:not([type="checkbox"]), textarea {{
      width: 100%;
      min-height: 38px;
      padding: 8px 10px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      background: #ffffff;
      color: var(--ink);
      font: inherit;
      font-size: 14px;
    }}
    textarea {{
      resize: vertical;
    }}
    input:not([type="checkbox"]) {{
      height: 42px;
    }}
    label:has(input[type="checkbox"]) {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 38px;
      color: var(--ink);
      font-weight: 500;
    }}
    input[type="checkbox"] {{
      width: 16px;
      min-height: 16px;
      margin: 0;
    }}
    button, [type="submit"], [type="button"], .button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 38px;
      padding: 0 13px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      background: #ffffff;
      color: var(--ink);
      font: inherit;
      font-size: 14px;
      font-weight: 650;
      line-height: 1.2;
      text-decoration: none;
      cursor: pointer;
      white-space: nowrap;
      transition: background 140ms ease, border-color 140ms ease, color 140ms ease;
    }}
    button:hover, [type="submit"]:hover, [type="button"]:hover, .button:hover {{
      border-color: #aeb8b2;
      background: #f8faf9;
      text-decoration: none;
    }}
    button.primary, .button.primary {{
      border-color: var(--primary);
      background: var(--primary);
      color: #fff;
    }}
    button.primary:hover, .button.primary:hover {{
      border-color: var(--primary-hover);
      background: var(--primary-hover);
    }}
    .button.ghost {{
      background: transparent;
    }}
    .table-panel {{
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
      margin: 0;
      font-size: 13px;
    }}
    th, td {{
      padding: 10px 11px;
      border-bottom: 1px solid #edf0ed;
      text-align: left;
      vertical-align: top;
    }}
    tbody tr:last-child td {{
      border-bottom: 0;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      background: var(--surface-soft);
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      background: #edf2ef;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .badge.ok {{
      background: #e4f4ea;
      color: var(--ok);
    }}
    .badge.watch {{
      background: #fff2cc;
      color: var(--watch);
    }}
    .badge.muted {{
      background: #edf2ef;
      color: var(--muted);
    }}
    .empty {{
      padding: 16px;
      border: 1px dashed var(--line-strong);
      border-radius: 8px;
      color: var(--muted);
      background: var(--surface-soft);
    }}
    .notice {{
      position: relative;
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 14px;
      padding: 12px 14px;
      border-radius: 8px;
      border: 1px solid var(--line-strong);
      background: var(--surface);
      box-shadow: var(--shadow);
    }}
    .notice.ok {{
      border-color: #b8dfc5;
      color: var(--ok);
      background: #f1faf4;
    }}
    .notice.error {{
      border-color: #edb8b4;
      color: var(--error);
      background: #fff5f4;
    }}
    .notice-close {{
      width: 28px;
      min-width: 28px;
      height: 28px;
      min-height: 28px;
      padding: 0;
      border: 0;
      background: transparent;
      color: currentColor;
      font-size: 18px;
      line-height: 1;
    }}
    .status-panel {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }}
    .error-text {{
      color: var(--error);
    }}
    .loading-bar {{
      position: fixed;
      top: 0;
      left: 0;
      z-index: 30;
      width: 100%;
      height: 3px;
      opacity: 0;
      background: linear-gradient(90deg, var(--primary), #d99a2b, var(--primary));
      background-size: 220% 100%;
      transition: opacity 120ms ease;
      animation: loading-shift 900ms linear infinite;
    }}
    .loading-bar.htmx-request {{
      opacity: 1;
    }}
    @keyframes loading-shift {{
      from {{
        background-position: 0 0;
      }}
      to {{
        background-position: 220% 0;
      }}
    }}
    .mobile-scrim {{
      display: none;
    }}
    @media (max-width: 720px) {{
      .app-shell {{
        grid-template-columns: 1fr;
      }}
      .sidebar {{
        position: fixed;
        inset: 0 auto 0 0;
        z-index: 20;
        width: min(84vw, 286px);
        transform: translateX(-102%);
        transition: transform 180ms ease;
      }}
      .sidebar.is-open {{
        transform: translateX(0);
      }}
      .mobile-scrim {{
        position: fixed;
        inset: 0;
        z-index: 19;
        display: block;
        background: rgba(16, 20, 18, 0.42);
      }}
      .topbar {{
        padding: 0 14px;
      }}
      button.nav-toggle {{
        display: inline-flex;
      }}
      main.content {{
        width: 100%;
        padding: 18px 12px 44px;
      }}
      section.toolbar {{
        align-items: flex-start;
        flex-direction: column;
      }}
      h1 {{
        font-size: 21px;
      }}
      .split {{
        grid-template-columns: 1fr;
      }}
      .split > div, .task-grid form, .status-panel {{
        padding: 14px;
      }}
      .grid-form {{
        padding: 14px;
      }}
      .filters {{
        width: 100%;
        align-items: stretch;
      }}
      .filters input, .filters select, .filters button, .filters .button {{
        width: 100%;
        min-width: 100%;
      }}
      .topbar-actions .button {{
        display: none;
      }}
      .metric-value {{
        font-size: 24px;
      }}
    }}
    @media (min-width: 721px) and (max-width: 1100px) {{
      .app-shell {{
        grid-template-columns: 218px minmax(0, 1fr);
      }}
      main.content {{
        width: calc(100vw - 218px);
      }}
    }}
  </style>
</head>
<body>
  <div
    id="app"
    class="app-shell"
    x-data="{{navOpen: false}}"
    hx-boost="true"
    hx-target="#app"
    hx-select="#app"
    hx-swap="outerHTML show:window:top"
    hx-indicator="#global-loading"
  >
    <div id="global-loading" class="loading-bar" aria-hidden="true"></div>
    <div class="mobile-scrim" x-cloak x-show="navOpen" @click="navOpen = false"></div>
    <aside class="sidebar" :class="{{'is-open': navOpen}}" aria-label="主导航">
      <div class="brand">
        <span class="brand-mark">US</span>
        <span class="brand-title">
          <strong>USStock</strong>
          <span>本地研究控制面板</span>
        </span>
      </div>
      <nav class="nav-list">{nav}</nav>
      <div class="sidebar-note">轻量本地面板，无登录和权限系统。数据动作仍由当前 Python 服务执行。</div>
    </aside>
    <div class="content-shell">
      <header class="topbar">
        <button
          class="nav-toggle"
          type="button"
          aria-label="打开导航"
          :aria-expanded="navOpen.toString()"
          @click="navOpen = !navOpen"
        >
          <span class="nav-toggle-lines" aria-hidden="true">
            <span></span>
            <span></span>
            <span></span>
          </span>
        </button>
        <div class="topbar-title">
          <span>Local Admin</span>
          <strong>{e(title)}</strong>
        </div>
        <div class="topbar-actions">
          <a class="button ghost" href="/tasks">同步任务</a>
        </div>
      </header>
      <main class="content">
        {notice_html}
        {body}
      </main>
    </div>
  </div>
</body>
</html>"""


def form_value(form: Mapping[str, list[str]], name: str) -> str:
    values = form.get(name)
    if not values:
        return ""
    return values[0].strip()


def form_bool(form: Mapping[str, list[str]], name: str) -> bool:
    return form_value(form, name).lower() in {"1", "true", "yes", "on"}


def parse_positive_int(value: str) -> int | None:
    if not value:
        return None
    parsed = int(value)
    if parsed <= 0:
        raise AdminPanelError("数量必须大于 0。")
    return parsed


def clean_blank(value: str) -> str | None:
    return value.strip() or None


def checked(value: bool) -> str:
    return "checked" if value else ""


def render_select_options(options: list[str], selected: str) -> str:
    return "".join(
        (
            f'<option value="{e(option)}"'
            f'{" selected" if option == selected else ""}>{e(option)}</option>'
        )
        for option in options
    )


def clip_text(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def e(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return f"{int(value):,}"
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return e(value)


def serve(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    database_url: str | None = None,
) -> None:
    database_url = get_database_url(database_url)
    server = AdminHTTPServer(
        (host, port),
        AdminRequestHandler,
        database_url=database_url,
    )
    url = f"http://{host}:{port}"
    print(f"本地管理面板已启动：{url}")
    print("按 Ctrl+C 停止。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在停止本地管理面板。")
    finally:
        server.server_close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local usstock admin panel.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="监听地址")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="监听端口")
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        serve(host=args.host, port=args.port, database_url=args.database_url)
        return 0
    except AdminPanelError as exc:
        print(f"管理面板启动失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
