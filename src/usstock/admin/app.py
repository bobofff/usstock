"""A small local admin panel for self-hosted research workflows."""

from __future__ import annotations

import argparse
import html
import sys
import traceback
import urllib.parse
from collections.abc import Mapping
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
from usstock.data import gdelt, sec


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7878
PAGE_SIZE = 100
MAX_POST_BYTES = 64 * 1024


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

        routes = {
            "/": render_dashboard,
            "/stocks": render_stocks,
            "/sec": render_sec_filings,
            "/gdelt": render_gdelt,
            "/finnhub": render_finnhub,
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
        if length > MAX_POST_BYTES:
            self.redirect("/tasks", "请求内容过大。", ok=False)
            return

        raw_body = self.rfile.read(length).decode("utf-8")
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

            self.redirect("/tasks", "未知操作。", ok=False)
        except Exception as exc:  # pragma: no cover - keeps the panel usable.
            traceback.print_exc()
            self.redirect("/tasks", f"操作失败：{exc}", ok=False)

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


def render_tasks(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    today = date.today()
    week_ago = today - timedelta(days=finnhub.DEFAULT_COMPANY_NEWS_DAYS)
    body = f"""
    <section class="toolbar">
      <h1>同步任务</h1>
    </section>
    <section class="task-grid">
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
    </section>
    """
    return layout("同步任务", body, active="/tasks", query=query)


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
    return f'<div class="table-wrap"><table><thead>{head}</thead><tbody>{body}</tbody></table></div>'


def metric_box(label: str, value: Any, hint: str) -> str:
    return f"""
    <div class="metric">
      <div class="metric-label">{e(label)}</div>
      <div class="metric-value">{fmt(value)}</div>
      <div class="metric-hint">{e(hint)}</div>
    </div>
    """


def empty_state(message: str) -> str:
    return f'<div class="empty">{e(message)}</div>'


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
        f'<div class="notice {e(notice_type)}">{e(notice)}</div>' if notice else ""
    )
    nav_items = [
        ("/", "控制台"),
        ("/stocks", "股票池"),
        ("/sec", "SEC"),
        ("/gdelt", "GDELT"),
        ("/finnhub", "Finnhub"),
        ("/tasks", "同步"),
    ]
    nav = "".join(
        f'<a class="{ "active" if href == active else "" }" href="{href}">{label}</a>'
        for href, label in nav_items
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{e(title)} · USStock</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1f2937;
      --muted: #667085;
      --line: #d8dee8;
      --primary: #176b87;
      --primary-hover: #12536a;
      --ok: #2f7d32;
      --watch: #9a5b00;
      --error: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      letter-spacing: 0;
    }}
    header {{
      position: sticky;
      top: 0;
      z-index: 2;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      min-height: 58px;
      padding: 0 22px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.96);
      backdrop-filter: blur(8px);
    }}
    .brand {{ font-weight: 700; font-size: 16px; }}
    nav {{ display: flex; align-items: center; gap: 4px; overflow-x: auto; }}
    nav a {{
      display: inline-flex;
      align-items: center;
      height: 36px;
      padding: 0 12px;
      border-radius: 6px;
      color: var(--muted);
      text-decoration: none;
      white-space: nowrap;
    }}
    nav a.active, nav a:hover {{ background: #e8f3f6; color: var(--primary); }}
    main {{
      width: min(1240px, calc(100vw - 28px));
      margin: 22px auto 56px;
    }}
    section {{
      margin-top: 18px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }}
    h1, h2 {{ margin: 0; line-height: 1.25; }}
    h1 {{ font-size: 24px; }}
    h2 {{ margin-bottom: 14px; font-size: 16px; }}
    p {{ margin: 0 0 14px; color: var(--muted); }}
    .toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 12px;
      padding: 0;
      border: 0;
      background: transparent;
    }}
    .metric {{
      min-height: 108px;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }}
    .metric-label, .metric-hint, .subtle {{ color: var(--muted); }}
    .metric-value {{
      margin: 7px 0 5px;
      font-size: 27px;
      font-weight: 750;
    }}
    .split {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 18px;
      padding: 0;
      border: 0;
      background: transparent;
    }}
    .split > div {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      min-width: 0;
    }}
    .task-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(270px, 1fr));
      gap: 18px;
      padding: 0;
      border: 0;
      background: transparent;
    }}
    .task-grid form {{
      display: grid;
      gap: 12px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }}
    .filters {{
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .grid-form {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 12px;
      align-items: end;
    }}
    .span-2 {{ grid-column: 1 / -1; }}
    label {{ display: grid; gap: 6px; color: var(--muted); }}
    input, textarea {{
      width: 100%;
      min-height: 38px;
      padding: 8px 10px;
      border: 1px solid #c9d1dc;
      border-radius: 6px;
      background: #fff;
      color: var(--text);
      font: inherit;
    }}
    textarea {{ resize: vertical; }}
    label:has(input[type="checkbox"]) {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 38px;
    }}
    input[type="checkbox"] {{ width: 16px; min-height: 16px; }}
    button, .button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 38px;
      padding: 0 13px;
      border: 1px solid #b8c1cc;
      border-radius: 6px;
      background: #fff;
      color: var(--text);
      font: inherit;
      text-decoration: none;
      cursor: pointer;
      white-space: nowrap;
    }}
    button.primary, .button.primary {{
      border-color: var(--primary);
      background: var(--primary);
      color: #fff;
    }}
    button.primary:hover, .button.primary:hover {{ background: var(--primary-hover); }}
    .table-wrap {{ overflow-x: auto; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
    }}
    th, td {{
      padding: 10px 11px;
      border-bottom: 1px solid #edf0f4;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      background: #fafbfc;
    }}
    a {{ color: var(--primary); }}
    .badge {{
      display: inline-flex;
      align-items: center;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      background: #eef2f6;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}
    .badge.ok {{ background: #e7f5e8; color: var(--ok); }}
    .badge.watch {{ background: #fff3d6; color: var(--watch); }}
    .badge.muted {{ background: #eef2f6; color: var(--muted); }}
    .empty {{
      padding: 16px;
      border: 1px dashed #c8d0dc;
      border-radius: 8px;
      color: var(--muted);
      background: #fbfcfd;
    }}
    .notice {{
      margin-bottom: 14px;
      padding: 12px 14px;
      border-radius: 8px;
      border: 1px solid #c8d0dc;
      background: #fff;
    }}
    .notice.ok {{ border-color: #b7dfba; color: var(--ok); background: #f1faf2; }}
    .notice.error {{ border-color: #f0b8b2; color: var(--error); background: #fff5f4; }}
    .error-text {{ color: var(--error); }}
    @media (max-width: 720px) {{
      header {{ align-items: flex-start; flex-direction: column; padding: 12px 14px; }}
      main {{ width: min(100vw - 18px, 1240px); margin-top: 14px; }}
      section, .split > div, .task-grid form {{ padding: 14px; }}
      h1 {{ font-size: 21px; }}
      .split {{ grid-template-columns: 1fr; }}
      .filters {{ align-items: stretch; }}
      .filters input {{ min-width: 100%; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="brand">USStock</div>
    <nav>{nav}</nav>
  </header>
  <main>
    {notice_html}
    {body}
  </main>
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
