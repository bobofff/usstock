"""A small local admin panel for self-hosted research workflows."""

from __future__ import annotations

import argparse
import contextvars
import hashlib
import html
import json
import math
import os
import re
import secrets
import sys
import threading
import traceback
import urllib.parse
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from usstock.backtest import engine as backtest_engine
from usstock.config.settings import PROJECT_ROOT, get_settings
from usstock.data import finnhub
from usstock.data import gdelt, sec
from usstock.data import market
from usstock.discovery import daily as discovery
from usstock.discovery import topic_candidates
from usstock.reports import daily_report
from usstock.screening import universe as stock_universe


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7878
DEFAULT_DISCOVERY_INTERVAL_MINUTES = 60
ADMIN_ACTION_TOKEN_QUERY_PARAM = "admin_token"
ADMIN_ACTION_COOKIE_NAME = "usstock_admin_action_token"
ADMIN_AUTH_LOG_PATH = PROJECT_ROOT / "logs" / "admin_auth.log"
TABLE_PAGE_SIZE = 20
TABLE_PAGE_SIZE_OPTIONS = (10, 20, 50, 100)
TOPIC_CLOUD_ROW_LIMIT = 100
MAX_POST_BYTES = 64 * 1024
TOPIC_CLOUD_SIZE = 420
TOPIC_CLOUD_RADIUS = 186
TOPIC_CLOUD_MAX_TERMS = 54
TOPIC_CLOUD_TEXT_LIMIT = 28
TOPIC_CLOUD_PALETTE = (
    "#2563eb",
    "#db2777",
    "#0f766e",
    "#7c3aed",
    "#c2410c",
    "#0891b2",
    "#be123c",
    "#4d7c0f",
)
PICO_CSS_URL = "https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css"
HTMX_JS_URL = "https://cdn.jsdelivr.net/npm/htmx.org@2.0.10/dist/htmx.min.js"
ALPINE_JS_URL = "https://cdn.jsdelivr.net/npm/alpinejs@3/dist/cdn.min.js"
MARKDOWN_INLINE_PATTERN = re.compile(
    r"`([^`]+)`|\[([^\]]+)\]\(((?:[^()\s]|\([^()\s]*\))+)\)"
)
TOPIC_PANES = [
    ("overview", "概览"),
    ("library", "主题库"),
    ("candidates", "候选主题"),
    ("mentions", "主题提及"),
    ("scores", "候选评分"),
]
TASK_PANES = [
    ("discovery", "自动发现"),
    ("manual", "手动补数"),
    ("backtest", "日报复盘"),
    ("topic-extraction", "主题后处理"),
]
DISCOVERY_PROGRESS_STAGES = (
    ("prepare", "准备数据库"),
    ("finnhub", "Finnhub 新闻"),
    ("gdelt", "GDELT 主题新闻"),
    ("sec", "SEC filings"),
    ("scoring", "主题匹配与评分"),
    ("persist", "保存结果"),
)


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
    skip_sec_sync: bool


class DiscoveryProgressStore:
    """Thread-safe in-memory progress for the current or latest discovery run."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._run_count = 0
        self._snapshot = self._idle_snapshot()

    def _idle_snapshot(self) -> dict[str, Any]:
        return {
            "run_id": None,
            "trigger": None,
            "status": "idle",
            "message": "尚未运行。点击运行一次后显示阶段进度。",
            "error": None,
            "result": None,
            "config": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "current_stage": None,
            "percent": 0,
            "run_count": self._run_count,
            "stages": self._fresh_stages(),
            "logs": [],
        }

    def _fresh_stages(self) -> list[dict[str, Any]]:
        return [
            {
                "key": key,
                "label": label,
                "status": "pending",
                "completed": 0,
                "total": None,
                "detail": "",
                "started_at": None,
                "finished_at": None,
            }
            for key, label in DISCOVERY_PROGRESS_STAGES
        ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot = dict(self._snapshot)
            snapshot["stages"] = [dict(stage) for stage in self._snapshot["stages"]]
            snapshot["logs"] = [dict(item) for item in self._snapshot["logs"]]
            return snapshot

    def is_running(self) -> bool:
        with self._lock:
            return self._snapshot.get("status") == "running"

    def begin(self, *, config: DiscoveryRunConfig, trigger: str) -> str | None:
        with self._lock:
            if self._snapshot.get("status") == "running":
                return None

            now = datetime.now()
            run_id = now.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
            self._snapshot = {
                "run_id": run_id,
                "trigger": trigger,
                "status": "running",
                "message": "自动发现任务已开始。",
                "error": None,
                "result": None,
                "config": config,
                "started_at": now,
                "finished_at": None,
                "updated_at": now,
                "current_stage": "prepare",
                "percent": 0,
                "run_count": self._run_count,
                "stages": self._fresh_stages(),
                "logs": [],
            }
            self._append_log_locked(
                "info",
                "自动发现任务已开始。",
                stage="prepare",
                status="running",
            )
            return run_id

    def progress_callback(self, run_id: str) -> Callable[[dict[str, Any]], None]:
        def callback(event: dict[str, Any]) -> None:
            self.record_event(run_id, event)

        return callback

    def record_event(self, run_id: str, event: Mapping[str, Any]) -> None:
        with self._lock:
            if self._snapshot.get("run_id") != run_id:
                return
            if self._snapshot.get("status") != "running":
                return

            now = datetime.now()
            stage_key = str(event.get("stage") or "")
            stage_status = str(event.get("status") or "")
            stage = self._stage_locked(stage_key) if stage_key else None
            if stage:
                if stage_status:
                    stage["status"] = stage_status
                if event.get("completed") is not None:
                    stage["completed"] = int(event["completed"])
                if event.get("total") is not None:
                    stage["total"] = int(event["total"])
                if event.get("detail") is not None:
                    stage["detail"] = str(event["detail"])
                if stage_status == "running" and stage.get("started_at") is None:
                    stage["started_at"] = now
                if stage_status in {"success", "warning", "fail", "skipped"}:
                    if stage.get("started_at") is None:
                        stage["started_at"] = now
                    stage["finished_at"] = now
                if stage_status != "pending":
                    self._snapshot["current_stage"] = stage_key

            message = str(event.get("message") or "")
            if message:
                self._snapshot["message"] = message
                self._append_log_locked(
                    self._log_level(stage_status),
                    message,
                    stage=stage_key,
                    status=stage_status,
                    detail=event.get("detail"),
                )

            self._snapshot["updated_at"] = now
            self._snapshot["percent"] = self._calculate_percent_locked()

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        message: str,
        result: Mapping[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            if self._snapshot.get("run_id") != run_id:
                return

            now = datetime.now()
            if status == "fail":
                current_stage = self._stage_locked(
                    str(self._snapshot.get("current_stage") or "")
                )
                if current_stage and current_stage.get("status") == "running":
                    current_stage["status"] = "fail"
                    current_stage["finished_at"] = now

            if status == "success":
                self._run_count += 1

            self._snapshot["status"] = status
            self._snapshot["message"] = message
            self._snapshot["error"] = str(error) if error else None
            self._snapshot["result"] = dict(result or {})
            self._snapshot["finished_at"] = now
            self._snapshot["updated_at"] = now
            self._snapshot["run_count"] = self._run_count
            self._snapshot["percent"] = 100 if status == "success" else self._calculate_percent_locked()
            self._append_log_locked(
                "error" if status == "fail" else "info",
                message,
                stage=str(self._snapshot.get("current_stage") or ""),
                status=status,
            )

    def _stage_locked(self, key: str) -> dict[str, Any] | None:
        for stage in self._snapshot["stages"]:
            if stage["key"] == key:
                return stage
        return None

    def _append_log_locked(
        self,
        level: str,
        message: str,
        *,
        stage: str = "",
        status: str = "",
        detail: Any = None,
    ) -> None:
        self._snapshot["logs"].append(
            {
                "timestamp": datetime.now(),
                "level": level,
                "stage": stage,
                "status": status,
                "detail": "" if detail is None else str(detail),
                "message": message,
            }
        )

    def _calculate_percent_locked(self) -> int:
        stages = self._snapshot["stages"]
        if not stages:
            return 0

        units = Decimal("0")
        for stage in stages:
            status = stage.get("status")
            if status in {"success", "warning", "skipped"}:
                units += Decimal("1")
                continue
            if status == "fail":
                continue
            if status == "running":
                total = stage.get("total")
                completed = stage.get("completed") or 0
                if total:
                    units += min(Decimal(completed) / Decimal(total), Decimal("0.99"))
                else:
                    units += Decimal("0.35")

        return int((units / Decimal(len(stages)) * Decimal("100")).to_integral_value())

    @staticmethod
    def _log_level(stage_status: str) -> str:
        if stage_status == "fail":
            return "error"
        if stage_status == "warning":
            return "warning"
        return "info"


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

        run_id = DISCOVERY_PROGRESS.begin(config=config, trigger="schedule")
        if not run_id:
            with self._lock:
                self._last_message = "已有自动发现任务运行，本轮定时跳过。"
                self._last_run_finished_at = datetime.now()
                self._is_running_once = False
            return

        try:
            result = run_discovery_daily(
                database_url,
                config,
                progress_callback=DISCOVERY_PROGRESS.progress_callback(run_id),
            )
            message = format_discovery_result_message(result)
            DISCOVERY_PROGRESS.finish(
                run_id,
                status="success",
                message=message,
                result=discovery_result_summary(result),
            )
            with self._lock:
                self._run_count += 1
                self._last_message = message
        except Exception as exc:  # pragma: no cover - background safety net.
            DISCOVERY_PROGRESS.finish(
                run_id,
                status="fail",
                message=f"自动发现失败：{exc}",
                error=exc,
            )
            with self._lock:
                self._last_error = str(exc)
                self._last_message = f"自动发现失败：{exc}"
        finally:
            with self._lock:
                self._last_run_finished_at = datetime.now()
                self._is_running_once = False


DISCOVERY_PROGRESS = DiscoveryProgressStore()
DISCOVERY_SCHEDULER = DiscoveryScheduler()


class AdminPanelError(RuntimeError):
    """Raised when the local admin panel cannot complete a request."""


@dataclass(frozen=True)
class AdminAccess:
    protected: bool
    allowed: bool


ADMIN_ACCESS_CONTEXT: contextvars.ContextVar[AdminAccess] = contextvars.ContextVar(
    "admin_access",
    default=AdminAccess(protected=False, allowed=True),
)


class AdminHTTPServer(ThreadingHTTPServer):
    """HTTP server carrying runtime settings."""

    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        database_url: str | None,
        admin_action_token: str,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.database_url = database_url
        self.admin_action_token = admin_action_token


class AdminRequestHandler(BaseHTTPRequestHandler):
    """Request handler for the minimal local panel."""

    server: AdminHTTPServer

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        set_admin_cookie = False
        if self.admin_query_token_was_provided(query):
            query_token_valid = self.admin_query_token_is_valid(query)
            self.write_admin_auth_log(
                "get_admin_token_query",
                {
                    "query_token_valid": query_token_valid,
                    "query_token_fingerprint": token_fingerprint(
                        form_value(query, ADMIN_ACTION_TOKEN_QUERY_PARAM)
                    ),
                    "clean_path": self.path_without_admin_token(parsed),
                },
                parsed=parsed,
                query=query,
            )
            if not query_token_valid:
                self.redirect(
                    self.path_without_admin_token(parsed),
                    "管理员令牌无效，已进入只读浏览。",
                    ok=False,
                )
                return
            set_admin_cookie = True

        context_token = ADMIN_ACCESS_CONTEXT.set(
            self.resolve_admin_access(query=query)
        )
        try:
            self.write_admin_auth_log(
                "get_access_resolved",
                {
                    "protected": current_admin_access().protected,
                    "allowed": current_admin_access().allowed,
                    "set_admin_cookie": set_admin_cookie,
                },
                parsed=parsed,
                query=query,
            )
            if parsed.path == "/partials/discovery-status":
                self.send_html(
                    HTTPStatus.OK,
                    render_discovery_scheduler_status(
                        DISCOVERY_SCHEDULER.snapshot(),
                        DISCOVERY_PROGRESS.snapshot(),
                        include_oob_actions=True,
                    ),
                    set_admin_cookie=set_admin_cookie,
                )
                return
            if parsed.path == "/partials/discovery-progress":
                self.send_html(
                    HTTPStatus.OK,
                    render_discovery_progress_fragment(
                        DISCOVERY_PROGRESS.snapshot(),
                        include_oob_actions=True,
                    ),
                    set_admin_cookie=set_admin_cookie,
                )
                return

            routes = {
                "/": render_dashboard,
                "/stocks": render_stocks,
                "/sec": render_sec_filings,
                "/gdelt": render_gdelt,
                "/finnhub": render_finnhub,
                "/topics": render_topics,
                "/reports": render_reports,
                "/backtest": render_backtest,
                "/tasks": render_tasks,
            }
            renderer = routes.get(parsed.path)
            if not renderer:
                self.send_html(
                    HTTPStatus.NOT_FOUND,
                    render_not_found(),
                    set_admin_cookie=set_admin_cookie,
                )
                return

            self.send_html(
                HTTPStatus.OK,
                renderer(
                    database_url=self.resolve_database_url(),
                    query=query,
                ),
                set_admin_cookie=set_admin_cookie,
            )
        finally:
            ADMIN_ACCESS_CONTEXT.reset(context_token)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length > MAX_POST_BYTES:
            self.redirect("/tasks", "请求内容过大。", ok=False)
            return

        raw_bytes = self.rfile.read(length)
        raw_body = raw_bytes.decode("utf-8")
        form = urllib.parse.parse_qs(raw_body, keep_blank_values=True)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

        context_token = ADMIN_ACCESS_CONTEXT.set(
            self.resolve_admin_access(query=query, form=form)
        )
        try:
            self.write_admin_auth_log(
                "post_access_resolved",
                {
                    "protected": current_admin_access().protected,
                    "allowed": current_admin_access().allowed,
                    "content_type": self.headers.get("Content-Type") or "",
                    "content_length": length,
                },
                parsed=parsed,
                query=query,
                form=form,
            )
            if not current_admin_access().allowed:
                self.write_admin_auth_log(
                    "post_denied_readonly",
                    {
                        "fallback_path": action_fallback_path(parsed.path, form),
                    },
                    parsed=parsed,
                    query=query,
                    form=form,
                )
                self.redirect(
                    action_fallback_path(parsed.path, form),
                    "当前为只读浏览，只有管理员可以执行数据动作。",
                    ok=False,
                )
                return

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

            if parsed.path == "/actions/discovery-daily":
                config = parse_discovery_run_config(form)
                run_id = start_discovery_once(
                    database_url=get_database_url(database_url),
                    config=config,
                )
                if not run_id:
                    self.redirect(
                        "/tasks?pane=discovery",
                        "自动发现正在运行，请等待当前任务完成。",
                        ok=False,
                    )
                    return
                self.redirect(
                    "/tasks?pane=discovery",
                    f"自动热点发现已开始：run_id={run_id}。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/report-daily":
                run_date = daily_report.parse_run_date(form_value(form, "run_date") or None)
                top_n = parse_positive_int(form_value(form, "top_n")) or daily_report.DEFAULT_TOP_N
                profile = form_value(form, "profile") or daily_report.DEFAULT_PROFILE
                report = daily_report.generate_daily_report(
                    database_url=database_url,
                    run_date=run_date,
                    profile=profile,
                    top_n=top_n,
                )
                report_href = "/reports?uid=" + urllib.parse.quote(report.report_uid)
                self.redirect(
                    report_href,
                    f"每日分析报告生成完成：候选={len(report.candidates)}。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/market-import-prices":
                csv_path = resolve_project_path(form_value(form, "csv_path"))
                ticker = clean_blank(form_value(form, "ticker"))
                data_source = form_value(form, "data_source") or market.DEFAULT_DATA_SOURCE
                count = market.import_prices_csv(
                    csv_path=csv_path,
                    database_url=database_url,
                    ticker=ticker,
                    data_source=data_source,
                )
                self.redirect(
                    safe_return_path(form, "/backtest"),
                    f"日线价格导入完成：{count} 行。",
                    ok=True,
                )
                return

            if parsed.path == "/actions/market-import-universe":
                file_path = resolve_project_path(form_value(form, "file_path"))
                result = stock_universe.import_stock_universe_file(
                    file_path=file_path,
                    database_url=database_url,
                    data_source=(
                        form_value(form, "data_source")
                        or stock_universe.DEFAULT_UNIVERSE_DATA_SOURCE
                    ),
                    source_url=clean_blank(form_value(form, "source_url")),
                    filter_config=parse_stock_universe_filter_config(form),
                    limit=parse_positive_int(form_value(form, "limit")),
                    dry_run=form_bool(form, "dry_run"),
                )
                self.redirect(
                    "/tasks?pane=manual",
                    format_stock_universe_import_message(result),
                    ok=result.accepted_count > 0,
                )
                return

            if parsed.path == "/actions/market-sync-nasdaq-universe":
                result = stock_universe.sync_nasdaq_stock_universe(
                    database_url=database_url,
                    filter_config=parse_stock_universe_filter_config(form),
                    limit=parse_positive_int(form_value(form, "limit")),
                    dry_run=form_bool(form, "dry_run"),
                    exchanges=parse_stock_universe_exchanges(form),
                    use_volume_as_avg_volume=form_bool(form, "use_volume_as_avg_volume"),
                )
                self.redirect(
                    "/tasks?pane=manual",
                    format_stock_universe_import_message(result),
                    ok=result.accepted_count > 0,
                )
                return

            if parsed.path == "/actions/market-sync-stooq":
                result = market.sync_stooq_daily_prices(
                    database_url=database_url,
                    tickers=market.parse_ticker_list(form_value(form, "tickers")),
                    from_date=market.parse_optional_date(
                        form_value(form, "from_date"),
                        field_name="开始日期",
                    ),
                    to_date=market.parse_optional_date(
                        form_value(form, "to_date"),
                        field_name="结束日期",
                    ),
                    from_report_candidates=form_bool(form, "from_report_candidates"),
                    profile=form_value(form, "profile") or backtest_engine.DEFAULT_PROFILE,
                    top_n=(
                        parse_positive_int(form_value(form, "top_n"))
                        or backtest_engine.DEFAULT_TOP_N
                    ),
                )
                self.redirect(
                    safe_return_path(form, "/backtest"),
                    format_market_sync_result_message(result),
                    ok=market_sync_notice_is_ok(result),
                )
                return

            if parsed.path == "/actions/market-sync-yfinance":
                result = market.sync_yfinance_daily_prices(
                    database_url=database_url,
                    tickers=market.parse_ticker_list(form_value(form, "tickers")),
                    from_date=market.parse_optional_date(
                        form_value(form, "from_date"),
                        field_name="开始日期",
                    ),
                    to_date=market.parse_optional_date(
                        form_value(form, "to_date"),
                        field_name="结束日期",
                    ),
                    from_report_candidates=form_bool(form, "from_report_candidates"),
                    profile=form_value(form, "profile") or backtest_engine.DEFAULT_PROFILE,
                    top_n=(
                        parse_positive_int(form_value(form, "top_n"))
                        or backtest_engine.DEFAULT_TOP_N
                    ),
                )
                self.redirect(
                    safe_return_path(form, "/backtest"),
                    format_market_sync_result_message(result),
                    ok=market_sync_notice_is_ok(result),
                )
                return

            if parsed.path == "/actions/backtest-reports":
                result = backtest_engine.run_report_backtest(
                    database_url=database_url,
                    start_date=backtest_engine.parse_run_date(form_value(form, "from_date")),
                    end_date=backtest_engine.parse_run_date(form_value(form, "to_date")),
                    profile=form_value(form, "profile") or backtest_engine.DEFAULT_PROFILE,
                    top_n=(
                        parse_positive_int(form_value(form, "top_n"))
                        or backtest_engine.DEFAULT_TOP_N
                    ),
                    price_source=clean_blank(form_value(form, "price_source")),
                    persist=not form_bool(form, "no_persist"),
                )
                self.redirect(
                    safe_return_path(form, "/backtest"),
                    format_backtest_result_message(result),
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

            if parsed.path == "/actions/topic-promote":
                slug = form_value(form, "candidate_slug")
                if not slug:
                    raise AdminPanelError("候选主题 slug 不能为空。")

                result = topic_candidates.promote_topic_candidates(
                    database_url=database_url,
                    slugs=(slug,),
                    activate=form_bool(form, "activate"),
                )
                if result.promoted_slugs:
                    self.redirect(
                        "/topics?pane=candidates",
                        f"{slug} 已晋升为正式主题。",
                        ok=True,
                    )
                else:
                    self.redirect(
                        "/topics?pane=candidates",
                        f"{slug} 未晋升：请确认它仍是待审核状态，且未匹配已有正式主题。",
                        ok=False,
                    )
                return

            if parsed.path == "/actions/topic-ignore":
                slug = form_value(form, "candidate_slug")
                if not slug:
                    raise AdminPanelError("候选主题 slug 不能为空。")

                count = topic_candidates.ignore_topic_candidates(
                    database_url=database_url,
                    slugs=(slug,),
                )
                if count:
                    self.redirect(
                        "/topics?pane=candidates",
                        f"{slug} 已忽略，后续抽取不会把它恢复为待审核。",
                        ok=True,
                    )
                else:
                    self.redirect(
                        "/topics?pane=candidates",
                        f"{slug} 未忽略：请确认它仍是待审核状态。",
                        ok=False,
                    )
                return

            if parsed.path == "/actions/discovery-schedule-start":
                config = parse_discovery_run_config(form)
                interval_minutes = (
                    parse_positive_int(form_value(form, "interval_minutes"))
                    or DEFAULT_DISCOVERY_INTERVAL_MINUTES
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
            self.redirect(
                action_fallback_path(parsed.path, form),
                f"操作失败：{exc}",
                ok=False,
            )
        finally:
            ADMIN_ACCESS_CONTEXT.reset(context_token)

    def resolve_database_url(self) -> str | None:
        return self.server.database_url

    def resolve_admin_access(
        self,
        *,
        query: Mapping[str, list[str]],
        form: Mapping[str, list[str]] | None = None,
    ) -> AdminAccess:
        expected = self.server.admin_action_token
        if not expected:
            access = AdminAccess(protected=False, allowed=True)
            self.write_admin_auth_log(
                "resolve_admin_access",
                {
                    "protected": access.protected,
                    "allowed": access.allowed,
                    "expected_token_configured": False,
                },
                query=query,
                form=form,
            )
            return access

        candidates = {
            "query": form_value(query, ADMIN_ACTION_TOKEN_QUERY_PARAM),
            "cookie": self.admin_cookie_token(),
            "header": (self.headers.get("X-Admin-Action-Token") or "").strip(),
        }
        if form is not None:
            candidates["form"] = form_value(form, ADMIN_ACTION_TOKEN_QUERY_PARAM)

        matches = {
            source: admin_token_matches(expected, token)
            for source, token in candidates.items()
        }
        access = AdminAccess(protected=True, allowed=any(matches.values()))
        self.write_admin_auth_log(
            "resolve_admin_access",
            {
                "protected": access.protected,
                "allowed": access.allowed,
                "expected_token_configured": True,
                "expected_token_fingerprint": token_fingerprint(expected),
                "source_present": {
                    source: bool(token) for source, token in candidates.items()
                },
                "source_match": matches,
                "source_fingerprint": {
                    source: token_fingerprint(token)
                    for source, token in candidates.items()
                    if token
                },
            },
            query=query,
            form=form,
        )
        return access

    def admin_query_token_was_provided(self, query: Mapping[str, list[str]]) -> bool:
        return ADMIN_ACTION_TOKEN_QUERY_PARAM in query

    def admin_query_token_is_valid(self, query: Mapping[str, list[str]]) -> bool:
        return admin_token_matches(
            self.server.admin_action_token,
            form_value(query, ADMIN_ACTION_TOKEN_QUERY_PARAM),
        )

    def admin_cookie_token(self) -> str:
        raw_cookie = self.headers.get("Cookie") or ""
        if not raw_cookie:
            self.write_admin_auth_log(
                "cookie_token_missing",
                {"reason": "no_cookie_header"},
            )
            return ""
        cookie = SimpleCookie()
        try:
            cookie.load(raw_cookie)
        except Exception as exc:
            self.write_admin_auth_log(
                "cookie_token_invalid",
                {
                    "error": str(exc),
                    "cookie_header_length": len(raw_cookie),
                },
            )
            return ""
        morsel = cookie.get(ADMIN_ACTION_COOKIE_NAME)
        token = urllib.parse.unquote(morsel.value.strip()) if morsel else ""
        self.write_admin_auth_log(
            "cookie_token_resolved",
            {
                "cookie_names": sorted(cookie.keys()),
                "admin_cookie_present": morsel is not None,
                "cookie_token_fingerprint": token_fingerprint(token),
            },
        )
        return token

    def admin_cookie_header(self) -> str:
        return (
            f"{ADMIN_ACTION_COOKIE_NAME}={urllib.parse.quote(self.server.admin_action_token, safe='')}; "
            "Path=/; HttpOnly; SameSite=Strict"
        )

    def path_without_admin_token(self, parsed: urllib.parse.ParseResult) -> str:
        params = [
            (key, value)
            for key, value in urllib.parse.parse_qsl(
                parsed.query,
                keep_blank_values=True,
            )
            if key != ADMIN_ACTION_TOKEN_QUERY_PARAM
        ]
        query = urllib.parse.urlencode(params)
        return urllib.parse.urlunparse(("", "", parsed.path or "/", "", query, ""))

    def redirect_to(self, path: str, *, set_admin_cookie: bool = False) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", path)
        if set_admin_cookie:
            self.send_header("Set-Cookie", self.admin_cookie_header())
        self.end_headers()

    def redirect(self, path: str, message: str, *, ok: bool) -> None:
        params = urllib.parse.urlencode(
            {
                "notice": clip_text(message, 600),
                "notice_type": "ok" if ok else "error",
            }
        )
        separator = "&" if "?" in path else "?"
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", f"{path}{separator}{params}")
        self.end_headers()

    def send_html(
        self,
        status: HTTPStatus,
        body: str,
        *,
        set_admin_cookie: bool = False,
    ) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        if set_admin_cookie:
            self.send_header("Set-Cookie", self.admin_cookie_header())
            self.write_admin_auth_log(
                "set_admin_cookie",
                {
                    "status": int(status),
                    "token_fingerprint": token_fingerprint(
                        self.server.admin_action_token
                    ),
                },
            )
        self.end_headers()
        self.wfile.write(payload)

    def write_admin_auth_log(
        self,
        event: str,
        payload: Mapping[str, Any] | None = None,
        *,
        parsed: urllib.parse.ParseResult | None = None,
        query: Mapping[str, list[str]] | None = None,
        form: Mapping[str, list[str]] | None = None,
    ) -> None:
        request_path = parsed.path if parsed else urllib.parse.urlparse(self.path).path
        request_query = query if query is not None else {}
        request_form = form if form is not None else {}
        write_admin_auth_log(
            event,
            {
                "method": self.command,
                "path": request_path,
                "query_keys": sorted(request_query.keys()),
                "form_keys": sorted(request_form.keys()),
                "has_query_admin_token": ADMIN_ACTION_TOKEN_QUERY_PARAM in request_query,
                "has_form_admin_token": ADMIN_ACTION_TOKEN_QUERY_PARAM in request_form,
                "has_admin_header": bool(
                    (self.headers.get("X-Admin-Action-Token") or "").strip()
                ),
                "has_cookie_header": bool(self.headers.get("Cookie")),
                "referer": self.headers.get("Referer") or "",
                "origin": self.headers.get("Origin") or "",
                **dict(payload or {}),
            },
        )

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[admin] " + fmt % args + "\n")


def current_admin_access() -> AdminAccess:
    return ADMIN_ACCESS_CONTEXT.get()


def admin_token_matches(expected: str | None, candidate: str | None) -> bool:
    if not expected or not candidate:
        return False
    return secrets.compare_digest(expected, candidate)


def token_fingerprint(token: str | None) -> str:
    if not token:
        return ""
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return f"len={len(token)} sha256={digest[:12]}"


def write_admin_auth_log(event: str, payload: Mapping[str, Any]) -> None:
    record = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "event": event,
        "pid": os.getpid(),
        "thread_id": threading.get_native_id(),
        **dict(payload),
    }
    try:
        ADMIN_AUTH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ADMIN_AUTH_LOG_PATH.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:  # pragma: no cover - diagnostics must not break requests.
        sys.stderr.write(f"[admin-auth-log] 写入失败: {exc}\n")


def admin_actions_are_allowed() -> bool:
    return current_admin_access().allowed


def action_fallback_path(
    action_path: str,
    form: Mapping[str, list[str]] | None = None,
) -> str:
    form = form or {}
    if action_path in {"/actions/topic-promote", "/actions/topic-ignore"}:
        return "/topics?pane=candidates"
    if action_path in {
        "/actions/market-import-universe",
        "/actions/market-sync-nasdaq-universe",
    }:
        return safe_return_path(form, "/tasks?pane=manual")
    if action_path in {
        "/actions/market-import-prices",
        "/actions/market-sync-stooq",
        "/actions/market-sync-yfinance",
        "/actions/backtest-reports",
    }:
        return safe_return_path(form, "/backtest")
    if action_path == "/actions/report-daily":
        return "/reports"
    return "/tasks"


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
            if tables["market_topics"] and tables["topic_mentions"]:
                topic_cloud_rows = fetch_all(
                    conn,
                    """
                    SELECT mt.topic_slug, mt.topic_name, mt.gdelt_query,
                           mt.keywords, mt.ticker_hints, mt.priority,
                           mt.is_active, mt.last_refreshed_at,
                           count(tm.id) AS mention_count,
                           count(tm.id) FILTER (
                               WHERE tm.detected_at >= now() - interval '24 hours'
                           ) AS recent_mentions,
                           count(DISTINCT tm.ticker) FILTER (
                               WHERE tm.ticker IS NOT NULL
                           ) AS ticker_count
                    FROM market_topics mt
                    LEFT JOIN topic_mentions tm ON tm.topic_slug = mt.topic_slug
                    GROUP BY mt.topic_slug, mt.topic_name, mt.gdelt_query,
                             mt.keywords, mt.ticker_hints, mt.priority,
                             mt.is_active, mt.last_refreshed_at
                    ORDER BY mt.is_active DESC, recent_mentions DESC,
                             mt.priority, mt.topic_slug
                    LIMIT %s
                    """,
                    (TOPIC_CLOUD_ROW_LIMIT,),
                )
            elif tables["market_topics"]:
                topic_cloud_rows = fetch_all(
                    conn,
                    """
                    SELECT topic_slug, topic_name, gdelt_query, keywords,
                           ticker_hints, priority, is_active, last_refreshed_at,
                           0 AS mention_count, 0 AS recent_mentions, 0 AS ticker_count
                    FROM market_topics
                    ORDER BY is_active DESC, priority, topic_slug
                    LIMIT %s
                    """,
                    (TOPIC_CLOUD_ROW_LIMIT,),
                )
            else:
                topic_cloud_rows = []

            topic_candidate_cloud_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT candidate_slug, topic_name, gdelt_query, keywords,
                           ticker_hints, source_types, article_count,
                           source_count, ticker_count, trend_score,
                           novelty_score, status, matched_topic_slug,
                           evidence, last_seen_at
                    FROM market_topic_candidates
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
                    (TOPIC_CLOUD_ROW_LIMIT,),
                )
                if tables["market_topic_candidates"]
                else []
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
                    f"""
                    SELECT ga.title, ga.domain, ga.language, ga.seen_at,
                           ga.article_url,
                           {(
                               "coalesce(array_remove(array_agg(DISTINCT tm.topic_slug ORDER BY tm.topic_slug), NULL), '{}'::text[])"
                               if tables["topic_mentions"]
                               else "'{}'::text[]"
                           )} AS topic_slugs
                    FROM gdelt_articles ga
                    {(
                        "LEFT JOIN topic_mentions tm "
                        "ON tm.source_type = 'gdelt_article' "
                        "AND tm.source_uid = ga.article_url"
                        if tables["topic_mentions"]
                        else ""
                    )}
                    GROUP BY ga.title, ga.domain, ga.language, ga.seen_at,
                             ga.article_url, ga.last_seen_at
                    ORDER BY coalesce(ga.seen_at, ga.last_seen_at) DESC
                    LIMIT 8
                    """,
                )
                if tables["gdelt_articles"]
                else []
            )
            recent_finnhub_articles = (
                fetch_all(
                    conn,
                    f"""
                    SELECT fa.headline, fa.source_name, fa.category,
                           fa.related_tickers, fa.published_at, fa.article_url,
                           {(
                               "coalesce(array_remove(array_agg(DISTINCT tm.topic_slug ORDER BY tm.topic_slug), NULL), '{}'::text[])"
                               if tables["topic_mentions"]
                               else "'{}'::text[]"
                           )} AS topic_slugs
                    FROM finnhub_articles fa
                    {(
                        "LEFT JOIN topic_mentions tm "
                        "ON tm.source_type = 'finnhub_article' "
                        "AND tm.source_uid = fa.article_uid"
                        if tables["topic_mentions"]
                        else ""
                    )}
                    GROUP BY fa.headline, fa.source_name, fa.category,
                             fa.related_tickers, fa.published_at, fa.article_url,
                             fa.last_seen_at
                    ORDER BY coalesce(fa.published_at, fa.last_seen_at) DESC
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
    <section class="split topic-clouds">
      {render_topic_cloud_panel("主题词云", topic_cloud_rows, cloud_type="topics")}
      {render_topic_cloud_panel("候选主题词云", topic_candidate_cloud_rows, cloud_type="candidates")}
    </section>
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
                    """,
                    tuple(params),
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
                    """,
                    tuple(params),
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
            has_mentions = table_exists(conn, "topic_mentions")
            topic_select = (
                "coalesce(array_remove(array_agg(DISTINCT tm.topic_slug ORDER BY tm.topic_slug), NULL), '{}'::text[])"
                if has_mentions
                else "'{}'::text[]"
            )
            topic_join = (
                "LEFT JOIN topic_mentions tm "
                "ON tm.source_type = 'gdelt_article' "
                "AND tm.source_uid = ga.article_url"
                if has_mentions
                else ""
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
                        ga.title ILIKE %s OR ga.query_text ILIKE %s
                        OR coalesce(ga.domain, '') ILIKE %s
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
                    SELECT ga.title, ga.domain, ga.language, ga.source_country,
                           ga.tone, ga.seen_at, ga.article_url, ga.query_text,
                           {topic_select} AS topic_slugs
                    FROM gdelt_articles ga
                    {topic_join}
                    {article_where}
                    GROUP BY ga.title, ga.domain, ga.language, ga.source_country,
                             ga.tone, ga.seen_at, ga.article_url, ga.query_text,
                             ga.last_seen_at
                    ORDER BY coalesce(ga.seen_at, ga.last_seen_at) DESC
                    """,
                    tuple(article_params),
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
            has_mentions = table_exists(conn, "topic_mentions")
            topic_select = (
                "coalesce(array_remove(array_agg(DISTINCT tm.topic_slug ORDER BY tm.topic_slug), NULL), '{}'::text[])"
                if has_mentions
                else "'{}'::text[]"
            )
            topic_join = (
                "LEFT JOIN topic_mentions tm "
                "ON tm.source_type = 'finnhub_article' "
                "AND tm.source_uid = fa.article_uid"
                if has_mentions
                else ""
            )
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
                        fa.headline ILIKE %s OR coalesce(fa.summary, '') ILIKE %s
                        OR coalesce(fa.source_name, '') ILIKE %s
                    )
                    """
                )
                article_params.extend([like, like, like])
            if ticker:
                article_conditions.append("%s = ANY(fa.related_tickers)")
                article_params.append(ticker)
                query_conditions.append("upper(coalesce(ticker, '')) = upper(%s)")
                query_params.append(ticker)
            if category:
                article_conditions.append("lower(coalesce(fa.category, '')) = lower(%s)")
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
                    SELECT fa.headline, fa.source_name, fa.category,
                           fa.related_tickers, fa.published_at, fa.article_url,
                           fa.endpoint,
                           {topic_select} AS topic_slugs
                    FROM finnhub_articles fa
                    {topic_join}
                    {article_where}
                    GROUP BY fa.headline, fa.source_name, fa.category,
                             fa.related_tickers, fa.published_at, fa.article_url,
                             fa.endpoint, fa.last_seen_at
                    ORDER BY coalesce(fa.published_at, fa.last_seen_at) DESC
                    """,
                    tuple(article_params),
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


def selected_pane(
    query: Mapping[str, list[str]],
    *,
    param: str,
    panes: list[tuple[str, str]],
    default: str,
) -> str:
    pane = form_value(query, param)
    allowed = {key for key, _ in panes}
    return pane if pane in allowed else default


def build_query_href(
    path: str,
    query: Mapping[str, list[str]],
    updates: Mapping[str, str | None],
) -> str:
    params: dict[str, str] = {}
    for key, values in query.items():
        if key in {"notice", "notice_type"} or not values:
            continue
        params[key] = values[0]
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    encoded = urllib.parse.urlencode(params)
    return f"{path}?{encoded}" if encoded else path


def render_pane_breadcrumbs(
    *,
    path: str,
    query: Mapping[str, list[str]],
    param: str,
    panes: list[tuple[str, str]],
    active: str,
) -> str:
    items = []
    for key, label in panes:
        active_class = " active" if key == active else ""
        aria_current = ' aria-current="page"' if key == active else ""
        href = build_query_href(path, query, {param: key})
        items.append(
            f'<a class="pane-crumb{active_class}" href="{e(href)}"{aria_current}>{e(label)}</a>'
        )
    return f'<nav class="pane-breadcrumbs" aria-label="页面分区">{"".join(items)}</nav>'


def render_topics(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    topic_filter = form_value(query, "topic")
    ticker_filter = form_value(query, "ticker").upper()
    candidate_status_filter = form_value(query, "candidate_status")
    pane = selected_pane(
        query,
        param="pane",
        panes=TOPIC_PANES,
        default="overview",
    )

    try:
        with connect_database(database_url) as conn:
            has_topics = table_exists(conn, "market_topics")
            has_topic_candidates = table_exists(conn, "market_topic_candidates")
            has_mentions = table_exists(conn, "topic_mentions")
            has_scores = table_exists(conn, "daily_candidate_scores")

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
                    """,
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
                    """,
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
                    """,
                    tuple(candidate_params),
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
                    """,
                    tuple(mention_params),
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
            score_rows = (
                fetch_all(
                    conn,
                    f"""
                    SELECT run_date, rank, ticker, company_name, score,
                           action_bias, primary_topic_slug, topic_slugs,
                           finnhub_article_count, gdelt_article_count,
                           sec_filing_count
                    FROM daily_candidate_scores
                    {score_where}
                    ORDER BY run_date DESC, rank NULLS LAST, score DESC
                    """,
                    tuple(score_params),
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

    if pane == "library":
        pane_content = f"""
        <section class="pane-content">
          <h2>主题库</h2>
          {render_topics_table(topic_rows)}
        </section>
        """
    elif pane == "candidates":
        pane_content = f"""
        <section class="pane-content">
          <h2>新闻候选主题</h2>
          {render_topic_candidates_table(topic_candidate_rows)}
        </section>
        """
    elif pane == "mentions":
        pane_content = f"""
        <section class="pane-content">
          <h2>最近主题提及</h2>
          {render_topic_mentions_table(mention_rows)}
        </section>
        """
    elif pane == "scores":
        pane_content = f"""
        <section class="pane-content">
          <h2>每日候选评分</h2>
          {render_candidate_scores_table(score_rows)}
        </section>
        """
    else:
        pane_content = f"""
        <section class="metrics">{"".join(metrics)}</section>
        <section class="split topic-clouds">
          {render_topic_cloud_panel("主题词云", topic_rows, cloud_type="topics")}
          {render_topic_cloud_panel("候选主题词云", topic_candidate_rows, cloud_type="candidates")}
        </section>
        {migration_hint}
        """

    body = f"""
    <section class="toolbar">
      <h1>主题</h1>
      <form method="get" class="filters">
        <input type="hidden" name="pane" value="{e(pane)}">
        <input name="topic" value="{e(topic_filter)}" placeholder="topic_slug">
        <input name="ticker" value="{e(ticker_filter)}" placeholder="ticker">
        <select name="candidate_status">
          {render_candidate_status_options(candidate_status_filter)}
        </select>
        <button type="submit">筛选</button>
        <a class="button primary" href="/tasks?pane=discovery">运行自动发现</a>
      </form>
    </section>
    {render_pane_breadcrumbs(path="/topics", query=query, param="pane", panes=TOPIC_PANES, active=pane)}
    {pane_content}
    """
    return layout("主题", body, active="/topics", query=query)


def render_reports(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    selected_uid = form_value(query, "uid")
    today = date.today()
    admin_disabled = " disabled" if not admin_actions_are_allowed() else ""

    try:
        with connect_database(database_url) as conn:
            has_reports = table_exists(conn, "daily_analysis_reports")
            summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE run_date = current_date) AS today,
                        count(*) FILTER (WHERE llm_used) AS llm_used,
                        max(generated_at) AS last_generated_at
                    FROM daily_analysis_reports
                    """,
                )
                if has_reports
                else {}
            )
            report_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT report_uid, run_date, profile, candidate_count,
                           llm_used, llm_model, summary, generated_at
                    FROM daily_analysis_reports
                    ORDER BY generated_at DESC
                    """,
                )
                if has_reports
                else []
            )
            if not selected_uid and report_rows:
                selected_uid = str(report_rows[0]["report_uid"])
            selected_report = (
                fetch_one(
                    conn,
                    """
                    SELECT report_uid, run_date, profile, candidate_count,
                           llm_used, llm_model, markdown_body, summary,
                           structured_payload, generated_at
                    FROM daily_analysis_reports
                    WHERE report_uid = %s
                    LIMIT 1
                    """,
                    (selected_uid,),
                )
                if has_reports and selected_uid
                else {}
            )
    except Exception as exc:
        return render_database_error(exc, active="/reports")

    metrics = [
        metric_box("报告总数", summary.get("total"), f"今日 {fmt(summary.get('today'))}"),
        metric_box("LLM 增强", summary.get("llm_used"), "默认启用"),
        metric_box("最近生成", summary.get("last_generated_at"), "daily_analysis_reports"),
    ]

    selected_html = render_selected_report(selected_report)
    body = f"""
    <section class="toolbar">
      <div>
        <h1>分析报告</h1>
        <p class="page-kicker">基于每日候选评分生成事件解释、相关理由和风险提示。</p>
      </div>
    </section>
    <section>
      <form class="discovery-panel" method="post" action="/actions/report-daily">
        <div class="task-panel-header">
          <div>
            <span class="eyebrow">报告生成</span>
            <h2>每日新闻驱动分析报告</h2>
            <p>复用 daily_candidate_scores 和 daily_watchlists，不重新抓取外部数据。</p>
          </div>
          <div class="button-row action-row">
            <button class="primary" type="submit"{admin_disabled}>生成报告</button>
          </div>
        </div>
        <div class="form-sections">
          <div class="form-section">
            <h3>报告参数</h3>
            <div class="field-grid">
              <label>运行日期 <input name="run_date" type="date" value="{today.isoformat()}"></label>
              <label>profile <input name="profile" value="{e(daily_report.DEFAULT_PROFILE)}"></label>
              <label>候选数量 <input name="top_n" type="number" min="1" value="{daily_report.DEFAULT_TOP_N}"></label>
            </div>
          </div>
        </div>
      </form>
    </section>
    <section class="metrics">{"".join(metrics)}</section>
    <section>
      <h2>历史报告</h2>
      {render_reports_table(report_rows, selected_uid=selected_uid)}
    </section>
    {selected_html}
    """
    return layout("分析报告", body, active="/reports", query=query)


def render_backtest(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    body = f"""
    <section class="toolbar">
      <div>
        <h1>日报复盘</h1>
        <p class="page-kicker">导入日线价格后，验证日报候选股在 T+1、T+5、T+20 的后续表现。</p>
      </div>
    </section>
    {render_backtest_workspace(database_url=database_url, query=query, return_path="/backtest")}
    """
    return layout("日报复盘", body, active="/backtest", query=query)


def render_backtest_workspace(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
    return_path: str,
) -> str:
    today = date.today()
    from_date = today - timedelta(days=30)
    price_summary: dict[str, Any] = {}
    performance_summary: dict[str, Any] = {}
    price_rows: list[dict[str, Any]] = []
    performance_rows: list[dict[str, Any]] = []
    has_prices = False
    has_performance = False

    try:
        with connect_database(database_url) as conn:
            has_prices = table_exists(conn, "market_daily_prices")
            has_performance = table_exists(conn, "daily_candidate_performance")
            price_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(DISTINCT ticker) AS ticker_count,
                        min(price_date) AS first_price_date,
                        max(price_date) AS last_price_date,
                        max(updated_at) AS last_updated_at
                    FROM market_daily_prices
                    """,
                )
                if has_prices
                else {}
            )
            performance_summary = (
                fetch_one(
                    conn,
                    """
                    SELECT
                        count(*) AS total,
                        count(*) FILTER (WHERE performance_status = 'complete') AS complete,
                        count(*) FILTER (WHERE performance_status = 'partial') AS partial,
                        count(*) FILTER (WHERE return_5d_pct > 0) AS wins_5d,
                        count(return_5d_pct) AS evaluated_5d,
                        avg(return_1d_pct) AS avg_return_1d_pct,
                        avg(return_5d_pct) AS avg_return_5d_pct,
                        avg(return_20d_pct) AS avg_return_20d_pct,
                        max(computed_at) AS last_computed_at
                    FROM daily_candidate_performance
                    """,
                )
                if has_performance
                else {}
            )
            price_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT ticker, price_date, open_price, high_price, low_price,
                           close_price, adjusted_close_price, volume, data_source,
                           updated_at
                    FROM market_daily_prices
                    ORDER BY price_date DESC, ticker
                    LIMIT 20
                    """,
                )
                if has_prices
                else []
            )
            performance_rows = (
                fetch_all(
                    conn,
                    """
                    SELECT run_date, profile, ticker, company_name, rank, score,
                           attention_label, primary_topic_slug, entry_date,
                           entry_close, return_1d_pct, return_5d_pct,
                           max_drawdown_5d_pct, return_20d_pct,
                           performance_status, missing_reason, computed_at
                    FROM daily_candidate_performance
                    ORDER BY computed_at DESC, run_date DESC, rank NULLS LAST, ticker
                    """,
                )
                if has_performance
                else []
            )
    except Exception as exc:
        return f"""
        {render_backtest_forms(today=today, from_date=from_date, return_path=return_path)}
        <section>
          <div class="empty error-text">复盘数据读取失败：{e(exc)}</div>
        </section>
        """

    evaluated_5d = performance_summary.get("evaluated_5d") or 0
    wins_5d = performance_summary.get("wins_5d") or 0
    win_rate_5d = None
    if evaluated_5d:
        win_rate_5d = Decimal(wins_5d) / Decimal(evaluated_5d) * Decimal("100")

    metrics = [
        metric_box(
            "价格行数",
            price_summary.get("total"),
            f"标的 {fmt(price_summary.get('ticker_count'))}",
        ),
        metric_box(
            "价格日期",
            price_summary.get("last_price_date"),
            f"起始 {fmt(price_summary.get('first_price_date'))}",
        ),
        metric_box(
            "复盘记录",
            performance_summary.get("total"),
            f"完整 {fmt(performance_summary.get('complete'))} / 部分 {fmt(performance_summary.get('partial'))}",
        ),
        metric_box(
            "T+5 胜率",
            format_percent_value(win_rate_5d),
            f"样本 {fmt(evaluated_5d)}",
        ),
        metric_box(
            "T+5 平均收益",
            format_percent_value(performance_summary.get("avg_return_5d_pct")),
            "daily_candidate_performance",
        ),
        metric_box(
            "最近复盘",
            performance_summary.get("last_computed_at"),
            "computed_at",
        ),
    ]

    return f"""
    {render_backtest_forms(today=today, from_date=from_date, return_path=return_path)}
    <section class="metrics">{"".join(metrics)}</section>
    <section>
      <div class="task-section-header">
        <div>
          <h2>最近复盘结果</h2>
          <p>收益率单位为百分比，T+5 回撤按入场后收盘价序列计算。</p>
        </div>
      </div>
      {render_candidate_performance_table(performance_rows)}
    </section>
    <section>
      <div class="task-section-header">
        <div>
          <h2>最近导入价格</h2>
          <p>复盘优先使用 adjusted close，缺失时使用 close。</p>
        </div>
      </div>
      {render_market_daily_prices_table(price_rows)}
    </section>
    """


def render_backtest_forms(*, today: date, from_date: date, return_path: str) -> str:
    return f"""
    <section class="task-grid">
      <form method="post" action="/actions/market-sync-yfinance">
        <input type="hidden" name="return_path" value="{e(return_path)}">
        <h2>自动同步日线价格</h2>
        <p>默认从已生成日报候选中提取 ticker，并通过 yfinance 同步 Yahoo Finance 日线数据。</p>
        <label>开始日期 <input name="from_date" type="date" value="{from_date.isoformat()}" required></label>
        <label>结束日期 <input name="to_date" type="date" value="{today.isoformat()}" required></label>
        <label>指定 ticker <input name="tickers" placeholder="可空；如 AAPL,NVDA,MSFT"></label>
        <label>profile <input name="profile" value="{e(backtest_engine.DEFAULT_PROFILE)}"></label>
        <label>每份日报前 N 个 <input name="top_n" type="number" min="1" value="{backtest_engine.DEFAULT_TOP_N}"></label>
        <label><input type="checkbox" name="from_report_candidates" value="1" checked> 从日报候选自动提取 ticker</label>
        <button class="primary" type="submit">同步价格</button>
      </form>
      <form method="post" action="/actions/backtest-reports">
        <input type="hidden" name="return_path" value="{e(return_path)}">
        <h2>复盘日报候选</h2>
        <p>读取已生成的 daily_analysis_reports，并写入候选股后续表现。</p>
        <label>开始日期 <input name="from_date" type="date" value="{from_date.isoformat()}" required></label>
        <label>结束日期 <input name="to_date" type="date" value="{today.isoformat()}" required></label>
        <label>profile <input name="profile" value="{e(backtest_engine.DEFAULT_PROFILE)}"></label>
        <label>每份日报前 N 个 <input name="top_n" type="number" min="1" value="{backtest_engine.DEFAULT_TOP_N}"></label>
        <label>价格来源 <input name="price_source" value="{e(market.YFINANCE_DATA_SOURCE)}"></label>
        <label><input type="checkbox" name="no_persist" value="1"> 只预览，不写入复盘表</label>
        <button class="primary" type="submit">运行复盘</button>
      </form>
      <form method="post" action="/actions/market-import-prices">
        <input type="hidden" name="return_path" value="{e(return_path)}">
        <h2>备用：导入 CSV</h2>
        <p>如果接口临时不可用，可以填写本机项目内 CSV 路径，例如 data/raw/prices.csv。</p>
        <label>CSV 路径 <input name="csv_path" required placeholder="data/raw/prices.csv"></label>
        <label>ticker <input name="ticker" placeholder="CSV 没有 symbol 列时填写，如 NVDA"></label>
        <label>数据来源 <input name="data_source" value="{e(market.DEFAULT_DATA_SOURCE)}"></label>
        <button class="primary" type="submit">导入价格</button>
      </form>
    </section>
    """


def render_tasks(
    *,
    database_url: str | None,
    query: Mapping[str, list[str]],
) -> str:
    today = date.today()
    week_ago = today - timedelta(days=finnhub.DEFAULT_COMPANY_NEWS_DAYS)
    pane = selected_pane(
        query,
        param="pane",
        panes=TASK_PANES,
        default="discovery",
    )
    discovery_status = render_discovery_scheduler_status(
        DISCOVERY_SCHEDULER.snapshot(),
        DISCOVERY_PROGRESS.snapshot(),
    )
    if pane == "manual":
        pane_content = render_manual_sync_tools(today=today, week_ago=week_ago)
    elif pane == "backtest":
        pane_content = render_backtest_workspace(
            database_url=database_url,
            query=query,
            return_path="/tasks?pane=backtest",
        )
    elif pane == "topic-extraction":
        pane_content = render_topic_extraction_task()
    else:
        progress_snapshot = DISCOVERY_PROGRESS.snapshot()
        pane_content = f"""
        {discovery_status}
        {render_discovery_task_panel(progress_snapshot)}
        """
    body = f"""
    <section class="toolbar">
      <div>
        <h1>同步任务</h1>
        <p class="page-kicker">主流程、补数工具和主题后处理分区展示。</p>
      </div>
    </section>
    {render_pane_breadcrumbs(path="/tasks", query=query, param="pane", panes=TASK_PANES, active=pane)}
    {pane_content}
    """
    return layout("同步任务", body, active="/tasks", query=query)


def render_discovery_task_panel(progress_snapshot: dict[str, Any]) -> str:
    return f"""
    <section>
      <form class="discovery-panel" method="post" action="/actions/discovery-daily">
        <div class="task-panel-header">
          <div>
            <span class="eyebrow">主流程</span>
            <h2>自动热点发现</h2>
            <p>同步 Finnhub、GDELT 和 SEC，刷新主题命中与每日候选评分。</p>
          </div>
          {render_discovery_action_row(progress_snapshot)}
        </div>
        {render_discovery_progress_fragment(progress_snapshot)}
        <div class="form-sections">
          <div class="form-section">
            <h3>定时设置</h3>
            <div class="field-grid">
              <label>每 N 分钟执行一次 <input name="interval_minutes" type="number" min="1" value="{DEFAULT_DISCOVERY_INTERVAL_MINUTES}"></label>
            </div>
          </div>
          <div class="form-section">
            <h3>发现参数</h3>
            <div class="field-grid">
              <label>候选数量 <input name="top_n" type="number" min="1" value="{discovery.DEFAULT_TOP_N}"></label>
              <label>回看小时 <input name="lookback_hours" type="number" min="1" value="{discovery.DEFAULT_LOOKBACK_HOURS}"></label>
              <label>GDELT 文章数 <input name="gdelt_max_records" type="number" min="1" max="250" value="{gdelt.DEFAULT_MAX_RECORDS}"></label>
            </div>
          </div>
          <div class="form-section">
            <h3>SEC 扫描</h3>
            <div class="field-grid">
              <label>扫描标的数 <input name="max_sec_tickers" type="number" min="1" value="{discovery.DEFAULT_MAX_SEC_TICKERS}"></label>
              <label>filing 数量 <input name="sec_filing_limit" type="number" min="1" value="{discovery.DEFAULT_SEC_FILING_LIMIT}"></label>
            </div>
            <div class="checkbox-stack">
              <label><input type="checkbox" name="include_company_facts" value="1"> 同步 company facts</label>
            </div>
          </div>
          <div class="form-section">
            <h3>数据源策略</h3>
            <div class="checkbox-stack">
              <label><input type="checkbox" name="skip_finnhub_sync" value="1"> 跳过 Finnhub 新闻</label>
              <label><input type="checkbox" name="skip_gdelt_sync" value="1"> 跳过 GDELT 主题新闻</label>
              <label><input type="checkbox" name="skip_sec_sync" value="1"> 跳过 SEC filings</label>
            </div>
          </div>
        </div>
      </form>
    </section>
    """


def render_discovery_action_row(
    progress_snapshot: dict[str, Any],
    *,
    include_oob: bool = False,
) -> str:
    running = progress_snapshot.get("status") == "running"
    admin_disabled = not admin_actions_are_allowed()
    oob_attr = ' hx-swap-oob="outerHTML"' if include_oob else ""
    run_disabled = " disabled" if running or admin_disabled else ""
    schedule_disabled = " disabled" if admin_disabled else ""
    run_busy = ' aria-busy="true"' if running else ""
    run_label = "同步中..." if running else "运行一次"
    return f"""
    <div id="discovery-action-row" class="button-row action-row"{oob_attr}>
      <button
        id="run-discovery-once-button"
        class="primary"
        type="submit"
        formaction="/actions/discovery-daily"
        {run_disabled}{run_busy}
      >{run_label}</button>
      <button
        type="submit"
        formaction="/actions/discovery-schedule-start"{schedule_disabled}
      >启动定时</button>
      <button
        type="submit"
        formaction="/actions/discovery-schedule-stop"{schedule_disabled}
      >停止定时</button>
    </div>
    """


def render_discovery_progress_fragment(
    progress_snapshot: dict[str, Any],
    *,
    include_oob_actions: bool = False,
) -> str:
    status = str(progress_snapshot.get("status") or "idle")
    percent = max(0, min(100, int(progress_snapshot.get("percent") or 0)))
    current_stage = discovery_progress_current_stage(progress_snapshot)
    started_at = progress_snapshot.get("started_at")
    finished_at = progress_snapshot.get("finished_at")
    result = progress_snapshot.get("result") or {}
    error = progress_snapshot.get("error")
    result_html = ""
    if result:
        result_html = f"""
        <div class="progress-summary">
          <span>候选 {fmt(result.get("candidate_count"))}</span>
          <span>警告 {fmt(result.get("warning_count"))}</span>
          <span>运行次数 {fmt(progress_snapshot.get("run_count"))}</span>
        </div>
        """
    error_html = f'<p class="error-text">{e(error)}</p>' if error else ""
    oob_html = (
        render_discovery_action_row(progress_snapshot, include_oob=True)
        if include_oob_actions
        else ""
    )

    return f"""
    <div
      id="discovery-progress"
      class="progress-panel progress-{e(status)}"
      hx-get="/partials/discovery-progress"
      hx-trigger="load, every 1s"
      hx-disinherit="hx-target hx-select hx-swap hx-indicator"
      hx-target="this"
      hx-swap="outerHTML"
    >
      <div class="progress-heading">
        <div>
          <span class="eyebrow">运行过程</span>
          <h3>{e(current_stage or "等待运行")}</h3>
          <p>{e(progress_snapshot.get("message"))}</p>
        </div>
        <div class="progress-meta">
          {render_discovery_progress_badge(status)}
          <span>{e(discovery_progress_trigger_label(progress_snapshot.get("trigger")))}</span>
          <span>耗时 {e(format_duration(started_at, finished_at))}</span>
        </div>
      </div>
      <div class="progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent}">
        <div class="progress-fill" style="width: {percent}%"></div>
      </div>
      <div class="progress-percent">{percent}%</div>
      <div class="stage-list">
        {render_discovery_stage_rows(progress_snapshot)}
      </div>
      {result_html}
      {error_html}
      <div class="progress-log">
        <div class="progress-log-title">历史节点</div>
        {render_discovery_log_rows(progress_snapshot)}
      </div>
    </div>
    {oob_html}
    """


def render_discovery_progress_badge(status: str) -> str:
    if status == "running":
        return '<span class="badge watch">运行中</span>'
    if status == "success":
        return '<span class="badge ok">已完成</span>'
    if status == "fail":
        return '<span class="badge error">失败</span>'
    return '<span class="badge muted">未运行</span>'


def discovery_progress_trigger_label(trigger: Any) -> str:
    if trigger == "manual":
        return "手动运行"
    if trigger == "schedule":
        return "定时运行"
    return "等待中"


def discovery_progress_current_stage(progress_snapshot: dict[str, Any]) -> str:
    stage_key = progress_snapshot.get("current_stage")
    for stage in progress_snapshot.get("stages") or []:
        if stage.get("key") == stage_key:
            return str(stage.get("label") or "")
    return ""


def render_discovery_stage_rows(progress_snapshot: dict[str, Any]) -> str:
    rows = []
    for stage in progress_snapshot.get("stages") or []:
        status = str(stage.get("status") or "pending")
        rows.append(
            f"""
            <div class="stage-row is-{e(status)}">
              <span class="stage-mark">{e(stage_status_mark(status))}</span>
              <div class="stage-copy">
                <strong>{e(stage.get("label"))}</strong>
                <span>{e(stage.get("detail") or stage_status_label(status))}</span>
              </div>
              <span class="stage-count">{e(format_stage_count(stage))}</span>
            </div>
            """
        )
    return "".join(rows)


def render_discovery_log_rows(progress_snapshot: dict[str, Any]) -> str:
    logs = progress_snapshot.get("logs") or []
    if not logs:
        return '<p class="subtle">暂无历史节点。</p>'

    rows = []
    for item in logs:
        timestamp = item.get("timestamp")
        time_text = timestamp.strftime("%H:%M:%S") if isinstance(timestamp, datetime) else "-"
        level = str(item.get("level") or "info")
        stage_label = discovery_stage_label(item.get("stage"))
        status_label = stage_status_label(str(item.get("status") or ""))
        detail = str(item.get("detail") or "")
        meta_parts = [part for part in (stage_label, status_label, detail) if part]
        rows.append(
            f"""
            <div class="progress-log-row log-{e(level)}">
              <time>{e(time_text)}</time>
              <div>
                <span class="progress-log-meta">{e(" / ".join(meta_parts))}</span>
                <span>{e(item.get("message"))}</span>
              </div>
            </div>
            """
        )
    return "".join(rows)


def discovery_stage_label(stage_key: Any) -> str:
    for key, label in DISCOVERY_PROGRESS_STAGES:
        if key == stage_key:
            return label
    return ""


def stage_status_label(status: str) -> str:
    labels = {
        "pending": "等待中",
        "running": "进行中",
        "success": "完成",
        "warning": "完成，有警告",
        "skipped": "已跳过",
        "fail": "失败",
    }
    return labels.get(status, status)


def stage_status_mark(status: str) -> str:
    marks = {
        "success": "✓",
        "warning": "!",
        "running": "→",
        "skipped": "-",
        "fail": "!",
    }
    return marks.get(status, "·")


def format_stage_count(stage: Mapping[str, Any]) -> str:
    total = stage.get("total")
    completed = stage.get("completed") or 0
    if total is None:
        return stage_status_label(str(stage.get("status") or "pending"))
    if int(total) <= 0:
        return "-"
    return f"{int(completed)}/{int(total)}"


def format_duration(started_at: Any, finished_at: Any = None) -> str:
    if not isinstance(started_at, datetime):
        return "-"

    ended_at = finished_at if isinstance(finished_at, datetime) else datetime.now()
    total_seconds = max(0, int((ended_at - started_at).total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def render_manual_sync_tools(*, today: date, week_ago: date) -> str:
    return f"""
    <section class="task-section">
      <div class="task-section-header">
        <div>
          <h2>手动补数工具</h2>
          <p>用于排查单个数据源或补齐局部数据，日常优先使用上面的自动热点发现。</p>
        </div>
      </div>
      <div class="task-grid">
        <form method="post" action="/actions/market-sync-nasdaq-universe">
          <input type="hidden" name="return_path" value="/tasks?pane=manual">
          <h2>接口扩充股票池</h2>
          <p>从 Nasdaq screener 接口拉取 NYSE/Nasdaq/AMEX 标的并复用同一套过滤规则。</p>
          <label>处理行数 <input name="limit" type="number" min="1" value="200"></label>
          <label>交易所 <input name="exchanges" value="NASDAQ,NYSE,AMEX"></label>
          <label>30 日均量 >= <input name="min_avg_volume_30d" type="number" min="0" value="{stock_universe.DEFAULT_MIN_AVG_VOLUME_30D}"></label>
          <label>市值 >= <input name="min_market_cap_usd" type="number" min="0" value="{stock_universe.DEFAULT_MIN_MARKET_CAP_USD}"></label>
          <label>价格 >= <input name="min_last_price" type="number" min="0" step="0.01" value="{stock_universe.DEFAULT_MIN_LAST_PRICE}"></label>
          <label><input type="checkbox" name="dry_run" value="1" checked> 只预览</label>
          <label><input type="checkbox" name="use_volume_as_avg_volume" value="1" checked> 使用接口 volume 作为流动性代理</label>
          <label><input type="checkbox" name="require_market_cap" value="1"> 要求市值字段</label>
          <button class="primary" type="submit">同步接口股票池</button>
        </form>
        <form method="post" action="/actions/market-import-universe">
          <input type="hidden" name="return_path" value="/tasks?pane=manual">
          <h2>扩充股票池</h2>
          <p>从本机 CSV/JSONL 导入 NYSE/Nasdaq/AMEX 活跃普通股、ADR 和 REIT。</p>
          <label>文件路径 <input name="file_path" required placeholder="data/raw/us_active_universe.csv"></label>
          <label>数据来源 <input name="data_source" value="{e(stock_universe.DEFAULT_UNIVERSE_DATA_SOURCE)}"></label>
          <label>来源 URL <input name="source_url" placeholder="可空"></label>
          <label>处理行数 <input name="limit" type="number" min="1" placeholder="先小批量验证"></label>
          <label>30 日均量 >= <input name="min_avg_volume_30d" type="number" min="0" value="{stock_universe.DEFAULT_MIN_AVG_VOLUME_30D}"></label>
          <label>市值 >= <input name="min_market_cap_usd" type="number" min="0" value="{stock_universe.DEFAULT_MIN_MARKET_CAP_USD}"></label>
          <label>价格 >= <input name="min_last_price" type="number" min="0" step="0.01" value="{stock_universe.DEFAULT_MIN_LAST_PRICE}"></label>
          <label><input type="checkbox" name="dry_run" value="1" checked> 只预览</label>
          <label><input type="checkbox" name="require_market_cap" value="1"> 要求市值字段</label>
          <button class="primary" type="submit">导入股票池</button>
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
      </div>
    </section>
    """


def render_topic_extraction_task() -> str:
    return f"""
    <section class="task-section">
      <div class="task-section-header">
        <div>
          <h2>主题后处理</h2>
          <p>从已入库新闻里抽取候选主题，结果进入候选表等待审核。</p>
        </div>
        <a class="button ghost" href="/topics">查看主题</a>
      </div>
      <div class="task-grid narrow-grid">
        <form method="post" action="/actions/topic-extract">
          <h2>新闻候选主题</h2>
          <label>回看小时 <input name="lookback_hours" type="number" min="1" value="{topic_candidates.DEFAULT_TOPIC_EXTRACTION_LOOKBACK_HOURS}"></label>
          <label>候选数量 <input name="max_candidates" type="number" min="1" value="{topic_candidates.DEFAULT_MAX_CANDIDATES}"></label>
          <label>最少文章数 <input name="min_articles" type="number" min="1" value="{topic_candidates.DEFAULT_MIN_ARTICLES}"></label>
          <label>最低分数 <input name="min_score" type="number" min="0" step="0.1" value="{topic_candidates.DEFAULT_MIN_SCORE}"></label>
          <label><input type="checkbox" name="include_existing_matches" value="1"> 保留已匹配正式主题的候选</label>
          <button class="primary" type="submit">抽取候选主题</button>
        </form>
      </div>
    </section>
    """


def render_discovery_scheduler_status(
    snapshot: dict[str, Any],
    progress_snapshot: dict[str, Any] | None = None,
    *,
    include_oob_actions: bool = False,
) -> str:
    progress_snapshot = progress_snapshot or {}
    active = bool(snapshot.get("active"))
    progress_running = progress_snapshot.get("status") == "running"
    badge = (
        '<span class="badge ok">定时运行中</span>'
        if active
        else '<span class="badge muted">定时未运行</span>'
    )
    running_once = (
        '<span class="badge watch">本轮执行中</span>'
        if snapshot.get("is_running_once") or progress_running
        else ""
    )
    error = snapshot.get("last_error")
    progress_error = progress_snapshot.get("error")
    if progress_error and not error:
        error = progress_error
    error_html = f'<p class="error-text">{e(error)}</p>' if error else ""
    config = snapshot.get("config")
    config_hint = ""
    if config:
        config_hint = (
            f"top_n={config.top_n}，"
            f"回看={config.lookback_hours}h，"
            f"SEC={config.max_sec_tickers} 个标的"
        )
    status_message = snapshot.get("last_message")
    if progress_running:
        current_stage = discovery_progress_current_stage(progress_snapshot)
        status_message = (
            f"{discovery_progress_trigger_label(progress_snapshot.get('trigger'))}"
            f"执行中：{current_stage or '处理中'}，"
            f"{fmt(progress_snapshot.get('percent'))}%。"
        )
    elif progress_snapshot.get("status") in {"success", "fail"}:
        status_message = progress_snapshot.get("message") or status_message
    history_html = ""
    if progress_snapshot.get("logs"):
        history_html = f"""
        <div class="progress-log status-history">
          <div class="progress-log-title">历史节点</div>
          {render_discovery_log_rows(progress_snapshot)}
        </div>
        """
    oob_html = (
        render_discovery_action_row(progress_snapshot, include_oob=True)
        if include_oob_actions
        else ""
    )

    return f"""
    <section class="status-panel" hx-get="/partials/discovery-status" hx-trigger="every 10s" hx-target="this" hx-swap="outerHTML">
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
      <p class="subtle">{e(status_message)}</p>
      {error_html}
      {history_html}
    </section>
    {oob_html}
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
        skip_sec_sync=form_bool(form, "skip_sec_sync"),
    )


def run_discovery_daily(
    database_url: str | None,
    config: DiscoveryRunConfig,
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
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
        skip_sec_sync=config.skip_sec_sync,
        progress_callback=progress_callback,
    )


def discovery_result_summary(
    result: discovery.DailyDiscoveryResult,
) -> dict[str, Any]:
    return {
        "run_date": result.run_date.isoformat(),
        "candidate_count": len(result.candidates),
        "warning_count": len(result.warnings),
        "stats": result.stats,
    }


def format_discovery_result_message(result: discovery.DailyDiscoveryResult) -> str:
    return (
        f"{result.run_date.isoformat()} 自动发现完成："
        f"候选 {len(result.candidates)} 个，"
        f"警告 {len(result.warnings)} 条。"
    )


def format_backtest_result_message(result: backtest_engine.BacktestResult) -> str:
    horizon = result.summary.get("horizons", {}).get("5", {})
    avg_return = horizon.get("avg_return_pct") or "-"
    win_rate = horizon.get("win_rate_pct") or "-"
    return (
        f"日报复盘完成：候选 {len(result.performances)} 条，"
        f"T+5 样本 {horizon.get('evaluated') or 0}，"
        f"胜率 {win_rate}%，平均收益 {avg_return}%。"
    )


def format_market_sync_result_message(result: market.MarketPriceSyncResult) -> str:
    message = (
        f"{result.provider} 日线同步完成：价格 {result.price_count} 行，"
        f"成功 {len(result.synced_tickers)} / 请求 {len(result.requested_tickers)} 个 ticker。"
    )
    if result.missing_tickers:
        message += " 缺少数据：" + ", ".join(result.missing_tickers[:10]) + "。"
    if result.failed_tickers:
        message += " 请求失败：" + ", ".join(result.failed_tickers[:10]) + "。"
    return message


def market_sync_notice_is_ok(result: market.MarketPriceSyncResult) -> bool:
    return bool(result.synced_tickers) and not result.failed_tickers


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
        topics = format_topic_badges(row.get("topic_slugs"))
        compact_topics = "" if topics == "-" else f"<div>{topics}</div>"
        extra = (
            ""
            if compact
            else (
                f"<td>{topics}</td>"
                f"<td>{e(row.get('query_text'))}</td>"
                f"<td>{fmt(row.get('tone'))}</td>"
            )
        )
        body.append(
            f"""
            <tr>
              <td>{title}<div class="subtle">{e(row.get("domain"))}</div>{compact_topics if compact else ""}</td>
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
        head = "<tr><th>标题</th><th>语言</th><th>地区</th><th>时间</th><th>主题</th><th>query</th><th>tone</th></tr>"
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
        topics = format_topic_badges(row.get("topic_slugs"))
        compact_topics = "" if topics == "-" else f"<div>{topics}</div>"
        source = row.get("source_name") or "-"
        extra = (
            ""
            if compact
            else (
                f"<td>{e(row.get('category'))}</td>"
                f"<td>{related}</td>"
                f"<td>{topics}</td>"
                f"<td>{e(row.get('endpoint'))}</td>"
            )
        )
        if compact:
            body.append(
                f"""
                <tr>
                  <td>{title}<div class="subtle">{e(row.get("category"))}</div>{compact_topics}</td>
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
            "<tr><th>标题</th><th>来源</th><th>分类</th><th>相关</th><th>主题</th>"
            "<th>接口</th><th>时间</th></tr>"
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
        actions = render_topic_candidate_actions(row)
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
              <td>{actions}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>候选主题</th><th>状态</th><th>分数</th><th>覆盖</th>"
            "<th>GDELT query</th><th>关键词</th><th>相关标的</th>"
            "<th>来源</th><th>匹配正式主题</th><th>证据</th><th>最近发现</th>"
            "<th>操作</th></tr>"
        ),
        "".join(body),
    )


def render_topic_candidate_actions(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "pending")
    matched = row.get("matched_topic_slug")
    slug = str(row.get("candidate_slug") or "").strip()
    if not admin_actions_are_allowed():
        return '<span class="subtle">只读</span>'
    if status != "pending":
        return '<span class="subtle">-</span>'
    if not slug:
        return '<span class="subtle">缺少 slug</span>'

    promote_action = (
        '<span class="subtle">已匹配正式主题</span>'
        if matched
        else f"""
        <form method="post" action="/actions/topic-promote" class="inline-action-form">
          <input type="hidden" name="candidate_slug" value="{e(slug)}">
          <label class="compact-checkbox">
            <input type="checkbox" name="activate" value="1">
            启用
          </label>
          <button
            class="secondary"
            type="submit"
            onclick="return confirm('确认晋升这个候选主题为正式主题？')"
          >晋升</button>
        </form>
        """
    )
    return f"""
    <div class="inline-action-stack">
      {promote_action}
      <form method="post" action="/actions/topic-ignore" class="inline-action-form compact-action-form">
        <input type="hidden" name="candidate_slug" value="{e(slug)}">
        <button
          class="secondary"
          type="submit"
          onclick="return confirm('确认忽略这个候选主题？')"
        >忽略</button>
      </form>
    </div>
    """

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
              <td>{fmt(row.get("sec_filing_count"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>日期</th><th>排名</th><th>ticker</th><th>评分</th>"
            "<th>动作</th><th>主主题</th><th>主题</th>"
            "<th>Finnhub</th><th>GDELT</th><th>SEC</th></tr>"
        ),
        "".join(body),
    )


def render_candidate_performance_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无复盘记录。先导入日线价格，再运行日报复盘。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td>{fmt(row.get("run_date"))}<div class="subtle">{e(row.get("profile"))}</div></td>
              <td><strong>{e(row.get("ticker"))}</strong><div class="subtle">{e(row.get("company_name"))}</div></td>
              <td>{fmt(row.get("rank"))}</td>
              <td>{fmt(row.get("score"))}</td>
              <td>{e(row.get("attention_label"))}<div class="subtle">{e(row.get("primary_topic_slug"))}</div></td>
              <td>{fmt(row.get("entry_date"))}<div class="subtle">{fmt(row.get("entry_close"))}</div></td>
              <td>{format_percent_value(row.get("return_1d_pct"))}</td>
              <td>{format_percent_value(row.get("return_5d_pct"))}</td>
              <td>{format_percent_value(row.get("max_drawdown_5d_pct"))}</td>
              <td>{format_percent_value(row.get("return_20d_pct"))}</td>
              <td>{render_performance_status(row.get("performance_status"))}<div class="subtle">{e(row.get("missing_reason"))}</div></td>
              <td>{fmt(row.get("computed_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>日报</th><th>ticker</th><th>排名</th><th>分数</th>"
            "<th>关注/主题</th><th>入场参考</th><th>T+1</th><th>T+5</th>"
            "<th>T+5 回撤</th><th>T+20</th><th>状态</th><th>计算时间</th></tr>"
        ),
        "".join(body),
    )


def render_market_daily_prices_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return empty_state("暂无日线价格。可以从 CSV 导入 market_daily_prices。")

    body = []
    for row in rows:
        body.append(
            f"""
            <tr>
              <td><strong>{e(row.get("ticker"))}</strong></td>
              <td>{fmt(row.get("price_date"))}</td>
              <td>{fmt(row.get("open_price"))}</td>
              <td>{fmt(row.get("high_price"))}</td>
              <td>{fmt(row.get("low_price"))}</td>
              <td>{fmt(row.get("close_price"))}</td>
              <td>{fmt(row.get("adjusted_close_price"))}</td>
              <td>{fmt(row.get("volume"))}</td>
              <td>{e(row.get("data_source"))}</td>
              <td>{fmt(row.get("updated_at"))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>ticker</th><th>日期</th><th>开盘</th><th>最高</th>"
            "<th>最低</th><th>收盘</th><th>复权收盘</th><th>成交量</th>"
            "<th>来源</th><th>更新时间</th></tr>"
        ),
        "".join(body),
    )


def render_performance_status(status: Any) -> str:
    text = str(status or "pending")
    if text == "complete":
        return '<span class="badge ok">完整</span>'
    if text == "partial":
        return '<span class="badge watch">部分</span>'
    if text in {"no_entry_price", "no_horizon_price"}:
        return '<span class="badge error">缺价格</span>'
    if text == "pending":
        return '<span class="badge muted">待计算</span>'
    return f'<span class="badge muted">{e(text)}</span>'


def format_percent_value(value: Any) -> str:
    if value is None or value == "":
        return "-"
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception:
        return e(value)
    return f"{decimal.quantize(Decimal('0.0001'))}%"


def render_reports_table(
    rows: list[dict[str, Any]],
    *,
    selected_uid: str,
) -> str:
    if not rows:
        return empty_state("暂无分析报告。可以先在本页生成一份。")

    body = []
    for row in rows:
        uid = str(row.get("report_uid") or "")
        active = " active-row" if uid == selected_uid else ""
        href = "/reports?uid=" + urllib.parse.quote(uid)
        body.append(
            f"""
            <tr class="{active}">
              <td><a href="{e(href)}">{e(row.get("run_date"))}</a></td>
              <td>{e(row.get("profile"))}</td>
              <td>{fmt(row.get("candidate_count"))}</td>
              <td>{'是' if row.get("llm_used") else '否'}<div class="subtle">{e(row.get("llm_model"))}</div></td>
              <td>{fmt(row.get("generated_at"))}</td>
              <td>{e(clip_text(str(row.get("summary") or ""), 120))}</td>
            </tr>
            """
        )
    return table(
        (
            "<tr><th>日期</th><th>profile</th><th>候选数</th>"
            "<th>LLM</th><th>生成时间</th><th>摘要</th></tr>"
        ),
        "".join(body),
    )


def render_selected_report(row: dict[str, Any]) -> str:
    if not row:
        return """
        <section>
          <h2>报告预览</h2>
          <div class="empty">暂无可预览报告。</div>
        </section>
        """

    payload = row.get("structured_payload") or {}
    candidates = payload.get("candidates") if isinstance(payload, dict) else []
    candidate_hint = ""
    if isinstance(candidates, list) and candidates:
        tickers = ", ".join(
            str(candidate.get("ticker"))
            for candidate in candidates[:8]
            if isinstance(candidate, dict) and candidate.get("ticker")
        )
        candidate_hint = f"<p class=\"subtle\">候选：{e(tickers)}</p>" if tickers else ""
    markdown = row.get("markdown_body") or ""
    return f"""
    <section>
      <div class="task-section-header">
        <div>
          <h2>报告预览</h2>
          <p>{e(row.get("report_uid"))}</p>
          {candidate_hint}
        </div>
      </div>
      <div class="report-preview">{render_markdown_preview_html(markdown)}</div>
    </section>
    """


def render_markdown_preview_html(markdown: str) -> str:
    lines = markdown.splitlines()
    parts: list[str] = []
    index = 0
    while index < len(lines):
        raw_line = lines[index]
        line = raw_line.strip()
        if not line:
            index += 1
            continue

        if line.startswith("```"):
            code_lines: list[str] = []
            index += 1
            while index < len(lines) and not lines[index].strip().startswith("```"):
                code_lines.append(lines[index])
                index += 1
            if index < len(lines):
                index += 1
            code = e("\n".join(code_lines))
            parts.append(f'<pre class="report-code"><code>{code}</code></pre>')
            continue

        if is_markdown_table_start(lines, index):
            table_html, index = render_markdown_table_html(lines, index)
            parts.append(table_html)
            continue

        heading_level = markdown_heading_level(line)
        if heading_level:
            tag = f"h{min(heading_level + 2, 6)}"
            text = line[heading_level:].strip()
            parts.append(f"<{tag}>{render_inline_markdown(text)}</{tag}>")
            index += 1
            continue

        if line.startswith(">"):
            quote_lines: list[str] = []
            while index < len(lines) and lines[index].strip().startswith(">"):
                quote_lines.append(lines[index].strip().lstrip(">").strip())
                index += 1
            quote = "<br>".join(render_inline_markdown(item) for item in quote_lines)
            parts.append(f"<blockquote>{quote}</blockquote>")
            continue

        if is_markdown_list_line(raw_line):
            list_items: list[tuple[int, str]] = []
            while index < len(lines) and is_markdown_list_line(lines[index]):
                item_line = lines[index]
                stripped = item_line.lstrip()
                indent = len(item_line) - len(stripped)
                list_items.append((indent // 2, stripped[2:].strip()))
                index += 1
            parts.append(render_markdown_list_html(list_items))
            continue

        paragraph_lines = [line]
        index += 1
        while index < len(lines):
            next_raw = lines[index]
            next_line = next_raw.strip()
            if (
                not next_line
                or next_line.startswith("```")
                or markdown_heading_level(next_line)
                or next_line.startswith(">")
                or is_markdown_list_line(next_raw)
                or is_markdown_table_start(lines, index)
            ):
                break
            paragraph_lines.append(next_line)
            index += 1
        paragraph = " ".join(paragraph_lines)
        parts.append(f"<p>{render_inline_markdown(paragraph)}</p>")

    return "\n".join(parts) or '<div class="empty">报告正文为空。</div>'


def markdown_heading_level(line: str) -> int:
    level = len(line) - len(line.lstrip("#"))
    if 1 <= level <= 6 and len(line) > level and line[level].isspace():
        return level
    return 0


def is_markdown_list_line(line: str) -> bool:
    stripped = line.lstrip()
    return len(stripped) > 2 and stripped[:2] in {"- ", "* "}


def render_markdown_list_html(items: list[tuple[int, str]]) -> str:
    if not items:
        return ""
    lines = ['<ul class="report-list">']
    for depth, text in items:
        normalized_depth = min(max(depth, 0), 3)
        lines.append(
            f'<li class="depth-{normalized_depth}">{render_inline_markdown(text)}</li>'
        )
    lines.append("</ul>")
    return "\n".join(lines)


def is_markdown_table_start(lines: list[str], index: int) -> bool:
    if index + 1 >= len(lines):
        return False
    header = lines[index].strip()
    divider = split_markdown_table_row(lines[index + 1])
    return "|" in header and bool(divider) and all(is_markdown_table_divider(cell) for cell in divider)


def is_markdown_table_divider(cell: str) -> bool:
    marker = cell.strip()
    if not marker:
        return False
    without_colons = marker.replace(":", "")
    return len(without_colons) >= 3 and set(without_colons) == {"-"}


def render_markdown_table_html(lines: list[str], index: int) -> tuple[str, int]:
    header = split_markdown_table_row(lines[index])
    alignments = [
        "right" if cell.strip().endswith(":") else "left"
        for cell in split_markdown_table_row(lines[index + 1])
    ]
    index += 2
    rows: list[list[str]] = []
    while index < len(lines) and "|" in lines[index]:
        row = split_markdown_table_row(lines[index])
        if not row:
            break
        rows.append(row)
        index += 1

    head = "".join(
        f'<th class="align-{alignments[column] if column < len(alignments) else "left"}">'
        f"{render_inline_markdown(cell)}</th>"
        for column, cell in enumerate(header)
    )
    body = []
    for row in rows:
        cells = []
        for column, cell in enumerate(row):
            alignment = alignments[column] if column < len(alignments) else "left"
            cells.append(
                f'<td class="align-{alignment}">{render_inline_markdown(cell)}</td>'
            )
        body.append(f"<tr>{''.join(cells)}</tr>")

    html_table = (
        '<div class="report-table-wrap">'
        '<table class="report-markdown-table">'
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody>"
        "</table>"
        "</div>"
    )
    return html_table, index


def split_markdown_table_row(row: str) -> list[str]:
    text = row.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]

    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if escaped:
        current.append("\\")
    cells.append("".join(current).strip())
    return cells


def render_inline_markdown(text: str) -> str:
    parts: list[str] = []
    cursor = 0
    for match in MARKDOWN_INLINE_PATTERN.finditer(text):
        parts.append(e(text[cursor : match.start()]))
        code_text = match.group(1)
        link_text = match.group(2)
        link_url = match.group(3)
        if code_text is not None:
            parts.append(f"<code>{e(code_text)}</code>")
        elif link_text is not None and link_url is not None:
            safe_url = safe_markdown_link_url(link_url)
            label = render_inline_markdown(link_text)
            if safe_url:
                parts.append(
                    f'<a href="{safe_url}" target="_blank" rel="noreferrer">{label}</a>'
                )
            else:
                parts.append(label)
        cursor = match.end()
    parts.append(e(text[cursor:]))
    return "".join(parts)


def safe_markdown_link_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme in {"http", "https", "mailto"}:
        return e(url)
    return ""


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


def format_topic_badges(value: Any, *, limit: int = 5) -> str:
    if isinstance(value, str):
        topics = [item.strip() for item in value.split(",")]
    elif isinstance(value, list | tuple):
        topics = [str(item).strip() for item in value]
    else:
        topics = []
    topics = [topic for topic in topics if topic]
    if not topics:
        return "-"

    unique_topics = list(dict.fromkeys(topics))
    clipped = unique_topics[:limit]
    badges = [
        f'<span class="badge topic">{e(topic)}</span>'
        for topic in clipped
    ]
    if len(unique_topics) > limit:
        badges.append(f'<span class="badge muted">+{len(unique_topics) - limit}</span>')
    return f'<span class="topic-badges">{"".join(badges)}</span>'


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


def render_topic_cloud_panel(
    title: str,
    rows: list[dict[str, Any]],
    *,
    cloud_type: str,
) -> str:
    terms = build_topic_cloud_terms(rows, cloud_type=cloud_type)
    if not terms:
        content = empty_state("暂无可生成词云的数据。")
        count = "0"
    else:
        content = render_topic_cloud_svg(terms, title=title, cloud_id=cloud_type)
        count = str(len(terms))

    return f"""
    <div class="topic-cloud-panel">
      <div class="topic-cloud-heading">
        <h2>{e(title)}</h2>
        <span>{e(count)} 词</span>
      </div>
      {content}
    </div>
    """


def build_topic_cloud_terms(
    rows: list[dict[str, Any]],
    *,
    cloud_type: str,
) -> list[tuple[str, float]]:
    weights: dict[str, float] = {}
    labels: dict[str, str] = {}

    def add_term(value: Any, weight: float) -> None:
        label = normalize_cloud_text(value)
        if not label:
            return
        key = label.casefold()
        weights[key] = weights.get(key, 0.0) + max(weight, 0.35)
        labels.setdefault(key, label)

    for row in rows:
        if cloud_type == "candidates":
            base = (
                6.0
                + numeric_value(row.get("trend_score")) * 1.15
                + numeric_value(row.get("article_count")) * 1.25
                + numeric_value(row.get("source_count")) * 1.6
                + numeric_value(row.get("ticker_count")) * 1.25
            )
            label = row.get("topic_name") or row.get("candidate_slug")
            add_term(label, base + 5.0)
            add_term(str(row.get("candidate_slug") or "").replace("_", " "), base * 0.45)
        else:
            active_bonus = 2.0 if row.get("is_active") else 0.0
            base = (
                5.0
                + active_bonus
                + numeric_value(row.get("recent_mentions")) * 2.4
                + numeric_value(row.get("mention_count")) * 0.4
                + numeric_value(row.get("ticker_count")) * 1.2
            )
            priority = numeric_value(row.get("priority"))
            if priority > 0:
                base += max(0.0, 6.0 - min(priority, 6.0))
            label = row.get("topic_name") or row.get("topic_slug")
            add_term(label, base + 5.0)
            add_term(str(row.get("topic_slug") or "").replace("_", " "), base * 0.4)

        for keyword in cloud_list_items(row.get("keywords"))[:12]:
            add_term(keyword, base * 0.5 + 1.0)
        for ticker in cloud_list_items(row.get("ticker_hints"))[:8]:
            add_term(str(ticker).upper(), base * 0.22 + 0.75)

    terms = sorted(
        ((labels[key], weight) for key, weight in weights.items()),
        key=lambda item: (-item[1], item[0].casefold()),
    )
    return terms[:TOPIC_CLOUD_MAX_TERMS]


def render_topic_cloud_svg(
    terms: list[tuple[str, float]],
    *,
    title: str,
    cloud_id: str,
) -> str:
    placed = layout_topic_cloud_terms(terms)
    if not placed:
        return empty_state("暂无可生成词云的数据。")

    title_id = f"topic-cloud-title-{cloud_id}"
    clip_id = f"topic-cloud-clip-{cloud_id}"
    words = []
    for index, item in enumerate(placed):
        color = TOPIC_CLOUD_PALETTE[index % len(TOPIC_CLOUD_PALETTE)]
        words.append(
            f"""
            <text
              class="topic-cloud-word"
              x="{item['x']:.1f}"
              y="{item['y']:.1f}"
              font-size="{item['font_size']:.1f}"
              fill="{color}"
            >{e(item["text"])}</text>
            """
        )

    return f"""
    <figure class="topic-cloud-figure">
      <svg class="topic-cloud-svg" viewBox="0 0 {TOPIC_CLOUD_SIZE} {TOPIC_CLOUD_SIZE}" role="img" aria-labelledby="{title_id}">
        <title id="{title_id}">{e(title)}</title>
        <defs>
          <clipPath id="{clip_id}">
            <circle cx="{TOPIC_CLOUD_SIZE / 2:.0f}" cy="{TOPIC_CLOUD_SIZE / 2:.0f}" r="{TOPIC_CLOUD_RADIUS}" />
          </clipPath>
        </defs>
        <circle class="topic-cloud-bg" cx="{TOPIC_CLOUD_SIZE / 2:.0f}" cy="{TOPIC_CLOUD_SIZE / 2:.0f}" r="{TOPIC_CLOUD_RADIUS}" />
        <g clip-path="url(#{clip_id})">
          {"".join(words)}
        </g>
      </svg>
    </figure>
    """


def layout_topic_cloud_terms(terms: list[tuple[str, float]]) -> list[dict[str, Any]]:
    if not terms:
        return []

    weights = [weight for _, weight in terms]
    min_weight = min(weights)
    max_weight = max(weights)
    span = max(max_weight - min_weight, 1.0)
    center = TOPIC_CLOUD_SIZE / 2
    placed: list[dict[str, Any]] = []
    boxes: list[tuple[float, float, float, float]] = []

    for index, (text, weight) in enumerate(terms):
        emphasis = math.sqrt(max(weight - min_weight, 0.0) / span)
        font_size = 12.0 + emphasis * 25.0
        if index == 0:
            font_size = max(font_size, 32.0)
        text_units = estimate_cloud_text_units(text)
        font_size = min(font_size, max(12.0, 330.0 / max(text_units, 1.0)))

        item = place_cloud_word(text, font_size, index, center, boxes)
        if item is None:
            item = place_cloud_word(text, max(11.0, font_size - 5.0), index, center, boxes)
        if item is None:
            continue

        placed.append(item)
        boxes.append(item["box"])

    return center_topic_cloud_terms(placed, center)


def center_topic_cloud_terms(
    placed: list[dict[str, Any]],
    center: float,
) -> list[dict[str, Any]]:
    if not placed:
        return placed

    total_weight = sum(item["font_size"] ** 1.4 for item in placed)
    visual_x = sum(item["x"] * item["font_size"] ** 1.4 for item in placed) / total_weight
    visual_y = sum(item["y"] * item["font_size"] ** 1.4 for item in placed) / total_weight
    min_x = min(item["box"][0] for item in placed)
    min_y = min(item["box"][1] for item in placed)
    max_x = max(item["box"][2] for item in placed)
    max_y = max(item["box"][3] for item in placed)
    bounds_x = (min_x + max_x) / 2
    bounds_y = (min_y + max_y) / 2
    target_x = visual_x * 0.68 + bounds_x * 0.32
    target_y = visual_y * 0.68 + bounds_y * 0.32
    dx = max(-48.0, min(48.0, center - target_x))
    dy = max(-48.0, min(48.0, center - target_y))
    if abs(dx) < 0.5 and abs(dy) < 0.5:
        return placed

    for factor in (1.0, 0.75, 0.5, 0.25):
        shift_x = dx * factor
        shift_y = dy * factor
        shifted_boxes = [
            (
                item["box"][0] + shift_x,
                item["box"][1] + shift_y,
                item["box"][2] + shift_x,
                item["box"][3] + shift_y,
            )
            for item in placed
        ]
        if all(box_inside_cloud(box, center) for box in shifted_boxes):
            for item, box in zip(placed, shifted_boxes, strict=True):
                item["x"] += shift_x
                item["y"] += shift_y
                item["box"] = box
            break

    return placed


def place_cloud_word(
    text: str,
    font_size: float,
    index: int,
    center: float,
    boxes: list[tuple[float, float, float, float]],
) -> dict[str, Any] | None:
    width = estimate_cloud_text_units(text) * font_size
    height = font_size * 1.08
    half_width = width / 2
    half_height = height / 2
    half_diagonal = math.sqrt(half_width * half_width + half_height * half_height)
    max_distance = TOPIC_CLOUD_RADIUS - half_diagonal - 5
    if max_distance <= 0:
        return None

    start_step = 0 if index == 0 else index * 4
    for step in range(start_step, start_step + 720):
        angle = step * 2.399963229728653 + index * 0.43
        distance = min(max_distance, 4.2 * math.sqrt(step))
        x = center + math.cos(angle) * distance
        y = center + math.sin(angle) * distance
        box = (x - half_width - 3, y - half_height - 3, x + half_width + 3, y + half_height + 3)
        if not box_inside_cloud(box, center):
            continue
        if any(boxes_overlap(box, existing) for existing in boxes):
            continue
        return {
            "text": text,
            "font_size": font_size,
            "x": x,
            "y": y,
            "box": box,
        }
    return None


def box_inside_cloud(box: tuple[float, float, float, float], center: float) -> bool:
    for x, y in (
        (box[0], box[1]),
        (box[0], box[3]),
        (box[2], box[1]),
        (box[2], box[3]),
    ):
        if math.hypot(x - center, y - center) > TOPIC_CLOUD_RADIUS - 3:
            return False
    return True


def boxes_overlap(
    first: tuple[float, float, float, float],
    second: tuple[float, float, float, float],
) -> bool:
    return not (
        first[2] < second[0]
        or first[0] > second[2]
        or first[3] < second[1]
        or first[1] > second[3]
    )


def estimate_cloud_text_units(text: str) -> float:
    units = 0.0
    for char in text:
        if char.isspace():
            units += 0.34
        elif ord(char) < 128:
            units += 0.58
        else:
            units += 0.96
    return max(units, 1.0)


def normalize_cloud_text(value: Any) -> str:
    text = " ".join(str(value or "").replace("_", " ").split())
    if not text or text == "-":
        return ""
    if len(text) > TOPIC_CLOUD_TEXT_LIMIT:
        text = clip_text(text, TOPIC_CLOUD_TEXT_LIMIT)
    return text


def cloud_list_items(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, list | tuple):
        raw_items = value
    else:
        raw_items = []
    return [str(item).strip() for item in raw_items if str(item).strip()]


def numeric_value(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return 0.0


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


def table(head: str, body: str, *, page_size: int = TABLE_PAGE_SIZE) -> str:
    resolved_page_size = (
        page_size if page_size in TABLE_PAGE_SIZE_OPTIONS else TABLE_PAGE_SIZE
    )
    page_size_options = "".join(
        (
            f'<option value="{option}"'
            f'{" selected" if option == resolved_page_size else ""}>{option}</option>'
        )
        for option in TABLE_PAGE_SIZE_OPTIONS
    )
    return (
        '<div class="table-panel" data-table-panel '
        f'data-page-size="{resolved_page_size}"><div class="table-wrap">'
        f"<table><thead>{head}</thead><tbody>{body}</tbody></table>"
        "</div>"
        '<div class="table-pagination" data-pagination hidden>'
        '<span class="table-page-info" data-page-info></span>'
        '<div class="table-page-actions">'
        '<label class="table-page-size-control">每页 '
        f'<select data-page-size-select aria-label="每页条数">{page_size_options}</select>'
        "</label>"
        '<button type="button" data-page-prev aria-label="上一页">上一页</button>'
        '<button type="button" data-page-next aria-label="下一页">下一页</button>'
        "</div>"
        "</div>"
        "</div>"
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
    admin_access = current_admin_access()
    admin_readonly = admin_access.protected and not admin_access.allowed
    app_class = "app-shell admin-readonly" if admin_readonly else "app-shell"
    admin_readonly_js = "true" if admin_readonly else "false"
    admin_badge_html = ""
    readonly_notice_html = ""
    sidebar_note = (
        "轻量本地面板，无登录和权限系统。数据动作仍由当前 Python 服务执行。"
    )
    if admin_access.protected:
        if admin_access.allowed:
            admin_badge_html = '<span class="admin-mode-badge ok">管理权限</span>'
            sidebar_note = (
                "当前浏览器已具备管理员动作权限，可执行同步、报告和数据维护。"
            )
        else:
            admin_badge_html = '<span class="admin-mode-badge readonly">只读浏览</span>'
            sidebar_note = (
                "当前为只读浏览，访客可以查看数据，但不能执行同步、报告和维护动作。"
            )
            readonly_notice_html = """
            <div class="notice readonly" role="status">
              <span>当前为只读浏览，数据同步、报告生成和维护动作仅管理员可执行。</span>
            </div>
            """
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
        ("/topics", "主题", "热点与评分"),
        ("/reports", "报告", "分析输出"),
        ("/backtest", "复盘", "日报验证"),
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
  <script>
    window.usstockPanelTheme = {{
      key: "usstock-panel-light",
      read() {{
        try {{
          return localStorage.getItem(this.key) !== "off";
        }} catch (error) {{
          return true;
        }}
      }},
      write(value) {{
        try {{
          localStorage.setItem(this.key, value ? "on" : "off");
        }} catch (error) {{
          // Keep the visual state even when storage is unavailable.
        }}
        document.documentElement.dataset.lightMode = value ? "on" : "off";
      }},
    }};
    document.documentElement.dataset.lightMode = window.usstockPanelTheme.read() ? "on" : "off";
  </script>
  <script>
    window.usstockTablePager = {{
      init(root) {{
        const scope = root || document;
        const panels = [];
        if (scope.matches && scope.matches("[data-table-panel]")) {{
          panels.push(scope);
        }}
        if (scope.querySelectorAll) {{
          panels.push(...scope.querySelectorAll("[data-table-panel]"));
        }}

        panels.forEach((panel) => {{
          if (panel.dataset.tablePagerReady === "1") {{
            return;
          }}
          panel.dataset.tablePagerReady = "1";

          const rows = Array.from(panel.querySelectorAll("tbody tr"));
          const controls = panel.querySelector("[data-pagination]");
          const info = panel.querySelector("[data-page-info]");
          const pageSizeSelect = panel.querySelector("[data-page-size-select]");
          const prev = panel.querySelector("[data-page-prev]");
          const next = panel.querySelector("[data-page-next]");
          if (!controls || !info || !prev || !next || rows.length === 0) {{
            return;
          }}

          const allowedPageSizes = {list(TABLE_PAGE_SIZE_OPTIONS)};
          const fallbackPageSize = {TABLE_PAGE_SIZE};
          const smallestPageSize = Math.min(...allowedPageSizes);
          const parsePageSize = (value) => {{
            const parsed = Number.parseInt(value || String(fallbackPageSize), 10);
            return allowedPageSizes.includes(parsed) ? parsed : fallbackPageSize;
          }};
          let pageSize = parsePageSize(panel.dataset.pageSize);
          let page = 1;

          const render = () => {{
            const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
            page = Math.min(Math.max(page, 1), totalPages);
            const start = (page - 1) * pageSize;
            const end = start + pageSize;
            rows.forEach((row, index) => {{
              row.hidden = index < start || index >= end;
            }});
            controls.hidden = rows.length <= smallestPageSize;
            info.textContent = "第 " + page + " / " + totalPages + " 页 · 共 " + rows.length + " 条";
            prev.disabled = page <= 1;
            next.disabled = page >= totalPages;
          }};

          if (pageSizeSelect) {{
            pageSizeSelect.value = String(pageSize);
            pageSizeSelect.addEventListener("change", () => {{
              pageSize = parsePageSize(pageSizeSelect.value);
              panel.dataset.pageSize = String(pageSize);
              page = 1;
              render();
            }});
          }}

          prev.addEventListener("click", () => {{
            page -= 1;
            render();
          }});
          next.addEventListener("click", () => {{
            page += 1;
            render();
          }});
          render();
        }});
      }},
    }};

    document.addEventListener("DOMContentLoaded", () => {{
      window.usstockTablePager.init(document);
    }});
    document.addEventListener("htmx:afterSettle", (event) => {{
      window.usstockTablePager.init(event.target || document);
    }});
  </script>
  <script>
    window.usstockAdminAccess = {{
      readonly: {admin_readonly_js},
      tokenName: "{e(ADMIN_ACTION_TOKEN_QUERY_PARAM)}",
      token: new URLSearchParams(window.location.search).get("{e(ADMIN_ACTION_TOKEN_QUERY_PARAM)}") || "",
      apply(root) {{
        this.applyAdminToken(root);
        if (!this.readonly) {{
          return;
        }}
        const scope = root || document;
        const forms = [];
        if (scope.matches && scope.matches('form[method="post"]')) {{
          forms.push(scope);
        }}
        if (scope.querySelectorAll) {{
          forms.push(...scope.querySelectorAll('form[method="post"]'));
        }}

        forms.forEach((form) => {{
          if (form.dataset.adminReadonlyReady === "1") {{
            return;
          }}
          form.dataset.adminReadonlyReady = "1";
          form.dataset.adminReadonly = "1";
          form.setAttribute("aria-disabled", "true");
          form.addEventListener("submit", (event) => {{
            event.preventDefault();
          }});
          form.querySelectorAll("input, textarea, select, button").forEach((element) => {{
            if (element.matches('input[type="hidden"]')) {{
              return;
            }}
            element.disabled = true;
            element.title = "只读浏览";
          }});
        }});
      }},
      applyAdminToken(root) {{
        if (!this.token) {{
          return;
        }}
        const scope = root || document;
        const forms = [];
        if (scope.matches && scope.matches('form[method="post"]')) {{
          forms.push(scope);
        }}
        if (scope.querySelectorAll) {{
          forms.push(...scope.querySelectorAll('form[method="post"]'));
        }}
        forms.forEach((form) => {{
          let input = form.querySelector('input[type="hidden"][name="' + this.tokenName + '"]');
          if (!input) {{
            input = document.createElement("input");
            input.type = "hidden";
            input.name = this.tokenName;
            form.appendChild(input);
          }}
          input.value = this.token;
        }});

        const links = [];
        if (scope.matches && scope.matches("a[href]")) {{
          links.push(scope);
        }}
        if (scope.querySelectorAll) {{
          links.push(...scope.querySelectorAll("a[href]"));
        }}
        links.forEach((link) => {{
          const href = link.getAttribute("href") || "";
          if (
            !href ||
            href.startsWith("#") ||
            href.startsWith("mailto:") ||
            href.startsWith("tel:") ||
            href.startsWith("javascript:")
          ) {{
            return;
          }}
          let url;
          try {{
            url = new URL(href, window.location.href);
          }} catch (error) {{
            return;
          }}
          if (url.origin !== window.location.origin || url.searchParams.has(this.tokenName)) {{
            return;
          }}
          url.searchParams.set(this.tokenName, this.token);
          link.href = url.pathname + url.search + url.hash;
        }});
      }},
    }};

    document.addEventListener("DOMContentLoaded", () => {{
      window.usstockAdminAccess.apply(document);
    }});
    document.addEventListener("htmx:afterSettle", (event) => {{
      window.usstockAdminAccess.apply(event.target || document);
    }});
  </script>
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
      --topbar-bg: rgba(246, 247, 248, 0.92);
      --input-bg: #ffffff;
      --button-bg: #ffffff;
      --button-hover: #f8faf9;
      --disabled-bg: #eef1ef;
      --table-row-line: #edf0ed;
      --brand-mark-bg: #dfeee8;
      --brand-mark-color: #13795b;
      --primary: #13795b;
      --primary-hover: #0f6049;
      --accent: #a15c10;
      --ok: #237847;
      --watch: #996600;
      --error: #b3261e;
      --badge-muted-bg: #edf2ef;
      --badge-ok-bg: #e4f4ea;
      --badge-watch-bg: #fff2cc;
      --notice-ok-bg: #f1faf4;
      --notice-error-bg: #fff5f4;
      --light-toggle-bg: #fff8df;
      --light-toggle-border: #e8c969;
      --light-toggle-glow: rgba(255, 199, 44, 0.5);
      --shadow: 0 18px 50px rgba(31, 37, 35, 0.08);
    }}
    html[data-light-mode="off"] {{
      color-scheme: dark;
    }}
    html[data-light-mode="off"] body {{
      background: #0f1512;
    }}
    * {{
      box-sizing: border-box;
    }}
    [x-cloak] {{
      display: none !important;
    }}
    [hidden] {{
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
      background: var(--bg);
      transition: background-color 200ms ease, color 200ms ease;
    }}
    .app-shell.lights-off,
    html[data-light-mode="off"] .app-shell {{
      color-scheme: dark;
      --bg: #0f1512;
      --surface: #171f1b;
      --surface-soft: #111814;
      --ink: #e8f0ec;
      --muted: #a3b2ab;
      --line: #2b3731;
      --line-strong: #3a4942;
      --nav: #070b09;
      --nav-soft: #132019;
      --topbar-bg: rgba(15, 21, 18, 0.9);
      --input-bg: #111814;
      --button-bg: #19231e;
      --button-hover: #213027;
      --disabled-bg: #1d2722;
      --table-row-line: #25312c;
      --brand-mark-bg: #f4c842;
      --brand-mark-color: #121712;
      --primary: #63d3a5;
      --primary-hover: #8ae8c5;
      --accent: #e2a64b;
      --ok: #73d89d;
      --watch: #f0c462;
      --error: #ff9a91;
      --badge-muted-bg: #22302a;
      --badge-ok-bg: #143524;
      --badge-watch-bg: #3a2d13;
      --notice-ok-bg: #10291c;
      --notice-error-bg: #321817;
      --light-toggle-bg: #151d19;
      --light-toggle-border: #34433c;
      --light-toggle-glow: rgba(99, 211, 165, 0);
      --shadow: 0 18px 54px rgba(0, 0, 0, 0.26);
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
      background: var(--brand-mark-bg);
      color: var(--brand-mark-color);
      font-weight: 800;
      box-shadow: 0 0 0 rgba(255, 208, 72, 0);
      transition: background-color 180ms ease, color 180ms ease, box-shadow 180ms ease;
    }}
    .app-shell.lights-on .brand-mark {{
      box-shadow: 0 0 22px rgba(244, 200, 66, 0.32);
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
    .admin-mode-badge {{
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 0 10px;
      border: 1px solid var(--line-strong);
      border-radius: 999px;
      color: var(--muted);
      background: var(--surface);
      font-size: 12px;
      font-weight: 760;
      white-space: nowrap;
    }}
    .admin-mode-badge.ok {{
      border-color: #b8dfc5;
      color: var(--ok);
      background: var(--notice-ok-bg);
    }}
    .admin-mode-badge.readonly {{
      border-color: #eddb9a;
      color: var(--watch);
      background: var(--badge-watch-bg);
    }}
    .pane-breadcrumbs {{
      display: flex;
      align-items: center;
      gap: 0;
      min-width: 0;
      overflow-x: auto;
      scrollbar-width: thin;
    }}
    .pane-crumb {{
      position: relative;
      display: inline-flex;
      align-items: center;
      flex: 0 0 auto;
      min-height: 34px;
      padding: 0 9px;
      border-radius: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
      text-decoration: none;
      transition: background 140ms ease, color 140ms ease;
    }}
    .pane-crumb + .pane-crumb::before {{
      content: "/";
      margin-right: 9px;
      color: var(--line-strong);
      font-weight: 500;
    }}
    .pane-crumb:hover, .pane-crumb.active {{
      background: var(--surface-soft);
      color: var(--primary);
    }}
    .pane-breadcrumbs {{
      margin-top: 14px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--line);
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
      background: var(--topbar-bg);
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
      flex: 0 0 auto;
    }}
    button.light-toggle {{
      gap: 8px;
      min-width: 82px;
      border-color: var(--light-toggle-border);
      background: var(--light-toggle-bg);
      box-shadow: 0 0 18px var(--light-toggle-glow);
    }}
    button.light-toggle:hover {{
      border-color: var(--light-toggle-border);
      background: var(--light-toggle-bg);
    }}
    .light-bulb {{
      position: relative;
      width: 15px;
      height: 15px;
      border-radius: 50% 50% 45% 45%;
      background: #ffd45a;
      box-shadow: 0 0 14px rgba(255, 207, 65, 0.76);
      transition: background-color 180ms ease, box-shadow 180ms ease, transform 180ms ease;
    }}
    .light-bulb::before {{
      content: "";
      position: absolute;
      left: 4px;
      right: 4px;
      bottom: -5px;
      height: 6px;
      border-radius: 2px;
      background: #9b7a22;
    }}
    .light-bulb::after {{
      content: "";
      position: absolute;
      top: 3px;
      right: 3px;
      width: 4px;
      height: 4px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.86);
    }}
    .app-shell.lights-off .light-bulb,
    html[data-light-mode="off"] .app-shell .light-bulb {{
      background: #59675f;
      box-shadow: none;
      transform: translateY(1px);
    }}
    .app-shell.lights-off .light-bulb::before,
    html[data-light-mode="off"] .app-shell .light-bulb::before {{
      background: #36413b;
    }}
    .app-shell.lights-off .light-bulb::after,
    html[data-light-mode="off"] .app-shell .light-bulb::after {{
      opacity: 0.28;
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
      width: 100%;
      min-height: calc(100vh - 64px);
      margin: 0;
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
    h3 {{
      margin: 0;
      color: var(--ink);
      font-size: 13px;
      line-height: 1.3;
    }}
    p {{
      margin: 0 0 14px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .page-kicker {{
      margin: 5px 0 0;
      font-size: 13px;
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
      transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
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
      transition: background-color 180ms ease, border-color 180ms ease;
    }}
    .topic-clouds {{
      align-items: stretch;
    }}
    .topic-cloud-panel {{
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 12px;
    }}
    .topic-cloud-heading {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    .topic-cloud-heading h2 {{
      margin: 0;
    }}
    .topic-cloud-heading span {{
      display: inline-flex;
      align-items: center;
      height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      color: var(--muted);
      background: var(--surface-soft);
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .topic-cloud-figure {{
      display: grid;
      place-items: center;
      min-height: 348px;
      margin: 0;
      padding: 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      transition: background-color 180ms ease, border-color 180ms ease;
    }}
    .topic-cloud-svg {{
      display: block;
      width: min(100%, 420px);
      height: auto;
    }}
    .topic-cloud-bg {{
      fill: var(--surface-soft);
      stroke: var(--line);
      stroke-width: 1.2;
    }}
    .topic-cloud-word {{
      font-family: var(--pico-font-family-sans-serif);
      font-weight: 760;
      text-anchor: middle;
      dominant-baseline: middle;
    }}
    .task-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(270px, 1fr));
      gap: 18px;
      align-items: flex-start;
    }}
    .narrow-grid {{
      grid-template-columns: repeat(auto-fit, minmax(320px, 520px));
    }}
    .task-grid form, .task-card {{
      display: grid;
      gap: 12px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 0 rgba(31, 37, 35, 0.02);
      transition: background-color 180ms ease, border-color 180ms ease;
    }}
    .task-section-header {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 12px;
    }}
    .task-section-header p {{
      margin: 4px 0 0;
      max-width: 760px;
    }}
    .discovery-panel {{
      display: grid;
      gap: 18px;
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 0 rgba(31, 37, 35, 0.02);
      transition: background-color 180ms ease, border-color 180ms ease;
    }}
    .task-panel-header {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 18px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--line);
    }}
    .task-panel-header p {{
      max-width: 740px;
      margin-bottom: 0;
    }}
    .eyebrow {{
      display: inline-block;
      margin-bottom: 7px;
      color: var(--primary);
      font-size: 12px;
      font-weight: 760;
    }}
    .form-sections {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }}
    .form-section {{
      display: grid;
      align-content: start;
      gap: 12px;
      min-width: 0;
    }}
    .field-grid, .checkbox-stack {{
      display: grid;
      gap: 12px;
    }}
    .inline-note {{
      margin: 0;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface-soft);
      font-size: 13px;
    }}
    .muted-card {{
      background: var(--surface-soft);
    }}
    .task-card-row {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
    }}
    .task-card-row p {{
      margin-bottom: 0;
      max-width: 760px;
    }}
    .status-line {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .status-line span {{
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 0 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      background: var(--surface);
      font-size: 12px;
      font-weight: 650;
    }}
    .progress-panel {{
      display: grid;
      gap: 12px;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface-soft);
      min-width: 0;
    }}
    .progress-heading {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      min-width: 0;
    }}
    .progress-heading h3 {{
      margin-bottom: 5px;
      color: var(--ink);
      font-size: 15px;
    }}
    .progress-heading p {{
      margin: 0;
      max-width: 780px;
      font-size: 13px;
      overflow-wrap: anywhere;
    }}
    .progress-meta {{
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
      min-width: 210px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }}
    .progress-track {{
      position: relative;
      overflow: hidden;
      width: 100%;
      height: 8px;
      border-radius: 999px;
      background: var(--line);
    }}
    .progress-fill {{
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--primary), var(--accent));
      transition: width 240ms ease;
    }}
    .progress-percent {{
      justify-self: end;
      margin-top: -8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 760;
    }}
    .stage-list {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 8px;
    }}
    .stage-row {{
      display: grid;
      grid-template-columns: 24px minmax(0, 1fr) auto;
      align-items: center;
      gap: 9px;
      min-height: 54px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }}
    .stage-mark {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 24px;
      height: 24px;
      border-radius: 999px;
      background: var(--badge-muted-bg);
      color: var(--muted);
      font-size: 13px;
      font-weight: 760;
    }}
    .stage-copy {{
      display: grid;
      gap: 3px;
      min-width: 0;
    }}
    .stage-copy strong {{
      overflow: hidden;
      color: var(--ink);
      font-size: 13px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .stage-copy span {{
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }}
    .stage-count {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 760;
      white-space: nowrap;
    }}
    .stage-row.is-running {{
      border-color: color-mix(in srgb, var(--primary) 42%, var(--line));
    }}
    .stage-row.is-running .stage-mark {{
      background: var(--badge-watch-bg);
      color: var(--watch);
    }}
    .stage-row.is-success .stage-mark {{
      background: var(--badge-ok-bg);
      color: var(--ok);
    }}
    .stage-row.is-warning .stage-mark,
    .stage-row.is-fail .stage-mark {{
      background: var(--notice-error-bg);
      color: var(--error);
    }}
    .progress-summary {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .progress-summary span {{
      min-height: 26px;
      padding: 4px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      background: var(--surface);
      font-size: 12px;
      font-weight: 650;
    }}
    .progress-log {{
      display: grid;
      gap: 7px;
      min-width: 0;
    }}
    .status-history {{
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }}
    .progress-log-title {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 760;
    }}
    .progress-log-row {{
      display: grid;
      grid-template-columns: 72px minmax(0, 1fr);
      gap: 8px;
      min-width: 0;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }}
    .progress-log-row time {{
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }}
    .progress-log-row > div {{
      display: grid;
      gap: 2px;
      min-width: 0;
    }}
    .progress-log-row span {{
      display: block;
      overflow-wrap: anywhere;
    }}
    .progress-log-meta {{
      color: var(--primary);
      font-size: 11px;
      font-weight: 760;
    }}
    .progress-log-row.log-warning span,
    .progress-log-row.log-error span {{
      color: var(--error);
    }}
    .button-row {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .inline-action-stack {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      min-width: 196px;
    }}
    .inline-action-form {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin: 0;
      min-width: 132px;
    }}
    .inline-action-form.compact-action-form {{
      min-width: 0;
    }}
    .inline-action-form button {{
      margin: 0;
      padding: 7px 10px;
      white-space: nowrap;
    }}
    .compact-checkbox {{
      display: inline-flex;
      align-items: center;
      gap: 5px;
      margin: 0;
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }}
    .compact-checkbox input {{
      margin: 0;
    }}
    .action-row {{
      justify-content: flex-end;
      min-width: 286px;
    }}
    .filters {{
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 10px;
      flex: 1 1 620px;
      flex-wrap: nowrap;
      min-width: 0;
      margin: 0;
    }}
    .filters input:not([type="checkbox"]), .filters select {{
      flex: 1 1 150px;
      width: auto;
      min-width: 128px;
      max-width: 190px;
      margin: 0;
    }}
    .filters select {{
      max-width: 220px;
    }}
    .filters button, .filters .button {{
      flex: 0 0 auto;
      width: auto;
      min-width: 0;
      margin: 0;
    }}
    .filters label {{
      flex: 0 0 auto;
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
      transition: background-color 180ms ease, border-color 180ms ease;
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
    input:not([type="checkbox"]), textarea, select {{
      width: 100%;
      min-height: 38px;
      padding: 8px 10px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      background: var(--input-bg);
      color: var(--ink);
      font: inherit;
      font-size: 14px;
      transition: background-color 180ms ease, border-color 180ms ease, color 180ms ease;
    }}
    textarea {{
      resize: vertical;
    }}
    input:not([type="checkbox"]), select {{
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
      width: auto;
      max-width: 100%;
      min-height: 38px;
      padding: 0 13px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      background: var(--button-bg);
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
      background: var(--button-hover);
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
    button:disabled, button:disabled:hover {{
      border-color: var(--line-strong);
      background: var(--disabled-bg);
      color: var(--muted);
      cursor: not-allowed;
      opacity: 0.78;
    }}
    .app-shell.admin-readonly form[method="post"] {{
      opacity: 0.78;
    }}
    .app-shell.admin-readonly form[method="post"] button {{
      cursor: not-allowed;
    }}
    .button.ghost {{
      background: transparent;
    }}
    .table-panel {{
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      transition: background-color 180ms ease, border-color 180ms ease;
    }}
    .pane-content .table-panel {{
      display: flex;
      flex-direction: column;
      width: 100%;
      height: calc(100vh - 278px);
      min-height: 520px;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    .pane-content .table-wrap {{
      flex: 1 1 auto;
      overflow: auto;
    }}
    .table-pagination {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 12px;
      border-top: 1px solid var(--line);
      background: var(--surface-soft);
    }}
    .table-page-info {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }}
    .table-page-actions {{
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .table-page-size-control {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      white-space: nowrap;
    }}
    .table-page-size-control select {{
      width: auto;
      min-width: 72px;
      min-height: 32px;
      margin: 0;
      padding: 0 28px 0 10px;
      font-size: 12px;
    }}
    .table-pagination button {{
      min-height: 32px;
      padding: 0 10px;
      font-size: 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
      margin: 0;
      font-size: 13px;
      color: var(--ink);
    }}
    .pane-content table {{
      min-width: max(100%, 1480px);
    }}
    th, td {{
      padding: 10px 11px;
      border-bottom: 1px solid var(--table-row-line);
      text-align: left;
      vertical-align: top;
    }}
    tbody tr:last-child td {{
      border-bottom: 0;
    }}
    tr.active-row td {{
      background: var(--surface-soft);
    }}
    th {{
      position: sticky;
      top: 0;
      z-index: 1;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      background: var(--surface-soft);
      white-space: nowrap;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      background: var(--badge-muted-bg);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .badge.ok {{
      background: var(--badge-ok-bg);
      color: var(--ok);
    }}
    .badge.watch {{
      background: var(--badge-watch-bg);
      color: var(--watch);
    }}
    .badge.muted {{
      background: var(--badge-muted-bg);
      color: var(--muted);
    }}
    .badge.error {{
      background: var(--notice-error-bg);
      color: var(--error);
    }}
    .badge.topic {{
      background: var(--badge-ok-bg);
      color: var(--ok);
    }}
    .topic-badges {{
      display: inline-flex;
      flex-wrap: wrap;
      gap: 4px;
      align-items: center;
      max-width: 280px;
      margin-top: 4px;
      vertical-align: middle;
    }}
    .report-preview {{
      max-height: 680px;
      overflow: auto;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface-soft);
      color: var(--ink);
      word-break: break-word;
      font-size: 14px;
      line-height: 1.65;
    }}
    .report-preview > *:first-child {{
      margin-top: 0;
    }}
    .report-preview > *:last-child {{
      margin-bottom: 0;
    }}
    .report-preview h3 {{
      margin: 0 0 14px;
      font-size: 20px;
      line-height: 1.3;
    }}
    .report-preview h4 {{
      margin: 22px 0 10px;
      font-size: 16px;
      line-height: 1.35;
    }}
    .report-preview h5,
    .report-preview h6 {{
      margin: 18px 0 8px;
      font-size: 14px;
      line-height: 1.4;
    }}
    .report-preview p,
    .report-preview blockquote,
    .report-preview .report-list,
    .report-preview .report-table-wrap,
    .report-preview .report-code {{
      margin: 0 0 14px;
    }}
    .report-preview blockquote {{
      padding: 10px 12px;
      border-left: 3px solid var(--primary);
      border-radius: 0 8px 8px 0;
      background: var(--surface);
      color: var(--muted);
    }}
    .report-preview .report-list {{
      padding-left: 20px;
    }}
    .report-preview li {{
      margin: 5px 0;
    }}
    .report-preview li.depth-1 {{
      margin-left: 18px;
      list-style-type: circle;
    }}
    .report-preview li.depth-2,
    .report-preview li.depth-3 {{
      margin-left: 36px;
      list-style-type: square;
    }}
    .report-preview code {{
      padding: 2px 5px;
      border-radius: 4px;
      background: var(--surface);
      color: var(--ink);
      font-size: 0.92em;
    }}
    .report-preview .report-code {{
      overflow: auto;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      white-space: pre;
    }}
    .report-preview .report-table-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }}
    .report-preview table {{
      width: 100%;
      min-width: 680px;
      font-size: 13px;
    }}
    .report-preview th {{
      position: static;
    }}
    .report-preview th,
    .report-preview td {{
      padding: 8px 10px;
    }}
    .report-preview .align-right {{
      text-align: right;
    }}
    .report-preview a {{
      word-break: break-word;
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
      background: var(--notice-ok-bg);
    }}
    .notice.error {{
      border-color: #edb8b4;
      color: var(--error);
      background: var(--notice-error-bg);
    }}
    .notice.readonly {{
      border-color: #eddb9a;
      color: var(--watch);
      background: var(--badge-watch-bg);
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
      transition: background-color 180ms ease, border-color 180ms ease;
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
      .task-panel-header, .task-section-header, .task-card-row {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .action-row {{
        width: 100%;
        min-width: 0;
        justify-content: flex-start;
      }}
      .action-row button {{
        flex: 1 1 128px;
      }}
      .progress-heading {{
        flex-direction: column;
      }}
      .progress-meta {{
        justify-content: flex-start;
        min-width: 0;
      }}
      .stage-list {{
        grid-template-columns: 1fr;
      }}
      .progress-log-row {{
        grid-template-columns: 64px minmax(0, 1fr);
      }}
      .form-sections {{
        grid-template-columns: 1fr;
      }}
      .split {{
        grid-template-columns: 1fr;
      }}
      .split > div, .task-grid form, .task-card, .status-panel, .discovery-panel {{
        padding: 14px;
      }}
      .topic-cloud-figure {{
        min-height: 286px;
      }}
      .grid-form {{
        padding: 14px;
      }}
      .filters {{
        width: 100%;
        align-items: stretch;
        flex-direction: column;
        flex-wrap: nowrap;
        justify-content: flex-start;
      }}
      .filters input:not([type="checkbox"]), .filters select,
      .filters button, .filters .button {{
        flex: 0 0 auto;
        width: 100%;
        min-width: 100%;
        max-width: none;
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
    class="{e(app_class)}"
    x-data="{{
      navOpen: false,
      lightsOn: window.usstockPanelTheme.read(),
      toggleLights() {{
        this.lightsOn = !this.lightsOn;
        window.usstockPanelTheme.write(this.lightsOn);
      }}
    }}"
    :class="{{'lights-off': !lightsOn, 'lights-on': lightsOn}}"
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
      <div class="sidebar-note">{e(sidebar_note)}</div>
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
          {admin_badge_html}
          <button
            class="light-toggle"
            type="button"
            :aria-label="lightsOn ? '关灯' : '开灯'"
            :title="lightsOn ? '关灯' : '开灯'"
            @click="toggleLights()"
          >
            <span class="light-bulb" aria-hidden="true"></span>
            <span x-text="lightsOn ? '关灯' : '开灯'">关灯</span>
          </button>
          <a class="button ghost" href="/tasks">同步任务</a>
        </div>
      </header>
      <main class="content">
        {notice_html}
        {readonly_notice_html}
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
    admin_action_token: str | None = None,
) -> None:
    database_url = get_database_url(database_url)
    configured_admin_token = admin_action_token or get_settings().admin_action_token
    resolved_admin_token = configured_admin_token or secrets.token_urlsafe(24)
    server = AdminHTTPServer(
        (host, port),
        AdminRequestHandler,
        database_url=database_url,
        admin_action_token=resolved_admin_token,
    )
    url = f"http://{host}:{port}"
    admin_url = (
        f"{url}/?{ADMIN_ACTION_TOKEN_QUERY_PARAM}="
        f"{urllib.parse.quote(resolved_admin_token)}"
    )
    write_admin_auth_log(
        "admin_server_start",
        {
            "host": host,
            "port": port,
            "project_root": str(PROJECT_ROOT),
            "admin_token_configured": bool(configured_admin_token),
            "admin_token_fingerprint": token_fingerprint(resolved_admin_token),
        },
    )
    print(f"本地管理面板已启动：{url}")
    print(f"管理员动作地址：{admin_url}")
    if not configured_admin_token:
        print("未配置 ADMIN_ACTION_TOKEN，本次启动使用临时管理员动作令牌。")
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
    parser.add_argument(
        "--admin-action-token",
        help=(
            "管理员动作令牌；未提供时读取 ADMIN_ACTION_TOKEN，"
            "否则启动时生成临时令牌"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        serve(
            host=args.host,
            port=args.port,
            database_url=args.database_url,
            admin_action_token=args.admin_action_token,
        )
        return 0
    except AdminPanelError as exc:
        print(f"管理面板启动失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
