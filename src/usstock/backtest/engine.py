"""Backtest engine entry points."""

from __future__ import annotations

import argparse
import statistics
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.db import migrations as db_migrations


DEFAULT_PROFILE = "default"
DEFAULT_TOP_N = 10
DEFAULT_HORIZONS = (1, 5, 20)
DEFAULT_PRICE_BUFFER_DAYS = 90
PERCENT_QUANT = Decimal("0.000001")
SUMMARY_QUANT = Decimal("0.0001")


class BacktestError(RuntimeError):
    """Raised when report backtesting fails."""


@dataclass(frozen=True)
class PricePoint:
    price_date: date
    close_price: Decimal
    data_source: str


@dataclass(frozen=True)
class ReportCandidate:
    run_date: date
    profile: str
    report_uid: str | None
    ticker: str
    company_name: str | None
    rank: int | None
    score: Decimal | None
    attention_label: str | None
    event_type: str | None
    risk_level: str | None
    primary_topic_slug: str | None
    topic_slugs: tuple[str, ...]
    action_bias: str | None
    source_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HorizonPerformance:
    horizon_days: int
    horizon_date: date
    close_price: Decimal
    return_pct: Decimal
    max_drawdown_pct: Decimal
    max_runup_pct: Decimal


@dataclass(frozen=True)
class CandidatePerformance:
    performance_uid: str
    candidate: ReportCandidate
    entry_date: date | None
    entry_close: Decimal | None
    price_source: str | None
    horizons: dict[int, HorizonPerformance]
    status: str
    missing_reason: str | None = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class BacktestResult:
    start_date: date
    end_date: date
    profile: str
    performances: tuple[CandidatePerformance, ...]
    summary: dict[str, Any]
    persisted: bool


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise BacktestError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def ensure_backtest_schema(database_url: str) -> int:
    applied = db_migrations.migrate(database_url=database_url)
    return len(applied)


def parse_run_date(value: str | None, *, default: date | None = None) -> date:
    if not value:
        if default is not None:
            return default
        raise BacktestError("缺少日期参数，格式应为 YYYY-MM-DD。")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise BacktestError(f"日期格式必须是 YYYY-MM-DD: {value}") from exc


def parse_decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_int_value(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker:
        raise BacktestError("日报候选缺少 ticker。")
    return ticker


def unique_strings(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list | tuple):
        return ()
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return tuple(result)


def candidate_sort_key(candidate: ReportCandidate) -> tuple[int, int, Decimal, str]:
    rank_missing = 1 if candidate.rank is None else 0
    rank = candidate.rank or 10_000
    score = candidate.score or Decimal("0")
    return (rank_missing, rank, -score, candidate.ticker)


def report_candidate_from_payload(
    *,
    run_date: date,
    profile: str,
    report_uid: str | None,
    payload: Mapping[str, Any],
) -> ReportCandidate:
    topics = unique_strings(payload.get("topics"))
    return ReportCandidate(
        run_date=run_date,
        profile=profile,
        report_uid=report_uid,
        ticker=normalize_ticker(payload.get("ticker")),
        company_name=str(payload["company_name"]) if payload.get("company_name") else None,
        rank=parse_int_value(payload.get("rank")),
        score=parse_decimal_value(payload.get("score")),
        attention_label=str(payload["attention_label"]) if payload.get("attention_label") else None,
        event_type=str(payload["event_type"]) if payload.get("event_type") else None,
        risk_level=str(payload["risk_level"]) if payload.get("risk_level") else None,
        primary_topic_slug=(
            str(payload["primary_topic_slug"])
            if payload.get("primary_topic_slug")
            else (topics[0] if topics else None)
        ),
        topic_slugs=topics,
        action_bias=str(payload["action_bias"]) if payload.get("action_bias") else None,
        source_payload=dict(payload),
    )


def fetch_report_candidates(
    conn: Connection,
    *,
    start_date: date,
    end_date: date,
    profile: str,
    top_n: int,
) -> list[ReportCandidate]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (run_date, profile)
               run_date, profile, report_uid, structured_payload
        FROM daily_analysis_reports
        WHERE run_date >= %s
          AND run_date <= %s
          AND profile = %s
          AND status = 'generated'
        ORDER BY run_date, profile, generated_at DESC, id DESC
        """,
        (start_date, end_date, profile),
    ).fetchall()

    candidates: list[ReportCandidate] = []
    for row in rows:
        payload = row[3] if isinstance(row[3], dict) else {}
        raw_candidates = payload.get("candidates")
        if not isinstance(raw_candidates, list):
            continue
        parsed = [
            report_candidate_from_payload(
                run_date=row[0],
                profile=row[1],
                report_uid=row[2],
                payload=item,
            )
            for item in raw_candidates
            if isinstance(item, dict) and item.get("ticker")
        ]
        parsed.sort(key=candidate_sort_key)
        candidates.extend(parsed[:top_n])
    return candidates


def fetch_price_points(
    conn: Connection,
    *,
    ticker: str,
    after_date: date,
    through_date: date,
    price_source: str | None,
) -> list[PricePoint]:
    rows = conn.execute(
        """
        SELECT price_date,
               COALESCE(adjusted_close_price, close_price) AS backtest_close,
               data_source
        FROM market_daily_prices
        WHERE ticker = %s
          AND price_date > %s
          AND price_date <= %s
          AND (CAST(%s AS text) IS NULL OR data_source = %s)
        ORDER BY price_date, data_source
        """,
        (ticker, after_date, through_date, price_source, price_source),
    ).fetchall()

    points_by_date: dict[date, PricePoint] = {}
    for row in rows:
        price_date = row[0]
        if price_date in points_by_date:
            continue
        close_price = parse_decimal_value(row[1])
        if close_price is None:
            continue
        points_by_date[price_date] = PricePoint(
            price_date=price_date,
            close_price=close_price,
            data_source=row[2],
        )
    return [points_by_date[key] for key in sorted(points_by_date)]


def percent_return(close_price: Decimal, entry_close: Decimal) -> Decimal:
    return ((close_price - entry_close) / entry_close * Decimal("100")).quantize(PERCENT_QUANT)


def build_horizon_performance(
    *,
    horizon_days: int,
    entry_close: Decimal,
    window: list[PricePoint],
) -> HorizonPerformance:
    target = window[horizon_days]
    returns = [percent_return(point.close_price, entry_close) for point in window[: horizon_days + 1]]
    return HorizonPerformance(
        horizon_days=horizon_days,
        horizon_date=target.price_date,
        close_price=target.close_price,
        return_pct=returns[-1],
        max_drawdown_pct=min(returns),
        max_runup_pct=max(returns),
    )


def performance_uid(candidate: ReportCandidate) -> str:
    report_part = candidate.report_uid or "candidate_scores"
    return f"daily_perf:{candidate.profile}:{candidate.run_date.isoformat()}:{report_part}:{candidate.ticker}"


def compute_candidate_performance(
    candidate: ReportCandidate,
    price_points: list[PricePoint],
    *,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
) -> CandidatePerformance:
    points = sorted(
        (point for point in price_points if point.price_date > candidate.run_date),
        key=lambda point: point.price_date,
    )
    if not points:
        return CandidatePerformance(
            performance_uid=performance_uid(candidate),
            candidate=candidate,
            entry_date=None,
            entry_close=None,
            price_source=None,
            horizons={},
            status="no_entry_price",
            missing_reason="日报日之后没有可用价格，无法确定入场参考价。",
        )

    entry = points[0]
    if entry.close_price <= 0:
        return CandidatePerformance(
            performance_uid=performance_uid(candidate),
            candidate=candidate,
            entry_date=entry.price_date,
            entry_close=entry.close_price,
            price_source=entry.data_source,
            horizons={},
            status="no_entry_price",
            missing_reason="入场参考价小于或等于 0，无法计算收益率。",
        )

    computed: dict[int, HorizonPerformance] = {}
    missing: list[int] = []
    for horizon in horizons:
        if len(points) <= horizon:
            missing.append(horizon)
            continue
        computed[horizon] = build_horizon_performance(
            horizon_days=horizon,
            entry_close=entry.close_price,
            window=points,
        )

    if len(computed) == len(horizons):
        status = "complete"
        missing_reason = None
    elif computed:
        status = "partial"
        missing_reason = "缺少部分后续交易日价格: " + ", ".join(f"T+{value}" for value in missing)
    else:
        status = "no_horizon_price"
        missing_reason = "有入场参考价，但没有足够的后续交易日价格。"

    return CandidatePerformance(
        performance_uid=performance_uid(candidate),
        candidate=candidate,
        entry_date=entry.price_date,
        entry_close=entry.close_price,
        price_source=entry.data_source,
        horizons=computed,
        status=status,
        missing_reason=missing_reason,
    )


def horizon_or_none(
    performance: CandidatePerformance,
    horizon_days: int,
) -> HorizonPerformance | None:
    return performance.horizons.get(horizon_days)


def performance_details(performance: CandidatePerformance) -> dict[str, Any]:
    candidate = performance.candidate
    return {
        "entry_rule": "use first available trading close after report run_date",
        "computed_at": performance.computed_at.isoformat(),
        "horizons": sorted(performance.horizons),
        "source_candidate": {
            "attention_label": candidate.attention_label,
            "event_type": candidate.event_type,
            "risk_level": candidate.risk_level,
            "source_payload": candidate.source_payload,
        },
    }


def upsert_candidate_performance(conn: Connection, performance: CandidatePerformance) -> None:
    horizons = {value: horizon_or_none(performance, value) for value in DEFAULT_HORIZONS}
    conn.execute(
        """
        INSERT INTO daily_candidate_performance (
            performance_uid,
            run_date,
            profile,
            report_uid,
            ticker,
            company_name,
            rank,
            score,
            attention_label,
            event_type,
            risk_level,
            primary_topic_slug,
            topic_slugs,
            action_bias,
            entry_date,
            entry_close,
            price_source,
            horizon_1d_date,
            horizon_1d_close,
            return_1d_pct,
            max_drawdown_1d_pct,
            max_runup_1d_pct,
            horizon_5d_date,
            horizon_5d_close,
            return_5d_pct,
            max_drawdown_5d_pct,
            max_runup_5d_pct,
            horizon_20d_date,
            horizon_20d_close,
            return_20d_pct,
            max_drawdown_20d_pct,
            max_runup_20d_pct,
            performance_status,
            missing_reason,
            details,
            computed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (performance_uid)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            rank = EXCLUDED.rank,
            score = EXCLUDED.score,
            attention_label = EXCLUDED.attention_label,
            event_type = EXCLUDED.event_type,
            risk_level = EXCLUDED.risk_level,
            primary_topic_slug = EXCLUDED.primary_topic_slug,
            topic_slugs = EXCLUDED.topic_slugs,
            action_bias = EXCLUDED.action_bias,
            entry_date = EXCLUDED.entry_date,
            entry_close = EXCLUDED.entry_close,
            price_source = EXCLUDED.price_source,
            horizon_1d_date = EXCLUDED.horizon_1d_date,
            horizon_1d_close = EXCLUDED.horizon_1d_close,
            return_1d_pct = EXCLUDED.return_1d_pct,
            max_drawdown_1d_pct = EXCLUDED.max_drawdown_1d_pct,
            max_runup_1d_pct = EXCLUDED.max_runup_1d_pct,
            horizon_5d_date = EXCLUDED.horizon_5d_date,
            horizon_5d_close = EXCLUDED.horizon_5d_close,
            return_5d_pct = EXCLUDED.return_5d_pct,
            max_drawdown_5d_pct = EXCLUDED.max_drawdown_5d_pct,
            max_runup_5d_pct = EXCLUDED.max_runup_5d_pct,
            horizon_20d_date = EXCLUDED.horizon_20d_date,
            horizon_20d_close = EXCLUDED.horizon_20d_close,
            return_20d_pct = EXCLUDED.return_20d_pct,
            max_drawdown_20d_pct = EXCLUDED.max_drawdown_20d_pct,
            max_runup_20d_pct = EXCLUDED.max_runup_20d_pct,
            performance_status = EXCLUDED.performance_status,
            missing_reason = EXCLUDED.missing_reason,
            details = EXCLUDED.details,
            computed_at = EXCLUDED.computed_at,
            updated_at = now()
        """,
        (
            performance.performance_uid,
            performance.candidate.run_date,
            performance.candidate.profile,
            performance.candidate.report_uid,
            performance.candidate.ticker,
            performance.candidate.company_name,
            performance.candidate.rank,
            performance.candidate.score,
            performance.candidate.attention_label,
            performance.candidate.event_type,
            performance.candidate.risk_level,
            performance.candidate.primary_topic_slug,
            list(performance.candidate.topic_slugs),
            performance.candidate.action_bias,
            performance.entry_date,
            performance.entry_close,
            performance.price_source,
            horizons[1].horizon_date if horizons[1] else None,
            horizons[1].close_price if horizons[1] else None,
            horizons[1].return_pct if horizons[1] else None,
            horizons[1].max_drawdown_pct if horizons[1] else None,
            horizons[1].max_runup_pct if horizons[1] else None,
            horizons[5].horizon_date if horizons[5] else None,
            horizons[5].close_price if horizons[5] else None,
            horizons[5].return_pct if horizons[5] else None,
            horizons[5].max_drawdown_pct if horizons[5] else None,
            horizons[5].max_runup_pct if horizons[5] else None,
            horizons[20].horizon_date if horizons[20] else None,
            horizons[20].close_price if horizons[20] else None,
            horizons[20].return_pct if horizons[20] else None,
            horizons[20].max_drawdown_pct if horizons[20] else None,
            horizons[20].max_runup_pct if horizons[20] else None,
            performance.status,
            performance.missing_reason,
            Jsonb(performance_details(performance)),
            performance.computed_at,
        ),
    )


def persist_performances(conn: Connection, performances: tuple[CandidatePerformance, ...]) -> int:
    with conn.transaction():
        for performance in performances:
            upsert_candidate_performance(conn, performance)
    return len(performances)


def mean_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return (sum(values) / Decimal(len(values))).quantize(SUMMARY_QUANT)


def median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(statistics.median(values)).quantize(SUMMARY_QUANT)


def format_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def summarize_horizon(
    performances: list[CandidatePerformance],
    horizon_days: int,
) -> dict[str, Any]:
    values = [
        performance.horizons[horizon_days].return_pct
        for performance in performances
        if horizon_days in performance.horizons
    ]
    drawdowns = [
        performance.horizons[horizon_days].max_drawdown_pct
        for performance in performances
        if horizon_days in performance.horizons
    ]
    runups = [
        performance.horizons[horizon_days].max_runup_pct
        for performance in performances
        if horizon_days in performance.horizons
    ]
    evaluated = len(values)
    wins = sum(1 for value in values if value > 0)
    win_rate = (
        (Decimal(wins) / Decimal(evaluated) * Decimal("100")).quantize(SUMMARY_QUANT)
        if evaluated
        else None
    )
    return {
        "horizon": f"T+{horizon_days}",
        "evaluated": evaluated,
        "win_count": wins,
        "win_rate_pct": format_decimal(win_rate),
        "avg_return_pct": format_decimal(mean_decimal(values)),
        "median_return_pct": format_decimal(median_decimal(values)),
        "avg_drawdown_pct": format_decimal(mean_decimal(drawdowns)),
        "avg_runup_pct": format_decimal(mean_decimal(runups)),
    }


def rank_bucket(performance: CandidatePerformance) -> str:
    rank = performance.candidate.rank
    if rank is not None and rank <= 3:
        return "Top 3"
    if rank is not None and rank <= 10:
        return "Top 10"
    return "Other"


def score_bucket(performance: CandidatePerformance) -> str:
    score = performance.candidate.score or Decimal("0")
    if score >= Decimal("60"):
        return "score >= 60"
    if score >= Decimal("40"):
        return "40 <= score < 60"
    return "score < 40"


def group_summary(
    performances: tuple[CandidatePerformance, ...],
    *,
    key_func: Callable[[CandidatePerformance], str],
    horizon_days: int = 5,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[CandidatePerformance]] = {}
    for performance in performances:
        grouped.setdefault(key_func(performance), []).append(performance)

    rows: list[dict[str, Any]] = []
    for key, values in grouped.items():
        horizon = summarize_horizon(values, horizon_days)
        rows.append(
            {
                "group": key,
                "count": len(values),
                "evaluated": horizon["evaluated"],
                "win_rate_pct": horizon["win_rate_pct"],
                "avg_return_pct": horizon["avg_return_pct"],
            }
        )
    rows.sort(key=lambda row: (-(row["evaluated"] or 0), row["group"]))
    return rows[:limit] if limit else rows


def primary_topic_key(performance: CandidatePerformance) -> str:
    return performance.candidate.primary_topic_slug or "unknown"


def summarize_performances(
    performances: tuple[CandidatePerformance, ...],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for performance in performances:
        status_counts[performance.status] = status_counts.get(performance.status, 0) + 1
    return {
        "candidate_count": len(performances),
        "status_counts": status_counts,
        "horizons": {
            str(horizon): summarize_horizon(list(performances), horizon)
            for horizon in DEFAULT_HORIZONS
        },
        "rank_buckets": group_summary(
            performances,
            key_func=rank_bucket,
            horizon_days=5,
        ),
        "score_buckets": group_summary(
            performances,
            key_func=score_bucket,
            horizon_days=5,
        ),
        "primary_topics": group_summary(
            performances,
            key_func=primary_topic_key,
            horizon_days=5,
            limit=10,
        ),
    }


def run_report_backtest(
    *,
    database_url: str | None = None,
    start_date: date,
    end_date: date,
    profile: str = DEFAULT_PROFILE,
    top_n: int = DEFAULT_TOP_N,
    price_source: str | None = None,
    persist: bool = True,
) -> BacktestResult:
    if end_date < start_date:
        raise BacktestError("结束日期不能早于开始日期。")
    if top_n <= 0:
        raise BacktestError("--top-n 必须大于 0。")

    database_url = get_database_url(database_url)
    ensure_backtest_schema(database_url)
    with psycopg.connect(database_url, autocommit=False) as conn:
        candidates = fetch_report_candidates(
            conn,
            start_date=start_date,
            end_date=end_date,
            profile=profile,
            top_n=top_n,
        )
        performances: list[CandidatePerformance] = []
        for candidate in candidates:
            through_date = candidate.run_date + timedelta(days=DEFAULT_PRICE_BUFFER_DAYS)
            points = fetch_price_points(
                conn,
                ticker=candidate.ticker,
                after_date=candidate.run_date,
                through_date=through_date,
                price_source=price_source,
            )
            performances.append(compute_candidate_performance(candidate, points))

        performance_tuple = tuple(performances)
        if persist and performance_tuple:
            persist_performances(conn, performance_tuple)

    return BacktestResult(
        start_date=start_date,
        end_date=end_date,
        profile=profile,
        performances=performance_tuple,
        summary=summarize_performances(performance_tuple),
        persisted=persist and bool(performance_tuple),
    )


def render_horizon_line(summary: Mapping[str, Any], horizon: int) -> str:
    item = summary["horizons"][str(horizon)]
    return (
        f"T+{horizon}: 样本 {item['evaluated']}，"
        f"胜率 {item['win_rate_pct'] or '-'}%，"
        f"平均收益 {item['avg_return_pct'] or '-'}%，"
        f"中位收益 {item['median_return_pct'] or '-'}%，"
        f"平均回撤 {item['avg_drawdown_pct'] or '-'}%。"
    )


def render_group_rows(title: str, rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    lines = [title]
    for row in rows:
        lines.append(
            f"- {row['group']}: 数量 {row['count']}，T+5 样本 {row['evaluated']}，"
            f"胜率 {row['win_rate_pct'] or '-'}%，平均收益 {row['avg_return_pct'] or '-'}%。"
        )
    return lines


def render_result(result: BacktestResult) -> str:
    summary = result.summary
    lines = [
        f"[完成] 日报复盘 {result.start_date.isoformat()} 至 {result.end_date.isoformat()} profile={result.profile}",
        f"候选记录: {summary['candidate_count']} 条；已写入数据库: {'是' if result.persisted else '否'}。",
    ]
    if not result.performances:
        lines.append("没有找到已生成的日报候选。请先运行 usstock report daily 并持久化报告。")
        return "\n".join(lines)

    status_counts = ", ".join(
        f"{status}={count}" for status, count in sorted(summary["status_counts"].items())
    )
    lines.append(f"计算状态: {status_counts or '-'}。")
    lines.extend(render_horizon_line(summary, horizon) for horizon in DEFAULT_HORIZONS)
    lines.extend(render_group_rows("按排名分组（T+5）:", summary["rank_buckets"]))
    lines.extend(render_group_rows("按评分分组（T+5）:", summary["score_buckets"]))
    lines.extend(render_group_rows("按主题分组（T+5，前 10）:", summary["primary_topics"]))
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest generated analysis reports.")
    subparsers = parser.add_subparsers(dest="command")

    reports_parser = subparsers.add_parser("reports", help="复盘每日分析报告候选股表现")
    reports_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    reports_parser.add_argument("--from-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    reports_parser.add_argument("--to-date", required=True, help="结束日期，格式 YYYY-MM-DD")
    reports_parser.add_argument("--profile", default=DEFAULT_PROFILE, help="报告配置名称")
    reports_parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="每份日报取前 N 个候选")
    reports_parser.add_argument("--price-source", help="只使用指定行情来源，例如 manual_csv")
    reports_parser.add_argument("--no-persist", action="store_true", help="只计算并打印，不写入表现复盘表")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "reports":
            result = run_report_backtest(
                database_url=args.database_url,
                start_date=parse_run_date(args.from_date),
                end_date=parse_run_date(args.to_date),
                profile=args.profile,
                top_n=args.top_n,
                price_source=args.price_source,
                persist=not args.no_persist,
            )
            print(render_result(result))
            return 0

        parser.print_help()
        return 2
    except Exception as exc:
        print(f"日报复盘失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
