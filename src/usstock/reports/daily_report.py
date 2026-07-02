"""Daily news-driven analysis report generation."""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import PROJECT_ROOT, get_settings
from usstock.db import migrations as db_migrations


REPORT_TYPE = "daily_analysis"
DEFAULT_PROFILE = "default"
DEFAULT_TOP_N = 10
DEFAULT_EVIDENCE_LIMIT = 5

POSITIVE_TERMS = {
    "approval",
    "approved",
    "award",
    "beats",
    "contract",
    "deal",
    "growth",
    "guidance",
    "partnership",
    "raises",
    "record",
    "upgrade",
    "wins",
}

NEGATIVE_TERMS = {
    "cut",
    "decline",
    "downgrade",
    "investigation",
    "lawsuit",
    "misses",
    "probe",
    "recall",
    "sanctions",
    "subpoena",
    "weak",
}

EVENT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("财报和业绩", ("earnings", "quarter", "revenue", "guidance", "forecast", "beats", "misses")),
    ("并购交易", ("merger", "acquisition", "buyout", "takeover", "strategic review")),
    ("医药监管和临床进展", ("fda", "clinical", "phase 3", "approval", "oncology", "trial")),
    ("订单和合作", ("contract", "order", "partnership", "supplier", "award", "deal")),
    ("法律和监管风险", ("lawsuit", "investigation", "probe", "recall", "subpoena")),
    ("宏观政策", ("federal reserve", "inflation", "interest rate", "tariff", "sanctions")),
    ("AI 和算力基础设施", ("artificial intelligence", "generative ai", "datacenter", "data center", "gpu", "llm")),
    ("能源和电力", ("power", "electricity", "nuclear", "uranium", "lng", "natural gas")),
)


@dataclass(frozen=True)
class SourceEvidence:
    """A compact source item used by the generated report."""

    source_type: str
    title: str | None
    url: str | None
    published_at: datetime | None
    relevance_score: Decimal
    topic_slug: str | None = None
    ticker: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateAnalysis:
    """Report-ready analysis for one candidate ticker."""

    rank: int | None
    ticker: str
    company_name: str | None
    score: Decimal
    action_bias: str
    attention_label: str
    event_type: str
    sentiment: str
    risk_level: str
    topics: tuple[str, ...]
    topic_names: tuple[str, ...]
    primary_topic_slug: str | None
    component_scores: dict[str, Decimal]
    counts: dict[str, int]
    latest_news_at: datetime | None
    latest_filing_date: date | None
    event_summary: str
    relation_reason: str
    watch_points: tuple[str, ...]
    risk_notes: tuple[str, ...]
    evidence: tuple[SourceEvidence, ...]


@dataclass(frozen=True)
class DailyAnalysisReport:
    """Full daily analysis report."""

    report_uid: str
    run_date: date
    profile: str
    generated_at: datetime
    executive_summary: str
    key_events: tuple[str, ...]
    risk_overview: tuple[str, ...]
    methodology_notes: tuple[str, ...]
    candidates: tuple[CandidateAnalysis, ...]
    warnings: tuple[str, ...]
    llm_used: bool
    llm_provider: str | None
    llm_model: str | None
    source_watchlist_uid: str | None
    markdown_body: str


@dataclass(frozen=True)
class LLMConfig:
    """Runtime options for optional OpenAI-compatible report enhancement."""

    enabled: bool
    api_key: str | None
    base_url: str
    model: str | None
    timeout_seconds: float
    provider: str = "openai_compatible"


def parse_run_date(value: str | None) -> date:
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"日期格式必须是 YYYY-MM-DD: {value}") from exc


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise RuntimeError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def ensure_report_schema(database_url: str) -> int:
    applied = db_migrations.migrate(database_url=database_url)
    return len(applied)


def decimal_value(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def int_value(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def parse_date_value(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def unique_non_empty(values: list[str | None], *, limit: int) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
        if len(result) >= limit:
            break
    return tuple(result)


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, SourceEvidence):
        return source_to_dict(value)
    if isinstance(value, CandidateAnalysis):
        return candidate_to_dict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def report_uid(run_date: date, profile: str, report_type: str = REPORT_TYPE) -> str:
    return f"{report_type}:{profile}:{run_date.isoformat()}"


def fetch_watchlist_payload(
    conn: Connection,
    *,
    run_date: date,
    profile: str,
) -> tuple[str | None, dict[str, Any] | None]:
    row = conn.execute(
        """
        SELECT watchlist_uid, raw_payload
        FROM daily_watchlists
        WHERE run_date = %s
          AND profile = %s
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        (run_date, profile),
    ).fetchone()
    if not row:
        return None, None
    payload = row[1] if isinstance(row[1], dict) else {}
    return row[0], payload


def resolve_profile_topic_slug(conn: Connection, *, profile: str) -> str | None:
    if not profile or profile.strip().lower() == DEFAULT_PROFILE:
        return None
    normalized = profile.strip().lower()
    row = conn.execute(
        """
        SELECT topic_slug
        FROM market_topics
        WHERE is_active
          AND (
                lower(topic_slug) = %s
                OR lower(topic_name) = %s
              )
        ORDER BY CASE WHEN lower(topic_slug) = %s THEN 0 ELSE 1 END,
                 priority,
                 topic_slug
        LIMIT 1
        """,
        (normalized, normalized, normalized),
    ).fetchone()
    return str(row[0]) if row else None


def candidate_matches_topic(candidate: Mapping[str, Any], topic_slug: str) -> bool:
    topics = {str(topic) for topic in (candidate.get("topics") or ()) if topic}
    primary_topic = candidate.get("primary_topic") or candidate.get("primary_topic_slug")
    if primary_topic:
        topics.add(str(primary_topic))
    return topic_slug in topics


def filter_candidate_payloads_by_topic(
    candidate_payloads: list[dict[str, Any]],
    *,
    topic_slug: str | None,
) -> list[dict[str, Any]]:
    if not topic_slug:
        return candidate_payloads
    return [
        candidate
        for candidate in candidate_payloads
        if candidate_matches_topic(candidate, topic_slug)
    ]


def fetch_candidate_score_payloads(
    conn: Connection,
    *,
    run_date: date,
    top_n: int,
    topic_slug: str | None = None,
) -> list[dict[str, Any]]:
    topic_filter = ""
    params: list[Any] = [run_date]
    if topic_slug:
        topic_filter = "AND %s = ANY(topic_slugs)"
        params.append(topic_slug)
    params.append(top_n)

    rows = conn.execute(
        f"""
        SELECT rank, ticker, company_name, score, action_bias, topic_slugs,
               primary_topic_slug, news_score, gdelt_score, sec_score,
               fundamental_score, liquidity_score, finnhub_article_count,
               gdelt_article_count, sec_filing_count, latest_news_at,
               latest_filing_date, rationale
        FROM daily_candidate_scores
        WHERE run_date = %s
          {topic_filter}
        ORDER BY rank NULLS LAST, score DESC, ticker
        LIMIT %s
        """,
        tuple(params),
    ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payloads.append(
            {
                "rank": row[0],
                "ticker": row[1],
                "company_name": row[2],
                "score": str(row[3]),
                "action_bias": row[4],
                "topics": list(row[5] or []),
                "primary_topic": row[6],
                "scores": {
                    "news": str(row[7]),
                    "gdelt": str(row[8]),
                    "sec": str(row[9]),
                    "fundamental": str(row[10]),
                    "liquidity": str(row[11]),
                },
                "counts": {
                    "finnhub_articles": row[12],
                    "gdelt_articles": row[13],
                    "sec_filings": row[14],
                },
                "latest_news_at": row[15].isoformat() if row[15] else None,
                "latest_filing_date": row[16].isoformat() if row[16] else None,
                "rationale": row[17] or {},
            }
        )
    return payloads


def fetch_topic_names(conn: Connection) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT topic_slug, topic_name
        FROM market_topics
        ORDER BY priority, topic_slug
        """
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def fetch_source_evidence(
    conn: Connection,
    *,
    run_date: date,
    tickers: tuple[str, ...],
    topics: tuple[str, ...],
    limit_per_key: int = DEFAULT_EVIDENCE_LIMIT,
) -> dict[str, tuple[SourceEvidence, ...]]:
    if not tickers and not topics:
        return {}

    rows = conn.execute(
        """
        SELECT ticker, topic_slug, source_type, source_title, source_url,
               published_at, relevance_score, evidence
        FROM topic_mentions
        WHERE (
                ticker = ANY(%s)
                OR (ticker IS NULL AND topic_slug = ANY(%s))
              )
          AND (
                published_at IS NULL
                OR published_at >= (%s::date - interval '7 days')
              )
        ORDER BY relevance_score DESC, published_at DESC NULLS LAST
        LIMIT 500
        """,
        (list(tickers), list(topics), run_date),
    ).fetchall()

    grouped: dict[str, list[SourceEvidence]] = {ticker: [] for ticker in tickers}
    for row in rows:
        source = SourceEvidence(
            ticker=row[0],
            topic_slug=row[1],
            source_type=row[2],
            title=row[3],
            url=row[4],
            published_at=row[5],
            relevance_score=decimal_value(row[6]),
            evidence=row[7] or {},
        )
        if source.ticker:
            key = source.ticker.upper()
            if key in grouped and len(grouped[key]) < limit_per_key:
                grouped[key].append(source)
            continue

        if source.topic_slug:
            for ticker in tickers:
                if len(grouped[ticker]) >= limit_per_key:
                    continue
                grouped[ticker].append(source)

    return {key: tuple(values[:limit_per_key]) for key, values in grouped.items()}


def load_report_context(
    conn: Connection,
    *,
    run_date: date,
    profile: str,
    top_n: int,
) -> tuple[
    str | None,
    list[dict[str, Any]],
    dict[str, str],
    dict[str, tuple[SourceEvidence, ...]],
    tuple[str, ...],
]:
    warnings: list[str] = []
    profile_topic_slug = resolve_profile_topic_slug(conn, profile=profile)
    source_watchlist_uid, watchlist_payload = fetch_watchlist_payload(
        conn,
        run_date=run_date,
        profile=profile,
    )
    if watchlist_payload and watchlist_payload.get("candidates"):
        watchlist_candidates = list(watchlist_payload["candidates"])
        topic_candidates = filter_candidate_payloads_by_topic(
            watchlist_candidates,
            topic_slug=profile_topic_slug,
        )
        if profile_topic_slug and not topic_candidates:
            warnings.append(
                f"profile={profile} 对应主题 {profile_topic_slug}，"
                "但观察清单没有该主题候选，已改用候选评分表筛选。"
            )
            candidate_payloads = fetch_candidate_score_payloads(
                conn,
                run_date=run_date,
                top_n=top_n,
                topic_slug=profile_topic_slug,
            )
        else:
            candidate_payloads = topic_candidates[:top_n]
    else:
        if profile_topic_slug is None and profile.strip().lower() != DEFAULT_PROFILE:
            warnings.append(
                f"未找到 profile={profile} 的观察清单或同名启用主题，已使用全局候选评分。"
            )
        candidate_payloads = fetch_candidate_score_payloads(
            conn,
            run_date=run_date,
            top_n=top_n,
            topic_slug=profile_topic_slug,
        )

    topic_names = fetch_topic_names(conn)
    tickers = tuple(
        str(candidate.get("ticker", "")).upper()
        for candidate in candidate_payloads
        if candidate.get("ticker")
    )
    topics = tuple(
        sorted(
            {
                str(topic)
                for candidate in candidate_payloads
                for topic in (candidate.get("topics") or ())
                if topic
            }
        )
    )
    evidence = fetch_source_evidence(
        conn,
        run_date=run_date,
        tickers=tickers,
        topics=topics,
    )
    return (
        source_watchlist_uid,
        candidate_payloads,
        topic_names,
        evidence,
        tuple(warnings),
    )


def classify_event_type(text: str, *, has_sec: bool) -> str:
    lower = text.lower()
    for event_type, terms in EVENT_RULES:
        if any(term in lower for term in terms):
            return event_type
    if has_sec:
        return "SEC 公告事件"
    return "主题热度和新闻驱动"


def classify_sentiment(text: str) -> str:
    lower = text.lower()
    positive = sum(1 for term in POSITIVE_TERMS if term in lower)
    negative = sum(1 for term in NEGATIVE_TERMS if term in lower)
    if positive and negative:
        return "多空交织"
    if positive:
        return "偏利好"
    if negative:
        return "偏利空"
    return "中性"


def attention_label(score: Decimal, action_bias: str) -> str:
    if action_bias == "review" or score >= Decimal("60"):
        return "优先复核"
    if score >= Decimal("40"):
        return "值得关注"
    if action_bias == "watch" or score >= Decimal("25"):
        return "观察"
    return "暂缓"


def risk_level(
    *,
    score: Decimal,
    sentiment: str,
    component_scores: Mapping[str, Decimal],
    counts: Mapping[str, int],
) -> str:
    liquidity = component_scores.get("liquidity", Decimal("0"))
    fundamental = component_scores.get("fundamental", Decimal("0"))
    if sentiment == "偏利空":
        return "高"
    if liquidity < Decimal("4") or score < Decimal("25"):
        return "高"
    if counts.get("finnhub_articles", 0) <= 1 and counts.get("sec_filings", 0) == 0:
        return "中高"
    if fundamental <= Decimal("0"):
        return "中"
    return "中低"


def build_event_summary(
    *,
    ticker: str,
    event_type: str,
    titles: tuple[str, ...],
    topics: tuple[str, ...],
    topic_names: tuple[str, ...],
    counts: Mapping[str, int],
) -> str:
    subject = topic_names[0] if topic_names else (topics[0] if topics else "近期市场主题")
    if titles:
        return f"{ticker} 进入短线机会候选，主要由“{titles[0]}”触发，事件类型归为{event_type}。"
    if counts.get("sec_filings", 0):
        return f"{ticker} 因近期 SEC 公告和 {subject} 主题命中进入短线机会候选，事件类型归为{event_type}。"
    return f"{ticker} 因 {subject} 相关热度和个股证据进入短线机会候选，事件类型归为{event_type}。"


def build_relation_reason(
    *,
    ticker: str,
    topic_names: tuple[str, ...],
    component_scores: Mapping[str, Decimal],
    counts: Mapping[str, int],
) -> str:
    parts: list[str] = []
    if topic_names:
        parts.append(f"关联主题为 {', '.join(topic_names[:3])}")
    if counts.get("finnhub_articles", 0):
        parts.append(f"Finnhub 相关新闻 {counts['finnhub_articles']} 篇")
    if counts.get("gdelt_articles", 0):
        parts.append(f"GDELT 主题新闻 {counts['gdelt_articles']} 篇")
    if counts.get("sec_filings", 0):
        parts.append(f"SEC 核心公告 {counts['sec_filings']} 条")
    if component_scores.get("liquidity", Decimal("0")) > 0:
        parts.append(f"流动性分 {component_scores['liquidity']}")
    if component_scores.get("fundamental", Decimal("0")) > 0:
        parts.append(f"基本面可用性分 {component_scores['fundamental']}")
    if not parts:
        parts.append("当前候选依据主要来自已有主题匹配和评分模型")
    return f"{ticker} 的相关性依据：" + "，".join(parts) + "。"


def build_watch_points(
    *,
    sentiment: str,
    counts: Mapping[str, int],
    component_scores: Mapping[str, Decimal],
) -> tuple[str, ...]:
    points = [
        "核对原始新闻或公告，确认事件是否仍在发酵。",
        "观察开盘后成交量、价格缺口和是否出现高位回落。",
    ]
    if counts.get("sec_filings", 0):
        points.append("优先阅读 SEC 公告正文，确认是否属于重大订单、指引变化或风险披露。")
    if counts.get("gdelt_articles", 0):
        points.append("观察主题新闻是否持续扩散，而不是单日噪音。")
    if component_scores.get("fundamental", Decimal("0")) <= 0:
        points.append("补齐结构化财务数据后再判断基本面支撑。")
    if sentiment in {"偏利空", "多空交织"}:
        points.append("把风险事件和市场反应分开看，避免只按标题方向下结论。")
    return tuple(points[:5])


def build_risk_notes(
    *,
    sentiment: str,
    counts: Mapping[str, int],
    component_scores: Mapping[str, Decimal],
) -> tuple[str, ...]:
    notes: list[str] = []
    if sentiment == "偏利空":
        notes.append("标题和关键词偏风险事件，短线波动可能放大。")
    if sentiment == "多空交织":
        notes.append("同一事件同时包含正负面线索，需要人工复核细节。")
    if counts.get("finnhub_articles", 0) <= 1 and counts.get("sec_filings", 0) == 0:
        notes.append("目前证据偏新闻驱动，缺少公告或多来源确认。")
    if component_scores.get("liquidity", Decimal("0")) < Decimal("4"):
        notes.append("流动性分偏低，可能存在成交不足或滑点风险。")
    if component_scores.get("fundamental", Decimal("0")) <= 0:
        notes.append("结构化财务支撑暂不充分，不能仅凭热点叙事判断。")
    if not notes:
        notes.append("仍需结合估值、技术面和仓位规则，报告不构成买卖建议。")
    return tuple(notes[:5])


def candidate_source_titles(
    candidate: Mapping[str, Any],
    evidence: tuple[SourceEvidence, ...],
) -> tuple[str, ...]:
    rationale = candidate.get("rationale") or {}
    values: list[str | None] = []
    values.extend(rationale.get("recent_titles") or [])
    values.extend(rationale.get("sec_titles") or [])
    values.extend(rationale.get("gdelt_titles") or [])
    values.extend(source.title for source in evidence)
    return unique_non_empty(values, limit=5)


def build_candidate_analysis(
    candidate: Mapping[str, Any],
    *,
    topic_names: Mapping[str, str],
    evidence: tuple[SourceEvidence, ...],
) -> CandidateAnalysis:
    ticker = str(candidate.get("ticker") or "").upper()
    score = decimal_value(candidate.get("score"))
    action_bias = str(candidate.get("action_bias") or "watch")
    topics = tuple(str(topic) for topic in (candidate.get("topics") or ()) if topic)
    display_topic_names = tuple(topic_names.get(topic, topic) for topic in topics)
    primary_topic = candidate.get("primary_topic")
    component_scores = {
        key: decimal_value(value)
        for key, value in (candidate.get("scores") or {}).items()
    }
    counts = {
        key: int_value(value)
        for key, value in (candidate.get("counts") or {}).items()
    }
    titles = candidate_source_titles(candidate, evidence)
    text = " ".join(
        [
            ticker,
            " ".join(topics),
            " ".join(display_topic_names),
            " ".join(titles),
        ]
    )
    event_type = classify_event_type(
        text,
        has_sec=counts.get("sec_filings", 0) > 0,
    )
    sentiment = classify_sentiment(text)
    label = attention_label(score, action_bias)
    risk = risk_level(
        score=score,
        sentiment=sentiment,
        component_scores=component_scores,
        counts=counts,
    )
    return CandidateAnalysis(
        rank=candidate.get("rank"),
        ticker=ticker,
        company_name=candidate.get("company_name"),
        score=score,
        action_bias=action_bias,
        attention_label=label,
        event_type=event_type,
        sentiment=sentiment,
        risk_level=risk,
        topics=topics,
        topic_names=display_topic_names,
        primary_topic_slug=str(primary_topic) if primary_topic else None,
        component_scores=component_scores,
        counts=counts,
        latest_news_at=parse_datetime(candidate.get("latest_news_at")),
        latest_filing_date=parse_date_value(candidate.get("latest_filing_date")),
        event_summary=build_event_summary(
            ticker=ticker,
            event_type=event_type,
            titles=titles,
            topics=topics,
            topic_names=display_topic_names,
            counts=counts,
        ),
        relation_reason=build_relation_reason(
            ticker=ticker,
            topic_names=display_topic_names,
            component_scores=component_scores,
            counts=counts,
        ),
        watch_points=build_watch_points(
            sentiment=sentiment,
            counts=counts,
            component_scores=component_scores,
        ),
        risk_notes=build_risk_notes(
            sentiment=sentiment,
            counts=counts,
            component_scores=component_scores,
        ),
        evidence=evidence,
    )


def build_key_events(candidates: tuple[CandidateAnalysis, ...]) -> tuple[str, ...]:
    grouped: dict[str, list[CandidateAnalysis]] = {}
    for candidate in candidates:
        key = candidate.topic_names[0] if candidate.topic_names else candidate.event_type
        grouped.setdefault(key, []).append(candidate)

    events: list[str] = []
    for topic, items in sorted(
        grouped.items(),
        key=lambda pair: max(candidate.score for candidate in pair[1]),
        reverse=True,
    )[:5]:
        tickers = ", ".join(candidate.ticker for candidate in items[:5])
        leader = max(items, key=lambda candidate: candidate.score)
        events.append(
            f"{topic}：{tickers} 等 {len(items)} 只标的进入短线机会候选，最高分为 {leader.ticker}({leader.score})。"
        )
    return tuple(events)


def build_executive_summary(
    *,
    run_date: date,
    candidates: tuple[CandidateAnalysis, ...],
) -> str:
    if not candidates:
        return f"{run_date.isoformat()} 暂无短线机会候选进入分析报告。"
    leader = candidates[0]
    review_count = sum(1 for candidate in candidates if candidate.attention_label == "优先复核")
    watch_count = sum(1 for candidate in candidates if candidate.attention_label in {"优先复核", "值得关注", "观察"})
    risk_count = sum(1 for candidate in candidates if candidate.risk_level in {"高", "中高"})
    return (
        f"{run_date.isoformat()} 共生成 {len(candidates)} 只新闻驱动短线机会候选，"
        f"其中 {review_count} 只需要优先复核，{watch_count} 只进入观察范围。"
        f"当前最高分为 {leader.ticker}({leader.score})，主要事件类型为{leader.event_type}。"
        f"高风险或中高风险标的 {risk_count} 只，需先核对原始来源。"
    )


def build_risk_overview(candidates: tuple[CandidateAnalysis, ...]) -> tuple[str, ...]:
    if not candidates:
        return ("没有候选股时不生成交易观察结论。",)

    notes = [
        "本报告是研究辅助和短线机会排序，不构成买卖建议。",
        "LLM 默认用于摘要和解释增强，不覆盖结构化评分；如调用失败则保留规则版报告。",
    ]
    high_risk = [candidate.ticker for candidate in candidates if candidate.risk_level in {"高", "中高"}]
    if high_risk:
        notes.append(f"风险较高标的：{', '.join(high_risk[:8])}，需要优先复核新闻真实性、流动性和公告细节。")
    news_only = [
        candidate.ticker
        for candidate in candidates
        if candidate.counts.get("finnhub_articles", 0) > 0
        and candidate.counts.get("sec_filings", 0) == 0
        and candidate.component_scores.get("fundamental", Decimal("0")) <= 0
    ]
    if news_only:
        notes.append(f"新闻驱动但基本面证据不足：{', '.join(news_only[:8])}。")
    return tuple(notes)


def build_methodology_notes() -> tuple[str, ...]:
    return (
        "候选池来自 daily_candidate_scores 和 daily_watchlists，不重新抓取外部数据。",
        "GDELT 主题新闻只作为板块热度和风向标，只有明确点名 ticker、公司名或公司关键词时才形成个股候选。",
        "事件类型、情绪和风险等级由规则模型基于标题、主题、SEC 计数、流动性分和基本面可用性分生成。",
        "LLM 默认参与生成摘要和风险提示，报告仍优先使用“值得关注、优先复核、暂缓”等研究表述，不输出直接买入或卖出指令。",
    )


def build_base_report(
    *,
    run_date: date,
    profile: str,
    source_watchlist_uid: str | None,
    candidate_payloads: list[dict[str, Any]],
    topic_names: Mapping[str, str],
    evidence_by_ticker: Mapping[str, tuple[SourceEvidence, ...]],
    warnings: tuple[str, ...] = (),
) -> DailyAnalysisReport:
    candidates = tuple(
        build_candidate_analysis(
            candidate,
            topic_names=topic_names,
            evidence=evidence_by_ticker.get(str(candidate.get("ticker", "")).upper(), ()),
        )
        for candidate in candidate_payloads
    )
    executive_summary = build_executive_summary(run_date=run_date, candidates=candidates)
    key_events = build_key_events(candidates)
    risk_overview = build_risk_overview(candidates)
    methodology_notes = build_methodology_notes()
    generated_at = datetime.now(timezone.utc)
    report = DailyAnalysisReport(
        report_uid=report_uid(run_date, profile),
        run_date=run_date,
        profile=profile,
        generated_at=generated_at,
        executive_summary=executive_summary,
        key_events=key_events,
        risk_overview=risk_overview,
        methodology_notes=methodology_notes,
        candidates=candidates,
        warnings=warnings,
        llm_used=False,
        llm_provider=None,
        llm_model=None,
        source_watchlist_uid=source_watchlist_uid,
        markdown_body="",
    )
    return replace(report, markdown_body=render_markdown(report))


def llm_config_from_settings(
    *,
    enabled: bool,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    timeout_seconds: float | None = None,
) -> LLMConfig:
    settings = get_settings()
    return LLMConfig(
        enabled=enabled,
        api_key=api_key or settings.llm_api_key,
        base_url=(base_url or settings.llm_base_url).rstrip("/"),
        model=model or settings.llm_model,
        timeout_seconds=timeout_seconds or settings.llm_request_timeout_seconds,
    )


def compact_report_context(report: DailyAnalysisReport) -> dict[str, Any]:
    return {
        "run_date": report.run_date.isoformat(),
        "profile": report.profile,
        "executive_summary": report.executive_summary,
        "key_events": list(report.key_events),
        "risk_overview": list(report.risk_overview),
        "candidates": [
            {
                "ticker": candidate.ticker,
                "company_name": candidate.company_name,
                "rank": candidate.rank,
                "score": str(candidate.score),
                "attention_label": candidate.attention_label,
                "event_type": candidate.event_type,
                "sentiment": candidate.sentiment,
                "risk_level": candidate.risk_level,
                "topics": list(candidate.topic_names),
                "counts": candidate.counts,
                "event_summary": candidate.event_summary,
                "relation_reason": candidate.relation_reason,
                "watch_points": list(candidate.watch_points),
                "risk_notes": list(candidate.risk_notes),
                "evidence_titles": [
                    source.title for source in candidate.evidence if source.title
                ][:3],
            }
            for candidate in report.candidates
        ],
    }


def call_openai_compatible_llm(
    *,
    config: LLMConfig,
    report: DailyAnalysisReport,
) -> dict[str, Any]:
    if not config.api_key:
        raise RuntimeError("缺少 LLM API key，请配置 REPORT_LLM_API_KEY、LLM_API_KEY 或 OPENAI_API_KEY。")
    if not config.model:
        raise RuntimeError("缺少 LLM 模型名，请配置 REPORT_LLM_MODEL、LLM_MODEL 或 OPENAI_MODEL。")

    prompt = {
        "role": "user",
        "content": (
            "请基于下面的结构化候选股上下文，生成中文研究辅助报告增强内容。"
            "不要输出直接买入、卖出或目标价。只允许输出 JSON，格式为："
            '{"executive_summary": "...", "risk_overview": ["..."], '
            '"candidates": {"TICKER": {"event_summary": "...", '
            '"relation_reason": "...", "watch_points": ["..."], "risk_notes": ["..."]}}}。'
            "\n\n上下文："
            + json.dumps(compact_report_context(report), ensure_ascii=False, default=json_default)
        ),
    }
    payload = {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是谨慎的美股投研助手。你的任务是解释事件、候选逻辑和风险，"
                    "必须保留不构成投资建议的边界。"
                ),
            },
            prompt,
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        f"{config.base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM 请求失败 HTTP {exc.code}: {body[:500]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"LLM 请求失败: {exc}") from exc

    content = (
        response_payload.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    return parse_llm_json(content)


def parse_llm_json(content: str) -> dict[str, Any]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("LLM 返回内容不是合法 JSON。") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("LLM 返回 JSON 顶层必须是对象。")
    return payload


def tuple_of_strings(value: Any, *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(value, list):
        return fallback
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    return parsed or fallback


def apply_llm_enhancement(
    report: DailyAnalysisReport,
    payload: Mapping[str, Any],
    config: LLMConfig,
) -> DailyAnalysisReport:
    candidate_payload = payload.get("candidates")
    by_ticker = candidate_payload if isinstance(candidate_payload, dict) else {}
    enhanced_candidates: list[CandidateAnalysis] = []
    for candidate in report.candidates:
        item = by_ticker.get(candidate.ticker)
        if not isinstance(item, dict):
            enhanced_candidates.append(candidate)
            continue
        enhanced_candidates.append(
            replace(
                candidate,
                event_summary=str(item.get("event_summary") or candidate.event_summary),
                relation_reason=str(item.get("relation_reason") or candidate.relation_reason),
                watch_points=tuple_of_strings(
                    item.get("watch_points"),
                    fallback=candidate.watch_points,
                ),
                risk_notes=tuple_of_strings(
                    item.get("risk_notes"),
                    fallback=candidate.risk_notes,
                ),
            )
        )

    enhanced = replace(
        report,
        executive_summary=str(payload.get("executive_summary") or report.executive_summary),
        risk_overview=tuple_of_strings(
            payload.get("risk_overview"),
            fallback=report.risk_overview,
        ),
        candidates=tuple(enhanced_candidates),
        llm_used=True,
        llm_provider=config.provider,
        llm_model=config.model,
    )
    return replace(enhanced, markdown_body=render_markdown(enhanced))


def maybe_enhance_with_llm(
    report: DailyAnalysisReport,
    *,
    config: LLMConfig,
) -> DailyAnalysisReport:
    if not config.enabled:
        return report
    try:
        payload = call_openai_compatible_llm(config=config, report=report)
        return apply_llm_enhancement(report, payload, config)
    except Exception as exc:
        warnings = (*report.warnings, f"LLM 增强失败，已使用规则版报告: {exc}")
        return replace(report, warnings=warnings, markdown_body=render_markdown(replace(report, warnings=warnings)))


def source_to_dict(source: SourceEvidence) -> dict[str, Any]:
    return {
        "source_type": source.source_type,
        "title": source.title,
        "url": source.url,
        "published_at": source.published_at.isoformat() if source.published_at else None,
        "relevance_score": str(source.relevance_score),
        "topic_slug": source.topic_slug,
        "ticker": source.ticker,
        "evidence": source.evidence,
    }


def candidate_to_dict(candidate: CandidateAnalysis) -> dict[str, Any]:
    return {
        "rank": candidate.rank,
        "ticker": candidate.ticker,
        "company_name": candidate.company_name,
        "score": str(candidate.score),
        "action_bias": candidate.action_bias,
        "attention_label": candidate.attention_label,
        "event_type": candidate.event_type,
        "sentiment": candidate.sentiment,
        "risk_level": candidate.risk_level,
        "topics": list(candidate.topics),
        "topic_names": list(candidate.topic_names),
        "primary_topic_slug": candidate.primary_topic_slug,
        "component_scores": {
            key: str(value) for key, value in candidate.component_scores.items()
        },
        "counts": candidate.counts,
        "latest_news_at": candidate.latest_news_at.isoformat() if candidate.latest_news_at else None,
        "latest_filing_date": candidate.latest_filing_date.isoformat() if candidate.latest_filing_date else None,
        "event_summary": candidate.event_summary,
        "relation_reason": candidate.relation_reason,
        "watch_points": list(candidate.watch_points),
        "risk_notes": list(candidate.risk_notes),
        "evidence": [source_to_dict(source) for source in candidate.evidence],
    }


def report_to_payload(report: DailyAnalysisReport) -> dict[str, Any]:
    return {
        "report_uid": report.report_uid,
        "run_date": report.run_date.isoformat(),
        "profile": report.profile,
        "generated_at": report.generated_at.isoformat(),
        "executive_summary": report.executive_summary,
        "key_events": list(report.key_events),
        "risk_overview": list(report.risk_overview),
        "methodology_notes": list(report.methodology_notes),
        "candidates": [candidate_to_dict(candidate) for candidate in report.candidates],
        "warnings": list(report.warnings),
        "llm": {
            "used": report.llm_used,
            "provider": report.llm_provider,
            "model": report.llm_model,
        },
        "source_watchlist_uid": report.source_watchlist_uid,
    }


def upsert_daily_analysis_report(conn: Connection, report: DailyAnalysisReport) -> None:
    conn.execute(
        """
        INSERT INTO daily_analysis_reports (
            report_uid,
            run_date,
            profile,
            report_type,
            candidate_count,
            llm_provider,
            llm_model,
            llm_used,
            status,
            summary,
            markdown_body,
            structured_payload,
            source_watchlist_uid,
            generated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'generated',
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (report_uid)
        DO UPDATE SET
            candidate_count = EXCLUDED.candidate_count,
            llm_provider = EXCLUDED.llm_provider,
            llm_model = EXCLUDED.llm_model,
            llm_used = EXCLUDED.llm_used,
            status = EXCLUDED.status,
            summary = EXCLUDED.summary,
            markdown_body = EXCLUDED.markdown_body,
            structured_payload = EXCLUDED.structured_payload,
            source_watchlist_uid = EXCLUDED.source_watchlist_uid,
            generated_at = EXCLUDED.generated_at,
            updated_at = now()
        """,
        (
            report.report_uid,
            report.run_date,
            report.profile,
            REPORT_TYPE,
            len(report.candidates),
            report.llm_provider,
            report.llm_model,
            report.llm_used,
            report.executive_summary,
            report.markdown_body,
            Jsonb(report_to_payload(report)),
            report.source_watchlist_uid,
            report.generated_at,
        ),
    )


def generate_daily_report(
    *,
    database_url: str | None = None,
    run_date: date | None = None,
    profile: str = DEFAULT_PROFILE,
    top_n: int = DEFAULT_TOP_N,
    llm_model: str | None = None,
    llm_base_url: str | None = None,
    llm_api_key: str | None = None,
    llm_timeout_seconds: float | None = None,
    persist: bool = True,
) -> DailyAnalysisReport:
    database_url = get_database_url(database_url)
    run_date = run_date or date.today()
    ensure_report_schema(database_url)
    config = llm_config_from_settings(
        enabled=True,
        model=llm_model,
        base_url=llm_base_url,
        api_key=llm_api_key,
        timeout_seconds=llm_timeout_seconds,
    )

    with psycopg.connect(database_url, autocommit=False) as conn:
        (
            source_watchlist_uid,
            candidates,
            topic_names,
            evidence,
            warnings,
        ) = load_report_context(
            conn,
            run_date=run_date,
            profile=profile,
            top_n=top_n,
        )
        report = build_base_report(
            run_date=run_date,
            profile=profile,
            source_watchlist_uid=source_watchlist_uid,
            candidate_payloads=candidates,
            topic_names=topic_names,
            evidence_by_ticker=evidence,
            warnings=warnings,
        )
        report = maybe_enhance_with_llm(report, config=config)
        if persist:
            with conn.transaction():
                upsert_daily_analysis_report(conn, report)
    return report


def markdown_table_cell(value: Any) -> str:
    text = str(value or "-")
    return text.replace("|", "\\|").replace("\n", " ")


def format_dt(value: datetime | None) -> str:
    if not value:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render_markdown(report: DailyAnalysisReport) -> str:
    lines = [
        f"# 美股新闻驱动分析报告 {report.run_date.isoformat()}",
        "",
        f"- profile: `{report.profile}`",
        f"- 生成时间: `{report.generated_at.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- LLM 增强: `{'是' if report.llm_used else '否'}`",
        "",
        "> 本报告用于研究和复盘，不构成投资建议；任何交易都需要人工确认和独立判断。",
        "",
        "## 核心摘要",
        "",
        report.executive_summary,
        "",
    ]
    if report.warnings:
        lines.extend(["## 生成警告", ""])
        lines.extend(f"- {warning}" for warning in report.warnings)
        lines.append("")

    lines.extend(["## 今日核心事件", ""])
    lines.extend(f"- {event}" for event in report.key_events or ("暂无核心事件。",))
    lines.append("")

    lines.extend(
        [
            "## 短线机会候选",
            "",
            "| 排名 | Ticker | 公司 | 关注级别 | 分数 | 事件 | 情绪 | 风险 | 主题 |",
            "| --- | --- | --- | --- | ---: | --- | --- | --- | --- |",
        ]
    )
    for candidate in report.candidates:
        rank = candidate.rank if candidate.rank is not None else "-"
        topic = ", ".join(candidate.topic_names[:2]) or "-"
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_table_cell(rank),
                    markdown_table_cell(candidate.ticker),
                    markdown_table_cell(candidate.company_name),
                    markdown_table_cell(candidate.attention_label),
                    markdown_table_cell(candidate.score),
                    markdown_table_cell(candidate.event_type),
                    markdown_table_cell(candidate.sentiment),
                    markdown_table_cell(candidate.risk_level),
                    markdown_table_cell(topic),
                ]
            )
            + " |"
        )
    lines.append("")

    for candidate in report.candidates:
        heading_rank = f"{candidate.rank}. " if candidate.rank is not None else ""
        company = f" - {candidate.company_name}" if candidate.company_name else ""
        lines.extend(
            [
                f"### {heading_rank}{candidate.ticker}{company}",
                "",
                f"- 事件摘要：{candidate.event_summary}",
                f"- 关联理由：{candidate.relation_reason}",
                f"- 关注级别：{candidate.attention_label}；风险等级：{candidate.risk_level}；情绪方向：{candidate.sentiment}。",
                f"- 最新新闻时间：{format_dt(candidate.latest_news_at)}；最新 filing 日期：{candidate.latest_filing_date or '-'}。",
                "- 关注点：",
            ]
        )
        lines.extend(f"  - {point}" for point in candidate.watch_points)
        lines.append("- 风险提示：")
        lines.extend(f"  - {note}" for note in candidate.risk_notes)
        if candidate.evidence:
            lines.append("- 来源证据：")
            for source in candidate.evidence[:DEFAULT_EVIDENCE_LIMIT]:
                title = source.title or source.source_type
                when = format_dt(source.published_at)
                if source.url:
                    lines.append(f"  - [{title}]({source.url}) · {source.source_type} · {when}")
                else:
                    lines.append(f"  - {title} · {source.source_type} · {when}")
        lines.append("")

    lines.extend(["## 总体风险", ""])
    lines.extend(f"- {note}" for note in report.risk_overview)
    lines.append("")

    lines.extend(["## 方法说明", ""])
    lines.extend(f"- {note}" for note in report.methodology_notes)
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def default_output_path(run_date: date, profile: str) -> Path:
    safe_profile = re.sub(r"[^A-Za-z0-9_.-]+", "-", profile).strip("-") or "default"
    return PROJECT_ROOT / "reports" / f"daily_analysis_{run_date.isoformat()}_{safe_profile}.md"


def save_markdown(report: DailyAnalysisReport, output_path: Path | None) -> Path:
    path = output_path or default_output_path(report.run_date, report.profile)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.markdown_body, encoding="utf-8")
    return path


def render_result(report: DailyAnalysisReport, *, output_path: Path | None = None) -> str:
    lines = [
        f"[完成] 每日分析报告 {report.run_date.isoformat()} profile={report.profile}",
        f"报告 UID: {report.report_uid}",
        f"候选股 {len(report.candidates)} 只，LLM 增强={'是' if report.llm_used else '否'}。",
    ]
    if output_path:
        lines.append(f"Markdown: {output_path}")
    if report.warnings:
        lines.append("警告：")
        lines.extend(f"- {warning}" for warning in report.warnings)
    if report.candidates:
        leaders = ", ".join(
            f"{candidate.ticker}({candidate.score}, {candidate.attention_label})"
            for candidate in report.candidates[:5]
        )
        lines.append(f"候选摘要: {leaders}")
    return "\n".join(lines)


def add_daily_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    parser.add_argument("--run-date", help="运行日期，格式 YYYY-MM-DD，默认今天")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="报告配置名称")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="报告候选股数量")
    parser.add_argument("--llm-model", help="覆盖 REPORT_LLM_MODEL/LLM_MODEL/OPENAI_MODEL")
    parser.add_argument("--llm-base-url", help="覆盖 REPORT_LLM_BASE_URL/LLM_BASE_URL/OPENAI_BASE_URL")
    parser.add_argument("--llm-api-key", help="覆盖环境变量中的 LLM API key")
    parser.add_argument("--llm-timeout-seconds", type=float, help="LLM 请求超时时间")
    parser.add_argument("--no-persist", action="store_true", help="不写入 daily_analysis_reports")
    parser.add_argument(
        "--save-markdown",
        nargs="?",
        const="",
        help="保存 Markdown。可选传入路径，不传路径则保存到 reports/。",
    )
    parser.add_argument("--print-markdown", action="store_true", help="直接打印完整 Markdown")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate analysis reports.")
    subparsers = parser.add_subparsers(dest="command")
    daily_parser = subparsers.add_parser("daily", help="生成每日新闻驱动分析报告")
    add_daily_args(daily_parser)
    return parser


def run_from_args(args: argparse.Namespace) -> DailyAnalysisReport:
    return generate_daily_report(
        database_url=args.database_url,
        run_date=parse_run_date(args.run_date),
        profile=args.profile,
        top_n=args.top_n,
        llm_model=args.llm_model,
        llm_base_url=args.llm_base_url,
        llm_api_key=args.llm_api_key,
        llm_timeout_seconds=args.llm_timeout_seconds,
        persist=not args.no_persist,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "daily":
            report = run_from_args(args)
            output_path = None
            if args.save_markdown is not None:
                explicit_path = Path(args.save_markdown) if args.save_markdown else None
                output_path = save_markdown(report, explicit_path)
            if args.print_markdown:
                print(report.markdown_body)
            else:
                print(render_result(report, output_path=output_path))
            return 0

        parser.print_help()
        return 2
    except Exception as exc:
        print(f"分析报告生成失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
