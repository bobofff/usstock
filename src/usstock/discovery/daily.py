"""Daily automatic market discovery workflow."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.config.settings import get_settings
from usstock.data import finnhub, gdelt, sec
from usstock.db import migrations as db_migrations


DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_GDELT_TIMESPAN = "24h"
DEFAULT_TOP_N = 25
DEFAULT_MAX_SEC_TICKERS = 50
DEFAULT_SEC_FILING_LIMIT = 20
DEFAULT_FINNHUB_CATEGORIES = ("general", "merger")
ProgressCallback = Callable[[dict[str, Any]], None]

STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "amid",
    "among",
    "and",
    "announces",
    "are",
    "around",
    "before",
    "being",
    "between",
    "billion",
    "brief",
    "but",
    "can",
    "company",
    "could",
    "from",
    "has",
    "have",
    "into",
    "its",
    "market",
    "million",
    "more",
    "new",
    "news",
    "not",
    "over",
    "per",
    "quarter",
    "report",
    "reports",
    "said",
    "says",
    "share",
    "shares",
    "stock",
    "than",
    "that",
    "the",
    "their",
    "this",
    "through",
    "under",
    "with",
    "would",
}

CATALYST_TERMS = {
    "approval": 4,
    "approved": 4,
    "award": 3,
    "beats": 3,
    "contract": 4,
    "deal": 3,
    "earnings": 3,
    "fda": 5,
    "forecast": 2,
    "guidance": 4,
    "investigation": 3,
    "lawsuit": 3,
    "merger": 5,
    "misses": 3,
    "order": 3,
    "partnership": 3,
    "raises": 3,
    "recall": 4,
    "restructuring": 3,
    "upgrade": 3,
}

SEC_FORM_WEIGHTS = {
    "8-K": Decimal("16"),
    "10-Q": Decimal("12"),
    "10-K": Decimal("12"),
    "S-1": Decimal("14"),
    "20-F": Decimal("10"),
    "6-K": Decimal("9"),
}


@dataclass(frozen=True)
class MarketTopic:
    topic_slug: str
    topic_name: str
    gdelt_query: str
    keywords: tuple[str, ...]
    sectors: tuple[str, ...] = ()
    ticker_hints: tuple[str, ...] = ()
    priority: int = 100
    data_source: str = "seed"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TopicMention:
    mention_uid: str
    topic_slug: str
    ticker: str | None
    source_type: str
    source_uid: str
    source_title: str | None
    source_url: str | None
    published_at: datetime | None
    relevance_score: Decimal
    evidence: dict[str, Any]


@dataclass
class CandidateAccumulator:
    ticker: str
    company_name: str | None = None
    topic_scores: Counter[str] = field(default_factory=Counter)
    topic_counts: Counter[str] = field(default_factory=Counter)
    keywords: Counter[str] = field(default_factory=Counter)
    finnhub_article_uids: set[str] = field(default_factory=set)
    gdelt_article_urls: set[str] = field(default_factory=set)
    sec_accessions: set[str] = field(default_factory=set)
    sec_forms: Counter[str] = field(default_factory=Counter)
    latest_news_at: datetime | None = None
    latest_filing_date: date | None = None
    recent_titles: list[str] = field(default_factory=list)
    gdelt_titles: list[str] = field(default_factory=list)
    sec_titles: list[str] = field(default_factory=list)
    market_cap_usd: Decimal | None = None
    avg_volume_30d: Decimal | None = None
    fact_count: int = 0
    active_in_universe: bool = False


@dataclass(frozen=True)
class CandidateScore:
    run_date: date
    ticker: str
    company_name: str | None
    score: Decimal
    rank: int | None
    topic_slugs: tuple[str, ...]
    primary_topic_slug: str | None
    news_score: Decimal
    gdelt_score: Decimal
    sec_score: Decimal
    fundamental_score: Decimal
    liquidity_score: Decimal
    finnhub_article_count: int
    gdelt_article_count: int
    sec_filing_count: int
    latest_news_at: datetime | None
    latest_filing_date: date | None
    action_bias: str
    rationale: dict[str, Any]


@dataclass(frozen=True)
class DailyDiscoveryResult:
    run_date: date
    profile: str
    candidates: tuple[CandidateScore, ...]
    warnings: tuple[str, ...]
    stats: dict[str, Any]


def emit_progress(
    progress_callback: ProgressCallback | None,
    **event: Any,
) -> None:
    if progress_callback is None:
        return
    progress_callback(event)


DEFAULT_MARKET_TOPICS = (
    MarketTopic(
        topic_slug="ai_infrastructure",
        topic_name="AI infrastructure",
        gdelt_query='("artificial intelligence" OR "generative AI" OR datacenter OR "data center")',
        keywords=(
            "artificial intelligence",
            "generative ai",
            "ai",
            "datacenter",
            "data center",
            "gpu",
            "accelerator",
            "inference",
            "training",
            "llm",
            "cloud capex",
        ),
        sectors=("Technology", "Semiconductors", "Cloud"),
        ticker_hints=("NVDA", "AMD", "AVGO", "TSM", "SMCI", "DELL", "MSFT", "GOOGL", "AMZN", "META"),
        priority=10,
    ),
    MarketTopic(
        topic_slug="semiconductors",
        topic_name="Semiconductors",
        gdelt_query="semiconductor OR chip OR foundry OR lithography OR memory",
        keywords=(
            "semiconductor",
            "chip",
            "foundry",
            "lithography",
            "memory",
            "wafer",
            "advanced packaging",
            "export control",
        ),
        sectors=("Technology", "Semiconductors"),
        ticker_hints=("NVDA", "AMD", "AVGO", "TSM", "ASML", "INTC", "MU", "QCOM", "MRVL", "AMAT", "LRCX"),
        priority=20,
    ),
    MarketTopic(
        topic_slug="power_energy",
        topic_name="Power and energy",
        gdelt_query='"power grid" OR electricity OR uranium OR "nuclear power" OR LNG OR "natural gas"',
        keywords=(
            "power grid",
            "electricity",
            "uranium",
            "nuclear",
            "nuclear power",
            "lng",
            "natural gas",
            "pipeline",
            "renewable",
            "utility",
        ),
        sectors=("Energy", "Utilities", "Industrials"),
        ticker_hints=("CEG", "VST", "NEE", "DUK", "SMR", "CCJ", "XOM", "CVX", "LNG", "ET"),
        priority=30,
    ),
    MarketTopic(
        topic_slug="biotech_fda",
        topic_name="Biotech and FDA",
        gdelt_query='"FDA approval" OR "clinical trial" OR "phase 3" OR "obesity drug" OR oncology',
        keywords=(
            "fda",
            "fda approval",
            "clinical trial",
            "phase 3",
            "obesity drug",
            "oncology",
            "biotech",
            "drug approval",
        ),
        sectors=("Healthcare", "Biotechnology", "Pharmaceuticals"),
        ticker_hints=("LLY", "NVO", "MRNA", "REGN", "VRTX", "BIIB", "GILD", "AMGN", "PFE", "MRK"),
        priority=40,
    ),
    MarketTopic(
        topic_slug="defense_geopolitics",
        topic_name="Defense and geopolitics",
        gdelt_query='defense OR missile OR drone OR "military contract" OR "geopolitical tension"',
        keywords=(
            "defense",
            "missile",
            "drone",
            "military contract",
            "geopolitical",
            "cyber warfare",
            "weapons",
            "aerospace",
        ),
        sectors=("Industrials", "Aerospace", "Defense"),
        ticker_hints=("LMT", "RTX", "NOC", "GD", "BA", "KTOS", "AVAV", "PLTR", "HII"),
        priority=50,
    ),
    MarketTopic(
        topic_slug="cybersecurity",
        topic_name="Cybersecurity",
        gdelt_query='cybersecurity OR ransomware OR "data breach" OR "cyber attack"',
        keywords=(
            "cybersecurity",
            "ransomware",
            "data breach",
            "cyber attack",
            "zero trust",
            "identity security",
            "endpoint security",
        ),
        sectors=("Technology", "Software"),
        ticker_hints=("CRWD", "PANW", "ZS", "FTNT", "OKTA", "NET", "S", "CYBR"),
        priority=60,
    ),
    MarketTopic(
        topic_slug="crypto_digital_assets",
        topic_name="Crypto and digital assets",
        gdelt_query='bitcoin OR crypto OR cryptocurrency OR stablecoin OR ethereum',
        keywords=(
            "bitcoin",
            "crypto",
            "cryptocurrency",
            "stablecoin",
            "ethereum",
            "digital asset",
            "blockchain",
            "spot etf",
        ),
        sectors=("Financial Services", "Technology"),
        ticker_hints=("COIN", "MSTR", "MARA", "RIOT", "HOOD", "SQ", "PYPL"),
        priority=70,
    ),
    MarketTopic(
        topic_slug="rates_macro",
        topic_name="Rates and macro",
        gdelt_query='"Federal Reserve" OR inflation OR "Treasury yield" OR recession OR "interest rates"',
        keywords=(
            "federal reserve",
            "inflation",
            "treasury yield",
            "interest rates",
            "rate cut",
            "recession",
            "jobs report",
            "cpi",
        ),
        sectors=("Financial Services", "Real Estate", "Consumer"),
        ticker_hints=("JPM", "BAC", "GS", "MS", "XLF", "TLT", "KRE", "VNQ"),
        priority=80,
    ),
    MarketTopic(
        topic_slug="tariffs_supply_chain",
        topic_name="Tariffs and supply chain",
        gdelt_query='tariff OR "supply chain" OR reshoring OR "export control" OR sanctions',
        keywords=(
            "tariff",
            "supply chain",
            "reshoring",
            "export control",
            "sanctions",
            "customs",
            "logistics",
        ),
        sectors=("Industrials", "Consumer", "Technology"),
        ticker_hints=("FDX", "UPS", "CAT", "DE", "AAPL", "TSLA", "WMT", "TGT"),
        priority=90,
    ),
    MarketTopic(
        topic_slug="ma_ipos",
        topic_name="M&A and IPOs",
        gdelt_query='merger OR acquisition OR IPO OR "takeover bid" OR "strategic review"',
        keywords=(
            "merger",
            "acquisition",
            "ipo",
            "takeover",
            "strategic review",
            "buyout",
            "deal",
        ),
        sectors=("Financial Services", "Technology", "Healthcare"),
        ticker_hints=("GS", "MS", "JPM", "KKR", "BX", "APO"),
        priority=100,
    ),
)


def get_database_url(database_url: str | None = None) -> str:
    database_url = database_url or get_settings().database_url
    if not database_url:
        raise RuntimeError("缺少 DATABASE_URL，请在环境变量或 .env 中配置。")
    return database_url


def ensure_discovery_schema(database_url: str) -> int:
    applied = db_migrations.migrate(database_url=database_url)
    return len(applied)


def normalize_ticker(value: str | None) -> str | None:
    if value is None:
        return None
    ticker = value.strip().upper()
    if not ticker:
        return None
    return ticker.replace(".", "-") if "/" in ticker else ticker


def decimal_or_zero(value: Decimal | int | float | str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def hash_uid(*parts: object) -> str:
    payload = json.dumps(parts, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_run_date(value: str | None) -> date:
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"日期格式必须是 YYYY-MM-DD: {value}") from exc


def normalize_text(value: object) -> str:
    return str(value or "").strip().lower()


def extract_keywords(text: str, *, limit: int = 12) -> list[str]:
    words = re.findall(r"[A-Za-z][A-Za-z0-9\-]{2,}", text.lower())
    counter: Counter[str] = Counter()
    for word in words:
        if word in STOPWORDS:
            continue
        if word.isdigit():
            continue
        counter[word] += 1

    return [word for word, _count in counter.most_common(limit)]


def catalyst_score(text: str) -> Decimal:
    lower = normalize_text(text)
    total = Decimal("0")
    for term, weight in CATALYST_TERMS.items():
        if term in lower:
            total += Decimal(weight)
    return min(total, Decimal("12"))


def topic_match_score(topic: MarketTopic, text: str) -> tuple[Decimal, list[str]]:
    lower = normalize_text(text)
    score = Decimal("0")
    matched: list[str] = []
    for keyword in topic.keywords:
        key = keyword.lower()
        if key in lower:
            score += Decimal("2.5") if " " in key else Decimal("1.5")
            matched.append(keyword)

    for sector in topic.sectors:
        if sector.lower() in lower:
            score += Decimal("1.0")
            matched.append(sector)

    return min(score, Decimal("12")), matched[:10]


def upsert_market_topics(conn: Connection, topics: tuple[MarketTopic, ...]) -> int:
    count = 0
    for topic in topics:
        conn.execute(
            """
            INSERT INTO market_topics (
                topic_slug,
                topic_name,
                gdelt_query,
                keywords,
                sectors,
                ticker_hints,
                priority,
                is_active,
                data_source,
                metadata,
                last_refreshed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, now())
            ON CONFLICT (topic_slug)
            DO UPDATE SET
                topic_name = EXCLUDED.topic_name,
                gdelt_query = EXCLUDED.gdelt_query,
                keywords = EXCLUDED.keywords,
                sectors = EXCLUDED.sectors,
                ticker_hints = EXCLUDED.ticker_hints,
                priority = EXCLUDED.priority,
                data_source = EXCLUDED.data_source,
                metadata = market_topics.metadata || EXCLUDED.metadata,
                last_refreshed_at = now(),
                updated_at = now()
            WHERE market_topics.data_source = 'seed'
            """,
            (
                topic.topic_slug,
                topic.topic_name,
                topic.gdelt_query,
                list(topic.keywords),
                list(topic.sectors),
                list(topic.ticker_hints),
                topic.priority,
                topic.data_source,
                Jsonb(topic.metadata),
            ),
        )
        count += 1
    return count


def seed_market_topics(database_url: str | None = None) -> int:
    database_url = get_database_url(database_url)
    ensure_discovery_schema(database_url)
    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.transaction():
            return upsert_market_topics(conn, DEFAULT_MARKET_TOPICS)


def fetch_active_topics(conn: Connection) -> list[MarketTopic]:
    rows = conn.execute(
        """
        SELECT topic_slug, topic_name, gdelt_query, keywords, sectors,
               ticker_hints, priority, data_source, metadata
        FROM market_topics
        WHERE is_active
        ORDER BY priority, topic_slug
        """
    ).fetchall()
    return [
        MarketTopic(
            topic_slug=row[0],
            topic_name=row[1],
            gdelt_query=row[2],
            keywords=tuple(row[3] or ()),
            sectors=tuple(row[4] or ()),
            ticker_hints=tuple(row[5] or ()),
            priority=row[6],
            data_source=row[7],
            metadata=row[8] or {},
        )
        for row in rows
    ]


def fetch_stock_universe(conn: Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ticker, company_name, sector, industry, business_description,
               market_cap_usd, avg_volume_30d, is_active, is_manual_watchlist,
               is_sp500, is_nasdaq100
        FROM stock_universe
        WHERE is_active
        """
    ).fetchall()
    return {
        row[0].upper(): {
            "ticker": row[0].upper(),
            "company_name": row[1],
            "sector": row[2],
            "industry": row[3],
            "business_description": row[4],
            "market_cap_usd": row[5],
            "avg_volume_30d": row[6],
            "is_active": row[7],
            "is_manual_watchlist": row[8],
            "is_sp500": row[9],
            "is_nasdaq100": row[10],
        }
        for row in rows
    }


def fetch_sec_scan_tickers(conn: Connection, limit: int) -> list[str]:
    if limit <= 0:
        return []
    rows = conn.execute(
        """
        SELECT ticker
        FROM stock_universe
        WHERE is_active
        ORDER BY is_manual_watchlist DESC,
                 is_sp500 DESC,
                 is_nasdaq100 DESC,
                 market_cap_usd DESC NULLS LAST,
                 ticker
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    return [row[0].upper() for row in rows]


def fetch_latest_finnhub_market_id(conn: Connection, category: str) -> int | None:
    row = conn.execute(
        """
        SELECT max(finnhub_id)
        FROM finnhub_articles
        WHERE endpoint = %s
          AND lower(coalesce(category, '')) = lower(%s)
          AND finnhub_id IS NOT NULL
        """,
        (finnhub.MARKET_NEWS_ENDPOINT, category),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def fetch_recent_finnhub_articles(
    conn: Connection,
    *,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT article_uid, finnhub_id, article_url, headline, summary, category,
               source_name, related_tickers, published_at, endpoint
        FROM finnhub_articles
        WHERE coalesce(published_at, last_seen_at, created_at)
              >= now() - (%s::int * interval '1 hour')
        ORDER BY coalesce(published_at, last_seen_at, created_at) DESC
        """,
        (lookback_hours,),
    ).fetchall()
    return [
        {
            "article_uid": row[0],
            "finnhub_id": row[1],
            "article_url": row[2],
            "headline": row[3],
            "summary": row[4],
            "category": row[5],
            "source_name": row[6],
            "related_tickers": list(row[7] or []),
            "published_at": row[8],
            "endpoint": row[9],
        }
        for row in rows
    ]


def fetch_recent_gdelt_articles(
    conn: Connection,
    *,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT article_url, title, domain, language, source_country, tone,
               query_text, seen_at, request_url
        FROM gdelt_articles
        WHERE coalesce(seen_at, last_seen_at, created_at)
              >= now() - (%s::int * interval '1 hour')
        ORDER BY coalesce(seen_at, last_seen_at, created_at) DESC
        """,
        (lookback_hours,),
    ).fetchall()
    return [
        {
            "article_url": row[0],
            "title": row[1],
            "domain": row[2],
            "language": row[3],
            "source_country": row[4],
            "tone": row[5],
            "query_text": row[6],
            "seen_at": row[7],
            "request_url": row[8],
        }
        for row in rows
    ]


def fetch_recent_sec_filings(
    conn: Connection,
    *,
    lookback_days: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ticker, company_name, accession_number, form_type, filing_date,
               report_date, items, primary_doc_description, filing_detail_url
        FROM sec_filings
        WHERE filing_date >= current_date - %s::int
          AND ticker IS NOT NULL
          AND form_type IN ('8-K', '10-Q', '10-K', 'S-1', '20-F', '6-K')
        ORDER BY filing_date DESC, ticker
        """,
        (lookback_days,),
    ).fetchall()
    return [
        {
            "ticker": row[0].upper(),
            "company_name": row[1],
            "accession_number": row[2],
            "form_type": row[3],
            "filing_date": row[4],
            "report_date": row[5],
            "items": row[6],
            "primary_doc_description": row[7],
            "filing_detail_url": row[8],
        }
        for row in rows
    ]


def fetch_fact_counts(conn: Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT upper(ticker), count(*)
        FROM sec_financial_facts
        WHERE ticker IS NOT NULL
        GROUP BY upper(ticker)
        """
    ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def maybe_sync_finnhub_market(
    *,
    database_url: str,
    categories: tuple[str, ...],
    incremental: bool,
    warnings: list[str],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    total = len(categories)
    warning_count = len(warnings)
    emit_progress(
        progress_callback,
        stage="finnhub",
        status="running",
        completed=0,
        total=total,
        message="开始同步 Finnhub market news。",
    )
    try:
        client = finnhub.make_finnhub_client()
    except Exception as exc:
        warnings.append(f"Finnhub market news 未同步: {exc}")
        emit_progress(
            progress_callback,
            stage="finnhub",
            status="warning",
            completed=0,
            total=total,
            detail=str(exc),
            message=f"Finnhub market news 未同步：{exc}",
        )
        return counts

    with psycopg.connect(database_url, autocommit=True) as conn:
        for index, category in enumerate(categories, start=1):
            min_id = fetch_latest_finnhub_market_id(conn, category) if incremental else None
            emit_progress(
                progress_callback,
                stage="finnhub",
                status="running",
                completed=index - 1,
                total=total,
                detail=f"category={category}",
                message=f"Finnhub {category} 同步中。",
            )
            try:
                counts[category] = finnhub.sync_market_news(
                    category=category,
                    min_id=min_id,
                    database_url=database_url,
                    client=client,
                )
                emit_progress(
                    progress_callback,
                    stage="finnhub",
                    status="running",
                    completed=index,
                    total=total,
                    detail=f"{category}: {counts[category]} 条",
                    message=f"Finnhub {category} 完成：{counts[category]} 条。",
                )
            except Exception as exc:
                warnings.append(f"Finnhub category={category} 同步失败: {exc}")
                emit_progress(
                    progress_callback,
                    stage="finnhub",
                    status="running",
                    completed=index,
                    total=total,
                    detail=f"{category}: 失败",
                    message=f"Finnhub {category} 同步失败：{exc}",
                )
    status = "warning" if len(warnings) > warning_count else "success"
    emit_progress(
        progress_callback,
        stage="finnhub",
        status=status,
        completed=total,
        total=total,
        detail=f"完成 {sum(counts.values())} 条",
        message=f"Finnhub market news 同步完成：{sum(counts.values())} 条。",
    )
    return counts


def maybe_sync_gdelt_topics(
    *,
    database_url: str,
    topics: list[MarketTopic],
    timespan: str,
    max_records: int,
    warnings: list[str],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    total = len(topics)
    warning_count = len(warnings)
    emit_progress(
        progress_callback,
        stage="gdelt",
        status="running",
        completed=0,
        total=total,
        message=f"开始同步 GDELT 主题新闻：{total} 个主题。",
    )
    try:
        client = gdelt.make_gdelt_client()
    except Exception as exc:
        warnings.append(f"GDELT 未同步: {exc}")
        emit_progress(
            progress_callback,
            stage="gdelt",
            status="warning",
            completed=0,
            total=total,
            detail=str(exc),
            message=f"GDELT 未同步：{exc}",
        )
        return counts

    for index, topic in enumerate(topics, start=1):
        emit_progress(
            progress_callback,
            stage="gdelt",
            status="running",
            completed=index - 1,
            total=total,
            detail=topic.topic_slug,
            message=f"GDELT {topic.topic_name} 同步中。",
        )
        try:
            article_count = gdelt.sync_articles(
                query=topic.gdelt_query,
                timespan=timespan,
                max_records=max_records,
                database_url=database_url,
                client=client,
            )
            timeline_count = gdelt.sync_timeline(
                query=topic.gdelt_query,
                timespan=timespan,
                database_url=database_url,
                client=client,
            )
            counts[topic.topic_slug] = {
                "articles": article_count,
                "timeline": timeline_count,
            }
            emit_progress(
                progress_callback,
                stage="gdelt",
                status="running",
                completed=index,
                total=total,
                detail=(
                    f"{topic.topic_slug}: article={article_count}, "
                    f"timeline={timeline_count}"
                ),
                message=(
                    f"GDELT {topic.topic_name} 完成："
                    f"article={article_count}，timeline={timeline_count}。"
                ),
            )
        except Exception as exc:
            warnings.append(f"GDELT topic={topic.topic_slug} 同步失败: {exc}")
            emit_progress(
                progress_callback,
                stage="gdelt",
                status="running",
                completed=index,
                total=total,
                detail=f"{topic.topic_slug}: 失败",
                message=f"GDELT {topic.topic_name} 同步失败：{exc}",
            )
    status = "warning" if len(warnings) > warning_count else "success"
    article_total = sum(item.get("articles", 0) for item in counts.values())
    timeline_total = sum(item.get("timeline", 0) for item in counts.values())
    emit_progress(
        progress_callback,
        stage="gdelt",
        status=status,
        completed=total,
        total=total,
        detail=f"article={article_total}, timeline={timeline_total}",
        message=(
            "GDELT 主题新闻同步完成："
            f"article={article_total}，timeline={timeline_total}。"
        ),
    )
    return counts


def maybe_sync_sec_filings(
    *,
    database_url: str,
    tickers: list[str],
    filing_limit: int,
    include_company_facts: bool,
    fact_limit: int | None,
    warnings: list[str],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, dict[str, int | str]]:
    counts: dict[str, dict[str, int | str]] = {}
    total = len(tickers)
    warning_count = len(warnings)
    emit_progress(
        progress_callback,
        stage="sec",
        status="running",
        completed=0,
        total=total,
        message=f"开始同步 SEC filings：{total} 个标的。",
    )
    if not tickers:
        emit_progress(
            progress_callback,
            stage="sec",
            status="skipped",
            completed=0,
            total=0,
            message="没有可扫描的 SEC 标的，已跳过。",
        )
        return counts
    try:
        client = sec.make_sec_client()
    except Exception as exc:
        warnings.append(f"SEC filings 未同步: {exc}")
        emit_progress(
            progress_callback,
            stage="sec",
            status="warning",
            completed=0,
            total=total,
            detail=str(exc),
            message=f"SEC filings 未同步：{exc}",
        )
        return counts

    for index, ticker in enumerate(tickers, start=1):
        emit_progress(
            progress_callback,
            stage="sec",
            status="running",
            completed=index - 1,
            total=total,
            detail=ticker,
            message=f"SEC {ticker} 同步中。",
        )
        try:
            cik, filing_count, fact_count = sec.sync_ticker(
                ticker=ticker,
                include_company_facts=include_company_facts,
                filing_limit=filing_limit,
                fact_limit=fact_limit,
                database_url=database_url,
                client=client,
            )
            counts[ticker] = {
                "cik": cik,
                "filings": filing_count,
                "facts": fact_count,
            }
            emit_progress(
                progress_callback,
                stage="sec",
                status="running",
                completed=index,
                total=total,
                detail=f"{ticker}: filing={filing_count}, fact={fact_count}",
                message=(
                    f"SEC {ticker} 完成："
                    f"filing={filing_count}，fact={fact_count}。"
                ),
            )
        except Exception as exc:
            warnings.append(f"SEC ticker={ticker} 同步失败: {exc}")
            emit_progress(
                progress_callback,
                stage="sec",
                status="running",
                completed=index,
                total=total,
                detail=f"{ticker}: 失败",
                message=f"SEC {ticker} 同步失败：{exc}",
            )
    status = "warning" if len(warnings) > warning_count else "success"
    filing_total = sum(int(item.get("filings", 0)) for item in counts.values())
    fact_total = sum(int(item.get("facts", 0)) for item in counts.values())
    emit_progress(
        progress_callback,
        stage="sec",
        status=status,
        completed=total,
        total=total,
        detail=f"filing={filing_total}, fact={fact_total}",
        message=(
            "SEC filings 同步完成："
            f"filing={filing_total}，fact={fact_total}。"
        ),
    )
    return counts


def candidate_for(
    candidates: dict[str, CandidateAccumulator],
    ticker: str,
    universe: dict[str, dict[str, Any]],
) -> CandidateAccumulator:
    normalized = ticker.upper()
    if normalized not in candidates:
        stock = universe.get(normalized, {})
        candidates[normalized] = CandidateAccumulator(
            ticker=normalized,
            company_name=stock.get("company_name"),
            market_cap_usd=stock.get("market_cap_usd"),
            avg_volume_30d=stock.get("avg_volume_30d"),
            active_in_universe=bool(stock.get("is_active")),
        )
    return candidates[normalized]


def add_topic_signal(
    candidate: CandidateAccumulator,
    topic_slug: str,
    score: Decimal,
) -> None:
    candidate.topic_scores[topic_slug] += int(score * Decimal("10"))
    candidate.topic_counts[topic_slug] += 1


def append_limited(values: list[str], value: str | None, *, limit: int = 5) -> None:
    if value and value not in values and len(values) < limit:
        values.append(value)


def build_stock_topic_mentions(
    *,
    topics: list[MarketTopic],
    universe: dict[str, dict[str, Any]],
    candidates: dict[str, CandidateAccumulator],
) -> list[TopicMention]:
    mentions: list[TopicMention] = []
    for ticker, stock in universe.items():
        text = " ".join(
            str(stock.get(key) or "")
            for key in ("company_name", "sector", "industry", "business_description")
        )
        for topic in topics:
            score, matched = topic_match_score(topic, text)
            if ticker in topic.ticker_hints:
                score += Decimal("3")
                matched.append("ticker_hint")
            if score <= 0:
                continue
            candidate = candidate_for(candidates, ticker, universe)
            add_topic_signal(candidate, topic.topic_slug, score)
            mentions.append(
                TopicMention(
                    mention_uid=hash_uid("stock_universe", ticker, topic.topic_slug),
                    topic_slug=topic.topic_slug,
                    ticker=ticker,
                    source_type="stock_universe",
                    source_uid=ticker,
                    source_title=stock.get("company_name"),
                    source_url=None,
                    published_at=None,
                    relevance_score=score,
                    evidence={"matched": matched[:10]},
                )
            )
    return mentions


def build_finnhub_mentions(
    *,
    articles: list[dict[str, Any]],
    topics: list[MarketTopic],
    universe: dict[str, dict[str, Any]],
    candidates: dict[str, CandidateAccumulator],
) -> list[TopicMention]:
    mentions: list[TopicMention] = []
    topic_by_hint: dict[str, list[MarketTopic]] = defaultdict(list)
    for topic in topics:
        for ticker in topic.ticker_hints:
            topic_by_hint[ticker].append(topic)

    for article in articles:
        text = f"{article.get('headline') or ''} {article.get('summary') or ''} {article.get('category') or ''}"
        article_keywords = extract_keywords(text, limit=8)
        matched_topics: list[tuple[MarketTopic, Decimal, list[str]]] = []
        for topic in topics:
            score, matched = topic_match_score(topic, text)
            if score > 0:
                matched_topics.append((topic, score, matched))

        related = [normalize_ticker(ticker) for ticker in article.get("related_tickers", [])]
        related_tickers = [ticker for ticker in related if ticker]
        for ticker in related_tickers:
            candidate = candidate_for(candidates, ticker, universe)
            candidate.finnhub_article_uids.add(article["article_uid"])
            candidate.latest_news_at = max_datetime(candidate.latest_news_at, article.get("published_at"))
            append_limited(candidate.recent_titles, article.get("headline"))
            for keyword in article_keywords:
                candidate.keywords[keyword] += 1

            topic_matches = matched_topics or [
                (topic, Decimal("3"), ["ticker_hint"])
                for topic in topic_by_hint.get(ticker, [])
            ]
            for topic, score, matched in topic_matches:
                relevance = score + Decimal("4") + catalyst_score(text)
                add_topic_signal(candidate, topic.topic_slug, relevance)
                mentions.append(
                    TopicMention(
                        mention_uid=hash_uid(
                            "finnhub_article",
                            article["article_uid"],
                            topic.topic_slug,
                            ticker,
                        ),
                        topic_slug=topic.topic_slug,
                        ticker=ticker,
                        source_type="finnhub_article",
                        source_uid=article["article_uid"],
                        source_title=article.get("headline"),
                        source_url=article.get("article_url"),
                        published_at=article.get("published_at"),
                        relevance_score=relevance,
                        evidence={
                            "matched": matched[:10],
                            "keywords": article_keywords,
                            "source": article.get("source_name"),
                            "category": article.get("category"),
                        },
                    )
                )
    return mentions


def build_gdelt_mentions(
    *,
    articles: list[dict[str, Any]],
    topics: list[MarketTopic],
    universe: dict[str, dict[str, Any]],
    candidates: dict[str, CandidateAccumulator],
) -> list[TopicMention]:
    mentions: list[TopicMention] = []
    topics_by_query = {topic.gdelt_query: topic for topic in topics}
    stock_texts = {
        ticker: " ".join(
            str(stock.get(key) or "")
            for key in ("company_name", "sector", "industry", "business_description")
        )
        for ticker, stock in universe.items()
    }

    for article in articles:
        topic = topics_by_query.get(article.get("query_text"))
        if not topic:
            continue
        text = article.get("title") or ""
        topic_score, matched = topic_match_score(topic, text)
        relevance = max(topic_score, Decimal("2")) + Decimal("1")
        mentions.append(
            TopicMention(
                mention_uid=hash_uid("gdelt_article", article["article_url"], topic.topic_slug),
                topic_slug=topic.topic_slug,
                ticker=None,
                source_type="gdelt_article",
                source_uid=article["article_url"],
                source_title=article.get("title"),
                source_url=article.get("article_url"),
                published_at=article.get("seen_at"),
                relevance_score=relevance,
                evidence={
                    "matched": matched[:10],
                    "domain": article.get("domain"),
                    "language": article.get("language"),
                    "source_country": article.get("source_country"),
                },
            )
        )

        mapped_tickers = set(topic.ticker_hints)
        for ticker, stock_text in stock_texts.items():
            score, _matched = topic_match_score(topic, stock_text)
            if score >= Decimal("3"):
                mapped_tickers.add(ticker)

        for ticker in sorted(mapped_tickers):
            if ticker not in universe and ticker not in topic.ticker_hints:
                continue
            candidate = candidate_for(candidates, ticker, universe)
            candidate.gdelt_article_urls.add(article["article_url"])
            candidate.latest_news_at = max_datetime(candidate.latest_news_at, article.get("seen_at"))
            append_limited(candidate.gdelt_titles, article.get("title"))
            add_topic_signal(candidate, topic.topic_slug, relevance)
    return mentions


def build_sec_mentions(
    *,
    filings: list[dict[str, Any]],
    topics: list[MarketTopic],
    universe: dict[str, dict[str, Any]],
    candidates: dict[str, CandidateAccumulator],
) -> list[TopicMention]:
    mentions: list[TopicMention] = []
    for filing in filings:
        ticker = normalize_ticker(filing.get("ticker"))
        if not ticker:
            continue
        candidate = candidate_for(candidates, ticker, universe)
        accession = filing["accession_number"]
        candidate.sec_accessions.add(accession)
        candidate.sec_forms[filing["form_type"]] += 1
        filing_date = filing.get("filing_date")
        if filing_date:
            candidate.latest_filing_date = max_date(candidate.latest_filing_date, filing_date)
        title = f"{filing.get('form_type')} {filing.get('primary_doc_description') or filing.get('items') or ''}".strip()
        append_limited(candidate.sec_titles, title)

        stock = universe.get(ticker, {})
        stock_text = " ".join(
            str(stock.get(key) or "")
            for key in ("company_name", "sector", "industry", "business_description")
        )
        filing_text = f"{title} {filing.get('items') or ''} {stock_text}"
        for topic in topics:
            score, matched = topic_match_score(topic, filing_text)
            if ticker in topic.ticker_hints:
                score += Decimal("2")
                matched.append("ticker_hint")
            if score <= 0:
                continue
            relevance = score + SEC_FORM_WEIGHTS.get(filing["form_type"], Decimal("4")) / Decimal("4")
            add_topic_signal(candidate, topic.topic_slug, relevance)
            mentions.append(
                TopicMention(
                    mention_uid=hash_uid("sec_filing", accession, topic.topic_slug, ticker),
                    topic_slug=topic.topic_slug,
                    ticker=ticker,
                    source_type="sec_filing",
                    source_uid=accession,
                    source_title=title,
                    source_url=filing.get("filing_detail_url"),
                    published_at=datetime.combine(
                        filing_date,
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    if filing_date
                    else None,
                    relevance_score=relevance,
                    evidence={
                        "matched": matched[:10],
                        "form_type": filing.get("form_type"),
                        "items": filing.get("items"),
                    },
                )
            )
    return mentions


def max_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def max_date(left: date | None, right: date | None) -> date | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def score_candidate(
    candidate: CandidateAccumulator,
    *,
    run_date: date,
) -> CandidateScore:
    news_count = len(candidate.finnhub_article_uids)
    gdelt_count = len(candidate.gdelt_article_urls)
    sec_count = len(candidate.sec_accessions)

    news_score = Decimal(news_count * 8) + Decimal(min(sum(candidate.keywords.values()), 8))
    for title in candidate.recent_titles:
        news_score += catalyst_score(title)
    news_score = min(news_score, Decimal("35"))

    gdelt_score = min(Decimal(gdelt_count) * Decimal("0.7"), Decimal("20"))

    sec_score = Decimal("0")
    for form_type, count in candidate.sec_forms.items():
        sec_score += SEC_FORM_WEIGHTS.get(form_type, Decimal("4")) * count
    sec_score = min(sec_score, Decimal("25"))

    fact_score = Decimal("0")
    if candidate.fact_count >= 100:
        fact_score = Decimal("10")
    elif candidate.fact_count > 0:
        fact_score = Decimal("6")

    liquidity_score = Decimal("0")
    market_cap = candidate.market_cap_usd
    volume = candidate.avg_volume_30d
    if market_cap is None:
        liquidity_score += Decimal("2")
    elif market_cap >= Decimal("10000000000"):
        liquidity_score += Decimal("8")
    elif market_cap >= Decimal("1000000000"):
        liquidity_score += Decimal("6")
    elif market_cap >= Decimal("300000000"):
        liquidity_score += Decimal("3")

    if volume is None:
        liquidity_score += Decimal("2")
    elif volume >= Decimal("1000000"):
        liquidity_score += Decimal("7")
    elif volume >= Decimal("250000"):
        liquidity_score += Decimal("4")
    elif volume >= Decimal("100000"):
        liquidity_score += Decimal("2")
    liquidity_score = min(liquidity_score, Decimal("15"))

    topic_strength = min(
        Decimal(sum(candidate.topic_scores.values())) / Decimal("20"),
        Decimal("8"),
    )
    score = (
        news_score
        + gdelt_score
        + sec_score
        + fact_score
        + liquidity_score
        + topic_strength
    )
    score = min(score.quantize(Decimal("0.0001")), Decimal("100.0000"))

    topics = tuple(
        topic
        for topic, _count in candidate.topic_counts.most_common(8)
    )
    primary_topic = topics[0] if topics else None
    if score >= Decimal("60"):
        action_bias = "review"
    elif score >= Decimal("25"):
        action_bias = "watch"
    else:
        action_bias = "skip"

    rationale = {
        "top_keywords": candidate.keywords.most_common(10),
        "recent_titles": candidate.recent_titles,
        "gdelt_titles": candidate.gdelt_titles,
        "sec_titles": candidate.sec_titles,
        "sec_forms": dict(candidate.sec_forms),
        "topic_counts": dict(candidate.topic_counts),
        "topic_scores": dict(candidate.topic_scores),
        "market_cap_usd": str(market_cap) if market_cap is not None else None,
        "avg_volume_30d": str(volume) if volume is not None else None,
        "active_in_universe": candidate.active_in_universe,
    }

    return CandidateScore(
        run_date=run_date,
        ticker=candidate.ticker,
        company_name=candidate.company_name,
        score=score,
        rank=None,
        topic_slugs=topics,
        primary_topic_slug=primary_topic,
        news_score=news_score.quantize(Decimal("0.0001")),
        gdelt_score=gdelt_score.quantize(Decimal("0.0001")),
        sec_score=sec_score.quantize(Decimal("0.0001")),
        fundamental_score=fact_score.quantize(Decimal("0.0001")),
        liquidity_score=liquidity_score.quantize(Decimal("0.0001")),
        finnhub_article_count=news_count,
        gdelt_article_count=gdelt_count,
        sec_filing_count=sec_count,
        latest_news_at=candidate.latest_news_at,
        latest_filing_date=candidate.latest_filing_date,
        action_bias=action_bias,
        rationale=rationale,
    )


def rank_candidates(
    candidates: dict[str, CandidateAccumulator],
    *,
    run_date: date,
    top_n: int,
) -> tuple[CandidateScore, ...]:
    scored = [
        score_candidate(candidate, run_date=run_date)
        for candidate in candidates.values()
        if candidate.finnhub_article_uids
        or candidate.gdelt_article_urls
        or candidate.sec_accessions
    ]
    scored.sort(
        key=lambda item: (
            item.score,
            item.sec_filing_count,
            item.finnhub_article_count,
        ),
        reverse=True,
    )
    ranked: list[CandidateScore] = []
    for index, score in enumerate(scored[:top_n], start=1):
        ranked.append(
            CandidateScore(
                run_date=score.run_date,
                ticker=score.ticker,
                company_name=score.company_name,
                score=score.score,
                rank=index,
                topic_slugs=score.topic_slugs,
                primary_topic_slug=score.primary_topic_slug,
                news_score=score.news_score,
                gdelt_score=score.gdelt_score,
                sec_score=score.sec_score,
                fundamental_score=score.fundamental_score,
                liquidity_score=score.liquidity_score,
                finnhub_article_count=score.finnhub_article_count,
                gdelt_article_count=score.gdelt_article_count,
                sec_filing_count=score.sec_filing_count,
                latest_news_at=score.latest_news_at,
                latest_filing_date=score.latest_filing_date,
                action_bias=score.action_bias,
                rationale=score.rationale,
            )
        )
    return tuple(ranked)


def upsert_topic_mentions(conn: Connection, mentions: list[TopicMention]) -> int:
    count = 0
    for mention in mentions:
        conn.execute(
            """
            INSERT INTO topic_mentions (
                mention_uid,
                topic_slug,
                ticker,
                source_type,
                source_uid,
                source_title,
                source_url,
                published_at,
                relevance_score,
                evidence,
                detected_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (mention_uid)
            DO UPDATE SET
                topic_slug = EXCLUDED.topic_slug,
                ticker = EXCLUDED.ticker,
                source_type = EXCLUDED.source_type,
                source_uid = EXCLUDED.source_uid,
                source_title = EXCLUDED.source_title,
                source_url = EXCLUDED.source_url,
                published_at = EXCLUDED.published_at,
                relevance_score = EXCLUDED.relevance_score,
                evidence = EXCLUDED.evidence,
                detected_at = now(),
                updated_at = now()
            """,
            (
                mention.mention_uid,
                mention.topic_slug,
                mention.ticker,
                mention.source_type,
                mention.source_uid,
                mention.source_title,
                mention.source_url,
                mention.published_at,
                mention.relevance_score,
                Jsonb(mention.evidence),
            ),
        )
        count += 1
    return count


def upsert_candidate_scores(conn: Connection, scores: tuple[CandidateScore, ...]) -> int:
    count = 0
    for score in scores:
        conn.execute(
            """
            INSERT INTO daily_candidate_scores (
                run_date,
                ticker,
                company_name,
                score,
                rank,
                topic_slugs,
                primary_topic_slug,
                news_score,
                gdelt_score,
                sec_score,
                fundamental_score,
                liquidity_score,
                finnhub_article_count,
                gdelt_article_count,
                sec_filing_count,
                latest_news_at,
                latest_filing_date,
                action_bias,
                rationale,
                generated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (run_date, ticker)
            DO UPDATE SET
                company_name = EXCLUDED.company_name,
                score = EXCLUDED.score,
                rank = EXCLUDED.rank,
                topic_slugs = EXCLUDED.topic_slugs,
                primary_topic_slug = EXCLUDED.primary_topic_slug,
                news_score = EXCLUDED.news_score,
                gdelt_score = EXCLUDED.gdelt_score,
                sec_score = EXCLUDED.sec_score,
                fundamental_score = EXCLUDED.fundamental_score,
                liquidity_score = EXCLUDED.liquidity_score,
                finnhub_article_count = EXCLUDED.finnhub_article_count,
                gdelt_article_count = EXCLUDED.gdelt_article_count,
                sec_filing_count = EXCLUDED.sec_filing_count,
                latest_news_at = EXCLUDED.latest_news_at,
                latest_filing_date = EXCLUDED.latest_filing_date,
                action_bias = EXCLUDED.action_bias,
                rationale = EXCLUDED.rationale,
                generated_at = now(),
                updated_at = now()
            """,
            (
                score.run_date,
                score.ticker,
                score.company_name,
                score.score,
                score.rank,
                list(score.topic_slugs),
                score.primary_topic_slug,
                score.news_score,
                score.gdelt_score,
                score.sec_score,
                score.fundamental_score,
                score.liquidity_score,
                score.finnhub_article_count,
                score.gdelt_article_count,
                score.sec_filing_count,
                score.latest_news_at,
                score.latest_filing_date,
                score.action_bias,
                Jsonb(score.rationale),
            ),
        )
        count += 1
    return count


def upsert_daily_watchlist(
    conn: Connection,
    result: DailyDiscoveryResult,
) -> None:
    payload = {
        "run_date": result.run_date.isoformat(),
        "profile": result.profile,
        "warnings": list(result.warnings),
        "stats": result.stats,
        "candidates": [candidate_to_dict(candidate) for candidate in result.candidates],
    }
    summary = build_summary(result)
    watchlist_uid = f"{result.profile}:{result.run_date.isoformat()}"
    conn.execute(
        """
        INSERT INTO daily_watchlists (
            watchlist_uid,
            run_date,
            profile,
            candidate_count,
            summary,
            raw_payload,
            generated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (watchlist_uid)
        DO UPDATE SET
            candidate_count = EXCLUDED.candidate_count,
            summary = EXCLUDED.summary,
            raw_payload = EXCLUDED.raw_payload,
            generated_at = now(),
            updated_at = now()
        """,
        (
            watchlist_uid,
            result.run_date,
            result.profile,
            len(result.candidates),
            summary,
            Jsonb(payload),
        ),
    )


def candidate_to_dict(candidate: CandidateScore) -> dict[str, Any]:
    return {
        "rank": candidate.rank,
        "ticker": candidate.ticker,
        "company_name": candidate.company_name,
        "score": str(candidate.score),
        "action_bias": candidate.action_bias,
        "topics": list(candidate.topic_slugs),
        "primary_topic": candidate.primary_topic_slug,
        "scores": {
            "news": str(candidate.news_score),
            "gdelt": str(candidate.gdelt_score),
            "sec": str(candidate.sec_score),
            "fundamental": str(candidate.fundamental_score),
            "liquidity": str(candidate.liquidity_score),
        },
        "counts": {
            "finnhub_articles": candidate.finnhub_article_count,
            "gdelt_articles": candidate.gdelt_article_count,
            "sec_filings": candidate.sec_filing_count,
        },
        "latest_news_at": candidate.latest_news_at.isoformat()
        if candidate.latest_news_at
        else None,
        "latest_filing_date": candidate.latest_filing_date.isoformat()
        if candidate.latest_filing_date
        else None,
        "rationale": candidate.rationale,
    }


def build_summary(result: DailyDiscoveryResult) -> str:
    if not result.candidates:
        return f"{result.run_date.isoformat()} no daily candidates generated."
    leaders = ", ".join(
        f"{candidate.ticker}({candidate.score})"
        for candidate in result.candidates[:5]
    )
    return f"{result.run_date.isoformat()} top candidates: {leaders}"


def run_daily_discovery(
    *,
    database_url: str | None = None,
    run_date: date | None = None,
    profile: str = "default",
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    gdelt_timespan: str = DEFAULT_GDELT_TIMESPAN,
    gdelt_max_records: int = gdelt.DEFAULT_MAX_RECORDS,
    finnhub_categories: tuple[str, ...] = DEFAULT_FINNHUB_CATEGORIES,
    incremental_finnhub: bool = True,
    max_sec_tickers: int = DEFAULT_MAX_SEC_TICKERS,
    sec_filing_limit: int = DEFAULT_SEC_FILING_LIMIT,
    include_company_facts: bool = False,
    fact_limit: int | None = None,
    skip_finnhub_sync: bool = False,
    skip_gdelt_sync: bool = False,
    skip_sec_sync: bool = False,
    top_n: int = DEFAULT_TOP_N,
    progress_callback: ProgressCallback | None = None,
) -> DailyDiscoveryResult:
    database_url = get_database_url(database_url)
    run_date = run_date or date.today()
    warnings: list[str] = []
    stats: dict[str, Any] = {}

    emit_progress(
        progress_callback,
        stage="prepare",
        status="running",
        completed=0,
        total=3,
        message="开始准备数据库结构。",
    )
    stats["migrations_applied"] = ensure_discovery_schema(database_url)
    emit_progress(
        progress_callback,
        stage="prepare",
        status="running",
        completed=1,
        total=3,
        detail=f"migrations={stats['migrations_applied']}",
        message=f"数据库结构检查完成：应用迁移 {stats['migrations_applied']} 个。",
    )

    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.transaction():
            stats["seed_topics"] = upsert_market_topics(conn, DEFAULT_MARKET_TOPICS)
        emit_progress(
            progress_callback,
            stage="prepare",
            status="running",
            completed=2,
            total=3,
            detail=f"seed_topics={stats['seed_topics']}",
            message=f"默认主题准备完成：写入或刷新 {stats['seed_topics']} 个。",
        )
        topics = fetch_active_topics(conn)
        universe = fetch_stock_universe(conn)
        sec_scan_tickers = fetch_sec_scan_tickers(conn, max_sec_tickers)
    emit_progress(
        progress_callback,
        stage="prepare",
        status="success",
        completed=3,
        total=3,
        detail=f"topics={len(topics)}, universe={len(universe)}, sec={len(sec_scan_tickers)}",
        message=(
            "基础数据准备完成："
            f"主题 {len(topics)} 个，股票池 {len(universe)} 个，"
            f"SEC 扫描 {len(sec_scan_tickers)} 个标的。"
        ),
    )

    if not skip_finnhub_sync:
        stats["finnhub_sync"] = maybe_sync_finnhub_market(
            database_url=database_url,
            categories=finnhub_categories,
            incremental=incremental_finnhub,
            warnings=warnings,
            progress_callback=progress_callback,
        )
    else:
        emit_progress(
            progress_callback,
            stage="finnhub",
            status="skipped",
            completed=0,
            total=0,
            message="已按参数跳过 Finnhub 新闻同步。",
        )
    if not skip_gdelt_sync:
        stats["gdelt_sync"] = maybe_sync_gdelt_topics(
            database_url=database_url,
            topics=topics,
            timespan=gdelt_timespan,
            max_records=gdelt_max_records,
            warnings=warnings,
            progress_callback=progress_callback,
        )
    else:
        emit_progress(
            progress_callback,
            stage="gdelt",
            status="skipped",
            completed=0,
            total=0,
            message="已按参数跳过 GDELT 主题新闻同步。",
        )
    if not skip_sec_sync:
        stats["sec_sync"] = maybe_sync_sec_filings(
            database_url=database_url,
            tickers=sec_scan_tickers,
            filing_limit=sec_filing_limit,
            include_company_facts=include_company_facts,
            fact_limit=fact_limit,
            warnings=warnings,
            progress_callback=progress_callback,
        )
    else:
        emit_progress(
            progress_callback,
            stage="sec",
            status="skipped",
            completed=0,
            total=0,
            message="已按参数跳过 SEC filings 同步。",
        )

    lookback_days = max(1, (lookback_hours + 23) // 24)
    candidates: dict[str, CandidateAccumulator] = {}
    mentions: list[TopicMention] = []

    emit_progress(
        progress_callback,
        stage="scoring",
        status="running",
        completed=0,
        total=4,
        message="开始读取近期数据并匹配主题。",
    )
    with psycopg.connect(database_url, autocommit=False) as conn:
        topics = fetch_active_topics(conn)
        universe = fetch_stock_universe(conn)
        fact_counts = fetch_fact_counts(conn)
        finnhub_articles = fetch_recent_finnhub_articles(conn, lookback_hours=lookback_hours)
        gdelt_articles = fetch_recent_gdelt_articles(conn, lookback_hours=lookback_hours)
        sec_filings = fetch_recent_sec_filings(conn, lookback_days=lookback_days)
        emit_progress(
            progress_callback,
            stage="scoring",
            status="running",
            completed=1,
            total=4,
            detail=(
                f"finnhub={len(finnhub_articles)}, "
                f"gdelt={len(gdelt_articles)}, sec={len(sec_filings)}"
            ),
            message=(
                "近期数据读取完成："
                f"Finnhub {len(finnhub_articles)} 篇，"
                f"GDELT {len(gdelt_articles)} 篇，"
                f"SEC {len(sec_filings)} 条。"
            ),
        )

        mentions.extend(
            build_stock_topic_mentions(
                topics=topics,
                universe=universe,
                candidates=candidates,
            )
        )
        mentions.extend(
            build_finnhub_mentions(
                articles=finnhub_articles,
                topics=topics,
                universe=universe,
                candidates=candidates,
            )
        )
        mentions.extend(
            build_gdelt_mentions(
                articles=gdelt_articles,
                topics=topics,
                universe=universe,
                candidates=candidates,
            )
        )
        mentions.extend(
            build_sec_mentions(
                filings=sec_filings,
                topics=topics,
                universe=universe,
                candidates=candidates,
            )
        )
        for ticker, fact_count in fact_counts.items():
            if ticker in candidates:
                candidates[ticker].fact_count = fact_count
        emit_progress(
            progress_callback,
            stage="scoring",
            status="running",
            completed=3,
            total=4,
            detail=f"mentions={len(mentions)}, candidates={len(candidates)}",
            message=(
                "主题命中匹配完成："
                f"提及 {len(mentions)} 条，候选标的 {len(candidates)} 个。"
            ),
        )

        scores = rank_candidates(candidates, run_date=run_date, top_n=top_n)
        emit_progress(
            progress_callback,
            stage="scoring",
            status="success",
            completed=4,
            total=4,
            detail=f"ranked={len(scores)}",
            message=f"候选评分完成：入选 {len(scores)} 个。",
        )
        stats.update(
            {
                "active_topics": len(topics),
                "stock_universe": len(universe),
                "recent_finnhub_articles": len(finnhub_articles),
                "recent_gdelt_articles": len(gdelt_articles),
                "recent_sec_filings": len(sec_filings),
                "topic_mentions": len(mentions),
                "candidate_count": len(scores),
            }
        )
        result = DailyDiscoveryResult(
            run_date=run_date,
            profile=profile,
            candidates=scores,
            warnings=tuple(warnings),
            stats=stats,
        )

        emit_progress(
            progress_callback,
            stage="persist",
            status="running",
            completed=0,
            total=3,
            message="开始保存主题提及、候选评分和观察列表。",
        )
        with conn.transaction():
            upsert_topic_mentions(conn, mentions)
            emit_progress(
                progress_callback,
                stage="persist",
                status="running",
                completed=1,
                total=3,
                detail=f"mentions={len(mentions)}",
                message=f"主题提及保存完成：{len(mentions)} 条。",
            )
            upsert_candidate_scores(conn, scores)
            emit_progress(
                progress_callback,
                stage="persist",
                status="running",
                completed=2,
                total=3,
                detail=f"candidates={len(scores)}",
                message=f"候选评分保存完成：{len(scores)} 个。",
            )
            upsert_daily_watchlist(conn, result)
        emit_progress(
            progress_callback,
            stage="persist",
            status="success",
            completed=3,
            total=3,
            detail=f"warnings={len(warnings)}",
            message=f"自动发现结果保存完成：警告 {len(warnings)} 条。",
        )

    return result


def render_result(result: DailyDiscoveryResult) -> str:
    lines = [
        f"[完成] 自动发现日报 {result.run_date.isoformat()} profile={result.profile}",
        f"候选股 {len(result.candidates)} 个，主题提及 {result.stats.get('topic_mentions', 0)} 条。",
    ]
    if result.warnings:
        lines.append("警告：")
        lines.extend(f"- {warning}" for warning in result.warnings)

    if result.candidates:
        lines.append("候选股：")
        for candidate in result.candidates:
            topics = ",".join(candidate.topic_slugs[:3]) or "-"
            lines.append(
                f"{candidate.rank:>2}. {candidate.ticker:<8} "
                f"score={candidate.score} action={candidate.action_bias} "
                f"topic={topics} news={candidate.finnhub_article_count} "
                f"gdelt={candidate.gdelt_article_count} "
                f"sec={candidate.sec_filing_count}"
            )
    return "\n".join(lines)


def add_daily_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    parser.add_argument("--run-date", help="运行日期，格式 YYYY-MM-DD，默认今天")
    parser.add_argument("--profile", default="default", help="日报配置名称")
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--gdelt-timespan", default=DEFAULT_GDELT_TIMESPAN)
    parser.add_argument("--gdelt-max-records", type=int, default=gdelt.DEFAULT_MAX_RECORDS)
    parser.add_argument(
        "--finnhub-category",
        action="append",
        dest="finnhub_categories",
        help="Finnhub market news 分类，可重复传入，默认 general 和 merger",
    )
    parser.add_argument("--no-incremental-finnhub", action="store_true")
    parser.add_argument("--max-sec-tickers", type=int, default=DEFAULT_MAX_SEC_TICKERS)
    parser.add_argument("--sec-filing-limit", type=int, default=DEFAULT_SEC_FILING_LIMIT)
    parser.add_argument("--include-company-facts", action="store_true")
    parser.add_argument("--fact-limit", type=int)
    parser.add_argument("--skip-finnhub-sync", action="store_true")
    parser.add_argument("--skip-gdelt-sync", action="store_true")
    parser.add_argument("--skip-sec-sync", action="store_true")
    parser.add_argument("--skip-sync", action="store_true", help="跳过所有外部同步，仅使用库内已有数据评分")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)


def add_extract_topics_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--max-candidates", type=int, default=25)
    parser.add_argument("--min-articles", type=int, default=2)
    parser.add_argument("--min-score", default="8")
    parser.add_argument(
        "--include-existing-matches",
        action="store_true",
        help="保留与现有正式主题相近的候选主题，默认会过滤",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印结果，不写入数据库")


def add_promote_topics_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")
    parser.add_argument(
        "--slug",
        action="append",
        dest="slugs",
        help="要晋升的候选主题 slug，可重复传入；不传则按阈值自动选择",
    )
    parser.add_argument("--min-score", default="20")
    parser.add_argument("--min-articles", type=int, default=3)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument(
        "--activate",
        action="store_true",
        help="晋升后立即启用主题；默认写入正式主题库但不启用",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run automatic market discovery.")
    subparsers = parser.add_subparsers(dest="command")

    seed_parser = subparsers.add_parser("seed-topics", help="写入或刷新默认市场主题库")
    seed_parser.add_argument("--database-url", help="PostgreSQL DATABASE_URL")

    daily_parser = subparsers.add_parser("daily", help="运行一次每日热点发现和候选股评分")
    add_daily_args(daily_parser)

    extract_parser = subparsers.add_parser("extract-topics", help="从最近新闻中抽取候选主题")
    add_extract_topics_args(extract_parser)

    promote_parser = subparsers.add_parser("promote-topics", help="将候选主题晋升到正式主题库")
    add_promote_topics_args(promote_parser)

    loop_parser = subparsers.add_parser("loop", help="按固定间隔循环运行每日热点发现")
    add_daily_args(loop_parser)
    loop_parser.add_argument("--interval-minutes", type=int, default=60)
    loop_parser.add_argument("--max-runs", type=int)

    return parser


def run_from_args(args: argparse.Namespace) -> DailyDiscoveryResult:
    skip_all = bool(args.skip_sync)
    categories = tuple(args.finnhub_categories or DEFAULT_FINNHUB_CATEGORIES)
    return run_daily_discovery(
        database_url=args.database_url,
        run_date=parse_run_date(args.run_date),
        profile=args.profile,
        lookback_hours=args.lookback_hours,
        gdelt_timespan=args.gdelt_timespan,
        gdelt_max_records=args.gdelt_max_records,
        finnhub_categories=categories,
        incremental_finnhub=not args.no_incremental_finnhub,
        max_sec_tickers=args.max_sec_tickers,
        sec_filing_limit=args.sec_filing_limit,
        include_company_facts=args.include_company_facts,
        fact_limit=args.fact_limit,
        skip_finnhub_sync=skip_all or args.skip_finnhub_sync,
        skip_gdelt_sync=skip_all or args.skip_gdelt_sync,
        skip_sec_sync=skip_all or args.skip_sec_sync,
        top_n=args.top_n,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "seed-topics":
            count = seed_market_topics(args.database_url)
            print(f"[完成] 默认主题库刷新 {count} 个主题")
            return 0

        if args.command == "daily":
            result = run_from_args(args)
            print(render_result(result))
            return 0

        if args.command == "extract-topics":
            from usstock.discovery import topic_candidates

            result = topic_candidates.run_topic_extraction(
                database_url=args.database_url,
                lookback_hours=args.lookback_hours,
                max_candidates=args.max_candidates,
                min_articles=args.min_articles,
                min_score=args.min_score,
                include_existing_matches=args.include_existing_matches,
                dry_run=args.dry_run,
            )
            print(topic_candidates.render_extraction_result(result))
            return 0

        if args.command == "promote-topics":
            from usstock.discovery import topic_candidates

            result = topic_candidates.promote_topic_candidates(
                database_url=args.database_url,
                slugs=tuple(args.slugs or ()),
                min_score=args.min_score,
                min_articles=args.min_articles,
                limit=args.limit,
                activate=args.activate,
            )
            print(topic_candidates.render_promotion_result(result))
            return 0

        if args.command == "loop":
            runs = 0
            while True:
                result = run_from_args(args)
                print(render_result(result), flush=True)
                runs += 1
                if args.max_runs is not None and runs >= args.max_runs:
                    return 0
                time.sleep(max(1, args.interval_minutes) * 60)

        parser.print_help()
        return 2
    except Exception as exc:
        print(f"自动发现流程失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
