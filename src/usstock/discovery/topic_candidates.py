"""Persist and promote news-derived market topic candidates."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from usstock.discovery.daily import (
    ensure_discovery_schema,
    fetch_active_topics,
    fetch_recent_finnhub_articles,
    fetch_recent_gdelt_articles,
    get_database_url,
)
from usstock.trends.extract import (
    ExistingTopicSignature,
    ExtractedTopicCandidate,
    NewsDocument,
    build_existing_topic_signatures,
    extract_news_topics,
)


DEFAULT_TOPIC_EXTRACTION_LOOKBACK_HOURS = 72
DEFAULT_MAX_CANDIDATES = 25
DEFAULT_MIN_ARTICLES = 2
DEFAULT_MIN_SCORE = Decimal("8")
EXTRACTION_ALGORITHM = "rule_v1"


@dataclass(frozen=True)
class TopicExtractionResult:
    candidates: tuple[ExtractedTopicCandidate, ...]
    stats: dict[str, Any]


@dataclass(frozen=True)
class TopicPromotionResult:
    promoted_slugs: tuple[str, ...]
    stats: dict[str, Any]


def run_topic_extraction(
    *,
    database_url: str | None = None,
    lookback_hours: int = DEFAULT_TOPIC_EXTRACTION_LOOKBACK_HOURS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_articles: int = DEFAULT_MIN_ARTICLES,
    min_score: Decimal | int | str = DEFAULT_MIN_SCORE,
    include_existing_matches: bool = False,
    dry_run: bool = False,
) -> TopicExtractionResult:
    database_url = get_database_url(database_url)
    stats: dict[str, Any] = {
        "migrations_applied": 0 if dry_run else ensure_discovery_schema(database_url),
    }

    with psycopg.connect(database_url, autocommit=False) as conn:
        topics = fetch_active_topics(conn)
        finnhub_articles = fetch_recent_finnhub_articles(
            conn,
            lookback_hours=lookback_hours,
        )
        gdelt_articles = fetch_recent_gdelt_articles(
            conn,
            lookback_hours=lookback_hours,
        )
        documents = build_news_documents(
            finnhub_articles=finnhub_articles,
            gdelt_articles=gdelt_articles,
        )
        signatures = active_topic_signatures(topics)
        candidates = extract_news_topics(
            documents,
            existing_topics=signatures,
            max_candidates=max_candidates,
            min_articles=min_articles,
            min_score=min_score,
            include_existing_matches=include_existing_matches,
        )

        stats.update(
            {
                "active_topics": len(topics),
                "recent_finnhub_articles": len(finnhub_articles),
                "recent_gdelt_articles": len(gdelt_articles),
                "news_documents": len(documents),
                "extracted_candidates": len(candidates),
                "dry_run": dry_run,
            }
        )

        if not dry_run:
            with conn.transaction():
                stats["upserted_candidates"] = upsert_topic_candidates(
                    conn,
                    candidates,
                    lookback_hours=lookback_hours,
                )

    return TopicExtractionResult(
        candidates=tuple(candidates),
        stats=stats,
    )


def build_news_documents(
    *,
    finnhub_articles: list[dict[str, Any]],
    gdelt_articles: list[dict[str, Any]],
) -> list[NewsDocument]:
    documents: list[NewsDocument] = []

    for article in finnhub_articles:
        source_uid = str(article.get("article_uid") or article.get("article_url") or "")
        title = str(article.get("headline") or "").strip()
        if not source_uid or not title:
            continue
        documents.append(
            NewsDocument(
                source_type="finnhub",
                source_uid=source_uid,
                title=title,
                body=str(article.get("summary") or ""),
                url=article.get("article_url"),
                source_name=article.get("source_name"),
                published_at=article.get("published_at"),
                tickers=tuple(
                    str(ticker).upper()
                    for ticker in article.get("related_tickers") or ()
                    if str(ticker).strip()
                ),
            )
        )

    for article in gdelt_articles:
        source_uid = str(article.get("article_url") or "")
        title = str(article.get("title") or "").strip()
        if not source_uid or not title:
            continue
        documents.append(
            NewsDocument(
                source_type="gdelt",
                source_uid=source_uid,
                title=title,
                url=article.get("article_url"),
                source_name=article.get("domain"),
                published_at=article.get("seen_at"),
            )
        )

    return documents


def active_topic_signatures(topics: list[Any]) -> list[ExistingTopicSignature]:
    return build_existing_topic_signatures(
        [
            {
                "topic_slug": topic.topic_slug,
                "topic_name": topic.topic_name,
                "gdelt_query": topic.gdelt_query,
                "keywords": topic.keywords,
            }
            for topic in topics
        ]
    )


def upsert_topic_candidates(
    conn: Connection,
    candidates: tuple[ExtractedTopicCandidate, ...] | list[ExtractedTopicCandidate],
    *,
    lookback_hours: int,
) -> int:
    count = 0
    for candidate in candidates:
        metadata = dict(candidate.metadata)
        metadata.update(
            {
                "algorithm": EXTRACTION_ALGORITHM,
                "extraction_window_hours": lookback_hours,
            }
        )
        conn.execute(
            """
            INSERT INTO market_topic_candidates (
                candidate_slug,
                topic_name,
                gdelt_query,
                keywords,
                ticker_hints,
                source_types,
                article_count,
                source_count,
                ticker_count,
                trend_score,
                novelty_score,
                status,
                matched_topic_slug,
                extraction_window_hours,
                evidence,
                metadata,
                last_seen_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'pending', %s, %s, %s, %s, now()
            )
            ON CONFLICT (candidate_slug)
            DO UPDATE SET
                topic_name = EXCLUDED.topic_name,
                gdelt_query = EXCLUDED.gdelt_query,
                keywords = EXCLUDED.keywords,
                ticker_hints = EXCLUDED.ticker_hints,
                source_types = EXCLUDED.source_types,
                article_count = EXCLUDED.article_count,
                source_count = EXCLUDED.source_count,
                ticker_count = EXCLUDED.ticker_count,
                trend_score = EXCLUDED.trend_score,
                novelty_score = EXCLUDED.novelty_score,
                status = CASE
                    WHEN market_topic_candidates.status IN ('promoted', 'rejected', 'ignored')
                    THEN market_topic_candidates.status
                    ELSE EXCLUDED.status
                END,
                matched_topic_slug = EXCLUDED.matched_topic_slug,
                extraction_window_hours = EXCLUDED.extraction_window_hours,
                evidence = EXCLUDED.evidence,
                metadata = market_topic_candidates.metadata || EXCLUDED.metadata,
                last_seen_at = now(),
                updated_at = now()
            """,
            (
                candidate.candidate_slug,
                candidate.topic_name,
                candidate.gdelt_query,
                list(candidate.keywords),
                list(candidate.ticker_hints),
                list(candidate.source_types),
                candidate.article_count,
                candidate.source_count,
                candidate.ticker_count,
                candidate.trend_score,
                candidate.novelty_score,
                candidate.matched_topic_slug,
                lookback_hours,
                Jsonb(list(candidate.evidence)),
                Jsonb(metadata),
            ),
        )
        count += 1
    return count


def promote_topic_candidates(
    *,
    database_url: str | None = None,
    slugs: tuple[str, ...] = (),
    min_score: Decimal | int | str = Decimal("20"),
    min_articles: int = 3,
    limit: int = 10,
    activate: bool = False,
) -> TopicPromotionResult:
    database_url = get_database_url(database_url)
    stats: dict[str, Any] = {
        "migrations_applied": ensure_discovery_schema(database_url),
    }

    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.transaction():
            rows = fetch_promotable_candidates(
                conn,
                slugs=slugs,
                min_score=Decimal(str(min_score)),
                min_articles=min_articles,
                limit=limit,
            )
            promoted = insert_promoted_topics(conn, rows, activate=activate)
            mark_candidates_promoted(conn, promoted)

    stats.update(
        {
            "requested_slugs": list(slugs),
            "promotable_candidates": len(rows),
            "promoted_topics": len(promoted),
            "activate": activate,
        }
    )
    return TopicPromotionResult(
        promoted_slugs=tuple(promoted),
        stats=stats,
    )


def fetch_promotable_candidates(
    conn: Connection,
    *,
    slugs: tuple[str, ...],
    min_score: Decimal,
    min_articles: int,
    limit: int,
) -> list[dict[str, Any]]:
    if slugs:
        rows = conn.execute(
            """
            SELECT candidate_slug, topic_name, gdelt_query, keywords,
                   ticker_hints, trend_score, novelty_score, evidence, metadata
            FROM market_topic_candidates
            WHERE status = 'pending'
              AND matched_topic_slug IS NULL
              AND candidate_slug = ANY(%s)
            ORDER BY trend_score DESC, last_seen_at DESC
            """,
            (list(slugs),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT candidate_slug, topic_name, gdelt_query, keywords,
                   ticker_hints, trend_score, novelty_score, evidence, metadata
            FROM market_topic_candidates
            WHERE status = 'pending'
              AND matched_topic_slug IS NULL
              AND trend_score >= %s
              AND article_count >= %s
            ORDER BY trend_score DESC, novelty_score DESC, last_seen_at DESC
            LIMIT %s
            """,
            (min_score, min_articles, limit),
        ).fetchall()

    return [
        {
            "candidate_slug": row[0],
            "topic_name": row[1],
            "gdelt_query": row[2],
            "keywords": list(row[3] or []),
            "ticker_hints": list(row[4] or []),
            "trend_score": row[5],
            "novelty_score": row[6],
            "evidence": row[7] or [],
            "metadata": row[8] or {},
        }
        for row in rows
    ]


def insert_promoted_topics(
    conn: Connection,
    rows: list[dict[str, Any]],
    *,
    activate: bool,
) -> list[str]:
    promoted: list[str] = []
    for row in rows:
        metadata = dict(row["metadata"])
        metadata.update(
            {
                "promoted_from": "market_topic_candidates",
                "trend_score": str(row["trend_score"]),
                "novelty_score": str(row["novelty_score"]),
                "evidence": row["evidence"],
            }
        )
        inserted = conn.execute(
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
            VALUES (
                %s, %s, %s, %s, '{}'::text[], %s, 150, %s,
                'news_extraction', %s, now()
            )
            ON CONFLICT (topic_slug) DO NOTHING
            RETURNING topic_slug
            """,
            (
                row["candidate_slug"],
                row["topic_name"],
                row["gdelt_query"],
                row["keywords"],
                row["ticker_hints"],
                activate,
                Jsonb(metadata),
            ),
        ).fetchone()
        if inserted:
            promoted.append(inserted[0])
    return promoted


def mark_candidates_promoted(conn: Connection, slugs: list[str]) -> None:
    if not slugs:
        return
    conn.execute(
        """
        UPDATE market_topic_candidates
        SET status = 'promoted',
            promoted_at = now(),
            updated_at = now()
        WHERE candidate_slug = ANY(%s)
        """,
        (slugs,),
    )


def render_extraction_result(result: TopicExtractionResult) -> str:
    lines = [
        "[完成] 新闻候选主题抽取",
        (
            f"候选主题 {len(result.candidates)} 个，"
            f"新闻文档 {result.stats.get('news_documents', 0)} 篇。"
        ),
    ]
    if result.stats.get("dry_run"):
        lines.append("dry-run 模式：未写入数据库。")

    for candidate in result.candidates:
        tickers = ",".join(candidate.ticker_hints[:5]) or "-"
        lines.append(
            f"- {candidate.candidate_slug} score={candidate.trend_score} "
            f"articles={candidate.article_count} tickers={tickers} "
            f"query={candidate.gdelt_query}"
        )
    return "\n".join(lines)


def render_promotion_result(result: TopicPromotionResult) -> str:
    if not result.promoted_slugs:
        return "[完成] 未晋升新的正式主题。"
    slugs = ", ".join(result.promoted_slugs)
    return f"[完成] 已晋升 {len(result.promoted_slugs)} 个主题：{slugs}"
