"""News-derived topic extraction helpers."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable


WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9&+\-]*")
SLUG_RE = re.compile(r"[^a-z0-9]+")

SIGNAL_SHORT_WORDS = {
    "ai",
    "ev",
    "fda",
    "ipo",
    "lng",
}

ACRONYMS = {
    "ai": "AI",
    "ev": "EV",
    "fda": "FDA",
    "ipo": "IPO",
    "lng": "LNG",
    "sec": "SEC",
    "m&a": "M&A",
}

STOPWORDS = {
    "about",
    "above",
    "after",
    "again",
    "against",
    "amid",
    "among",
    "analyst",
    "analysts",
    "and",
    "announces",
    "are",
    "around",
    "as",
    "at",
    "be",
    "before",
    "being",
    "between",
    "billion",
    "brief",
    "but",
    "by",
    "can",
    "ceo",
    "chief",
    "co",
    "company",
    "corp",
    "corporation",
    "could",
    "day",
    "days",
    "down",
    "during",
    "for",
    "from",
    "group",
    "has",
    "have",
    "his",
    "holdings",
    "inc",
    "into",
    "its",
    "ltd",
    "market",
    "markets",
    "million",
    "more",
    "new",
    "news",
    "not",
    "of",
    "on",
    "over",
    "per",
    "plc",
    "quarter",
    "report",
    "reports",
    "says",
    "share",
    "shares",
    "stock",
    "stocks",
    "than",
    "that",
    "the",
    "their",
    "this",
    "through",
    "to",
    "under",
    "up",
    "us",
    "was",
    "with",
    "would",
}


@dataclass(frozen=True)
class NewsDocument:
    source_type: str
    source_uid: str
    title: str
    body: str = ""
    url: str | None = None
    source_name: str | None = None
    published_at: datetime | date | None = None
    tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExistingTopicSignature:
    topic_slug: str
    terms: frozenset[str]


@dataclass(frozen=True)
class ExtractedTopicCandidate:
    candidate_slug: str
    topic_name: str
    gdelt_query: str
    keywords: tuple[str, ...]
    ticker_hints: tuple[str, ...]
    source_types: tuple[str, ...]
    article_count: int
    source_count: int
    ticker_count: int
    trend_score: Decimal
    novelty_score: Decimal
    matched_topic_slug: str | None
    evidence: tuple[dict[str, Any], ...]
    metadata: dict[str, Any]


@dataclass
class _TopicBucket:
    phrase: str
    phrase_scores: Counter[str] = field(default_factory=Counter)
    evidence_by_uid: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_names: Counter[str] = field(default_factory=Counter)
    source_types: Counter[str] = field(default_factory=Counter)
    tickers: Counter[str] = field(default_factory=Counter)
    keyword_scores: Counter[str] = field(default_factory=Counter)
    score: Decimal = Decimal("0")

    def merge(self, other: "_TopicBucket") -> None:
        self.phrase_scores.update(other.phrase_scores)
        self.evidence_by_uid.update(other.evidence_by_uid)
        self.source_names.update(other.source_names)
        self.source_types.update(other.source_types)
        self.tickers.update(other.tickers)
        self.keyword_scores.update(other.keyword_scores)
        self.score += other.score


def extract_news_topics(
    documents: Iterable[NewsDocument],
    *,
    existing_topics: Iterable[ExistingTopicSignature] = (),
    max_candidates: int = 25,
    min_articles: int = 2,
    min_score: Decimal | int | str = Decimal("8"),
    include_existing_matches: bool = False,
    evidence_limit: int = 5,
) -> list[ExtractedTopicCandidate]:
    buckets = _merge_similar_buckets(_collect_buckets(documents))
    existing = list(existing_topics)
    min_score_decimal = Decimal(str(min_score))
    candidates: list[ExtractedTopicCandidate] = []

    for bucket in buckets:
        article_count = len(bucket.evidence_by_uid)
        if article_count < min_articles:
            continue

        matched_topic_slug, similarity = _best_existing_match(bucket, existing)
        if matched_topic_slug and not include_existing_matches:
            continue

        source_count = len(bucket.source_names)
        ticker_count = len(bucket.tickers)
        trend_score = (
            bucket.score
            + Decimal(article_count * 4)
            + Decimal(source_count * 2)
            + Decimal(ticker_count)
        ).quantize(Decimal("0.0001"))
        if trend_score < min_score_decimal:
            continue

        novelty_score = (Decimal("100") * (Decimal("1") - similarity)).quantize(
            Decimal("0.0001")
        )
        keywords = _rank_keywords(bucket)
        topic_name = topic_name_from_phrase(bucket.phrase)
        evidence = _rank_evidence(bucket, limit=evidence_limit)

        candidates.append(
            ExtractedTopicCandidate(
                candidate_slug=slugify_topic(topic_name),
                topic_name=topic_name,
                gdelt_query=build_gdelt_query(keywords),
                keywords=tuple(keywords),
                ticker_hints=tuple(
                    ticker for ticker, _count in bucket.tickers.most_common(12)
                ),
                source_types=tuple(
                    source_type
                    for source_type, _count in bucket.source_types.most_common()
                ),
                article_count=article_count,
                source_count=source_count,
                ticker_count=ticker_count,
                trend_score=trend_score,
                novelty_score=novelty_score,
                matched_topic_slug=matched_topic_slug,
                evidence=tuple(evidence),
                metadata={
                    "primary_phrase": bucket.phrase,
                    "similarity_to_existing": str(similarity),
                    "phrase_scores": dict(bucket.phrase_scores.most_common(8)),
                },
            )
        )

    candidates.sort(
        key=lambda item: (
            item.trend_score,
            item.novelty_score,
            item.article_count,
            item.topic_name,
        ),
        reverse=True,
    )
    return candidates[:max_candidates]


def build_existing_topic_signatures(
    topics: Iterable[dict[str, Any]],
) -> list[ExistingTopicSignature]:
    signatures: list[ExistingTopicSignature] = []
    for topic in topics:
        terms: set[str] = set()
        for value in (
            topic.get("topic_slug"),
            topic.get("topic_name"),
            topic.get("gdelt_query"),
        ):
            terms.update(tokenize_terms(str(value or "").replace("_", " ")))
        for keyword in topic.get("keywords") or ():
            keyword_text = str(keyword or "").strip().lower()
            if keyword_text:
                terms.add(keyword_text)
            terms.update(tokenize_terms(keyword_text))

        topic_slug = str(topic.get("topic_slug") or "").strip()
        if topic_slug and terms:
            signatures.append(
                ExistingTopicSignature(
                    topic_slug=topic_slug,
                    terms=frozenset(terms),
                )
            )
    return signatures


def tokenize_terms(text: str) -> list[str]:
    terms: list[str] = []
    for raw_word in WORD_RE.findall(text.lower()):
        word = raw_word.strip("-+")
        if not word:
            continue
        if word in STOPWORDS:
            continue
        if len(word) < 3 and word not in SIGNAL_SHORT_WORDS:
            continue
        terms.append(word)
    return terms


def topic_name_from_phrase(phrase: str) -> str:
    words = []
    for word in phrase.split():
        words.append(ACRONYMS.get(word, word.capitalize()))
    return " ".join(words)


def slugify_topic(topic_name: str) -> str:
    slug = SLUG_RE.sub("_", topic_name.lower()).strip("_")
    return (slug or "topic")[:80]


def build_gdelt_query(keywords: Iterable[str], *, limit: int = 8) -> str:
    parts: list[str] = []
    for keyword in keywords:
        item = keyword.strip().lower()
        if not item or item in parts:
            continue
        if " " in item or "&" in item:
            parts.append(f'"{item}"')
        else:
            parts.append(item)
        if len(parts) >= limit:
            break
    return " OR ".join(parts) or "market"


def _collect_buckets(documents: Iterable[NewsDocument]) -> list[_TopicBucket]:
    buckets: dict[str, _TopicBucket] = {}
    for document in documents:
        phrases = _document_phrases(document)
        if not phrases:
            continue

        for phrase, phrase_score in phrases.items():
            bucket = buckets.setdefault(phrase, _TopicBucket(phrase=phrase))
            bucket.score += Decimal(str(phrase_score))
            bucket.phrase_scores[phrase] += int(phrase_score * 10)
            bucket.keyword_scores[phrase] += int(phrase_score * 10)

            for token in phrase.split():
                bucket.keyword_scores[token] += 1

            source_name = document.source_name or document.source_type or "unknown"
            bucket.source_names[source_name] += 1
            bucket.source_types[document.source_type] += 1
            for ticker in document.tickers:
                ticker = ticker.strip().upper()
                if ticker:
                    bucket.tickers[ticker] += 1

            bucket.evidence_by_uid.setdefault(
                document.source_uid,
                {
                    "source_type": document.source_type,
                    "source_uid": document.source_uid,
                    "title": document.title,
                    "url": document.url,
                    "source_name": source_name,
                    "published_at": _format_time(document.published_at),
                    "tickers": list(document.tickers),
                    "matched_phrase": phrase,
                },
            )

    return list(buckets.values())


def _document_phrases(document: NewsDocument) -> Counter[str]:
    title_terms = tokenize_terms(document.title)
    body_terms = tokenize_terms(document.body)
    title_phrases = _phrases_from_terms(title_terms)
    body_phrases = _phrases_from_terms(body_terms)
    scores: Counter[str] = Counter()

    for phrase in title_phrases:
        scores[phrase] += _phrase_weight(phrase, title=True)
    for phrase in body_phrases:
        scores[phrase] += _phrase_weight(phrase, title=False)

    return Counter(dict(scores.most_common(14)))


def _phrases_from_terms(terms: list[str]) -> list[str]:
    phrases: list[str] = []
    for size in (3, 2, 1):
        for index in range(0, len(terms) - size + 1):
            phrase_terms = terms[index : index + size]
            if len(set(phrase_terms)) != len(phrase_terms):
                continue
            phrase = " ".join(phrase_terms)
            if size == 1 and phrase in STOPWORDS:
                continue
            phrases.append(phrase)
    return phrases


def _phrase_weight(phrase: str, *, title: bool) -> float:
    size = len(phrase.split())
    weight = 1.0 + (size - 1) * 1.2
    if title:
        weight += 1.4
    if any(word in SIGNAL_SHORT_WORDS for word in phrase.split()):
        weight += 0.8
    return weight


def _merge_similar_buckets(buckets: list[_TopicBucket]) -> list[_TopicBucket]:
    ordered = sorted(
        buckets,
        key=lambda item: (len(item.evidence_by_uid), item.score, len(item.phrase)),
        reverse=True,
    )
    merged: list[_TopicBucket] = []
    for bucket in ordered:
        target = next(
            (
                existing
                for existing in merged
                if _phrase_similarity(existing.phrase, bucket.phrase) >= Decimal("0.67")
            ),
            None,
        )
        if target:
            target.merge(bucket)
        else:
            merged.append(bucket)
    return merged


def _phrase_similarity(left: str, right: str) -> Decimal:
    left_terms = set(left.split())
    right_terms = set(right.split())
    if not left_terms or not right_terms:
        return Decimal("0")
    if left_terms.issubset(right_terms) or right_terms.issubset(left_terms):
        return Decimal("1")
    overlap = len(left_terms & right_terms)
    return Decimal(overlap) / Decimal(max(len(left_terms), len(right_terms)))


def _best_existing_match(
    bucket: _TopicBucket,
    existing_topics: list[ExistingTopicSignature],
) -> tuple[str | None, Decimal]:
    candidate_terms = set(bucket.phrase.split())
    candidate_terms.update(bucket.keyword_scores)
    if not candidate_terms:
        return None, Decimal("0")

    best_slug: str | None = None
    best_similarity = Decimal("0")
    for topic in existing_topics:
        overlap = candidate_terms & set(topic.terms)
        if not overlap:
            continue
        similarity = Decimal(len(overlap)) / Decimal(max(len(candidate_terms), 1))
        phrase_match = bucket.phrase in topic.terms
        if phrase_match:
            similarity = max(similarity, Decimal("0.75"))
        if similarity > best_similarity:
            best_slug = topic.topic_slug
            best_similarity = similarity

    if best_similarity >= Decimal("0.35"):
        return best_slug, min(best_similarity, Decimal("1"))
    return None, best_similarity


def _rank_keywords(bucket: _TopicBucket) -> list[str]:
    keywords: list[str] = [bucket.phrase]
    for keyword, _score in bucket.keyword_scores.most_common(12):
        if keyword not in keywords and len(keyword) >= 2:
            keywords.append(keyword)
        if len(keywords) >= 10:
            break
    return keywords


def _rank_evidence(bucket: _TopicBucket, *, limit: int) -> list[dict[str, Any]]:
    evidence = list(bucket.evidence_by_uid.values())
    evidence.sort(
        key=lambda item: (
            item.get("published_at") or "",
            item.get("source_name") or "",
            item.get("title") or "",
        ),
        reverse=True,
    )
    return evidence[:limit]


def _format_time(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()
