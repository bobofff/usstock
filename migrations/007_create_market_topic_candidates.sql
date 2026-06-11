-- Migration: 007_create_market_topic_candidates
-- Purpose: create a reviewable candidate topic table for news-derived themes.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Candidate
--   topics are linked to market_topics and source articles by application-level
--   slugs, URLs, and source IDs.

-- migrate:up

CREATE TABLE IF NOT EXISTS market_topic_candidates (
    id BIGSERIAL PRIMARY KEY,
    candidate_slug TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    gdelt_query TEXT NOT NULL,
    keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
    ticker_hints TEXT[] NOT NULL DEFAULT '{}'::text[],
    source_types TEXT[] NOT NULL DEFAULT '{}'::text[],
    article_count INTEGER NOT NULL DEFAULT 0,
    source_count INTEGER NOT NULL DEFAULT 0,
    ticker_count INTEGER NOT NULL DEFAULT 0,
    trend_score NUMERIC(12, 4) NOT NULL DEFAULT 0,
    novelty_score NUMERIC(12, 4) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    matched_topic_slug TEXT,
    extraction_window_hours INTEGER NOT NULL,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT market_topic_candidates_candidate_slug_not_blank
        CHECK (length(trim(candidate_slug)) > 0),
    CONSTRAINT market_topic_candidates_topic_name_not_blank
        CHECK (length(trim(topic_name)) > 0),
    CONSTRAINT market_topic_candidates_gdelt_query_not_blank
        CHECK (length(trim(gdelt_query)) > 0),
    CONSTRAINT market_topic_candidates_article_count_non_negative
        CHECK (article_count >= 0),
    CONSTRAINT market_topic_candidates_source_count_non_negative
        CHECK (source_count >= 0),
    CONSTRAINT market_topic_candidates_ticker_count_non_negative
        CHECK (ticker_count >= 0),
    CONSTRAINT market_topic_candidates_trend_score_non_negative
        CHECK (trend_score >= 0),
    CONSTRAINT market_topic_candidates_novelty_score_non_negative
        CHECK (novelty_score >= 0),
    CONSTRAINT market_topic_candidates_status_known
        CHECK (status IN ('pending', 'promoted', 'rejected', 'ignored')),
    CONSTRAINT market_topic_candidates_window_positive
        CHECK (extraction_window_hours > 0)
);

COMMENT ON TABLE market_topic_candidates IS
    '市场热点候选主题表，用于保存从新闻中自动抽取、尚待审核或晋升的主题。';
COMMENT ON COLUMN market_topic_candidates.id IS
    '内部自增主键。';
COMMENT ON COLUMN market_topic_candidates.candidate_slug IS
    '候选主题唯一标识，由主题名称或核心关键词规范化生成。';
COMMENT ON COLUMN market_topic_candidates.topic_name IS
    '候选主题展示名称，通常来自核心关键词短语。';
COMMENT ON COLUMN market_topic_candidates.gdelt_query IS
    '建议用于 GDELT DOC API 的 query 参数。';
COMMENT ON COLUMN market_topic_candidates.keywords IS
    '候选主题关键词数组，用于后续匹配新闻、公司业务描述和公告文本。';
COMMENT ON COLUMN market_topic_candidates.ticker_hints IS
    '候选主题在新闻中出现的相关股票代码数组。';
COMMENT ON COLUMN market_topic_candidates.source_types IS
    '产生该候选主题的数据源类型数组，例如 finnhub、gdelt。';
COMMENT ON COLUMN market_topic_candidates.article_count IS
    '抽取窗口内支持该候选主题的文章数量。';
COMMENT ON COLUMN market_topic_candidates.source_count IS
    '支持该候选主题的不同新闻来源数量。';
COMMENT ON COLUMN market_topic_candidates.ticker_count IS
    '支持该候选主题的不同股票代码数量。';
COMMENT ON COLUMN market_topic_candidates.trend_score IS
    '候选主题趋势分数，综合文章数、来源覆盖、ticker 覆盖和关键词强度。';
COMMENT ON COLUMN market_topic_candidates.novelty_score IS
    '候选主题新颖度分数，越高表示越不像已有正式主题。';
COMMENT ON COLUMN market_topic_candidates.status IS
    '候选主题状态：pending 待审核、promoted 已晋升、rejected 已拒绝、ignored 已忽略。';
COMMENT ON COLUMN market_topic_candidates.matched_topic_slug IS
    '若候选主题与已有正式主题相近，则保存匹配到的主题标识。';
COMMENT ON COLUMN market_topic_candidates.extraction_window_hours IS
    '生成该候选主题时使用的新闻回看窗口，单位小时。';
COMMENT ON COLUMN market_topic_candidates.evidence IS
    '候选主题证据列表，保存来源文章标题、链接、来源、时间和命中关键词。';
COMMENT ON COLUMN market_topic_candidates.metadata IS
    '候选主题扩展元数据，用于保存抽取参数、算法版本和调试信息。';
COMMENT ON COLUMN market_topic_candidates.first_seen_at IS
    '系统第一次发现该候选主题的时间。';
COMMENT ON COLUMN market_topic_candidates.last_seen_at IS
    '系统最近一次发现或刷新该候选主题的时间。';
COMMENT ON COLUMN market_topic_candidates.promoted_at IS
    '候选主题晋升为正式主题的时间。';
COMMENT ON COLUMN market_topic_candidates.rejected_at IS
    '候选主题被拒绝的时间。';
COMMENT ON COLUMN market_topic_candidates.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN market_topic_candidates.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_topic_candidates_slug
    ON market_topic_candidates (candidate_slug);

CREATE INDEX IF NOT EXISTS idx_market_topic_candidates_status_score
    ON market_topic_candidates (status, trend_score DESC, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_topic_candidates_last_seen
    ON market_topic_candidates (last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_topic_candidates_keywords
    ON market_topic_candidates USING GIN (keywords);

CREATE INDEX IF NOT EXISTS idx_market_topic_candidates_ticker_hints
    ON market_topic_candidates USING GIN (ticker_hints);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_market_topic_candidates_updated_at ON market_topic_candidates;
CREATE TRIGGER trg_market_topic_candidates_updated_at
BEFORE UPDATE ON market_topic_candidates
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_market_topic_candidates_updated_at ON market_topic_candidates;

DROP INDEX IF EXISTS idx_market_topic_candidates_ticker_hints;
DROP INDEX IF EXISTS idx_market_topic_candidates_keywords;
DROP INDEX IF EXISTS idx_market_topic_candidates_last_seen;
DROP INDEX IF EXISTS idx_market_topic_candidates_status_score;
DROP INDEX IF EXISTS idx_market_topic_candidates_slug;

DROP TABLE IF EXISTS market_topic_candidates;
