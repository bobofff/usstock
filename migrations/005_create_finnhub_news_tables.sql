-- Migration: 005_create_finnhub_news_tables
-- Purpose: create Finnhub News API ingestion tables for query runs and
-- standardized financial news articles.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Tables
--   are linked by query_uid through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS finnhub_news_queries (
    id BIGSERIAL PRIMARY KEY,
    query_uid TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    category TEXT,
    ticker TEXT,
    from_date DATE,
    to_date DATE,
    min_id BIGINT,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT finnhub_news_queries_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT finnhub_news_queries_endpoint_not_blank
        CHECK (length(trim(endpoint)) > 0),
    CONSTRAINT finnhub_news_queries_request_url_not_blank
        CHECK (length(trim(request_url)) > 0),
    CONSTRAINT finnhub_news_queries_min_id_positive
        CHECK (min_id IS NULL OR min_id > 0),
    CONSTRAINT finnhub_news_queries_date_order
        CHECK (from_date IS NULL OR to_date IS NULL OR from_date <= to_date)
);

COMMENT ON TABLE finnhub_news_queries IS
    'Finnhub 新闻查询记录表，用于保存 market news 或 company news 每次请求的参数、脱敏请求地址和原始响应。';
COMMENT ON COLUMN finnhub_news_queries.id IS
    '内部自增主键。';
COMMENT ON COLUMN finnhub_news_queries.query_uid IS
    '查询去重键，由应用层根据 endpoint、分类、ticker、日期范围和 min_id 等参数生成。';
COMMENT ON COLUMN finnhub_news_queries.endpoint IS
    'Finnhub 新闻接口类型，例如 market_news 或 company_news。';
COMMENT ON COLUMN finnhub_news_queries.category IS
    'Finnhub market news 分类，例如 general、forex、crypto、merger。';
COMMENT ON COLUMN finnhub_news_queries.ticker IS
    'Finnhub company news 查询的股票代码，market news 为空。';
COMMENT ON COLUMN finnhub_news_queries.from_date IS
    'Finnhub company news 查询开始日期。';
COMMENT ON COLUMN finnhub_news_queries.to_date IS
    'Finnhub company news 查询结束日期。';
COMMENT ON COLUMN finnhub_news_queries.min_id IS
    'Finnhub market news 的 minId 增量抓取参数。';
COMMENT ON COLUMN finnhub_news_queries.request_url IS
    '脱敏后的完整请求地址，不包含 Finnhub API token，用于审计和复现查询。';
COMMENT ON COLUMN finnhub_news_queries.raw_payload IS
    'Finnhub News API 返回的完整原始 JSON。';
COMMENT ON COLUMN finnhub_news_queries.fetched_at IS
    '该查询响应的抓取时间。';
COMMENT ON COLUMN finnhub_news_queries.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN finnhub_news_queries.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_finnhub_news_queries_query_uid
    ON finnhub_news_queries (query_uid);

CREATE INDEX IF NOT EXISTS idx_finnhub_news_queries_endpoint
    ON finnhub_news_queries (endpoint);

CREATE INDEX IF NOT EXISTS idx_finnhub_news_queries_ticker
    ON finnhub_news_queries (ticker);

CREATE INDEX IF NOT EXISTS idx_finnhub_news_queries_fetched_at
    ON finnhub_news_queries (fetched_at DESC);

CREATE TABLE IF NOT EXISTS finnhub_articles (
    id BIGSERIAL PRIMARY KEY,
    article_uid TEXT NOT NULL,
    finnhub_id BIGINT,
    article_url TEXT NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    category TEXT,
    source_name TEXT,
    image_url TEXT,
    related_tickers TEXT[] NOT NULL DEFAULT '{}'::text[],
    published_at TIMESTAMPTZ,
    source_type TEXT NOT NULL DEFAULT 'financial_news',
    query_uid TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT finnhub_articles_article_uid_not_blank
        CHECK (length(trim(article_uid)) > 0),
    CONSTRAINT finnhub_articles_article_url_not_blank
        CHECK (length(trim(article_url)) > 0),
    CONSTRAINT finnhub_articles_headline_not_blank
        CHECK (length(trim(headline)) > 0),
    CONSTRAINT finnhub_articles_source_type_not_blank
        CHECK (length(trim(source_type)) > 0),
    CONSTRAINT finnhub_articles_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT finnhub_articles_endpoint_not_blank
        CHECK (length(trim(endpoint)) > 0),
    CONSTRAINT finnhub_articles_request_url_not_blank
        CHECK (length(trim(request_url)) > 0)
);

COMMENT ON TABLE finnhub_articles IS
    'Finnhub 金融新闻文章表，用于保存 market news 和 company news 返回的标准化字段与原始 JSON。';
COMMENT ON COLUMN finnhub_articles.id IS
    '内部自增主键。';
COMMENT ON COLUMN finnhub_articles.article_uid IS
    '文章去重键，优先使用 Finnhub id，缺失时由 URL、标题和发布时间生成。';
COMMENT ON COLUMN finnhub_articles.finnhub_id IS
    'Finnhub 返回的新闻 ID，可能为空。';
COMMENT ON COLUMN finnhub_articles.article_url IS
    '新闻原文 URL。';
COMMENT ON COLUMN finnhub_articles.headline IS
    '新闻标题。';
COMMENT ON COLUMN finnhub_articles.summary IS
    '新闻摘要或正文片段。';
COMMENT ON COLUMN finnhub_articles.category IS
    'Finnhub 返回或查询使用的新闻分类。';
COMMENT ON COLUMN finnhub_articles.source_name IS
    '新闻来源名称，例如 MarketWatch、Reuters 等。';
COMMENT ON COLUMN finnhub_articles.image_url IS
    '新闻配图 URL。';
COMMENT ON COLUMN finnhub_articles.related_tickers IS
    'Finnhub 返回的相关股票代码列表。';
COMMENT ON COLUMN finnhub_articles.published_at IS
    '新闻发布时间，由 Finnhub datetime Unix 时间戳转换而来。';
COMMENT ON COLUMN finnhub_articles.source_type IS
    '统一数据源类型，Finnhub News 默认为 financial_news。';
COMMENT ON COLUMN finnhub_articles.query_uid IS
    '产生该文章的查询去重键。';
COMMENT ON COLUMN finnhub_articles.endpoint IS
    '产生该文章的 Finnhub 新闻接口类型。';
COMMENT ON COLUMN finnhub_articles.request_url IS
    '产生该文章的脱敏 Finnhub 请求地址。';
COMMENT ON COLUMN finnhub_articles.raw_payload IS
    '该文章在 Finnhub 响应中的原始 JSON 片段。';
COMMENT ON COLUMN finnhub_articles.first_seen_at IS
    '系统第一次写入该文章的时间。';
COMMENT ON COLUMN finnhub_articles.last_seen_at IS
    '系统最近一次通过 Finnhub 查询看到该文章的时间。';
COMMENT ON COLUMN finnhub_articles.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN finnhub_articles.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_finnhub_articles_article_uid
    ON finnhub_articles (article_uid);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_finnhub_id
    ON finnhub_articles (finnhub_id);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_published_at
    ON finnhub_articles (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_category
    ON finnhub_articles (category);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_source_name
    ON finnhub_articles (source_name);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_related_tickers
    ON finnhub_articles USING GIN (related_tickers);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_query_uid
    ON finnhub_articles (query_uid);

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_endpoint
    ON finnhub_articles (endpoint);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_finnhub_news_queries_updated_at ON finnhub_news_queries;
CREATE TRIGGER trg_finnhub_news_queries_updated_at
BEFORE UPDATE ON finnhub_news_queries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_finnhub_articles_updated_at ON finnhub_articles;
CREATE TRIGGER trg_finnhub_articles_updated_at
BEFORE UPDATE ON finnhub_articles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_finnhub_articles_updated_at ON finnhub_articles;
DROP TRIGGER IF EXISTS trg_finnhub_news_queries_updated_at ON finnhub_news_queries;

DROP INDEX IF EXISTS idx_finnhub_articles_endpoint;
DROP INDEX IF EXISTS idx_finnhub_articles_query_uid;
DROP INDEX IF EXISTS idx_finnhub_articles_related_tickers;
DROP INDEX IF EXISTS idx_finnhub_articles_source_name;
DROP INDEX IF EXISTS idx_finnhub_articles_category;
DROP INDEX IF EXISTS idx_finnhub_articles_published_at;
DROP INDEX IF EXISTS idx_finnhub_articles_finnhub_id;
DROP INDEX IF EXISTS idx_finnhub_articles_article_uid;
DROP INDEX IF EXISTS idx_finnhub_news_queries_fetched_at;
DROP INDEX IF EXISTS idx_finnhub_news_queries_ticker;
DROP INDEX IF EXISTS idx_finnhub_news_queries_endpoint;
DROP INDEX IF EXISTS idx_finnhub_news_queries_query_uid;

DROP TABLE IF EXISTS finnhub_articles;
DROP TABLE IF EXISTS finnhub_news_queries;
