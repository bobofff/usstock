-- Migration: 004_create_gdelt_doc_tables
-- Purpose: create GDELT DOC API ingestion tables for query runs, article
-- results, and raw timeline volume points.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Tables
--   are linked by query_uid and article_url through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS gdelt_doc_queries (
    id BIGSERIAL PRIMARY KEY,
    query_uid TEXT NOT NULL,
    query_text TEXT NOT NULL,
    mode TEXT NOT NULL,
    format TEXT NOT NULL DEFAULT 'json',
    timespan TEXT,
    start_datetime TIMESTAMPTZ,
    end_datetime TIMESTAMPTZ,
    sort TEXT,
    max_records INTEGER,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT gdelt_doc_queries_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT gdelt_doc_queries_query_text_not_blank
        CHECK (length(trim(query_text)) > 0),
    CONSTRAINT gdelt_doc_queries_mode_not_blank
        CHECK (length(trim(mode)) > 0),
    CONSTRAINT gdelt_doc_queries_format_not_blank
        CHECK (length(trim(format)) > 0),
    CONSTRAINT gdelt_doc_queries_request_url_not_blank
        CHECK (length(trim(request_url)) > 0),
    CONSTRAINT gdelt_doc_queries_max_records_positive
        CHECK (max_records IS NULL OR max_records > 0)
);

COMMENT ON TABLE gdelt_doc_queries IS
    'GDELT DOC API 查询记录表，用于保存每次文章列表或时间线查询的参数、请求地址和原始响应。';
COMMENT ON COLUMN gdelt_doc_queries.id IS
    '内部自增主键。';
COMMENT ON COLUMN gdelt_doc_queries.query_uid IS
    '查询去重键，由应用层根据 query、mode、timespan、时间范围、排序和数量等参数生成。';
COMMENT ON COLUMN gdelt_doc_queries.query_text IS
    'GDELT DOC API query 参数原文。';
COMMENT ON COLUMN gdelt_doc_queries.mode IS
    'GDELT DOC API mode 参数，例如 artlist、timelinevolraw。';
COMMENT ON COLUMN gdelt_doc_queries.format IS
    'GDELT DOC API format 参数，当前默认 json。';
COMMENT ON COLUMN gdelt_doc_queries.timespan IS
    'GDELT DOC API timespan 参数，例如 1h、24h、7d。';
COMMENT ON COLUMN gdelt_doc_queries.start_datetime IS
    'GDELT DOC API startdatetime 参数转换后的时间。';
COMMENT ON COLUMN gdelt_doc_queries.end_datetime IS
    'GDELT DOC API enddatetime 参数转换后的时间。';
COMMENT ON COLUMN gdelt_doc_queries.sort IS
    'GDELT DOC API sort 参数，例如 datedesc、dateasc、hybridrel。';
COMMENT ON COLUMN gdelt_doc_queries.max_records IS
    'GDELT DOC API maxrecords 参数。';
COMMENT ON COLUMN gdelt_doc_queries.request_url IS
    '完整请求地址，用于审计和复现查询。';
COMMENT ON COLUMN gdelt_doc_queries.raw_payload IS
    'GDELT DOC API 返回的完整原始 JSON。';
COMMENT ON COLUMN gdelt_doc_queries.fetched_at IS
    '该查询响应的抓取时间。';
COMMENT ON COLUMN gdelt_doc_queries.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN gdelt_doc_queries.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_doc_queries_query_uid
    ON gdelt_doc_queries (query_uid);

CREATE INDEX IF NOT EXISTS idx_gdelt_doc_queries_fetched_at
    ON gdelt_doc_queries (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_gdelt_doc_queries_mode
    ON gdelt_doc_queries (mode);

CREATE TABLE IF NOT EXISTS gdelt_articles (
    id BIGSERIAL PRIMARY KEY,
    article_url TEXT NOT NULL,
    mobile_url TEXT,
    title TEXT NOT NULL,
    seen_at TIMESTAMPTZ,
    domain TEXT,
    language TEXT,
    source_country TEXT,
    social_image_url TEXT,
    tone NUMERIC(12, 6),
    source_type TEXT NOT NULL DEFAULT 'global_news',
    query_uid TEXT NOT NULL,
    query_text TEXT NOT NULL,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT gdelt_articles_article_url_not_blank
        CHECK (length(trim(article_url)) > 0),
    CONSTRAINT gdelt_articles_title_not_blank
        CHECK (length(trim(title)) > 0),
    CONSTRAINT gdelt_articles_source_type_not_blank
        CHECK (length(trim(source_type)) > 0),
    CONSTRAINT gdelt_articles_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT gdelt_articles_query_text_not_blank
        CHECK (length(trim(query_text)) > 0),
    CONSTRAINT gdelt_articles_request_url_not_blank
        CHECK (length(trim(request_url)) > 0)
);

COMMENT ON TABLE gdelt_articles IS
    'GDELT 文章表，用于保存 DOC API ArticleList 返回的全球新闻文章标准化字段和原始 JSON。';
COMMENT ON COLUMN gdelt_articles.id IS
    '内部自增主键。';
COMMENT ON COLUMN gdelt_articles.article_url IS
    '文章原始 URL，用作文章去重主键之一。';
COMMENT ON COLUMN gdelt_articles.mobile_url IS
    '文章移动端或 AMP URL，若 GDELT 返回则保存。';
COMMENT ON COLUMN gdelt_articles.title IS
    '文章标题。';
COMMENT ON COLUMN gdelt_articles.seen_at IS
    'GDELT 识别到的文章发布时间或收录时间。';
COMMENT ON COLUMN gdelt_articles.domain IS
    '新闻来源域名。';
COMMENT ON COLUMN gdelt_articles.language IS
    '文章语言。';
COMMENT ON COLUMN gdelt_articles.source_country IS
    '新闻来源所在国家或地区。';
COMMENT ON COLUMN gdelt_articles.social_image_url IS
    '文章社交分享图片 URL。';
COMMENT ON COLUMN gdelt_articles.tone IS
    'GDELT 返回的文章情绪/语调分数，可能为空。';
COMMENT ON COLUMN gdelt_articles.source_type IS
    '统一数据源类型，GDELT DOC API 默认为 global_news。';
COMMENT ON COLUMN gdelt_articles.query_uid IS
    '产生该文章的查询去重键。';
COMMENT ON COLUMN gdelt_articles.query_text IS
    '产生该文章的 GDELT query 参数原文。';
COMMENT ON COLUMN gdelt_articles.request_url IS
    '产生该文章的完整 GDELT 请求地址。';
COMMENT ON COLUMN gdelt_articles.raw_payload IS
    '该文章在 GDELT 响应中的原始 JSON 片段。';
COMMENT ON COLUMN gdelt_articles.first_seen_at IS
    '系统第一次写入该文章的时间。';
COMMENT ON COLUMN gdelt_articles.last_seen_at IS
    '系统最近一次通过 GDELT 查询看到该文章的时间。';
COMMENT ON COLUMN gdelt_articles.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN gdelt_articles.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_articles_article_url
    ON gdelt_articles (article_url);

CREATE INDEX IF NOT EXISTS idx_gdelt_articles_seen_at
    ON gdelt_articles (seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_gdelt_articles_domain
    ON gdelt_articles (domain);

CREATE INDEX IF NOT EXISTS idx_gdelt_articles_language
    ON gdelt_articles (language);

CREATE INDEX IF NOT EXISTS idx_gdelt_articles_source_country
    ON gdelt_articles (source_country);

CREATE INDEX IF NOT EXISTS idx_gdelt_articles_query_uid
    ON gdelt_articles (query_uid);

CREATE TABLE IF NOT EXISTS gdelt_timeline_points (
    id BIGSERIAL PRIMARY KEY,
    point_uid TEXT NOT NULL,
    query_uid TEXT NOT NULL,
    query_text TEXT NOT NULL,
    mode TEXT NOT NULL,
    bucket_start_at TIMESTAMPTZ NOT NULL,
    article_count NUMERIC(20, 6),
    norm_count NUMERIC(20, 6),
    volume_share NUMERIC(20, 10),
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT gdelt_timeline_points_point_uid_not_blank
        CHECK (length(trim(point_uid)) > 0),
    CONSTRAINT gdelt_timeline_points_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT gdelt_timeline_points_query_text_not_blank
        CHECK (length(trim(query_text)) > 0),
    CONSTRAINT gdelt_timeline_points_mode_not_blank
        CHECK (length(trim(mode)) > 0),
    CONSTRAINT gdelt_timeline_points_article_count_non_negative
        CHECK (article_count IS NULL OR article_count >= 0),
    CONSTRAINT gdelt_timeline_points_norm_count_non_negative
        CHECK (norm_count IS NULL OR norm_count >= 0),
    CONSTRAINT gdelt_timeline_points_volume_share_non_negative
        CHECK (volume_share IS NULL OR volume_share >= 0)
);

COMMENT ON TABLE gdelt_timeline_points IS
    'GDELT 时间线点位表，用于保存 DOC API timelinevolraw 等模式返回的主题热度时间序列。';
COMMENT ON COLUMN gdelt_timeline_points.id IS
    '内部自增主键。';
COMMENT ON COLUMN gdelt_timeline_points.point_uid IS
    '时间线点位去重键，由应用层根据 query、mode 和时间桶生成。';
COMMENT ON COLUMN gdelt_timeline_points.query_uid IS
    '产生该点位的查询去重键。';
COMMENT ON COLUMN gdelt_timeline_points.query_text IS
    '产生该点位的 GDELT query 参数原文。';
COMMENT ON COLUMN gdelt_timeline_points.mode IS
    'GDELT DOC API timeline 模式，例如 timelinevolraw。';
COMMENT ON COLUMN gdelt_timeline_points.bucket_start_at IS
    'GDELT 时间线桶开始时间。';
COMMENT ON COLUMN gdelt_timeline_points.article_count IS
    '该时间桶内匹配查询的文章数量，timelinevolraw 模式下为原始文章数。';
COMMENT ON COLUMN gdelt_timeline_points.norm_count IS
    '该时间桶内 GDELT 监测到的全量文章数量，用于归一化。';
COMMENT ON COLUMN gdelt_timeline_points.volume_share IS
    '该主题文章数占全量监测文章数的比例。';
COMMENT ON COLUMN gdelt_timeline_points.raw_payload IS
    '该时间线点位在 GDELT 响应中的原始 JSON 片段。';
COMMENT ON COLUMN gdelt_timeline_points.fetched_at IS
    '该时间线点位的抓取时间。';
COMMENT ON COLUMN gdelt_timeline_points.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN gdelt_timeline_points.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_timeline_points_point_uid
    ON gdelt_timeline_points (point_uid);

CREATE INDEX IF NOT EXISTS idx_gdelt_timeline_points_query_time
    ON gdelt_timeline_points (query_uid, bucket_start_at DESC);

CREATE INDEX IF NOT EXISTS idx_gdelt_timeline_points_bucket_start
    ON gdelt_timeline_points (bucket_start_at DESC);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_gdelt_doc_queries_updated_at ON gdelt_doc_queries;
CREATE TRIGGER trg_gdelt_doc_queries_updated_at
BEFORE UPDATE ON gdelt_doc_queries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_gdelt_articles_updated_at ON gdelt_articles;
CREATE TRIGGER trg_gdelt_articles_updated_at
BEFORE UPDATE ON gdelt_articles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_gdelt_timeline_points_updated_at ON gdelt_timeline_points;
CREATE TRIGGER trg_gdelt_timeline_points_updated_at
BEFORE UPDATE ON gdelt_timeline_points
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_gdelt_timeline_points_updated_at ON gdelt_timeline_points;
DROP TRIGGER IF EXISTS trg_gdelt_articles_updated_at ON gdelt_articles;
DROP TRIGGER IF EXISTS trg_gdelt_doc_queries_updated_at ON gdelt_doc_queries;

DROP INDEX IF EXISTS idx_gdelt_timeline_points_bucket_start;
DROP INDEX IF EXISTS idx_gdelt_timeline_points_query_time;
DROP INDEX IF EXISTS idx_gdelt_timeline_points_point_uid;
DROP INDEX IF EXISTS idx_gdelt_articles_query_uid;
DROP INDEX IF EXISTS idx_gdelt_articles_source_country;
DROP INDEX IF EXISTS idx_gdelt_articles_language;
DROP INDEX IF EXISTS idx_gdelt_articles_domain;
DROP INDEX IF EXISTS idx_gdelt_articles_seen_at;
DROP INDEX IF EXISTS idx_gdelt_articles_article_url;
DROP INDEX IF EXISTS idx_gdelt_doc_queries_mode;
DROP INDEX IF EXISTS idx_gdelt_doc_queries_fetched_at;
DROP INDEX IF EXISTS idx_gdelt_doc_queries_query_uid;

DROP TABLE IF EXISTS gdelt_timeline_points;
DROP TABLE IF EXISTS gdelt_articles;
DROP TABLE IF EXISTS gdelt_doc_queries;
