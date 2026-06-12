-- Migration: 008_create_reddit_tables
-- Purpose: create Reddit API ingestion tables and add low-weight Reddit
-- community-signal columns to daily discovery scores.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Tables
--   are linked by query_uid, post_uid, ticker, and topic_slug through
--   application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS reddit_post_queries (
    id BIGSERIAL PRIMARY KEY,
    query_uid TEXT NOT NULL,
    subreddit TEXT NOT NULL,
    listing TEXT NOT NULL,
    time_filter TEXT,
    limit_count INTEGER NOT NULL,
    after_token TEXT,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT reddit_post_queries_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT reddit_post_queries_subreddit_not_blank
        CHECK (length(trim(subreddit)) > 0),
    CONSTRAINT reddit_post_queries_listing_not_blank
        CHECK (length(trim(listing)) > 0),
    CONSTRAINT reddit_post_queries_limit_positive
        CHECK (limit_count > 0),
    CONSTRAINT reddit_post_queries_request_url_not_blank
        CHECK (length(trim(request_url)) > 0)
);

COMMENT ON TABLE reddit_post_queries IS
    'Reddit 帖子查询记录表，用于保存每次 subreddit listing 请求的参数、脱敏请求地址和原始响应。';
COMMENT ON COLUMN reddit_post_queries.id IS
    '内部自增主键。';
COMMENT ON COLUMN reddit_post_queries.query_uid IS
    '查询去重键，由 subreddit、listing、时间过滤器、分页游标和 limit 等参数生成。';
COMMENT ON COLUMN reddit_post_queries.subreddit IS
    'Reddit 社区名称，例如 stocks、investing、wallstreetbets。';
COMMENT ON COLUMN reddit_post_queries.listing IS
    'Reddit listing 类型，例如 new、hot、top、rising、controversial。';
COMMENT ON COLUMN reddit_post_queries.time_filter IS
    'top 或 controversial listing 使用的时间过滤器，例如 day、week、month、year、all。';
COMMENT ON COLUMN reddit_post_queries.limit_count IS
    '本次查询请求的帖子数量上限。';
COMMENT ON COLUMN reddit_post_queries.after_token IS
    'Reddit listing 分页游标 after，用于增量翻页。';
COMMENT ON COLUMN reddit_post_queries.request_url IS
    '脱敏后的完整请求地址，不包含 OAuth token，用于审计和复现查询。';
COMMENT ON COLUMN reddit_post_queries.raw_payload IS
    'Reddit API 返回的完整原始 JSON。';
COMMENT ON COLUMN reddit_post_queries.fetched_at IS
    '该查询响应的抓取时间。';
COMMENT ON COLUMN reddit_post_queries.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN reddit_post_queries.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_post_queries_query_uid
    ON reddit_post_queries (query_uid);

CREATE INDEX IF NOT EXISTS idx_reddit_post_queries_subreddit_fetched
    ON reddit_post_queries (subreddit, fetched_at DESC);

CREATE TABLE IF NOT EXISTS reddit_posts (
    id BIGSERIAL PRIMARY KEY,
    post_uid TEXT NOT NULL,
    reddit_id TEXT NOT NULL,
    fullname TEXT NOT NULL,
    subreddit TEXT NOT NULL,
    title TEXT NOT NULL,
    selftext TEXT,
    author_name TEXT,
    permalink_url TEXT NOT NULL,
    external_url TEXT,
    score INTEGER,
    upvote_ratio NUMERIC(8, 4),
    comment_count INTEGER NOT NULL DEFAULT 0,
    over_18 BOOLEAN NOT NULL DEFAULT FALSE,
    spoiler BOOLEAN NOT NULL DEFAULT FALSE,
    stickied BOOLEAN NOT NULL DEFAULT FALSE,
    is_video BOOLEAN NOT NULL DEFAULT FALSE,
    link_flair_text TEXT,
    candidate_tickers TEXT[] NOT NULL DEFAULT '{}'::text[],
    candidate_keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
    created_utc TIMESTAMPTZ,
    source_type TEXT NOT NULL DEFAULT 'reddit_post',
    query_uid TEXT NOT NULL,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT reddit_posts_post_uid_not_blank
        CHECK (length(trim(post_uid)) > 0),
    CONSTRAINT reddit_posts_reddit_id_not_blank
        CHECK (length(trim(reddit_id)) > 0),
    CONSTRAINT reddit_posts_fullname_not_blank
        CHECK (length(trim(fullname)) > 0),
    CONSTRAINT reddit_posts_subreddit_not_blank
        CHECK (length(trim(subreddit)) > 0),
    CONSTRAINT reddit_posts_title_not_blank
        CHECK (length(trim(title)) > 0),
    CONSTRAINT reddit_posts_permalink_url_not_blank
        CHECK (length(trim(permalink_url)) > 0),
    CONSTRAINT reddit_posts_comment_count_non_negative
        CHECK (comment_count >= 0),
    CONSTRAINT reddit_posts_source_type_not_blank
        CHECK (length(trim(source_type)) > 0),
    CONSTRAINT reddit_posts_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT reddit_posts_request_url_not_blank
        CHECK (length(trim(request_url)) > 0)
);

COMMENT ON TABLE reddit_posts IS
    'Reddit 社区帖子表，用于保存投资相关 subreddit 的标准化帖子字段、候选 ticker、关键词和原始 JSON。';
COMMENT ON COLUMN reddit_posts.id IS
    '内部自增主键。';
COMMENT ON COLUMN reddit_posts.post_uid IS
    '帖子去重键，优先使用 Reddit fullname。';
COMMENT ON COLUMN reddit_posts.reddit_id IS
    'Reddit 帖子短 ID。';
COMMENT ON COLUMN reddit_posts.fullname IS
    'Reddit fullname，例如 t3_abcdef，用于分页和唯一识别。';
COMMENT ON COLUMN reddit_posts.subreddit IS
    '帖子所属 subreddit。';
COMMENT ON COLUMN reddit_posts.title IS
    '帖子标题。';
COMMENT ON COLUMN reddit_posts.selftext IS
    '帖子正文文本，链接帖可能为空。';
COMMENT ON COLUMN reddit_posts.author_name IS
    '帖子作者用户名，删除或隐藏时可能为空。';
COMMENT ON COLUMN reddit_posts.permalink_url IS
    'Reddit 帖子永久链接。';
COMMENT ON COLUMN reddit_posts.external_url IS
    '帖子指向的外部链接，纯文本帖通常为空。';
COMMENT ON COLUMN reddit_posts.score IS
    'Reddit 当前 score。';
COMMENT ON COLUMN reddit_posts.upvote_ratio IS
    'Reddit 当前 upvote ratio。';
COMMENT ON COLUMN reddit_posts.comment_count IS
    'Reddit 当前评论数。';
COMMENT ON COLUMN reddit_posts.over_18 IS
    '帖子是否标记为 NSFW。';
COMMENT ON COLUMN reddit_posts.spoiler IS
    '帖子是否标记为 spoiler。';
COMMENT ON COLUMN reddit_posts.stickied IS
    '帖子是否为置顶帖。';
COMMENT ON COLUMN reddit_posts.is_video IS
    '帖子是否为视频帖。';
COMMENT ON COLUMN reddit_posts.link_flair_text IS
    '帖子 flair 文本。';
COMMENT ON COLUMN reddit_posts.candidate_tickers IS
    '从标题和正文中抽取的候选股票 ticker。';
COMMENT ON COLUMN reddit_posts.candidate_keywords IS
    '从标题和正文中抽取的候选关键词。';
COMMENT ON COLUMN reddit_posts.created_utc IS
    'Reddit 返回的帖子创建时间。';
COMMENT ON COLUMN reddit_posts.source_type IS
    '统一数据源类型，Reddit 帖子默认为 reddit_post。';
COMMENT ON COLUMN reddit_posts.query_uid IS
    '产生该帖子的查询去重键。';
COMMENT ON COLUMN reddit_posts.request_url IS
    '产生该帖子的脱敏 Reddit 请求地址。';
COMMENT ON COLUMN reddit_posts.raw_payload IS
    '该帖子在 Reddit API 响应中的原始 JSON 片段。';
COMMENT ON COLUMN reddit_posts.first_seen_at IS
    '系统第一次写入该帖子的时间。';
COMMENT ON COLUMN reddit_posts.last_seen_at IS
    '系统最近一次通过 Reddit 查询看到该帖子的时间。';
COMMENT ON COLUMN reddit_posts.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN reddit_posts.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_posts_post_uid
    ON reddit_posts (post_uid);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_fullname
    ON reddit_posts (fullname);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_subreddit_created
    ON reddit_posts (subreddit, created_utc DESC);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_candidate_tickers
    ON reddit_posts USING GIN (candidate_tickers);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_query_uid
    ON reddit_posts (query_uid);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_source_type
    ON reddit_posts (source_type);

ALTER TABLE daily_candidate_scores
    ADD COLUMN IF NOT EXISTS reddit_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reddit_post_count INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN daily_candidate_scores.reddit_score IS
    'Reddit 社区讨论热度贡献分；仅作为低权重辅助信号。';
COMMENT ON COLUMN daily_candidate_scores.reddit_post_count IS
    '最近窗口内关联该 ticker 的 Reddit 帖子数量。';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'daily_candidate_scores_reddit_score_non_negative'
    ) THEN
        ALTER TABLE daily_candidate_scores
            ADD CONSTRAINT daily_candidate_scores_reddit_score_non_negative
            CHECK (reddit_score >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'daily_candidate_scores_reddit_post_count_non_negative'
    ) THEN
        ALTER TABLE daily_candidate_scores
            ADD CONSTRAINT daily_candidate_scores_reddit_post_count_non_negative
            CHECK (reddit_post_count >= 0);
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reddit_post_queries_updated_at ON reddit_post_queries;
CREATE TRIGGER trg_reddit_post_queries_updated_at
BEFORE UPDATE ON reddit_post_queries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_reddit_posts_updated_at ON reddit_posts;
CREATE TRIGGER trg_reddit_posts_updated_at
BEFORE UPDATE ON reddit_posts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_reddit_posts_updated_at ON reddit_posts;
DROP TRIGGER IF EXISTS trg_reddit_post_queries_updated_at ON reddit_post_queries;

ALTER TABLE daily_candidate_scores
    DROP CONSTRAINT IF EXISTS daily_candidate_scores_reddit_post_count_non_negative,
    DROP CONSTRAINT IF EXISTS daily_candidate_scores_reddit_score_non_negative,
    DROP COLUMN IF EXISTS reddit_post_count,
    DROP COLUMN IF EXISTS reddit_score;

DROP INDEX IF EXISTS idx_reddit_posts_source_type;
DROP INDEX IF EXISTS idx_reddit_posts_query_uid;
DROP INDEX IF EXISTS idx_reddit_posts_candidate_tickers;
DROP INDEX IF EXISTS idx_reddit_posts_subreddit_created;
DROP INDEX IF EXISTS idx_reddit_posts_fullname;
DROP INDEX IF EXISTS idx_reddit_posts_post_uid;
DROP INDEX IF EXISTS idx_reddit_post_queries_subreddit_fetched;
DROP INDEX IF EXISTS idx_reddit_post_queries_query_uid;

DROP TABLE IF EXISTS reddit_posts;
DROP TABLE IF EXISTS reddit_post_queries;
