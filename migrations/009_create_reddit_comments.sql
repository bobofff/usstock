-- Migration: 009_create_reddit_comments
-- Purpose: create a table for Devvit realtime keyword-matched Reddit comments.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Comments
--   are linked to posts by post_fullname and query_uid through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS reddit_comments (
    id BIGSERIAL PRIMARY KEY,
    comment_uid TEXT NOT NULL,
    reddit_id TEXT NOT NULL,
    fullname TEXT NOT NULL,
    post_fullname TEXT NOT NULL,
    parent_fullname TEXT,
    subreddit TEXT NOT NULL,
    body TEXT NOT NULL,
    author_name TEXT,
    permalink_url TEXT NOT NULL,
    score INTEGER,
    candidate_tickers TEXT[] NOT NULL DEFAULT '{}'::text[],
    candidate_keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
    matched_keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
    created_utc TIMESTAMPTZ,
    source_type TEXT NOT NULL DEFAULT 'reddit_comment',
    query_uid TEXT NOT NULL,
    request_url TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT reddit_comments_comment_uid_not_blank
        CHECK (length(trim(comment_uid)) > 0),
    CONSTRAINT reddit_comments_reddit_id_not_blank
        CHECK (length(trim(reddit_id)) > 0),
    CONSTRAINT reddit_comments_fullname_not_blank
        CHECK (length(trim(fullname)) > 0),
    CONSTRAINT reddit_comments_post_fullname_not_blank
        CHECK (length(trim(post_fullname)) > 0),
    CONSTRAINT reddit_comments_subreddit_not_blank
        CHECK (length(trim(subreddit)) > 0),
    CONSTRAINT reddit_comments_body_not_blank
        CHECK (length(trim(body)) > 0),
    CONSTRAINT reddit_comments_permalink_url_not_blank
        CHECK (length(trim(permalink_url)) > 0),
    CONSTRAINT reddit_comments_source_type_not_blank
        CHECK (length(trim(source_type)) > 0),
    CONSTRAINT reddit_comments_query_uid_not_blank
        CHECK (length(trim(query_uid)) > 0),
    CONSTRAINT reddit_comments_request_url_not_blank
        CHECK (length(trim(request_url)) > 0)
);

COMMENT ON TABLE reddit_comments IS
    'Reddit 评论表，用于保存 Devvit 实时关键词命中的评论、候选 ticker、候选关键词和原始 JSON。';
COMMENT ON COLUMN reddit_comments.id IS
    '内部自增主键。';
COMMENT ON COLUMN reddit_comments.comment_uid IS
    '评论去重键，优先使用 Reddit comment fullname。';
COMMENT ON COLUMN reddit_comments.reddit_id IS
    'Reddit 评论短 ID。';
COMMENT ON COLUMN reddit_comments.fullname IS
    'Reddit 评论 fullname，例如 t1_abcdef，用于唯一识别评论。';
COMMENT ON COLUMN reddit_comments.post_fullname IS
    '评论所属帖子 fullname，例如 t3_abcdef；通过应用逻辑关联 reddit_posts。';
COMMENT ON COLUMN reddit_comments.parent_fullname IS
    '父级评论或帖子 fullname，顶层评论通常等于 post_fullname。';
COMMENT ON COLUMN reddit_comments.subreddit IS
    '评论所属 subreddit。';
COMMENT ON COLUMN reddit_comments.body IS
    '评论正文文本。';
COMMENT ON COLUMN reddit_comments.author_name IS
    '评论作者用户名，删除或隐藏时可能为空。';
COMMENT ON COLUMN reddit_comments.permalink_url IS
    'Reddit 评论永久链接。';
COMMENT ON COLUMN reddit_comments.score IS
    'Reddit 当前评论 score。';
COMMENT ON COLUMN reddit_comments.candidate_tickers IS
    '从评论正文中抽取的候选股票 ticker。';
COMMENT ON COLUMN reddit_comments.candidate_keywords IS
    '从评论正文中抽取的候选关键词。';
COMMENT ON COLUMN reddit_comments.matched_keywords IS
    'Devvit 实时触发器命中的关键词。';
COMMENT ON COLUMN reddit_comments.created_utc IS
    'Reddit 返回的评论创建时间。';
COMMENT ON COLUMN reddit_comments.source_type IS
    '统一数据源类型，实时评论默认为 reddit_comment。';
COMMENT ON COLUMN reddit_comments.query_uid IS
    '产生该评论的触发事件去重键。';
COMMENT ON COLUMN reddit_comments.request_url IS
    '产生该评论的 Devvit 触发事件标识。';
COMMENT ON COLUMN reddit_comments.raw_payload IS
    '该评论在 Devvit 触发事件中的原始 JSON 片段。';
COMMENT ON COLUMN reddit_comments.first_seen_at IS
    '系统第一次写入该评论的时间。';
COMMENT ON COLUMN reddit_comments.last_seen_at IS
    '系统最近一次通过 Devvit 触发器看到该评论的时间。';
COMMENT ON COLUMN reddit_comments.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN reddit_comments.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_comments_comment_uid
    ON reddit_comments (comment_uid);

CREATE INDEX IF NOT EXISTS idx_reddit_comments_fullname
    ON reddit_comments (fullname);

CREATE INDEX IF NOT EXISTS idx_reddit_comments_post_fullname
    ON reddit_comments (post_fullname);

CREATE INDEX IF NOT EXISTS idx_reddit_comments_subreddit_created
    ON reddit_comments (subreddit, created_utc DESC);

CREATE INDEX IF NOT EXISTS idx_reddit_comments_candidate_tickers
    ON reddit_comments USING GIN (candidate_tickers);

CREATE INDEX IF NOT EXISTS idx_reddit_comments_matched_keywords
    ON reddit_comments USING GIN (matched_keywords);

CREATE TRIGGER trg_reddit_comments_updated_at
BEFORE UPDATE ON reddit_comments
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_reddit_comments_updated_at ON reddit_comments;

DROP INDEX IF EXISTS idx_reddit_comments_matched_keywords;
DROP INDEX IF EXISTS idx_reddit_comments_candidate_tickers;
DROP INDEX IF EXISTS idx_reddit_comments_subreddit_created;
DROP INDEX IF EXISTS idx_reddit_comments_post_fullname;
DROP INDEX IF EXISTS idx_reddit_comments_fullname;
DROP INDEX IF EXISTS idx_reddit_comments_comment_uid;

DROP TABLE IF EXISTS reddit_comments;
