-- Migration: 006_create_discovery_tables
-- Purpose: create automatic market-discovery tables for seed topics,
-- detected source mentions, daily candidate scores, and generated watchlists.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Tables
--   are linked by topic_slug, ticker, source_uid, and run_date through
--   application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS market_topics (
    id BIGSERIAL PRIMARY KEY,
    topic_slug TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    gdelt_query TEXT NOT NULL,
    keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
    sectors TEXT[] NOT NULL DEFAULT '{}'::text[],
    ticker_hints TEXT[] NOT NULL DEFAULT '{}'::text[],
    priority INTEGER NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    data_source TEXT NOT NULL DEFAULT 'seed',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refreshed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT market_topics_topic_slug_not_blank
        CHECK (length(trim(topic_slug)) > 0),
    CONSTRAINT market_topics_topic_name_not_blank
        CHECK (length(trim(topic_name)) > 0),
    CONSTRAINT market_topics_gdelt_query_not_blank
        CHECK (length(trim(gdelt_query)) > 0),
    CONSTRAINT market_topics_priority_positive
        CHECK (priority > 0)
);

COMMENT ON TABLE market_topics IS
    '市场热点主题库，用于保存系统自动同步 GDELT 的主题 query、关键词、相关行业和种子 ticker。';
COMMENT ON COLUMN market_topics.id IS
    '内部自增主键。';
COMMENT ON COLUMN market_topics.topic_slug IS
    '主题唯一标识，例如 ai_infrastructure、semiconductors、energy。';
COMMENT ON COLUMN market_topics.topic_name IS
    '主题中文或英文展示名称。';
COMMENT ON COLUMN market_topics.gdelt_query IS
    '用于 GDELT DOC API 的 query 参数，替代人工输入关键词。';
COMMENT ON COLUMN market_topics.keywords IS
    '主题关键词数组，用于从新闻标题、摘要和股票池业务描述中匹配热点。';
COMMENT ON COLUMN market_topics.sectors IS
    '主题常见相关板块或行业数组，用于辅助映射股票池。';
COMMENT ON COLUMN market_topics.ticker_hints IS
    '该主题的种子美股 ticker 数组，用作初始主题到标的映射。';
COMMENT ON COLUMN market_topics.priority IS
    '同步优先级，数值越小越优先。';
COMMENT ON COLUMN market_topics.is_active IS
    '是否启用该主题，停用后自动发现流程不再同步。';
COMMENT ON COLUMN market_topics.data_source IS
    '主题来源，例如 seed、news_extraction、manual。';
COMMENT ON COLUMN market_topics.metadata IS
    '主题扩展元数据，用于保存来源、备注、权重等信息。';
COMMENT ON COLUMN market_topics.first_seen_at IS
    '系统第一次写入该主题的时间。';
COMMENT ON COLUMN market_topics.last_refreshed_at IS
    '该主题最近一次被自动发现流程刷新时间。';
COMMENT ON COLUMN market_topics.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN market_topics.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_topics_topic_slug
    ON market_topics (topic_slug);

CREATE INDEX IF NOT EXISTS idx_market_topics_active_priority
    ON market_topics (is_active, priority, topic_slug);

CREATE INDEX IF NOT EXISTS idx_market_topics_keywords
    ON market_topics USING GIN (keywords);

CREATE TABLE IF NOT EXISTS topic_mentions (
    id BIGSERIAL PRIMARY KEY,
    mention_uid TEXT NOT NULL,
    topic_slug TEXT NOT NULL,
    ticker TEXT,
    source_type TEXT NOT NULL,
    source_uid TEXT NOT NULL,
    source_title TEXT,
    source_url TEXT,
    published_at TIMESTAMPTZ,
    relevance_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT topic_mentions_mention_uid_not_blank
        CHECK (length(trim(mention_uid)) > 0),
    CONSTRAINT topic_mentions_topic_slug_not_blank
        CHECK (length(trim(topic_slug)) > 0),
    CONSTRAINT topic_mentions_source_type_not_blank
        CHECK (length(trim(source_type)) > 0),
    CONSTRAINT topic_mentions_source_uid_not_blank
        CHECK (length(trim(source_uid)) > 0),
    CONSTRAINT topic_mentions_relevance_non_negative
        CHECK (relevance_score >= 0)
);

COMMENT ON TABLE topic_mentions IS
    '热点主题提及表，用于保存 Finnhub、GDELT、SEC 等来源中识别到的主题和 ticker 关系。';
COMMENT ON COLUMN topic_mentions.id IS
    '内部自增主键。';
COMMENT ON COLUMN topic_mentions.mention_uid IS
    '主题提及去重键，由来源类型、来源 UID、主题和 ticker 等信息生成。';
COMMENT ON COLUMN topic_mentions.topic_slug IS
    '主题唯一标识，对应 market_topics.topic_slug，由应用层维护关联。';
COMMENT ON COLUMN topic_mentions.ticker IS
    '识别到的相关股票代码；宏观主题未映射到具体标的时可为空。';
COMMENT ON COLUMN topic_mentions.source_type IS
    '数据来源类型，例如 finnhub_article、gdelt_article、sec_filing、stock_universe。';
COMMENT ON COLUMN topic_mentions.source_uid IS
    '来源内部唯一标识，例如 Finnhub article_uid、GDELT article_url、SEC accession_number。';
COMMENT ON COLUMN topic_mentions.source_title IS
    '来源标题或摘要标题。';
COMMENT ON COLUMN topic_mentions.source_url IS
    '来源 URL。';
COMMENT ON COLUMN topic_mentions.published_at IS
    '来源发布时间或公告日期。';
COMMENT ON COLUMN topic_mentions.relevance_score IS
    '主题匹配相关性分数，越高表示关系越强。';
COMMENT ON COLUMN topic_mentions.evidence IS
    '命中关键词、来源字段、原始片段等证据 JSON。';
COMMENT ON COLUMN topic_mentions.detected_at IS
    '系统检测到该主题提及的时间。';
COMMENT ON COLUMN topic_mentions.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN topic_mentions.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_topic_mentions_mention_uid
    ON topic_mentions (mention_uid);

CREATE INDEX IF NOT EXISTS idx_topic_mentions_topic_detected
    ON topic_mentions (topic_slug, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_topic_mentions_ticker_detected
    ON topic_mentions (ticker, detected_at DESC)
    WHERE ticker IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_topic_mentions_source
    ON topic_mentions (source_type, source_uid);

CREATE TABLE IF NOT EXISTS daily_candidate_scores (
    id BIGSERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    rank INTEGER,
    topic_slugs TEXT[] NOT NULL DEFAULT '{}'::text[],
    primary_topic_slug TEXT,
    news_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    gdelt_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    sec_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    fundamental_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    liquidity_score NUMERIC(10, 4) NOT NULL DEFAULT 0,
    finnhub_article_count INTEGER NOT NULL DEFAULT 0,
    gdelt_article_count INTEGER NOT NULL DEFAULT 0,
    sec_filing_count INTEGER NOT NULL DEFAULT 0,
    latest_news_at TIMESTAMPTZ,
    latest_filing_date DATE,
    action_bias TEXT NOT NULL DEFAULT 'watch',
    rationale JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT daily_candidate_scores_ticker_not_blank
        CHECK (length(trim(ticker)) > 0),
    CONSTRAINT daily_candidate_scores_score_non_negative
        CHECK (score >= 0),
    CONSTRAINT daily_candidate_scores_rank_positive
        CHECK (rank IS NULL OR rank > 0),
    CONSTRAINT daily_candidate_scores_news_score_non_negative
        CHECK (news_score >= 0),
    CONSTRAINT daily_candidate_scores_gdelt_score_non_negative
        CHECK (gdelt_score >= 0),
    CONSTRAINT daily_candidate_scores_sec_score_non_negative
        CHECK (sec_score >= 0),
    CONSTRAINT daily_candidate_scores_fundamental_score_non_negative
        CHECK (fundamental_score >= 0),
    CONSTRAINT daily_candidate_scores_liquidity_score_non_negative
        CHECK (liquidity_score >= 0),
    CONSTRAINT daily_candidate_scores_finnhub_article_count_non_negative
        CHECK (finnhub_article_count >= 0),
    CONSTRAINT daily_candidate_scores_gdelt_article_count_non_negative
        CHECK (gdelt_article_count >= 0),
    CONSTRAINT daily_candidate_scores_sec_filing_count_non_negative
        CHECK (sec_filing_count >= 0),
    CONSTRAINT daily_candidate_scores_action_bias_not_blank
        CHECK (length(trim(action_bias)) > 0)
);

COMMENT ON TABLE daily_candidate_scores IS
    '每日候选股评分表，用于保存自动热点发现流程生成的股票评分、排名、主题和证据。';
COMMENT ON COLUMN daily_candidate_scores.id IS
    '内部自增主键。';
COMMENT ON COLUMN daily_candidate_scores.run_date IS
    '候选股所属运行日期。';
COMMENT ON COLUMN daily_candidate_scores.ticker IS
    '候选股票代码。';
COMMENT ON COLUMN daily_candidate_scores.company_name IS
    '候选股票公司名称，来自股票池或 SEC/Finnhub 关联信息。';
COMMENT ON COLUMN daily_candidate_scores.score IS
    '综合评分，越高表示越值得进入观察清单。';
COMMENT ON COLUMN daily_candidate_scores.rank IS
    '当日候选股排名。';
COMMENT ON COLUMN daily_candidate_scores.topic_slugs IS
    '触发该候选股的主题标识数组。';
COMMENT ON COLUMN daily_candidate_scores.primary_topic_slug IS
    '该候选股最主要的触发主题。';
COMMENT ON COLUMN daily_candidate_scores.news_score IS
    'Finnhub 新闻热度和相关 ticker 贡献分。';
COMMENT ON COLUMN daily_candidate_scores.gdelt_score IS
    'GDELT 全球新闻热度贡献分。';
COMMENT ON COLUMN daily_candidate_scores.sec_score IS
    'SEC 公告事件贡献分。';
COMMENT ON COLUMN daily_candidate_scores.fundamental_score IS
    '结构化财务数据和基本面可用性贡献分。';
COMMENT ON COLUMN daily_candidate_scores.liquidity_score IS
    '市值、成交量等流动性和可交易性贡献分。';
COMMENT ON COLUMN daily_candidate_scores.finnhub_article_count IS
    '最近窗口内关联该 ticker 的 Finnhub 新闻数量。';
COMMENT ON COLUMN daily_candidate_scores.gdelt_article_count IS
    '最近窗口内关联主题的 GDELT 文章数量。';
COMMENT ON COLUMN daily_candidate_scores.sec_filing_count IS
    '最近窗口内该 ticker 的 SEC 核心公告数量。';
COMMENT ON COLUMN daily_candidate_scores.latest_news_at IS
    '该候选股最近一条相关新闻发布时间。';
COMMENT ON COLUMN daily_candidate_scores.latest_filing_date IS
    '该候选股最近一条 SEC filing 日期。';
COMMENT ON COLUMN daily_candidate_scores.action_bias IS
    '系统建议动作，例如 watch、review、skip。';
COMMENT ON COLUMN daily_candidate_scores.rationale IS
    '评分原因、命中关键词、来源计数和证据摘要 JSON。';
COMMENT ON COLUMN daily_candidate_scores.generated_at IS
    '该评分生成时间。';
COMMENT ON COLUMN daily_candidate_scores.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN daily_candidate_scores.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_candidate_scores_run_ticker
    ON daily_candidate_scores (run_date, ticker);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_scores_run_rank
    ON daily_candidate_scores (run_date, rank);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_scores_run_score
    ON daily_candidate_scores (run_date, score DESC);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_scores_ticker
    ON daily_candidate_scores (ticker);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_scores_topics
    ON daily_candidate_scores USING GIN (topic_slugs);

CREATE TABLE IF NOT EXISTS daily_watchlists (
    id BIGSERIAL PRIMARY KEY,
    watchlist_uid TEXT NOT NULL,
    run_date DATE NOT NULL,
    profile TEXT NOT NULL DEFAULT 'default',
    candidate_count INTEGER NOT NULL DEFAULT 0,
    summary TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT daily_watchlists_watchlist_uid_not_blank
        CHECK (length(trim(watchlist_uid)) > 0),
    CONSTRAINT daily_watchlists_profile_not_blank
        CHECK (length(trim(profile)) > 0),
    CONSTRAINT daily_watchlists_candidate_count_non_negative
        CHECK (candidate_count >= 0)
);

COMMENT ON TABLE daily_watchlists IS
    '每日观察清单表，用于保存自动发现流程输出的候选股列表和摘要。';
COMMENT ON COLUMN daily_watchlists.id IS
    '内部自增主键。';
COMMENT ON COLUMN daily_watchlists.watchlist_uid IS
    '观察清单去重键，通常由运行日期和配置 profile 组成。';
COMMENT ON COLUMN daily_watchlists.run_date IS
    '观察清单所属日期。';
COMMENT ON COLUMN daily_watchlists.profile IS
    '生成配置名称，例如 default、aggressive、conservative。';
COMMENT ON COLUMN daily_watchlists.candidate_count IS
    '该观察清单包含的候选股数量。';
COMMENT ON COLUMN daily_watchlists.summary IS
    '观察清单摘要文本。';
COMMENT ON COLUMN daily_watchlists.raw_payload IS
    '候选股、评分明细和运行统计的完整 JSON。';
COMMENT ON COLUMN daily_watchlists.generated_at IS
    '观察清单生成时间。';
COMMENT ON COLUMN daily_watchlists.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN daily_watchlists.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_watchlists_watchlist_uid
    ON daily_watchlists (watchlist_uid);

CREATE INDEX IF NOT EXISTS idx_daily_watchlists_run_date
    ON daily_watchlists (run_date DESC);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_market_topics_updated_at ON market_topics;
CREATE TRIGGER trg_market_topics_updated_at
BEFORE UPDATE ON market_topics
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_topic_mentions_updated_at ON topic_mentions;
CREATE TRIGGER trg_topic_mentions_updated_at
BEFORE UPDATE ON topic_mentions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_daily_candidate_scores_updated_at ON daily_candidate_scores;
CREATE TRIGGER trg_daily_candidate_scores_updated_at
BEFORE UPDATE ON daily_candidate_scores
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_daily_watchlists_updated_at ON daily_watchlists;
CREATE TRIGGER trg_daily_watchlists_updated_at
BEFORE UPDATE ON daily_watchlists
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_daily_watchlists_updated_at ON daily_watchlists;
DROP TRIGGER IF EXISTS trg_daily_candidate_scores_updated_at ON daily_candidate_scores;
DROP TRIGGER IF EXISTS trg_topic_mentions_updated_at ON topic_mentions;
DROP TRIGGER IF EXISTS trg_market_topics_updated_at ON market_topics;

DROP INDEX IF EXISTS idx_daily_watchlists_run_date;
DROP INDEX IF EXISTS idx_daily_watchlists_watchlist_uid;
DROP INDEX IF EXISTS idx_daily_candidate_scores_topics;
DROP INDEX IF EXISTS idx_daily_candidate_scores_ticker;
DROP INDEX IF EXISTS idx_daily_candidate_scores_run_score;
DROP INDEX IF EXISTS idx_daily_candidate_scores_run_rank;
DROP INDEX IF EXISTS idx_daily_candidate_scores_run_ticker;
DROP INDEX IF EXISTS idx_topic_mentions_source;
DROP INDEX IF EXISTS idx_topic_mentions_ticker_detected;
DROP INDEX IF EXISTS idx_topic_mentions_topic_detected;
DROP INDEX IF EXISTS idx_topic_mentions_mention_uid;
DROP INDEX IF EXISTS idx_market_topics_keywords;
DROP INDEX IF EXISTS idx_market_topics_active_priority;
DROP INDEX IF EXISTS idx_market_topics_topic_slug;

DROP TABLE IF EXISTS daily_watchlists;
DROP TABLE IF EXISTS daily_candidate_scores;
DROP TABLE IF EXISTS topic_mentions;
DROP TABLE IF EXISTS market_topics;
