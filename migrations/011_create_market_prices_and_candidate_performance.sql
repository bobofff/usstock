-- Migration: 011_create_market_prices_and_candidate_performance
-- Purpose: store local daily market prices and report-candidate forward
-- performance snapshots for backtesting daily analysis reports.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   This migration intentionally does not add foreign key constraints. Prices
--   and performance rows are linked to tickers, reports, and candidate scores
--   through application logic.

-- migrate:up

CREATE TABLE IF NOT EXISTS market_daily_prices (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    price_date DATE NOT NULL,
    open_price NUMERIC(18, 6),
    high_price NUMERIC(18, 6),
    low_price NUMERIC(18, 6),
    close_price NUMERIC(18, 6) NOT NULL,
    adjusted_close_price NUMERIC(18, 6),
    volume NUMERIC(20, 2),
    currency TEXT NOT NULL DEFAULT 'USD',
    data_source TEXT NOT NULL DEFAULT 'manual',
    source_uid TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT market_daily_prices_ticker_not_blank
        CHECK (length(trim(ticker)) > 0),
    CONSTRAINT market_daily_prices_close_price_non_negative
        CHECK (close_price >= 0),
    CONSTRAINT market_daily_prices_open_price_non_negative
        CHECK (open_price IS NULL OR open_price >= 0),
    CONSTRAINT market_daily_prices_high_price_non_negative
        CHECK (high_price IS NULL OR high_price >= 0),
    CONSTRAINT market_daily_prices_low_price_non_negative
        CHECK (low_price IS NULL OR low_price >= 0),
    CONSTRAINT market_daily_prices_adjusted_close_price_non_negative
        CHECK (adjusted_close_price IS NULL OR adjusted_close_price >= 0),
    CONSTRAINT market_daily_prices_volume_non_negative
        CHECK (volume IS NULL OR volume >= 0),
    CONSTRAINT market_daily_prices_currency_not_blank
        CHECK (length(trim(currency)) > 0),
    CONSTRAINT market_daily_prices_data_source_not_blank
        CHECK (length(trim(data_source)) > 0)
);

COMMENT ON TABLE market_daily_prices IS
    '市场日线价格表，用于保存本地导入或行情接口同步的股票每日 OHLCV 数据，供日报复盘和回测使用。';
COMMENT ON COLUMN market_daily_prices.id IS
    '内部自增主键。';
COMMENT ON COLUMN market_daily_prices.ticker IS
    '股票代码/交易代码，例如 AAPL、NVDA、BRK.B。';
COMMENT ON COLUMN market_daily_prices.price_date IS
    '价格所属交易日期。';
COMMENT ON COLUMN market_daily_prices.open_price IS
    '当日开盘价。';
COMMENT ON COLUMN market_daily_prices.high_price IS
    '当日最高价。';
COMMENT ON COLUMN market_daily_prices.low_price IS
    '当日最低价。';
COMMENT ON COLUMN market_daily_prices.close_price IS
    '当日收盘价。';
COMMENT ON COLUMN market_daily_prices.adjusted_close_price IS
    '复权收盘价；存在时回测优先使用该价格。';
COMMENT ON COLUMN market_daily_prices.volume IS
    '当日成交量。';
COMMENT ON COLUMN market_daily_prices.currency IS
    '价格货币，默认 USD。';
COMMENT ON COLUMN market_daily_prices.data_source IS
    '行情数据来源，例如 manual_csv、yfinance、polygon、broker。';
COMMENT ON COLUMN market_daily_prices.source_uid IS
    '来源侧去重键或批次标识。';
COMMENT ON COLUMN market_daily_prices.metadata IS
    '扩展 JSON 字段，用于保存导入批次、原始列名、供应商字段等信息。';
COMMENT ON COLUMN market_daily_prices.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN market_daily_prices.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_daily_prices_ticker_date_source
    ON market_daily_prices (ticker, price_date, data_source);

CREATE INDEX IF NOT EXISTS idx_market_daily_prices_ticker_date
    ON market_daily_prices (ticker, price_date);

CREATE INDEX IF NOT EXISTS idx_market_daily_prices_date
    ON market_daily_prices (price_date);

CREATE TABLE IF NOT EXISTS daily_candidate_performance (
    id BIGSERIAL PRIMARY KEY,
    performance_uid TEXT NOT NULL,
    run_date DATE NOT NULL,
    profile TEXT NOT NULL DEFAULT 'default',
    report_uid TEXT,
    ticker TEXT NOT NULL,
    company_name TEXT,
    rank INTEGER,
    score NUMERIC(10, 4),
    attention_label TEXT,
    event_type TEXT,
    risk_level TEXT,
    primary_topic_slug TEXT,
    topic_slugs TEXT[] NOT NULL DEFAULT '{}'::text[],
    action_bias TEXT,
    entry_date DATE,
    entry_close NUMERIC(18, 6),
    price_source TEXT,
    horizon_1d_date DATE,
    horizon_1d_close NUMERIC(18, 6),
    return_1d_pct NUMERIC(12, 6),
    max_drawdown_1d_pct NUMERIC(12, 6),
    max_runup_1d_pct NUMERIC(12, 6),
    horizon_5d_date DATE,
    horizon_5d_close NUMERIC(18, 6),
    return_5d_pct NUMERIC(12, 6),
    max_drawdown_5d_pct NUMERIC(12, 6),
    max_runup_5d_pct NUMERIC(12, 6),
    horizon_20d_date DATE,
    horizon_20d_close NUMERIC(18, 6),
    return_20d_pct NUMERIC(12, 6),
    max_drawdown_20d_pct NUMERIC(12, 6),
    max_runup_20d_pct NUMERIC(12, 6),
    performance_status TEXT NOT NULL DEFAULT 'pending',
    missing_reason TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT daily_candidate_performance_uid_not_blank
        CHECK (length(trim(performance_uid)) > 0),
    CONSTRAINT daily_candidate_performance_profile_not_blank
        CHECK (length(trim(profile)) > 0),
    CONSTRAINT daily_candidate_performance_ticker_not_blank
        CHECK (length(trim(ticker)) > 0),
    CONSTRAINT daily_candidate_performance_rank_positive
        CHECK (rank IS NULL OR rank > 0),
    CONSTRAINT daily_candidate_performance_score_non_negative
        CHECK (score IS NULL OR score >= 0),
    CONSTRAINT daily_candidate_performance_entry_close_non_negative
        CHECK (entry_close IS NULL OR entry_close >= 0),
    CONSTRAINT daily_candidate_performance_1d_close_non_negative
        CHECK (horizon_1d_close IS NULL OR horizon_1d_close >= 0),
    CONSTRAINT daily_candidate_performance_5d_close_non_negative
        CHECK (horizon_5d_close IS NULL OR horizon_5d_close >= 0),
    CONSTRAINT daily_candidate_performance_20d_close_non_negative
        CHECK (horizon_20d_close IS NULL OR horizon_20d_close >= 0),
    CONSTRAINT daily_candidate_performance_status_known
        CHECK (performance_status IN ('pending', 'complete', 'partial', 'no_entry_price', 'no_horizon_price'))
);

COMMENT ON TABLE daily_candidate_performance IS
    '每日候选股表现复盘表，用于保存日报候选股在后续交易日的收益率、最大回撤、最大上涨和可计算状态。';
COMMENT ON COLUMN daily_candidate_performance.id IS
    '内部自增主键。';
COMMENT ON COLUMN daily_candidate_performance.performance_uid IS
    '表现记录去重键，通常由 profile、run_date、report_uid 和 ticker 组成。';
COMMENT ON COLUMN daily_candidate_performance.run_date IS
    '候选股所属日报日期。';
COMMENT ON COLUMN daily_candidate_performance.profile IS
    '日报生成配置名称，例如 default、aggressive、conservative。';
COMMENT ON COLUMN daily_candidate_performance.report_uid IS
    '来源日报去重键，由应用层维护关联。';
COMMENT ON COLUMN daily_candidate_performance.ticker IS
    '候选股票代码。';
COMMENT ON COLUMN daily_candidate_performance.company_name IS
    '候选股票公司名称。';
COMMENT ON COLUMN daily_candidate_performance.rank IS
    '该候选股在日报中的排名。';
COMMENT ON COLUMN daily_candidate_performance.score IS
    '该候选股在日报中的综合评分。';
COMMENT ON COLUMN daily_candidate_performance.attention_label IS
    '日报中的关注级别，例如 优先复核、值得关注、观察、暂缓。';
COMMENT ON COLUMN daily_candidate_performance.event_type IS
    '日报中的事件类型。';
COMMENT ON COLUMN daily_candidate_performance.risk_level IS
    '日报中的风险等级。';
COMMENT ON COLUMN daily_candidate_performance.primary_topic_slug IS
    '该候选股最主要的触发主题。';
COMMENT ON COLUMN daily_candidate_performance.topic_slugs IS
    '触发该候选股的主题标识数组。';
COMMENT ON COLUMN daily_candidate_performance.action_bias IS
    '评分系统建议动作，例如 watch、review、skip。';
COMMENT ON COLUMN daily_candidate_performance.entry_date IS
    '复盘入场参考日期，默认使用日报日之后第一个有价格的交易日。';
COMMENT ON COLUMN daily_candidate_performance.entry_close IS
    '入场参考日期的复权收盘价或收盘价。';
COMMENT ON COLUMN daily_candidate_performance.price_source IS
    '用于计算表现的行情数据来源。';
COMMENT ON COLUMN daily_candidate_performance.horizon_1d_date IS
    '入场后第 1 个交易日的日期。';
COMMENT ON COLUMN daily_candidate_performance.horizon_1d_close IS
    '入场后第 1 个交易日的复权收盘价或收盘价。';
COMMENT ON COLUMN daily_candidate_performance.return_1d_pct IS
    '入场后第 1 个交易日相对 entry_close 的收益率百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_drawdown_1d_pct IS
    '入场到第 1 个交易日之间基于收盘价计算的最大回撤百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_runup_1d_pct IS
    '入场到第 1 个交易日之间基于收盘价计算的最大上涨百分比。';
COMMENT ON COLUMN daily_candidate_performance.horizon_5d_date IS
    '入场后第 5 个交易日的日期。';
COMMENT ON COLUMN daily_candidate_performance.horizon_5d_close IS
    '入场后第 5 个交易日的复权收盘价或收盘价。';
COMMENT ON COLUMN daily_candidate_performance.return_5d_pct IS
    '入场后第 5 个交易日相对 entry_close 的收益率百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_drawdown_5d_pct IS
    '入场到第 5 个交易日之间基于收盘价计算的最大回撤百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_runup_5d_pct IS
    '入场到第 5 个交易日之间基于收盘价计算的最大上涨百分比。';
COMMENT ON COLUMN daily_candidate_performance.horizon_20d_date IS
    '入场后第 20 个交易日的日期。';
COMMENT ON COLUMN daily_candidate_performance.horizon_20d_close IS
    '入场后第 20 个交易日的复权收盘价或收盘价。';
COMMENT ON COLUMN daily_candidate_performance.return_20d_pct IS
    '入场后第 20 个交易日相对 entry_close 的收益率百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_drawdown_20d_pct IS
    '入场到第 20 个交易日之间基于收盘价计算的最大回撤百分比。';
COMMENT ON COLUMN daily_candidate_performance.max_runup_20d_pct IS
    '入场到第 20 个交易日之间基于收盘价计算的最大上涨百分比。';
COMMENT ON COLUMN daily_candidate_performance.performance_status IS
    '表现计算状态：pending 待计算、complete 完整、partial 部分窗口可算、no_entry_price 缺少入场价、no_horizon_price 缺少后续价格。';
COMMENT ON COLUMN daily_candidate_performance.missing_reason IS
    '表现无法完整计算时的原因说明。';
COMMENT ON COLUMN daily_candidate_performance.details IS
    '扩展 JSON 字段，用于保存收益窗口、来源报告片段和计算参数。';
COMMENT ON COLUMN daily_candidate_performance.computed_at IS
    '表现计算时间。';
COMMENT ON COLUMN daily_candidate_performance.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN daily_candidate_performance.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_candidate_performance_uid
    ON daily_candidate_performance (performance_uid);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_performance_run_profile
    ON daily_candidate_performance (run_date DESC, profile);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_performance_ticker
    ON daily_candidate_performance (ticker, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_candidate_performance_primary_topic
    ON daily_candidate_performance (primary_topic_slug, run_date DESC)
    WHERE primary_topic_slug IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_daily_candidate_performance_status
    ON daily_candidate_performance (performance_status, computed_at DESC);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_market_daily_prices_updated_at ON market_daily_prices;
CREATE TRIGGER trg_market_daily_prices_updated_at
BEFORE UPDATE ON market_daily_prices
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_daily_candidate_performance_updated_at ON daily_candidate_performance;
CREATE TRIGGER trg_daily_candidate_performance_updated_at
BEFORE UPDATE ON daily_candidate_performance
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TRIGGER IF EXISTS trg_daily_candidate_performance_updated_at ON daily_candidate_performance;
DROP TRIGGER IF EXISTS trg_market_daily_prices_updated_at ON market_daily_prices;

DROP INDEX IF EXISTS idx_daily_candidate_performance_status;
DROP INDEX IF EXISTS idx_daily_candidate_performance_primary_topic;
DROP INDEX IF EXISTS idx_daily_candidate_performance_ticker;
DROP INDEX IF EXISTS idx_daily_candidate_performance_run_profile;
DROP INDEX IF EXISTS idx_daily_candidate_performance_uid;
DROP INDEX IF EXISTS idx_market_daily_prices_date;
DROP INDEX IF EXISTS idx_market_daily_prices_ticker_date;
DROP INDEX IF EXISTS idx_market_daily_prices_ticker_date_source;

DROP TABLE IF EXISTS daily_candidate_performance;
DROP TABLE IF EXISTS market_daily_prices;
