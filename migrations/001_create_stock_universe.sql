-- Migration: 001_create_stock_universe
-- Purpose: create the initial US stock universe table used by screening,
-- trend matching, fundamental auditing, and report generation.
--
-- Dialect target: PostgreSQL.
--
-- Note:
--   ticker is the stock code / trading symbol, for example AAPL, NVDA, BRK.B.

-- migrate:up

CREATE TABLE IF NOT EXISTS stock_universe (
    ticker TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    exchange TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT NOT NULL DEFAULT 'US',
    currency TEXT NOT NULL DEFAULT 'USD',
    asset_type TEXT NOT NULL DEFAULT 'equity'
        CHECK (asset_type IN ('equity', 'adr', 'etf', 'reit', 'tokenized_equity', 'other')),

    sec_cik TEXT,
    isin TEXT,
    figi TEXT,
    cusip TEXT,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_sp500 BOOLEAN NOT NULL DEFAULT FALSE,
    is_nasdaq100 BOOLEAN NOT NULL DEFAULT FALSE,
    is_china_adr BOOLEAN NOT NULL DEFAULT FALSE,
    is_manual_watchlist BOOLEAN NOT NULL DEFAULT FALSE,

    business_description TEXT,
    market_cap_usd NUMERIC(20, 2),
    avg_volume_30d NUMERIC(20, 2),
    last_price NUMERIC(18, 6),

    data_source TEXT NOT NULL DEFAULT 'manual',
    source_url TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refreshed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT stock_universe_ticker_not_blank
        CHECK (length(trim(ticker)) > 0),
    CONSTRAINT stock_universe_company_name_not_blank
        CHECK (length(trim(company_name)) > 0),
    CONSTRAINT stock_universe_market_cap_non_negative
        CHECK (market_cap_usd IS NULL OR market_cap_usd >= 0),
    CONSTRAINT stock_universe_avg_volume_non_negative
        CHECK (avg_volume_30d IS NULL OR avg_volume_30d >= 0),
    CONSTRAINT stock_universe_last_price_non_negative
        CHECK (last_price IS NULL OR last_price >= 0)
);

COMMENT ON TABLE stock_universe IS
    '初始股票池表，用于美股投研、热点匹配、基本面筛选、评分和日报生成。';
COMMENT ON COLUMN stock_universe.ticker IS
    '股票代码/交易代码，例如 AAPL、NVDA、BRK.B；作为本表主键。';
COMMENT ON COLUMN stock_universe.company_name IS
    '公司名称。';
COMMENT ON COLUMN stock_universe.exchange IS
    '主要上市交易所，例如 NASDAQ、NYSE、AMEX。';
COMMENT ON COLUMN stock_universe.sector IS
    '所属板块，例如 Technology、Healthcare。';
COMMENT ON COLUMN stock_universe.industry IS
    '所属细分行业。';
COMMENT ON COLUMN stock_universe.country IS
    '公司或证券所属国家/地区，默认 US。';
COMMENT ON COLUMN stock_universe.currency IS
    '交易货币，默认 USD。';
COMMENT ON COLUMN stock_universe.asset_type IS
    '资产类型：equity 普通股票、adr 存托凭证、etf 交易型基金、reit 房地产信托、tokenized_equity 代币化股票、other 其他。';
COMMENT ON COLUMN stock_universe.sec_cik IS
    'SEC Central Index Key，用于关联 SEC EDGAR 财报、公告和 company facts 数据。';
COMMENT ON COLUMN stock_universe.isin IS
    'ISIN 国际证券识别码。';
COMMENT ON COLUMN stock_universe.figi IS
    'FIGI 金融工具全局识别码。';
COMMENT ON COLUMN stock_universe.cusip IS
    'CUSIP 北美证券识别码。';
COMMENT ON COLUMN stock_universe.is_active IS
    '是否启用该标的；退市、暂停跟踪或不再纳入股票池时设为 false。';
COMMENT ON COLUMN stock_universe.is_sp500 IS
    '是否属于 S&P 500 成分股。';
COMMENT ON COLUMN stock_universe.is_nasdaq100 IS
    '是否属于 Nasdaq 100 成分股。';
COMMENT ON COLUMN stock_universe.is_china_adr IS
    '是否属于中概 ADR 或主要中国相关美股标的。';
COMMENT ON COLUMN stock_universe.is_manual_watchlist IS
    '是否由用户手工加入观察池。';
COMMENT ON COLUMN stock_universe.business_description IS
    '公司业务描述，用于热点主题匹配、关键词检索和向量检索。';
COMMENT ON COLUMN stock_universe.market_cap_usd IS
    '美元口径市值。';
COMMENT ON COLUMN stock_universe.avg_volume_30d IS
    '最近 30 个交易日平均成交量。';
COMMENT ON COLUMN stock_universe.last_price IS
    '最近一次刷新得到的价格。';
COMMENT ON COLUMN stock_universe.data_source IS
    '该记录的主要数据来源，例如 manual、wikipedia、nasdaq、sec、yfinance。';
COMMENT ON COLUMN stock_universe.source_url IS
    '该记录来源页面或接口地址。';
COMMENT ON COLUMN stock_universe.first_seen_at IS
    '系统第一次发现或写入该标的的时间。';
COMMENT ON COLUMN stock_universe.last_refreshed_at IS
    '该标的基础信息最近一次刷新的时间。';
COMMENT ON COLUMN stock_universe.metadata IS
    '扩展 JSON 字段，用于保存数据提供商特有字段、抓取元信息或临时补充信息。';
COMMENT ON COLUMN stock_universe.notes IS
    '人工备注。';
COMMENT ON COLUMN stock_universe.created_at IS
    '记录创建时间。';
COMMENT ON COLUMN stock_universe.updated_at IS
    '记录最近更新时间，由触发器自动维护。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_universe_sec_cik
    ON stock_universe (sec_cik)
    WHERE sec_cik IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_universe_isin
    ON stock_universe (isin)
    WHERE isin IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_universe_figi
    ON stock_universe (figi)
    WHERE figi IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stock_universe_active
    ON stock_universe (is_active);

CREATE INDEX IF NOT EXISTS idx_stock_universe_exchange
    ON stock_universe (exchange);

CREATE INDEX IF NOT EXISTS idx_stock_universe_sector
    ON stock_universe (sector);

CREATE INDEX IF NOT EXISTS idx_stock_universe_membership
    ON stock_universe (
        is_sp500,
        is_nasdaq100,
        is_china_adr,
        is_manual_watchlist
    );

CREATE INDEX IF NOT EXISTS idx_stock_universe_market_cap
    ON stock_universe (market_cap_usd);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_stock_universe_updated_at ON stock_universe;

CREATE TRIGGER trg_stock_universe_updated_at
BEFORE UPDATE ON stock_universe
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- migrate:down

-- DROP TRIGGER IF EXISTS trg_stock_universe_updated_at ON stock_universe;
-- DROP FUNCTION IF EXISTS set_updated_at();
-- DROP INDEX IF EXISTS idx_stock_universe_market_cap;
-- DROP INDEX IF EXISTS idx_stock_universe_membership;
-- DROP INDEX IF EXISTS idx_stock_universe_sector;
-- DROP INDEX IF EXISTS idx_stock_universe_exchange;
-- DROP INDEX IF EXISTS idx_stock_universe_active;
-- DROP INDEX IF EXISTS idx_stock_universe_figi;
-- DROP INDEX IF EXISTS idx_stock_universe_isin;
-- DROP INDEX IF EXISTS idx_stock_universe_sec_cik;
-- DROP TABLE IF EXISTS stock_universe;
