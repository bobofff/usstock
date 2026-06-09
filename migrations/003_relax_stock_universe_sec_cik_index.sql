-- Migration: 003_relax_stock_universe_sec_cik_index
-- Purpose: allow multiple tickers to share the same SEC CIK, which is common
-- for companies with multiple listed share classes.
--
-- Dialect target: PostgreSQL.

-- migrate:up

DROP INDEX IF EXISTS idx_stock_universe_sec_cik;

CREATE INDEX IF NOT EXISTS idx_stock_universe_sec_cik
    ON stock_universe (sec_cik)
    WHERE sec_cik IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_stock_universe_sec_cik;

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_universe_sec_cik
    ON stock_universe (sec_cik)
    WHERE sec_cik IS NOT NULL;
