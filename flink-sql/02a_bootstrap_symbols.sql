-- ============================================================
-- Bootstrap Symbols Dimension Table
-- ============================================================
-- This script populates the symbols dimension table with sample
-- stock symbol metadata (sector, industry, company name).
--
-- Run this BEFORE starting the stream enrichment job:
--   make sql file=flink-sql/02a_bootstrap_symbols.sql
--
-- Then run the enrichment pipeline:
--   make sql file=flink-sql/02_stream_enrichment.sql
-- ============================================================

-- Set up Iceberg catalog
\i connectors/catalog-iceberg.sql

USE app;

-- Ensure table exists (idempotent)
CREATE TABLE IF NOT EXISTS symbols (
  symbol STRING,
  sector STRING,
  industry STRING,
  company_name STRING,
  last_updated TIMESTAMP(3),
  PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
);

-- Insert sample symbol metadata
-- Using UPSERT mode (will update if symbol exists)
INSERT INTO symbols VALUES
  ('AAPL', 'Technology', 'Consumer Electronics', 'Apple Inc.', CURRENT_TIMESTAMP),
  ('GOOGL', 'Technology', 'Internet Services', 'Alphabet Inc.', CURRENT_TIMESTAMP),
  ('MSFT', 'Technology', 'Software', 'Microsoft Corporation', CURRENT_TIMESTAMP),
  ('TSLA', 'Automotive', 'Electric Vehicles', 'Tesla Inc.', CURRENT_TIMESTAMP),
  ('AMZN', 'Consumer Cyclical', 'E-commerce', 'Amazon.com Inc.', CURRENT_TIMESTAMP),
  ('JPM', 'Financial', 'Banking', 'JPMorgan Chase & Co.', CURRENT_TIMESTAMP),
  ('JNJ', 'Healthcare', 'Pharmaceuticals', 'Johnson & Johnson', CURRENT_TIMESTAMP),
  ('XOM', 'Energy', 'Oil & Gas', 'Exxon Mobil Corporation', CURRENT_TIMESTAMP),
  ('WMT', 'Consumer Defensive', 'Retail', 'Walmart Inc.', CURRENT_TIMESTAMP),
  ('V', 'Financial', 'Payment Networks', 'Visa Inc.', CURRENT_TIMESTAMP),
  ('PG', 'Consumer Defensive', 'Household Products', 'Procter & Gamble Co.', CURRENT_TIMESTAMP),
  ('NVDA', 'Technology', 'Semiconductors', 'NVIDIA Corporation', CURRENT_TIMESTAMP),
  ('MA', 'Financial', 'Payment Networks', 'Mastercard Inc.', CURRENT_TIMESTAMP),
  ('HD', 'Consumer Cyclical', 'Home Improvement', 'Home Depot Inc.', CURRENT_TIMESTAMP),
  ('DIS', 'Communication Services', 'Entertainment', 'Walt Disney Co.', CURRENT_TIMESTAMP),
  ('BAC', 'Financial', 'Banking', 'Bank of America Corp.', CURRENT_TIMESTAMP),
  ('INTC', 'Technology', 'Semiconductors', 'Intel Corporation', CURRENT_TIMESTAMP),
  ('CSCO', 'Technology', 'Networking Equipment', 'Cisco Systems Inc.', CURRENT_TIMESTAMP),
  ('NFLX', 'Communication Services', 'Streaming Services', 'Netflix Inc.', CURRENT_TIMESTAMP),
  ('PFE', 'Healthcare', 'Pharmaceuticals', 'Pfizer Inc.', CURRENT_TIMESTAMP);

-- Verify data was inserted
SELECT
  COUNT(*) AS total_symbols,
  COUNT(DISTINCT sector) AS unique_sectors
FROM symbols;

-- Show sample data
SELECT * FROM symbols ORDER BY symbol LIMIT 10;
