-- ============================================================
-- Stream Enrichment Pipeline: Ticks with Symbol Metadata
-- ============================================================
-- This script enriches real-time stock tick data from Kafka
-- with static dimension data (sector information) from Iceberg.
--
-- Flow:
--   Kafka ticks (symbol, ts, price)
--     + Iceberg symbols (symbol, sector) [dimension table]
--     → Kafka ticks_enriched (symbol, ts, price, sector)
--
-- Join Strategy: Lookup join with Iceberg dimension table
--
-- Usage:
--   make sql file=flink-sql/02_stream_enrichment.sql
--
-- Prerequisites:
--   - Kafka topics: ticks, ticks_enriched (created by make topics)
--   - Iceberg catalog initialized (make seed)
-- ============================================================

-- Step 1: Set up Iceberg catalog and working database
\i connectors/catalog-iceberg.sql

-- Ensure we're in the correct database
USE app;

-- ============================================================
-- Step 2: Create Iceberg dimension table for symbol metadata
-- ============================================================
-- This table stores static/slowly-changing data about symbols
-- like sector, industry, company name, etc.

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

-- ============================================================
-- Bootstrap Dimension Table with Sample Data
-- ============================================================
-- The dimension table needs to be populated before enrichment.
-- Run these INSERT statements to bootstrap the table:
--
-- Option 1: Insert sample data directly
-- (Execute this separately before running the streaming job)
/*
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
  ('V', 'Financial', 'Payment Networks', 'Visa Inc.', CURRENT_TIMESTAMP);
*/

-- Option 2: Load from external source (CSV, Parquet, etc.)
-- For production, you might load dimension data from:
-- - External database via JDBC
-- - S3/MinIO files via Flink's file source
-- - Batch ETL job

-- Option 3: Sync from operational database (recommended for production)
/*
-- Create a batch Flink job that periodically syncs dimension data:
INSERT INTO symbols
SELECT symbol, sector, industry, company_name, CURRENT_TIMESTAMP
FROM external_catalog.dimension_db.symbols_master;
*/

-- ============================================================
-- Step 3: Create Kafka source table for tick stream
-- ============================================================
-- Real-time stock tick data from Kafka

CREATE TABLE IF NOT EXISTS ticks_stream (
  symbol STRING,
  price DOUBLE,
  volume BIGINT,
  timestamp_ms BIGINT,
  event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-enrichment-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================================
-- Step 4: Create Kafka sink table for enriched ticks
-- ============================================================
-- Output: ticks with sector information attached

CREATE TABLE IF NOT EXISTS ticks_enriched_sink (
  symbol STRING,
  price DOUBLE,
  volume BIGINT,
  event_time TIMESTAMP(3),
  sector STRING,
  industry STRING,
  company_name STRING,
  timestamp_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- ============================================================
-- Step 5: Stream Enrichment - Lookup Join with Dimension Table
-- ============================================================
-- Join streaming ticks with Iceberg dimension table
-- This uses a lookup join where Flink reads from Iceberg for each tick
--
-- Join Strategies:
-- 1. Regular Join (used here): Simple inner join with dimension table
--    - Suitable when dimension table is relatively small
--    - Dimension table is cached by Flink for performance
--
-- 2. Temporal Join: For versioned dimension tables
--    - Use when you need to join based on processing time or event time
--    - Requires FOR SYSTEM_TIME AS OF syntax
--
-- 3. Broadcast Join: For very small dimension tables
--    - Broadcast hint tells Flink to replicate dimension to all operators
--    - Use: SELECT /*+ BROADCAST(symbols) */ ...

INSERT INTO ticks_enriched_sink
SELECT
  t.symbol,
  t.price,
  t.volume,
  t.event_time,
  COALESCE(s.sector, 'Unknown') AS sector,
  COALESCE(s.industry, 'Unknown') AS industry,
  COALESCE(s.company_name, 'Unknown') AS company_name,
  t.timestamp_ms
FROM ticks_stream t
LEFT JOIN symbols s
  ON t.symbol = s.symbol;

-- ============================================================
-- Alternative Join Strategies (Commented)
-- ============================================================

-- Option A: Temporal Join (for versioned dimensions)
-- Use this if your dimension table has versioning/timestamps
/*
INSERT INTO ticks_enriched_sink
SELECT
  t.symbol,
  t.price,
  t.volume,
  t.event_time,
  s.sector,
  s.industry,
  s.company_name,
  t.timestamp_ms
FROM ticks_stream t
LEFT JOIN symbols FOR SYSTEM_TIME AS OF t.event_time AS s
  ON t.symbol = s.symbol;
*/

-- Option B: Broadcast Join (for small dimension tables <10MB)
-- Hint tells Flink to broadcast the dimension table to all operators
/*
INSERT INTO ticks_enriched_sink
SELECT
  t.symbol,
  t.price,
  t.volume,
  t.event_time,
  s.sector,
  s.industry,
  s.company_name,
  t.timestamp_ms
FROM ticks_stream t
LEFT JOIN symbols /*+ BROADCAST(symbols) */ s
  ON t.symbol = s.symbol;
*/

-- ============================================================
-- Notes:
-- ============================================================
-- 1. Dimension Table Refresh:
--    - The dimension table is cached by Flink
--    - Updates to the Iceberg table will be picked up on job restart
--    - For real-time updates, consider using a versioned approach
--
-- 2. Performance Tuning:
--    - For small dimensions (<10MB): Use BROADCAST hint
--    - For medium dimensions: Use regular join (default)
--    - For large dimensions: Consider external lookup system (Redis, etc.)
--
-- 3. Missing Symbols:
--    - LEFT JOIN ensures all ticks are processed
--    - COALESCE provides default values for unknown symbols
--    - Monitor "Unknown" sector for data quality
--
-- 4. Testing the Pipeline:
--    a. Bootstrap dimension table (run INSERT statements above)
--    b. Start this enrichment job
--    c. Produce test ticks:
--       echo '{"symbol":"AAPL","price":175.50,"volume":1000,"timestamp_ms":1673539200000}' | \
--         docker exec -i kafka kafka-console-producer.sh \
--           --bootstrap-server localhost:9092 --topic ticks
--    d. Consume enriched ticks:
--       docker exec kafka kafka-console-consumer.sh \
--         --bootstrap-server localhost:9092 --topic ticks_enriched --from-beginning
--
-- 5. Monitoring:
--    - Check job status: make list-jobs
--    - View logs: make logs SERVICE=flink-jobmanager
--    - Query dimension table: SELECT * FROM symbols;
-- ============================================================
