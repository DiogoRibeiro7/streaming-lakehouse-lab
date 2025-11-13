-- ============================================================
-- Feature Views Pipeline: Sliding Window Aggregations
-- ============================================================
-- This pipeline computes time-based features from clean tick data
-- using sliding windows for machine learning applications.
--
-- Features:
--   - Mean price over 15-minute sliding window (1-minute hop)
--   - Additional statistics: min, max, stddev, volume, count
--   - Per-symbol features stored in Iceberg
--
-- ML Use Cases:
--   - Price prediction models
--   - Anomaly detection
--   - Trading signal generation
--
-- Usage:
--   make sql file=flink-sql/04_feature_views.sql
--
-- Prerequisites:
--   - ticks_clean table populated (run 03_dedup_upserts.sql)
--   - Iceberg catalog initialized
-- ============================================================

-- Step 1: Set up Iceberg catalog
\i connectors/catalog-iceberg.sql

USE app;

-- ============================================================
-- Step 2: Create Iceberg source for clean ticks
-- ============================================================
-- Note: Reading from Iceberg as a stream requires Flink 1.17+
-- For older versions, read from Kafka instead

-- Option A: Read from Iceberg (batch or streaming)
-- Uncomment if using Flink 1.17+ with streaming Iceberg reads
/*
CREATE TABLE IF NOT EXISTS ticks_clean_source (
  symbol STRING,
  event_time TIMESTAMP(3),
  price DOUBLE,
  volume BIGINT,
  sector STRING,
  industry STRING,
  company_name STRING,
  ingestion_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'iceberg_catalog',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3a://lakehouse/warehouse'
);
*/

-- Option B: Read from Kafka (recommended for real-time)
-- Use ticks_enriched as source for continuous feature computation
CREATE TABLE IF NOT EXISTS ticks_source (
  symbol STRING,
  price DOUBLE,
  volume BIGINT,
  event_time TIMESTAMP(3),
  sector STRING,
  industry STRING,
  company_name STRING,
  timestamp_ms BIGINT,
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-features-consumer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- ============================================================
-- Step 3: Create Iceberg feature table
-- ============================================================
-- This table stores pre-aggregated features for ML models
-- Partitioned by date and hour for efficient time-travel queries

CREATE TABLE IF NOT EXISTS features_ticks (
  -- Window identifiers
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),

  -- Feature keys
  symbol STRING,
  sector STRING,

  -- Price features
  mean_price DOUBLE,
  min_price DOUBLE,
  max_price DOUBLE,
  stddev_price DOUBLE,
  price_range DOUBLE,  -- max - min

  -- Volume features
  total_volume BIGINT,
  mean_volume DOUBLE,

  -- Statistical features
  tick_count BIGINT,

  -- Metadata
  feature_timestamp TIMESTAMP(3),

  -- Primary key for upserts
  PRIMARY KEY (symbol, window_start) NOT ENFORCED
) PARTITIONED BY (DATE_FORMAT(window_start, 'yyyy-MM-dd'), DATE_FORMAT(window_start, 'HH'))
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy',
  'write.upsert.enabled' = 'true'
);

-- ============================================================
-- Step 4: Sliding Window Feature Computation
-- ============================================================
-- Sliding Window Configuration:
--   - Window Size: 15 minutes (captures medium-term trends)
--   - Slide/Hop: 1 minute (new features every minute)
--   - Result: Each 1-minute produces a feature vector with 15-min history
--
-- Window Behavior:
--   10:00:00 - 10:15:00  [first window]
--   10:01:00 - 10:16:00  [overlaps 14 minutes with previous]
--   10:02:00 - 10:17:00  [overlaps 13 minutes with first]
--   ...
--
-- Trade-offs:
--   - Larger window = more stable features, slower reaction
--   - Smaller slide = more frequent updates, more computation

INSERT INTO features_ticks
SELECT
  -- Window identifiers
  HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) AS window_start,
  HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) AS window_end,

  -- Keys
  symbol,
  sector,

  -- Price features
  AVG(price) AS mean_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  STDDEV_POP(price) AS stddev_price,
  MAX(price) - MIN(price) AS price_range,

  -- Volume features
  SUM(volume) AS total_volume,
  AVG(volume) AS mean_volume,

  -- Count
  COUNT(*) AS tick_count,

  -- Feature timestamp (when this feature was computed)
  CURRENT_TIMESTAMP AS feature_timestamp

FROM ticks_source
GROUP BY
  HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE),
  symbol,
  sector;

-- ============================================================
-- Alternative Window Configurations (Commented)
-- ============================================================

-- Option A: Tumbling Window (non-overlapping, simpler)
-- Use when you want distinct time periods with no overlap
/*
INSERT INTO features_ticks
SELECT
  TUMBLE_START(event_time, INTERVAL '15' MINUTE) AS window_start,
  TUMBLE_END(event_time, INTERVAL '15' MINUTE) AS window_end,
  symbol,
  sector,
  AVG(price) AS mean_price,
  -- ... other features
FROM ticks_source
GROUP BY
  TUMBLE(event_time, INTERVAL '15' MINUTE),
  symbol,
  sector;
*/

-- Option B: Session Window (dynamic windows based on activity gaps)
-- Use when you want to capture bursts of activity
/*
INSERT INTO features_ticks
SELECT
  SESSION_START(event_time, INTERVAL '5' MINUTE) AS window_start,
  SESSION_END(event_time, INTERVAL '5' MINUTE) AS window_end,
  symbol,
  -- ... features
FROM ticks_source
GROUP BY
  SESSION(event_time, INTERVAL '5' MINUTE),
  symbol;
*/

-- Option C: Multiple Window Sizes (multi-scale features)
-- Compute features at different time scales for richer representations
/*
-- 5-minute features
CREATE TABLE features_ticks_5m AS
SELECT
  HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
  symbol,
  AVG(price) AS mean_price_5m,
  -- ...
FROM ticks_source
GROUP BY HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE), symbol;

-- 60-minute features
CREATE TABLE features_ticks_60m AS
SELECT
  HOP_START(event_time, INTERVAL '5' MINUTE, INTERVAL '60' MINUTE) AS window_start,
  symbol,
  AVG(price) AS mean_price_60m,
  -- ...
FROM ticks_source
GROUP BY HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '60' MINUTE), symbol;
*/

-- ============================================================
-- ML Use Cases: Offline Training vs Online Serving
-- ============================================================

-- ───────────────────────────────────────────────────────────
-- OFFLINE TRAINING (Batch Feature Extraction)
-- ───────────────────────────────────────────────────────────
-- Use Case: Training ML models on historical data
--
-- Benefits of Iceberg for Offline Training:
--   ✓ Time travel: Access features at any point in time
--   ✓ Partition pruning: Fast queries for date ranges
--   ✓ Schema evolution: Add new features without recomputation
--   ✓ ACID guarantees: Consistent training datasets
--
-- Example: Extract training features for a specific date range
/*
-- Query features for model training (Jan 1-31, 2025)
SELECT
  window_start,
  symbol,
  sector,
  mean_price,
  stddev_price,
  price_range,
  total_volume,
  tick_count
FROM features_ticks
WHERE
  window_start >= TIMESTAMP '2025-01-01 00:00:00'
  AND window_start < TIMESTAMP '2025-02-01 00:00:00'
  AND sector IN ('Technology', 'Financial')  -- Filter relevant sectors
ORDER BY symbol, window_start;
*/

-- ───────────────────────────────────────────────────────────
-- TIME-TRAVEL SNAPSHOT SELECTION (Point-in-Time Features)
-- ───────────────────────────────────────────────────────────
-- Use Case: Prevent data leakage by ensuring features are
--           computed with only data available at prediction time
--
-- Iceberg Time Travel Capabilities:
--   1. AS OF TIMESTAMP: Query data as it existed at a specific time
--   2. AS OF VERSION: Query data at a specific snapshot ID
--
-- Example: Get features as they existed on Jan 15, 2025 at 3pm
/*
SELECT *
FROM features_ticks
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-15 15:00:00'
WHERE symbol = 'AAPL';
*/

-- Example: Get features from a specific snapshot (for reproducibility)
/*
-- First, find the snapshot ID
SELECT snapshot_id, committed_at
FROM features_ticks.snapshots
WHERE committed_at <= TIMESTAMP '2025-01-15 15:00:00'
ORDER BY committed_at DESC
LIMIT 1;

-- Then query using that snapshot
SELECT *
FROM features_ticks VERSION AS OF <snapshot-id>
WHERE symbol = 'AAPL';
*/

-- ───────────────────────────────────────────────────────────
-- ONLINE SERVING (Real-time Feature Lookup)
-- ───────────────────────────────────────────────────────────
-- Use Case: Serve features to production models in real-time
--
-- Architecture Options:
--
-- Option 1: Direct Iceberg Reads (Acceptable for batch predictions)
--   Pros: Simple, no additional infrastructure
--   Cons: Higher latency (~100-500ms), not suitable for <10ms serving
--
-- Option 2: Feature Store (Recommended for low-latency serving)
--   Iceberg → ETL → Redis/DynamoDB/Feast
--   Pros: Low latency (<10ms), optimized for point lookups
--   Cons: Additional infrastructure, eventual consistency
--
-- Option 3: Materialized View (Hybrid approach)
--   Iceberg → Flink → Materialized View (e.g., Postgres, Cassandra)
--   Pros: Balance of freshness and latency (~10-50ms)
--   Cons: Moderate complexity
--
-- Example: Latest features for online serving (Option 1)
/*
-- Get most recent features for a symbol
SELECT
  symbol,
  mean_price,
  stddev_price,
  total_volume,
  tick_count,
  feature_timestamp
FROM features_ticks
WHERE
  symbol = 'AAPL'
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY window_start DESC
LIMIT 1;
*/

-- ───────────────────────────────────────────────────────────
-- FEATURE FRESHNESS MONITORING
-- ───────────────────────────────────────────────────────────
-- Monitor the staleness of features to ensure data quality
/*
SELECT
  symbol,
  MAX(window_end) AS latest_feature_time,
  CURRENT_TIMESTAMP AS current_time,
  TIMESTAMPDIFF(
    MINUTE,
    MAX(window_end),
    CURRENT_TIMESTAMP
  ) AS staleness_minutes
FROM features_ticks
GROUP BY symbol
HAVING staleness_minutes > 10  -- Alert if features are >10 minutes stale
ORDER BY staleness_minutes DESC;
*/

-- ───────────────────────────────────────────────────────────
-- FEATURE STORE EXPORT (ETL to Redis for Online Serving)
-- ───────────────────────────────────────────────────────────
-- Periodically export latest features to a low-latency store
/*
-- Flink job to continuously sync latest features to Redis
INSERT INTO redis_features_sink  -- Assume Redis connector configured
SELECT
  symbol,
  mean_price,
  stddev_price,
  total_volume,
  window_start
FROM (
  SELECT
    symbol,
    mean_price,
    stddev_price,
    total_volume,
    window_start,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_start DESC) AS rn
  FROM features_ticks
)
WHERE rn = 1;  -- Only latest features per symbol
*/

-- ============================================================
-- Verification and Exploration Queries
-- ============================================================

-- Check feature computation is working
/*
USE CATALOG iceberg_catalog;
USE app;

-- Count features per symbol
SELECT
  symbol,
  COUNT(*) AS feature_count,
  MIN(window_start) AS first_feature,
  MAX(window_start) AS last_feature
FROM features_ticks
GROUP BY symbol
ORDER BY feature_count DESC
LIMIT 10;

-- Sample features for a specific symbol
SELECT
  window_start,
  window_end,
  symbol,
  mean_price,
  stddev_price,
  total_volume,
  tick_count
FROM features_ticks
WHERE symbol = 'AAPL'
ORDER BY window_start DESC
LIMIT 20;

-- Check feature distribution
SELECT
  symbol,
  AVG(mean_price) AS avg_mean_price,
  STDDEV(mean_price) AS stddev_mean_price,
  MIN(mean_price) AS min_mean_price,
  MAX(mean_price) AS max_mean_price
FROM features_ticks
GROUP BY symbol
ORDER BY symbol;

-- View Iceberg snapshots for time travel
SELECT
  snapshot_id,
  parent_id,
  committed_at,
  operation,
  summary
FROM features_ticks.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- View table partitions
SELECT
  partition,
  record_count,
  file_count
FROM features_ticks.partitions
ORDER BY partition DESC
LIMIT 20;
*/

-- ============================================================
-- Advanced Feature Engineering Patterns
-- ============================================================

-- Pattern 1: Lag Features (compare current to previous window)
/*
SELECT
  symbol,
  window_start,
  mean_price,
  LAG(mean_price, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_mean_price,
  mean_price - LAG(mean_price, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS price_change
FROM features_ticks;
*/

-- Pattern 2: Rate of Change
/*
SELECT
  symbol,
  window_start,
  mean_price,
  (mean_price - LAG(mean_price, 1) OVER (PARTITION BY symbol ORDER BY window_start))
    / LAG(mean_price, 1) OVER (PARTITION BY symbol ORDER BY window_start) AS price_pct_change
FROM features_ticks;
*/

-- Pattern 3: Cross-sectional Features (rank within sector)
/*
SELECT
  symbol,
  sector,
  window_start,
  mean_price,
  RANK() OVER (PARTITION BY sector, window_start ORDER BY mean_price DESC) AS price_rank_in_sector
FROM features_ticks;
*/

-- ============================================================
-- Performance Considerations
-- ============================================================
--
-- COMPUTE COST:
--   - Sliding windows are expensive: 15 overlapping windows per 15 minutes
--   - Trade-off: Window size vs freshness vs compute cost
--   - Consider tumbling windows if overlap is not critical
--
-- STORAGE COST:
--   - Each 1-minute slide produces 1 feature row per symbol
--   - For 20 symbols: 1,440 rows/hour, 34,560 rows/day
--   - With 100 features per row: ~1-5 GB/month (compressed Parquet)
--
-- QUERY PERFORMANCE:
--   - Partition by date and hour for fast time-range queries
--   - Create sorted indexes on (symbol, window_start) for point queries
--   - Use partition pruning: WHERE window_start >= '2025-01-01'
--
-- FEATURE STALENESS:
--   - 1-minute slide → features are at most 1 minute stale
--   - Watermark (10 seconds) → allows late data up to 10s
--   - Total staleness = slide + watermark = ~70 seconds
--
-- SCALABILITY:
--   - Horizontal: Increase parallelism (SET parallelism.default = '8')
--   - Vertical: Use RocksDB state backend for large state
--   - Partition source by symbol for better load distribution
-- ============================================================

-- ============================================================
-- Best Practices
-- ============================================================
--
-- 1. FEATURE NAMING: Use descriptive names with time window suffix
--    mean_price_15m, stddev_price_15m, etc.
--
-- 2. METADATA: Always include window_start, window_end, feature_timestamp
--    for debugging and lineage tracking
--
-- 3. NULL HANDLING: Use COALESCE for missing values
--    COALESCE(AVG(price), 0.0) AS mean_price
--
-- 4. MONITORING: Set up alerts for:
--    - Feature staleness (> 5 minutes)
--    - NULL/NaN values
--    - Extreme outliers
--
-- 5. VERSION CONTROL: Tag Iceberg snapshots for model training
--    to ensure reproducibility
--
-- 6. BACKFILLING: Use Iceberg's time travel to backfill features
--    for new models without re-running entire pipeline
-- ============================================================
